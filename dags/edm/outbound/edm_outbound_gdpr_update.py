import json
from functools import cache, cached_property
from typing import Dict, List

import pendulum
from airflow import DAG

from edm.acquisition.configs import get_lake_consolidated_table_config
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.airflow.operators.mssql import MsSqlToS3Operator
from include.airflow.operators.snowflake import BaseSnowflakeOperator
from include.airflow.operators.snowflake_load import SnowflakeBrandIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.acquisition.table_config import brand_database_conn_mapping
from include.utils.context_managers import ConnClosing
from include.utils.snowflake import CopyConfigCsv
from include.utils.string import unindent
from task_configs.dag_config.gdpr_config import GDPRLakeTargetConfig, PIIConfig, pii_config

default_args = {
    "start_date": pendulum.datetime(2022, 8, 1, tz="America/Los_Angeles"),
    'owner': owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_outbound_gdpr_update',
    default_args=default_args,
    catchup=False,
    schedule='30 20 * * *',
)


class PIIMaskOperator(BaseSnowflakeOperator):
    """
    Operator to remove PII values from snowflake tables for customer erasure requests.

    Steps in PII removal process:
        - get list of customer_ids from SQL Server - gdpr.dbo.request_job and gdpr.dbo.request;
          load list to lake.gdpr.request_job
        - update snowflake tables by removing PII data for customer_ids in lake.gdpr.request_job
        - call stored procedure in SQL Server to update statuscode=2009 (completed)
          for processed customers
        - update lake.gdpr.request_job to set statuscode=2009 and audit flags

    Args:
        config: list of PIIConfig objects containing table and PII mask details
        mssql_database: target mssql database to update gdpr status
        mssql_schema: target mssql schema to update gdpr status
        mssql_conn_id: mssql connection id
    """

    def __init__(
        self,
        config: List[PIIConfig],
        mssql_database: str,
        mssql_schema: str,
        **kwargs,
    ) -> None:
        self.config = config
        self.mssql_database = mssql_database
        self.mssql_schema = mssql_schema
        super().__init__(**kwargs)

    @cache
    def mssql_hook(self, mssql_conn_id: str) -> MsSqlOdbcHook:
        return MsSqlOdbcHook(
            mssql_conn_id=mssql_conn_id,
            database=self.mssql_database,
            schema=self.mssql_schema,
        )

    @property
    def full_table_name(self) -> str:
        full_table_name: str = GDPRLakeTargetConfig.FULL_TABLE_NAME.format(brand="consolidated")
        return full_table_name

    @cached_property
    def ddl_request_job_id_temp_table(self) -> str:
        """create temp table containing customer_id and request_job_id"""
        return unindent(
            val=f'''CREATE OR REPLACE TEMP TABLE request_job_id_list AS
            SELECT DISTINCT
                customer_id,
                meta_original_customer_id,
                request_job_id
            FROM (
                SELECT
                    customer_id,
                    meta_original_customer_id,
                    min(request_job_id) as request_job_id
                FROM
                    {self.full_table_name}
                WHERE
                    snowflake_updated = 0
                GROUP BY
                    customer_id,
                    meta_original_customer_id
            );''',
            char_count=12,
        )

    @cached_property
    def dml_mask_lake_cols(self) -> str:
        """generate update statements to mask PII columns for customer_ids in request_job_id_list
        general format of update statements:
            update {table}
            set col_1=mask_value_1, col_2=mask_value_2, col_n=mask_value_n
            where {filter_condition}
        """
        dmls = []
        for conf in self.config:
            customer_id_column = (
                "meta_original_customer_id" if conf.use_meta_original_customer_id else "customer_id"
            )
            set_clause = ',\n\t'.join(map(str, conf.mask_config))
            if conf.ref_tbl:
                filter_con = f'''{conf.key_on} in (
                    SELECT DISTINCT
                        c.{conf.ref_col}
                    FROM
                        request_job_id_list r
                    JOIN
                        {conf.ref_tbl} c
                    ON
                        r.{customer_id_column} = c.customer_id
                )'''
            else:
                filter_con = f'''{conf.key_on} in (
                    SELECT DISTINCT
                        {customer_id_column}
                    FROM
                        request_job_id_list
                )'''
            dmls.append(
                f'''UPDATE
                    {conf.table}
                SET
                    {set_clause}
                WHERE {filter_con};'''
            )
        return unindent(val='\n\n'.join(dmls), char_count=16)

    @cached_property
    def dml_sf_updated(self) -> str:
        """update lake.gdpr.request_job to set snowflake_updated=1 before calling mssql sp"""
        return unindent(
            val=f'''UPDATE
                {self.full_table_name} a
            FROM
                request_job_id_list b
            SET
                snowflake_updated=1
            WHERE
                a.customer_id=b.customer_id AND a.request_job_id=b.request_job_id;''',
            char_count=12,
        )

    @property
    def company_id_to_brand_mapping(self) -> Dict[int, str]:
        return {
            10: conn_ids.MsSql.justfab_app_airflow_rw,
            20: conn_ids.MsSql.db50_app_airflow_rw,
            30: conn_ids.MsSql.savagex_app_airflow_rw,
        }

    def mssql_exec_sp(self) -> None:
        """call sp in mssql to update statuscode=2009 for processed request_job_ids"""
        with ConnClosing(self.snowflake_hook.get_conn()) as sf_cnx:
            sf_cursor = sf_cnx.cursor()
            sf_cursor.execute(
                f"""
                SELECT DISTINCT meta_company_id, array_agg(meta_original_request_job_id) request_job_ids
                FROM {self.full_table_name}
                WHERE snowflake_updated=1 AND pushed_to_mssql=0
                GROUP BY meta_company_id
            """
            )
            request_job_ids_by_company_id = {i[0]: json.loads(i[1]) for i in sf_cursor.fetchall()}
        for company_id, request_job_ids in request_job_ids_by_company_id.items():
            mssql_conn_id = self.company_id_to_brand_mapping[company_id]
            with ConnClosing(self.mssql_hook(mssql_conn_id=mssql_conn_id).get_conn()) as ms_cnx:
                ms_cursor = ms_cnx.cursor()
                for request_job_id in request_job_ids:
                    ms_cursor.execute(
                        f'exec [dbo].[pr_request_job_completed_upd] @request_job_id={request_job_id}'
                    )

    @cached_property
    def dml_post_update_lake_request_job(self) -> str:
        """after executing sp in mssql, update lake request_job table to set statuscode and flags"""
        return unindent(
            val=f'''
            UPDATE
                {self.full_table_name}
            SET
                statuscode=2009,
                pushed_to_mssql=1,
                meta_update_datetime = current_timestamp()
            WHERE
                snowflake_updated=1 AND pushed_to_mssql=0;''',
            char_count=12,
        )

    def dry_run(self) -> None:
        print(self.ddl_request_job_id_temp_table)
        print(self.dml_mask_lake_cols)
        print(self.dml_sf_updated)
        print(self.dml_post_update_lake_request_job)

    def execute(self, context=None) -> None:
        # create temp table and update PII values in snowflake
        self.run_sql_or_path(
            sql_or_path='\n'.join(
                [
                    self.ddl_request_job_id_temp_table,
                    self.dml_mask_lake_cols,
                    self.dml_sf_updated,
                ]
            )
        )
        # call stored proc in mssql
        self.mssql_exec_sp()
        # update request_job table in snowflake after mssql update
        self.run_sql_or_path(sql_or_path=self.dml_post_update_lake_request_job)


with dag:
    pii_update = PIIMaskOperator(
        task_id='pii_update',
        config=pii_config,
        mssql_database='gdpr',
        mssql_schema='dbo',
        role=snowflake_roles.etl_service_account,
        warehouse='DA_WH_ETL_LIGHT',
    )
    to_lake_consolidated = get_lake_consolidated_table_config(
        "lake_consolidated.gdpr.request_job"
    ).to_lake_consolidated_operator
    for brand, db_conn_map in brand_database_conn_mapping.items():
        full_table_name = GDPRLakeTargetConfig.FULL_TABLE_NAME.format(brand=brand.lower())
        s3_prefix = f'lake/{full_table_name}/v2/{brand.lower()}'
        request_job_to_s3 = MsSqlToS3Operator(
            task_id=f"{full_table_name}.to_s3",
            sql='''SELECT
                r.customer_id,
                request_job_id,
                rj.request_id,
                system_id,
                rj.datetime_added,
                rj.statuscode,
                0 as snowflake_updated,
                0 as pushed_to_mssql
            FROM
                gdpr.dbo.request_job rj
            JOIN
                gdpr.dbo.request r
            ON
                rj.request_id=r.request_id
            WHERE
                system_id = 3 AND rj.statuscode = 2000
            ORDER BY
                datetime_added''',
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f'{s3_prefix}/gdpr_request_job_{{{{ ts_nodash }}}}.csv.gz',
            mssql_conn_id=db_conn_map["gdpr"],
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            s3_replace=True,
        )
        request_job_to_snowflake = SnowflakeBrandIncrementalLoadOperator(
            task_id=f"{full_table_name}.to_snowflake",
            brand=brand.lower(),
            database=GDPRLakeTargetConfig.TARGET_DATABASE.format(brand=brand.lower()),
            schema=GDPRLakeTargetConfig.TARGET_SCHEMA,
            table=GDPRLakeTargetConfig.TARGET_TABLE,
            staging_database="lake_stg",
            staging_schema="public",
            column_list=GDPRLakeTargetConfig.COLUMN_LIST,
            files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
            copy_config=CopyConfigCsv(header_rows=1, field_delimiter="\t", record_delimiter="\n"),
            warehouse="DA_WH_ETL_LIGHT",
            initial_load=True,
        )
        request_job_to_s3 >> request_job_to_snowflake >> to_lake_consolidated >> pii_update
