from dataclasses import dataclass

import pendulum
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.sftp_to_s3 import SFTPToS3BatchOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator, SnowflakeSqlOperator
from include.airflow.operators.snowflake_load import SnowflakeCopyOperator
from include.config import conn_ids, owners, s3_buckets, stages
from include.config.email_lists import data_integration_support

stage_name = stages.tsos_da_int_inbound

default_args = {
    "start_date": pendulum.datetime(2020, 1, 28, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_shoppertrak_traffic_counter",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
)


@dataclass
class ShoppertrakConfig:
    schema: str
    table: str
    sftp_conn_id: str
    file_pattern: str
    remote_dir: str
    task_group: str

    @property
    def sftp_to_s3(self):
        return SFTPToS3BatchOperator(
            task_id=f"to_s3_{self.schema}.{self.table}",
            s3_prefix=f"lake/{self.schema}.{self.table}/v3",
            s3_bucket=s3_buckets.tsos_da_int_inbound,
            sftp_conn_id=self.sftp_conn_id,
            email_on_retry=False,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            files_per_batch=100,
            remote_dir=self.remote_dir,
            file_pattern=self.file_pattern,
            remove_remote_files=True,
        )

    @property
    def copy_query_exec(self):
        copy_query = f"""
                    COPY INTO lake.{self.schema}.{self.table}_stg (site_id, customer_site_id, date, time,
                                            traffic_enters, traffic_exits, imputation_indicator, datetime_added)
                    FROM (select
                            $1 AS site_id,
                            $2 AS customer_site_id,
                            TO_DATE($3,'yyyymmdd') AS date,
                            TO_TIME($4,'hhmiss') AS time,
                            $5 AS traffic_enters,
                            $6 AS traffic_exits,
                            $7 AS imputation_indicator,
                            TO_DATE(split_part(split_part(metadata$filename, '.', -3),'_',-1),
                                    'yyyymmdd') AS datetime_added
                           FROM '{stage_name}/lake/{self.schema}.{self.table}/v3/'
                         )
                    FILE_FORMAT=(
                        TYPE = CSV,
                        FIELD_DELIMITER = ',',
                        RECORD_DELIMITER = '\n',
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
                        SKIP_HEADER = 0,
                        ESCAPE_UNENCLOSED_FIELD = NONE,
                        NULL_IF = ('')
                    )
                    ON_ERROR = 'continue';
                """

        return SnowflakeCopyOperator(
            task_id=f"copy_query_exec_{self.schema}.{self.table}",
            sql_or_path=copy_query,
        )

    @property
    def to_snowflake(self):
        merge_statement = f"""
                    MERGE INTO lake.{self.schema}.{self.table} t
                    USING (
                        SELECT
                            a.*
                        FROM (
                            SELECT
                                *,
                                hash(*) AS meta_row_hash,
                                current_timestamp AS meta_create_datetime,
                                current_timestamp AS meta_update_datetime,
                                row_number() OVER ( PARTITION BY site_id, customer_site_id, date,time
                                                    ORDER BY datetime_added DESC) AS rn
                            FROM lake.{self.schema}.{self.table}_stg
                         ) a
                        WHERE a.rn = 1
                    ) s ON equal_null(t.site_id, s.site_id)
                        AND equal_null(t.customer_site_id, s.customer_site_id)
                        AND equal_null(t.date, s.date)
                        AND equal_null(t.time, s.time)
                    WHEN NOT MATCHED THEN INSERT (site_id, customer_site_id, date, time, traffic_enters,
                    traffic_exits, imputation_indicator, datetime_added, meta_row_hash, meta_create_datetime,
                    meta_update_datetime)
                    VALUES (site_id, customer_site_id, date, time, traffic_enters, traffic_exits,
                        imputation_indicator, datetime_added, meta_row_hash, meta_create_datetime, meta_update_datetime)
                    WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN UPDATE
                    SET t.site_id = s.site_id,
                        t.customer_site_id = s.customer_site_id,
                        t.date = s.date,
                        t.time = s.time,
                        t.traffic_enters = s.traffic_enters,
                        t.traffic_exits = s.traffic_exits,
                        t.imputation_indicator = s.imputation_indicator,
                        t.datetime_added = s.datetime_added,
                        t.meta_row_hash = s.meta_row_hash,
                        t.meta_update_datetime = s.meta_update_datetime;

                    DELETE FROM lake.{self.schema}.{self.table}_stg;
                """

        return SnowflakeSqlOperator(
            task_id=f"target_data_load_{self.schema}.{self.table}",
            sql_or_path=merge_statement,
            warehouse='DA_WH_ETL_LIGHT',
        )


shoppertrak_config_list = [
    ShoppertrakConfig(
        schema="shoppertrak",
        table="traffic_counter",
        sftp_conn_id=conn_ids.SFTP.sftp_shoppertrak,
        remote_dir='',
        file_pattern='Fabletics_*.csv',
        task_group='Fabletics-Traffic-Counter',
    ),
    ShoppertrakConfig(
        schema="shoppertrak",
        table="traffic_counter_sxf",
        sftp_conn_id=conn_ids.SFTP.sftp_shoppertrak_sxf,
        remote_dir='',
        file_pattern='savagex_*.csv',
        task_group='SavageX-Traffic-Counter',
    ),
]

with dag:
    for cfg in shoppertrak_config_list:
        with TaskGroup(group_id=cfg.task_group) as tg:
            to_s3 = cfg.sftp_to_s3
            copy_query_exec = cfg.copy_query_exec
            to_lake = cfg.to_snowflake

            to_s3 >> copy_query_exec >> to_lake

            if cfg.task_group == 'SavageX-Traffic-Counter':
                retail_traffic = SnowflakeProcedureOperator(
                    procedure='sxf.retail_traffic.sql',
                    database='reporting_prod',
                    warehouse='DA_WH_ETL_LIGHT',
                )
                to_lake >> retail_traffic
