from dataclasses import dataclass
from typing import Optional

import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_data_science
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_to_mssql import SnowflakeSqlToMSSqlWatermarkOperator
from include.config import conn_ids, owners
from include.utils.copy_paste_helper import TableConfig
from include.utils.snowflake import Column


@dataclass
class Config:
    server: str
    company_id: Optional[int] = None

    @property
    def task_id(self):
        return f"push_to_{self.server}"

    @property
    def conn_id(self):
        conn_id_map = {
            "db166": conn_ids.MsSql.db166_app_airflow_rw,
            "justfab": conn_ids.MsSql.justfab_app_airflow_rw,
            "fabletics": conn_ids.MsSql.fabletics_app_airflow_rw,
            "savagex": conn_ids.MsSql.savagex_app_airflow_rw,
            "jfdev": conn_ids.MsSql.ds_qa_justfab_app_airflow_rw,
            "fldev": conn_ids.MsSql.ds_qa_fabletics_app_airflow_rw,
        }
        return conn_id_map[self.server]


class SnowflakeSqlToMSSqlWatermarkOperatorWithCompanyID(SnowflakeSqlToMSSqlWatermarkOperator):
    def __init__(self, company_id: Optional[int] = None, **kwargs) -> None:
        self.company_id = company_id
        super().__init__(**kwargs)

    def build_query(self) -> str:
        query: str = super().build_query()
        if self.company_id:
            return f"{query} and meta_company_id = {self.company_id}"
        return query


default_args = {
    "start_date": pendulum.datetime(2022, 3, 1, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_science,
    "email": 'datascience@techstyle.com',
    "on_failure_callback": slack_failure_data_science,
}

dag_id = "data_science_segment_sales_impressions"

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule="30 2,5,8,11,14,17,20,23 * * *",
    catchup=False,
    max_active_tasks=20,
)

config_list = [
    Config(server='db166'),
    Config(server='justfab', company_id=10),
    Config(server='fabletics', company_id=20),
    Config(server='savagex', company_id=30),
    Config(server='jfdev', company_id=10),
    Config(server='fldev', company_id=20),
]

table_config = TableConfig(
    database='reporting_prod',
    schema='data_science',
    target_database='ultraimport',
    target_schema='dbo',
    table='export_postreg_segment_spi_ranking',
    initial_load_value='2023-03-01',
    watermark_column='meta_update_datetime',
    column_list=[
        Column('mpid', 'BIGINT', uniqueness=True),
        Column(
            'impressions_date',
            'DATETIME',
            uniqueness=True,
            source_name='impressions_date::DATETIME',
        ),
        Column('grid_name', 'VARCHAR(50)', uniqueness=True),
        Column('total_impressions', 'BIGINT'),
        Column('lead_impressions', 'BIGINT'),
        Column('vip_impressions', 'BIGINT'),
        Column('total_sales', 'BIGINT'),
        Column('lead_sales', 'BIGINT'),
        Column('vip_sales', 'BIGINT'),
        Column(
            'src_meta_create_datetime',
            'DATETIME',
            source_name='meta_create_datetime',
            delta_column=1,
        ),
        Column(
            'src_meta_update_datetime',
            'DATETIME',
            source_name='meta_update_datetime',
            delta_column=0,
        ),
    ],
)

with dag:
    process_snowflake_table_fl_spi = SnowflakeProcedureOperator(
        procedure="data_science.export_postreg_segment_spi_ranking_fl.sql",
        database="reporting_prod",
    )
    process_snowflake_table_fl_spv = SnowflakeProcedureOperator(
        procedure="data_science.export_postreg_segment_spv_ranking_fl.sql",
        database="reporting_prod",
    )
    process_snowflake_table_sd_spi = SnowflakeProcedureOperator(
        procedure="data_science.export_postreg_segment_spi_ranking_sd.sql",
        database="reporting_prod",
    )
    process_snowflake_table_sx = SnowflakeProcedureOperator(
        procedure="data_science.export_postreg_segment_spi_ranking_sx.sql",
        database="reporting_prod",
    )
    process_snowflake_table_sx_spv = SnowflakeProcedureOperator(
        procedure="data_science.export_postreg_segment_spv_ranking_sx.sql",
        database="reporting_prod",
    )
    process_snowflake_table_jf_spi = SnowflakeProcedureOperator(
        procedure="data_science.export_postreg_segment_spi_ranking_jf.sql",
        database="reporting_prod",
    )
    process_snowflake_table_fk_spi = SnowflakeProcedureOperator(
        procedure="data_science.export_postreg_segment_spi_ranking_fk.sql",
        database="reporting_prod",
    )

    for cfg in config_list:
        snowflake_to_sql = SnowflakeSqlToMSSqlWatermarkOperatorWithCompanyID(
            task_id=cfg.task_id,
            mssql_conn_id=cfg.conn_id,
            if_exists='append',
            column_list=table_config.column_list,
            watermark_column=table_config.watermark_column,
            initial_load_value=table_config.initial_load_value,
            src_database=table_config.database,
            src_schema=table_config.schema,
            src_table=table_config.table,
            tgt_database=table_config.target_database,
            tgt_schema=table_config.target_schema,
            tgt_table='import_product_impression_by_page_segment',
            strict_inequality=table_config.strict_inequality,
            company_id=cfg.company_id,
        )

        (
            process_snowflake_table_fl_spi
            >> process_snowflake_table_fl_spv
            >> process_snowflake_table_sd_spi
            >> process_snowflake_table_sx
            >> process_snowflake_table_sx_spv
            >> process_snowflake_table_jf_spi
            >> process_snowflake_table_fk_spi
            >> snowflake_to_sql
        )
