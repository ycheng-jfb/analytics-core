from dataclasses import dataclass
from typing import Optional

import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_data_science
from include.airflow.operators.mssql import MsSqlOperator
from include.airflow.operators.snowflake_to_mssql import SnowflakeSqlToMSSqlOperator
from include.config import conn_ids, owners

default_args = {
    "start_date": pendulum.datetime(2022, 3, 1, tz="America/Los_Angeles"),
    "owner": owners.data_science,
    "email": "datascience@techstyle.com",
    "on_failure_callback": slack_failure_data_science,
}

dag = DAG(
    dag_id="data_science_outbound_product_tags",
    default_args=default_args,
    schedule="50 1 * * *",
    catchup=False,
)

filter_names = [
    "CORE_FASHION",
    "ITEM_STATUS",
    "COLLECTION",
    "ECO_SYSTEM",
    "Mens Bottom Type",
]
del_sql = "execute ultraimport.dbo.pr_import_product_tags_truncate;"
query_sql = f"""
    select MPID, REPLACE(lower(NAME),' ','_') as NAME, REPLACE(lower(VALUE),' ','_') as VALUE,
    DATE_EXPECTED, replace(lower(TAG_TYPE),' ','_') as TAG_TYPE,
    sysdate() as datetime_added, sysdate() as datetime_modified
    from REPORTING_BASE_PROD.DATA_SCIENCE.PRODUCT_RECOMMENDATION_TAGS
    WHERE NAME IN ('{"', '".join(filter_names)}')
"""


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
            "justfab": conn_ids.MsSql.justfab_app_airflow_rw,
            "fabletics": conn_ids.MsSql.fabletics_app_airflow_rw,
            "savagex": conn_ids.MsSql.savagex_app_airflow_rw,
            # Hardcoded temporarily to solve new conn_id issue
            "jfdev": conn_ids.MsSql.ds_qa_justfab_app_airflow_rw,
            "fldev": conn_ids.MsSql.ds_qa_fabletics_app_airflow_rw,
        }
        return conn_id_map[self.server]

    @property
    def mssql_query(self) -> str:
        if self.company_id:
            return f"{query_sql} and meta_company_id = {self.company_id}"
        return query_sql


config_list = [
    Config(server="justfab", company_id=10),
    Config(server="fabletics", company_id=20),
    Config(server="savagex", company_id=30),
    Config(server="jfdev", company_id=10),
    Config(server="fldev", company_id=20),
]

with dag:
    for cfg in config_list:
        delete_mssql = MsSqlOperator(
            task_id=f"delete_mssql_{cfg.task_id}",
            mssql_conn_id=cfg.conn_id,
            sql=del_sql,
        )
        snowflake_to_mssql = SnowflakeSqlToMSSqlOperator(
            task_id=f"snowflake_to_mssql_{cfg.task_id}",
            mssql_conn_id=cfg.conn_id,
            tgt_database="ultraimport",
            tgt_schema="dbo",
            tgt_table="import_product_tags",
            sql_or_path=cfg.mssql_query,
            if_exists="append",
        )
        delete_mssql >> snowflake_to_mssql
