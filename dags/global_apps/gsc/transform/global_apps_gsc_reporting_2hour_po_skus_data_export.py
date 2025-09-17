import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake_to_mssql import SnowflakeToMsSqlBCPOperator
from include.config import email_lists, owners, conn_ids

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.global_apps_analytics,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_gsc_reporting_2hour_po_skus_data_export",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    doc_md=__doc__,
)

with dag:
    gsc_po_skus_data_to_mssql = SnowflakeToMsSqlBCPOperator(
        task_id="reporting_base_prod.gsc.po_skus_data.to_mssql",
        snowflake_database="reporting_base_prod",
        snowflake_schema="gsc",
        snowflake_table="po_skus_data",
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        mssql_target_database="ssrs_reports",
        mssql_target_schema="dbo",
        mssql_target_table="po_skus_data",
        unique_columns=["po_id", "po_dtl_id"],
        watermark_column="meta_update_datetime",
    )
