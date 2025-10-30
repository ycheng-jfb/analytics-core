import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake_to_mssql import SnowflakeSqlToMSSqlOperatorTruncateAndLoad
from include.config import email_lists, owners, conn_ids

schema = 'sharepoint'
table = 'shift_forecast'
database = 'lake_view'
sql = f"select forecast_date, fc_code, shift_label, units, sla_day, meta_row_hash from {database}.{schema}.{table}"

default_args = {
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_inbound_shift_forecast',
    default_args=default_args,
    start_date=pendulum.datetime(2021, 6, 10, tz='America/Los_Angeles'),
    catchup=False,
    schedule='30 5 * * *',
)

with dag:

    to_evolve = SnowflakeSqlToMSSqlOperatorTruncateAndLoad(
        task_id='shift_forecast_to_evolve',
        tgt_database='ultrawarehouse',
        tgt_schema='rpt',
        tgt_table='shift_forecast',
        snowflake_conn_id=conn_ids.Snowflake.default,
        mssql_conn_id=conn_ids.MsSql.dbp40_app_airflow_rw,
        if_exists="append",
        sql_or_path=sql,
    )
