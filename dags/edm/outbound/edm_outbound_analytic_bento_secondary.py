import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake_to_mssql import SnowflakeSqlToMSSqlWatermarkOperator
from include.config import conn_ids, email_lists, owners
from task_configs.dag_config.bento_export import tables

default_args = {
    'start_date': pendulum.datetime(2019, 10, 24, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_outbound_analytic_bento_secondary',
    default_args=default_args,
    schedule='30 5,20 * * *',
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)

with dag:
    for table in tables:
        snowflake_to_sql_fl = SnowflakeSqlToMSSqlWatermarkOperator(
            task_id=f'{table.target_table_name}_to_mssql_fl',
            mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow_rw_analytic,
            snowflake_conn_id=conn_ids.Snowflake.default,
            if_exists='append',
            column_list=table.column_list,
            watermark_column=table.watermark_column,
            initial_load_value=table.initial_load_value,
            src_database=table.database,
            src_schema=table.schema,
            src_table=table.table,
            tgt_database=table.target_database,
            tgt_schema=table.target_schema,
            tgt_table=table.target_table_name.strip('dim_'),
            strict_inequality=table.strict_inequality,
        )
        snowflake_to_sql_sxf = SnowflakeSqlToMSSqlWatermarkOperator(
            task_id=f'{table.target_table_name}_to_mssql_sxf',
            mssql_conn_id=conn_ids.MsSql.savagex_app_airflow_rw_analytic,
            snowflake_conn_id=conn_ids.Snowflake.default,
            if_exists='append',
            column_list=table.column_list,
            watermark_column=table.watermark_column,
            initial_load_value=table.initial_load_value,
            src_database=table.database,
            src_schema=table.schema,
            src_table=table.table,
            tgt_database=table.target_database,
            tgt_schema=table.target_schema,
            tgt_table=table.target_table_name.strip('dim_'),
            strict_inequality=table.strict_inequality,
        )
