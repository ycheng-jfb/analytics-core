import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeBackupTableToS3
from include.config import email_lists, owners
from include.config import stages

default_args = {
    'start_date': pendulum.datetime(2023, 12, 8, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id='edm_backup_snapshot',
    default_args=default_args,
    schedule='0 3 * * 1',
    catchup=False,
)

with dag:
    backup_daily_cash_final_output = SnowflakeBackupTableToS3(
        task_id='backup_daily_cash_final_output',
        database='edw_prod',
        schema='snapshot',
        table='daily_cash_final_output',
        where_clause=SnowflakeBackupTableToS3.timestamp_filter_where_clause(
            'snapshot_datetime', 42
        ),
        delete_after_backup=True,
    )
    backup_finance_kpi_final_output = SnowflakeBackupTableToS3(
        task_id='backup_finance_kpi_final_output',
        database='edw_prod',
        schema='snapshot',
        table='finance_kpi_final_output',
        where_clause=SnowflakeBackupTableToS3.timestamp_filter_where_clause(
            'snapshot_datetime', 42
        ),
        delete_after_backup=True,
    )
    backup_finance_sales_ops = SnowflakeBackupTableToS3(
        task_id='backup_finance_sales_ops',
        database='edw_prod',
        schema='snapshot',
        table='finance_sales_ops',
        where_clause=SnowflakeBackupTableToS3.timestamp_filter_where_clause(
            'snapshot_datetime', 42
        ),
        delete_after_backup=True,
    )
    backup_customer_lifetime_value_monthly = SnowflakeBackupTableToS3(
        task_id='backup_fact_credit_event',
        database='edw_prod',
        schema='snapshot',
        table='customer_lifetime_value_monthly',
        where_clause=SnowflakeBackupTableToS3.timestamp_filter_where_clause(
            'snapshot_datetime', 42
        ),
        delete_after_backup=True,
    )
