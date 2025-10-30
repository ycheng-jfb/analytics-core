import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.alert import DisabledDagsAlertOperator
from include.config import owners
from include.config.email_lists import data_integration_support

default_args = {
    'start_date': pendulum.datetime(2019, 11, 19, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

with DAG(
    dag_id='edm_alert_disabled_dag',
    default_args=default_args,
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    schedule='0 * * * *',
) as dag:
    op = DisabledDagsAlertOperator(
        task_id='disabled_dags_alert',
        dag_ids={
            'edw_acquisition_and_edw',
            'edm_acquisition_edw_sources',
            'edm_acquisition_other',
            'edm_acquisition_ultra_warehouse_no_articles',
            'edm_alerts_snowflake_dba_configs_daily',
            'edm_alerts_snowflake_dba_configs_hourly',
            'edm_alerts_source_hvr_articles',
            'edm_edw_load',
            'edm_lake_consolidated_edw_sources',
            'edm_lake_consolidated_edw_sources',
            'edm_lake_consolidated_high_frequency',
            'edm_lake_consolidated_intra_day',
            'edm_lake_consolidated_other',
            'edm_reporting_session',
            'edm_outbound_mssql_sftp_storeforce_realtime',
            'edw_data_validation_and_alert',
            'edw_p1_reporting_data_validation_and_alerting',
        },
    )
