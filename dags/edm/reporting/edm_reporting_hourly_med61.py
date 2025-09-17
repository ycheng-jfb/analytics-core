from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_media_p1, slack_sla_miss_media_p1
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake import SnowflakeAlertOperator, SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners, conn_ids

default_args = {
    "start_date": pendulum.datetime(2022, 4, 1, 7, tz="America/Los_Angeles"),
    'owner': owners.data_integrations,
    "email": email_lists.data_integration_support + email_lists.airflow_media_support,
    "on_failure_callback": slack_failure_media_p1,
    "sla": timedelta(minutes=40),
    "retries": 1,
    'retry_delay': timedelta(minutes=2),
}


def trigger_dag(**kwargs):
    execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    if execution_time.minute == 40:
        return [trigger_med61_hourly_projections.task_id, med61_validation_check.task_id]


dag = DAG(
    dag_id='edm_reporting_hourly_med61',
    default_args=default_args,
    schedule="10,40 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=100,
    dagrun_timeout=timedelta(minutes=45),
    sla_miss_callback=slack_sla_miss_media_p1,
)
with dag:
    med61_realtime_acquisition = SnowflakeProcedureOperator(
        procedure='shared.med61_realtime_acquisition.sql',
        database='reporting_base_prod',
        warehouse='DA_WH_ETL_HEAVY',
    )
    med61_validation_check = SnowflakeAlertOperator(
        task_id="med61_validation_check",
        sql_or_path=Path(
            SQL_DIR, "reporting_base_prod", "procedures", "shared.med61_metrics_validation.sql"
        ),
        distribution_list=[],
        subject=f"{{ macros.datetime.today().strftime('%Y-%m-%d') }}: MED61 metrics are not updated from the past 1 hour",
        body="Please find the MED61 metrics update status below. No of rows fetched",
        alert_type="slack",
        slack_conn_id=conn_ids.SlackAlert.media_p1,
        slack_channel_name="airflow-media-p1",
    )
    tableau_refresh_med_61 = TableauRefreshOperator(
        data_source_id='998e517f-917b-4ad5-8d58-5777e706376f', task_id='med61-tableau-refresh'
    )

    tableau_refresh_med_61_order = TableauRefreshOperator(
        data_source_name='Real Time Order Metrics', task_id='med61-tableau-refresh-order'
    )
    check_the_schedule_for_med61_projections_and_validation = BranchPythonOperator(
        task_id="check_the_schedule_for_med61_projections_and_validation",
        python_callable=trigger_dag,
    )
    trigger_med61_hourly_projections = TFGTriggerDagRunOperator(
        task_id='trigger_med61_hourly_projections',
        trigger_dag_id='edm_reporting_med61_hourly_projections',
        execution_date='{{ data_interval_end }}',
    )
    med61_hourly_projections_vs_actuals = SnowflakeProcedureOperator(
        procedure='dbo.med61_hourly_projections_vs_actuals.sql',
        database='reporting_media_prod',
        warehouse='DA_WH_ETL_HEAVY',
    )
    tableau_refresh_med_61_projection = TableauRefreshOperator(
        data_source_name='Real Time Hourly Projections vs Actuals',
        task_id='med61-tableau-refresh-projection',
    )
    med61_realtime_acquisition >> [tableau_refresh_med_61, tableau_refresh_med_61_order]
    (
        med61_realtime_acquisition
        >> check_the_schedule_for_med61_projections_and_validation
        >> [trigger_med61_hourly_projections, med61_validation_check]
    )
    (
        med61_realtime_acquisition
        >> med61_hourly_projections_vs_actuals
        >> tableau_refresh_med_61_projection
    )
