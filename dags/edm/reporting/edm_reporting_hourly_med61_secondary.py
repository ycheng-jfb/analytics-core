from datetime import timedelta

import pendulum
from airflow import DAG
from include.airflow.callbacks.slack import (
    slack_failure_media_p1,
    slack_sla_miss_media_p1,
)
from include.airflow.operators.snowflake import SnowflakeProcedureOperator

from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2022, 4, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support + email_lists.airflow_media_support,
    "on_failure_callback": slack_failure_media_p1,
    "sla": timedelta(minutes=40),
}


dag = DAG(
    dag_id="edm_reporting_hourly_med61_secondary",
    default_args=default_args,
    schedule="10,40 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=45),
    sla_miss_callback=slack_sla_miss_media_p1,
)
with dag:
    med61_realtime_acquisition = SnowflakeProcedureOperator(
        procedure="shared.med61_realtime_acquisition_secondary.sql",
        database="reporting_base_prod",
        warehouse="DA_WH_ETL_HEAVY",
    )
    tableau_refresh_med_61 = TableauRefreshOperator(
        data_source_name="Real Time Acquisition Metrics (Cyber5 Backup)",
        task_id="med61-tableau-refresh",
    )

    tableau_refresh_med_61_order = TableauRefreshOperator(
        data_source_name="Real Time Order Metrics (Cyber5 Backup)",
        task_id="med61-tableau-refresh-order",
    )
    med61_realtime_acquisition >> [tableau_refresh_med_61, tableau_refresh_med_61_order]
