from pathlib import Path

import pendulum
from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import (
    SnowflakeAlertOperator,
    SnowflakeProcedureOperator,
)
from include.config import conn_ids, email_lists, owners

from airflow import DAG

default_args = {
    "start_date": pendulum.datetime(2020, 5, 19, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support
    + email_lists.fabletics_analytics_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_fole",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
with dag:
    fact_order_line_extended_daily_refresh = SnowflakeProcedureOperator(
        procedure="fabletics.fact_order_line_extended_new.sql",
        database="reporting_base_prod",
    )

    fol_extended_missing_order_number_alert = SnowflakeAlertOperator(
        task_id="fol_extended_missing_order_number_alert",
        distribution_list=email_lists.fabletics_analytics_support,
        database="REPORTING_BASE_PROD",
        subject="Alert: Fabletics fol_extended_missing_order_number",
        slack_conn_id=conn_ids.SlackAlert.slack_default,
        slack_channel_name="airflow-alerts-edm",
        body="Fabletics found fol extended missing order number:",
        alert_type=["mail", "slack"],
        sql_or_path=Path(
            SQL_DIR,
            "util",
            "procedures",
            "fabletics.fol_extended_missing_order_lines.sql",
        ),
    )

    fol_vs_fso_alert = SnowflakeAlertOperator(
        task_id="fol_vs_fso_alert",
        distribution_list=email_lists.fabletics_analytics_support,
        database="EDW_PROD",
        subject="Alert: Fabletics fol_vs_fso_alert",
        slack_conn_id=conn_ids.SlackAlert.slack_default,
        slack_channel_name="airflow-alerts-edm",
        body="Fabletics found issue on fol vs fso",
        alert_type=["mail", "slack"],
        sql_or_path=Path(SQL_DIR, "util", "procedures", "fabletics.fol_vs_fso.sql"),
    )

    fact_order_line_extended_daily_refresh >> [
        fol_extended_missing_order_number_alert,
        fol_vs_fso_alert,
    ]
