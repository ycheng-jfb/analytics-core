import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake import (
    SnowflakeAlertOperator,
    SnowflakeProcedureOperator,
)
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import owners
from include.config.email_lists import data_integration_support

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.analytics_engineering,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_session",
    default_args=default_args,
    schedule="0 5,11,20 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

session_alert_sql = """
SELECT
    store_brand,
    previous_date,
    last_month_same_date,
    average_sessions_percent,
    previous_date_sessions_percent,
    last_month_same_date_sessions_percent,
    average_percent_diff,
    date_percent_diff
FROM reporting_base_prod.reference.session_segment_percent
WHERE alert = TRUE;
"""


def check_alert_time(data_interval_end: pendulum.datetime):
    run_time = data_interval_end.in_timezone("America/Los_Angeles")
    if run_time.hour == 11:
        return "session_alert"
    elif run_time.hour in [5, 20]:
        return "daily_platform"
    else:
        return []


with dag:
    check_alert_time_br = BranchPythonOperator(
        task_id="check_alert_time",
        python_callable=check_alert_time,
    )
    session_alert = EmptyOperator(task_id="session_alert")
    daily_platform = EmptyOperator(task_id="daily_platform")
    chain_tasks(check_alert_time_br, [session_alert, daily_platform])

    session_uri = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="staging.session_uri.sql",
        warehouse="DA_WH_ETL_HEAVY",
    )
    bots_confirmed = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="staging.bot_confirmed.sql",
        warehouse="DA_WH_ETL_HEAVY",
    )
    membership_state = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="staging.membership_state.sql",
        watermark_tables=[
            "lake_consolidated_view.ultra_merchant.session",
        ],
        warehouse="DA_WH_ETL_HEAVY",
    )
    site_visit_metrics = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="staging.site_visit_metrics.sql",
        warehouse="DA_WH_ETL_HEAVY",
    )
    mobile_app_session_os_updated = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="shared.mobile_app_session_os_updated.sql",
        watermark_tables=[
            "reporting_base_prod.staging.session_uri",
            "reporting_base_prod.staging.session_ga",
            "lake_consolidated_view.ultra_merchant.session",
        ],
        warehouse="DA_WH_ETL_HEAVY",
    )
    visitor_session = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="staging.visitor_session.sql",
        watermark_tables=[
            "lake_consolidated_view.ultra_merchant.visitor_session",
        ],
        warehouse="DA_WH_ETL_HEAVY",
    )
    migrated_session = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="staging.migrated_session.sql",
        watermark_tables=[
            "lake_consolidated_view.ultra_merchant.session_migration_log",
        ],
        warehouse="DA_WH_ETL_HEAVY",
    )

    customer_session = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="staging.customer_session.sql",
        warehouse="DA_WH_ETL_HEAVY",
    )

    segment_branch_open = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="staging.segment_branch_open_events.sql",
        warehouse="DA_WH_ETL_HEAVY",
    )

    session_channel_mapping = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="shared.media_source_channel_mapping.sql",
        warehouse="DA_WH_ETL_HEAVY",
    )

    media_channel = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="staging.session_media_channel.sql",
        warehouse="DA_WH_ETL_HEAVY",
    )

    session_ab_test_start = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="shared.session_ab_test_start.sql",
        watermark_tables=[
            "lake_consolidated_view.ultra_merchant.session_detail",
            "lake_consolidated_view.ultra_merchant.session",
        ],
        warehouse="DA_WH_ETL_HEAVY",
    )

    session = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="shared.session.sql",
        warehouse="DA_WH_ETL_HEAVY",
    )

    session_segment_percent = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="reference.session_segment_percent.sql",
    )

    session_segment_alert = SnowflakeAlertOperator(
        task_id="session_segment_alert",
        distribution_list=[
            "hnohwar@techstyle.com",
            "yyoruk@techstyle.com",
            "lasplund@techstyle.com",
            "jhuff@techstyle.com",
            "achang@techstyle.com",
            "talinan@techstyle.com",
            "dragan@techstyle.com",
            "dyin@fabletics.com",
            "emaldonado@fabletics.com",
            "thaddad@techstyle.com",
        ],
        database="reporting_base_prod",
        subject="Session segment alert",
        body="""<p>Below is the date for which segment percentage is
                 lower than average percentage of past month or
                 lower than percentage of same day of last month</p>
                 <a href="https://10ay.online.tableau.com/#/site/techstyle/views/SessionsInSegmentValidation/IP-SessionsnotinSegment?:iid=2">Tableau Dashboard</a>""",
        sql_or_path=session_alert_sql,
    )

    session_indicator_snowflake_procedure = SnowflakeProcedureOperator(
        procedure="shared.session_indicator.sql",
        database="reporting_base_prod",
        warehouse="DA_WH_ETL_HEAVY",
    )

    session_segment_events = SnowflakeProcedureOperator(
        procedure="staging.session_segment_events.sql",
        database="reporting_base_prod",
        warehouse="DA_WH_ETL_HEAVY",
    )

    traffic_estimation_snowflake_procedure = SnowflakeProcedureOperator(
        procedure="shared.tfg016_traffic_estimation.sql",
        database="reporting_prod",
        warehouse="DA_WH_ETL_HEAVY",
    )

    tableau_refresh = TableauRefreshOperator(
        task_id="tableau_tfg016_traffic_estimation",
        data_source_id="372f822a-f3a0-49f5-9359-8086a548d3e0",
    )

    chain_tasks(
        [
            bots_confirmed,
            session_uri,
            membership_state,
            site_visit_metrics,
            visitor_session,
            migrated_session,
            customer_session,
            segment_branch_open,
            session_ab_test_start,
            session_segment_events,
        ],
        session,
        session_channel_mapping,
        media_channel,
    )

    trigger_svm = TFGTriggerDagRunOperator(
        task_id="trigger_analytics_reporting_single_view_media",
        trigger_dag_id="analytics_reporting_single_view_media",
        execution_date="{{ data_interval_end }}",
    )

    trigger_psource = TFGTriggerDagRunOperator(
        task_id="trigger_psource_payments_by_day",
        trigger_dag_id="edm_reporting_psource_payments_by_day",
        execution_date="{{ data_interval_end }}",
    )

    trigger_daily_platform = TFGTriggerDagRunOperator(
        task_id="trigger_reporting_daily_platform",
        trigger_dag_id="edm_reporting_daily_platform",
        execution_date="{{ data_interval_end }}",
    )

    session_uri >> mobile_app_session_os_updated
    mobile_app_session_os_updated >> session
    session_segment_events >> session
    media_channel >> trigger_svm
    media_channel >> trigger_psource
    bots_confirmed >> session_uri
    (
        session
        >> session_indicator_snowflake_procedure
        >> traffic_estimation_snowflake_procedure
        >> tableau_refresh
    )

    (daily_platform >> session_channel_mapping >> trigger_daily_platform)

    session_alert >> session_segment_percent
    session >> session_segment_percent
    session_segment_percent >> session_segment_alert
    customer_session >> segment_branch_open
    segment_branch_open >> session_segment_events
