import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2021, 1, 19, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.edw_engineering,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_daily_platform",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=100,
)
with dag:
    sessions_by_platform = SnowflakeProcedureOperator(
        procedure="shared.sessions_by_platform.sql",
        database="reporting_prod",
    )
    sessions_by_platform_new = SnowflakeProcedureOperator(
        procedure="shared.sessions_by_platform_stg.sql",
        database="reporting_prod",
    )
    session_order_stg = SnowflakeProcedureOperator(
        procedure="shared.sessions_order_stg.sql",
        database="reporting_prod",
    )
    daily_platform_sessions_orders = SnowflakeProcedureOperator(
        procedure="shared.daily_platform_sessions_orders.sql",
        database="reporting_prod",
    )
    customer_action_period_mobile = SnowflakeProcedureOperator(
        procedure="shared.customer_action_period_mobile_app.sql",
        database="reporting_prod",
    )
    mobile_app_sessions_stg = SnowflakeProcedureOperator(
        procedure="shared.mobile_app_sessions_stg.sql",
        database="reporting_prod",
    )
    monthly_engagement_mobile_app = SnowflakeProcedureOperator(
        procedure="shared.monthly_engagement_mobile_app.sql",
        database="reporting_prod",
    )
    daily_installation_mobile_app = SnowflakeProcedureOperator(
        procedure="shared.daily_install_to_order_mobile_app.sql",
        database="reporting_prod",
    )
    preferred_platform_after_mobile = SnowflakeProcedureOperator(
        procedure="shared.preferred_platform_after_mobile_app.sql",
        database="reporting_prod",
    )
    daily_platform_sessions = TableauRefreshOperator(
        task_id="trigger_tfg010_daily_platform_sessions_orders",
        data_source_name="TFG010 - Daily Platform Sessions Orders",
    )
    monthly_mobile_app_engagement = TableauRefreshOperator(
        task_id="trigger_tfg011_monthly_mobile_app_engagement",
        data_source_name="TFG011 - Monthly Mobile App Engagement",
    )
    preferred_platform_after_first_order = TableauRefreshOperator(
        task_id="trigger_tfg012_preferred_platform_after_first_app_order",
        data_source_name="TFG012 - Preferred Platform After First App Order",
    )
    customer_action_period_mobile_app = TableauRefreshOperator(
        task_id="trigger_tfg013_customer_action_period_mobile_app",
        data_source_name="TFG013 - Customer Action Period Mobile App",
    )
    daily_install_to_app_order = TableauRefreshOperator(
        task_id="trigger_tfg014_daily_install_to_app_order",
        data_source_name="TFG014 - Daily Install to App Order",
    )

    (
        sessions_by_platform_new,
        sessions_by_platform
        >> [
            session_order_stg,
            mobile_app_sessions_stg,
        ],
    )
    session_order_stg >> [
        monthly_engagement_mobile_app,
        daily_installation_mobile_app,
        preferred_platform_after_mobile,
        daily_platform_sessions_orders,
        customer_action_period_mobile,
    ]
    mobile_app_sessions_stg >> [
        monthly_engagement_mobile_app,
        daily_installation_mobile_app,
    ]

    daily_platform_sessions_orders >> daily_platform_sessions
    monthly_engagement_mobile_app >> monthly_mobile_app_engagement
    daily_installation_mobile_app >> daily_install_to_app_order
    customer_action_period_mobile >> customer_action_period_mobile_app
    preferred_platform_after_mobile >> preferred_platform_after_first_order
