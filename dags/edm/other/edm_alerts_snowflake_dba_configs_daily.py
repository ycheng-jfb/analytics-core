from pathlib import Path

import pendulum
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator

from include import SQL_DIR

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import (
    SnowflakeAlertOperator,
    SnowflakeProcedureOperator,
    SnowflakeSqlOperator,
)
from include.config import owners, conn_ids
from include.config.email_lists import (
    data_integration_support,
    edw_support,
    data_analytics_data_platforms,
    data_integration_slack_channel,
)

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id='edm_alerts_snowflake_dba_configs_daily',
    default_args=default_args,
    schedule='5 1 * * *',
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


def check_day_of_week(**kwargs):
    execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    task_list = []

    monday_tasks = [
        'alert_for_missing_lake_consolidated_columns',
        'alert_for_long_disabled_users',
        'alert_for_non_transient_hvr_tables',
        'alert_for_service_account_cred_rotation',
        'alert_for_terminated_users',
        'notify_for_old_work_objects',
        'notify_inactive_users',
    ]

    if execution_time.weekday() == 0:
        task_list.extend(monday_tasks)
    return task_list


with dag:
    check_for_monday = BranchPythonOperator(
        python_callable=check_day_of_week,
        provide_context=True,
        task_id='check_for_monday',
    )

    alert_for_missing_lake_consolidated_columns = SnowflakeAlertOperator(
        task_id='alert_for_missing_lake_consolidated_columns',
        sql_or_path=Path(
            SQL_DIR, 'util', 'procedures', 'public.alert_for_missing_lake_consolidated_columns.sql'
        ),
        database='snowflake',
        alert_type=['mail', 'slack'],
        slack_conn_id='slack_data_integrations',
        slack_channel_name="data-integrations",
        subject="Alert: LAKE_CONSOLIDATED Columns are missing!",
        body=(
            "LAKE_CONSOLIDATED Tables are missing the following columns."
            "The configs for these Tables need to be updated, the Tables altered to support these Columns,"
            "and the Tables backfilled:"
        ),
        distribution_list=data_analytics_data_platforms,
    )

    alert_for_missing_lake_view_columns = SnowflakeAlertOperator(
        task_id='alert_for_missing_lake_view_columns',
        sql_or_path=Path(
            SQL_DIR, 'util', 'procedures', 'public.alert_for_missing_lake_view_columns.sql'
        ),
        database='snowflake',
        alert_type=['mail', 'slack'],
        slack_conn_id='slack_data_integrations',
        slack_channel_name="data-integrations",
        subject="Alert: Lake View Columns are missing!",
        body=(
            "Lake Views are missing the following columns that exist in the Source Table."
            "The Views need to be Altered to include these missing Columns!"
        ),
        distribution_list=data_analytics_data_platforms,
    )

    alert_for_long_disabled_users = SnowflakeAlertOperator(
        task_id='alert_for_long_disabled_users',
        sql_or_path=Path(SQL_DIR, 'util', 'procedures', 'public.alert_for_long_disabled_users.sql'),
        database='snowflake',
        alert_type=['mail', 'slack'],
        slack_conn_id='slack_data_integrations',
        slack_channel_name="data-integrations",
        subject="Alert: Long Disabled Users who need to be dropped!",
        body=(
            "This list of Users have been disabled for 3 or more months, and need to be dropped."
        ),
        distribution_list=data_integration_support,
    )

    alert_for_old_edw_clones = SnowflakeAlertOperator(
        task_id='alert_for_old_edw_clones',
        sql_or_path=Path(SQL_DIR, 'util', 'procedures', 'public.alert_for_old_edw_clones.sql'),
        database='snowflake',
        alert_type=["mail", "slack"],
        slack_conn_id=conn_ids.SlackAlert.slack_default,
        slack_channel_name="airflow-alerts-edm",
        subject="Alert: Old Snowflake Clones",
        body=(
            "The following Cloned Databases are more than 7 days old. "
            "These clones will have outdated data, may have outdated schema, "
            "and may need to be dropped or recloned:"
        ),
        distribution_list=edw_support,
    )

    notify_for_old_work_objects = SnowflakeAlertOperator(
        task_id='notify_for_old_work_objects',
        sql_or_path=Path(SQL_DIR, 'util', 'procedures', 'public.notify_for_old_work_objects.sql'),
        subject="Alert: WORK Objects you are responsible for are outdated and need to be removed",
        body=(
            "WORK Objects that you are responsible for are older then 15 days, and so need to be "
            "dropped to reduce technical debt, and to reduce storage costs."
        ),
        database='snowflake',
        alert_type=["mail", "slack", "user_notification"],
        slack_conn_id=conn_ids.SlackAlert.slack_default,
        slack_channel_name="airflow-alerts-edm",
        distribution_list=data_integration_support,
    )

    alert_for_non_transient_hvr_tables = SnowflakeAlertOperator(
        task_id='alert_for_non_transient_hvr_tables',
        sql_or_path=Path(
            SQL_DIR, 'util', 'procedures', 'public.alert_for_non_transient_hvr_tables.sql'
        ),
        database='snowflake',
        alert_type=['mail', 'slack'],
        slack_conn_id='slack_data_integrations',
        slack_channel_name="data-integrations",
        subject="Alert: HVR Tables that need to be set to Transient",
        body=(
            "The following Tables populated by HVR are not set as Transient. "
            "These Tables are increasing costs due to increases in Failsafe storage:"
        ),
        distribution_list=data_integration_support,
    )

    alert_for_service_account_cred_rotation = SnowflakeAlertOperator(
        task_id='alert_for_service_account_cred_rotation',
        sql_or_path=Path(
            SQL_DIR, 'util', 'procedures', 'public.alert_for_service_account_cred_rotation.sql'
        ),
        database='snowflake',
        alert_type=['mail', 'slack'],
        slack_conn_id='slack_data_integrations',
        slack_channel_name="data-integrations",
        subject="Alert: Snowflake Service Accounts needing password rotation.",
        body="""
        The following Service Accounts in Snowflake need their passwords rotated
        within the 90 day limit, as required by SOX.
        """,
        distribution_list=[
            'rpoornima@techstyle.com',
            'savangala@fabletics.com',
            'rtanneeru@techstyle.com',
        ],
    )

    alert_for_terminated_users = SnowflakeAlertOperator(
        task_id='alert_for_terminated_users',
        sql_or_path=Path(SQL_DIR, 'util', 'procedures', 'public.alert_for_terminated_users.sql'),
        database='snowflake',
        alert_type=['mail', 'slack'],
        slack_conn_id='slack_data_integrations',
        slack_channel_name="data-integrations",
        subject="Alert: Terminated Users who aren't deactivated in Snowflake",
        body="",
        distribution_list=data_integration_support,
    )

    notify_inactive_users = SnowflakeAlertOperator(
        task_id='notify_inactive_users',
        distribution_list=[],
        database='snowflake',
        sql_or_path=Path(SQL_DIR, 'util', 'procedures', 'public.identify_inactive_users.sql'),
        subject="Alert: Your Snowflake Account USERNAME will be deleted due to inactivity",
        body=(
            "Your Snowflake Account USERNAME has been inactive for greater than 75 days "
            "and is scheduled for deactivation. If you would like to keep your Snowflake Account, "
            "please login to Snowflake to reset your Account's activity status."
        ),
        alert_type=['slack', 'user_notification'],
        slack_conn_id='slack_data_integrations',
        slack_channel_name="data-integrations",
    )

    disable_users = SnowflakeSqlOperator(
        task_id='disable_users',
        sql_or_path='CALL UTIL.PUBLIC.DISABLE_INACTIVE_USERS();',
        warehouse='DA_WH_ETL_LIGHT',
    )

    task_alter_default_secondary_roles_to_empty = SnowflakeSqlOperator(
        task_id='task_alter_default_secondary_roles_to_empty',
        sql_or_path='CALL util.public.set_default_secondary_roles_to_empty();',
        warehouse='DA_WH_ETL_LIGHT',
    )

    load_query_history = SnowflakeProcedureOperator(
        database='util',
        procedure='public.query_history_enriched.sql',
        warehouse='DA_WH_ETL_LIGHT',
    )

    check_for_monday >> [
        alert_for_long_disabled_users,
        alert_for_missing_lake_consolidated_columns,
        alert_for_non_transient_hvr_tables,
        alert_for_service_account_cred_rotation,
        alert_for_terminated_users,
        notify_for_old_work_objects,
        notify_inactive_users,
    ]
