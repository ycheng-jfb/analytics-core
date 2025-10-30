import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.creatoriq import CreatorIQPatchOperator
from include.config import owners
from include.config.email_lists import airflow_media_support

default_args = {
    "start_date": pendulum.datetime(2020, 12, 10, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
}


def get_sql_cmd(data_interval_start):
    """
    A function to select the different sql statement based on passed date.

    Args:
        data_interval_start: date on which the dag is running.

    Returns:
        A string with SQL statement that needs to be executed to return publisherIds.

    """
    exec_date = pendulum.instance(data_interval_start)
    if exec_date.hour == 1:
        return (
            "select distinct CAMPAIGNID from LAKE.CREATORIQ.CAMPAIGN_LIST ct "
            "join LAKE.CREATORIQ.EXCLUDE_LIST el on el.id <> ct.CAMPAIGNID and "
            "lower(el.process) like '%patch%' where DATEDIFF(day,ENDDATE,CURRENT_TIMESTAMP()) <= 7 "
            "and DATEDIFF(day,ENDDATE,CURRENT_TIMESTAMP()) >= -2"
        )

    else:
        return (
            "select distinct campaignid from lake.creatoriq.campaign_list "
            "where campaignname ilike '%%hdyh%%' and "
            "startdate = date_trunc(month,current_date()::timestamp_ltz)"
        )


dag = DAG(
    dag_id="media_inbound_creatoriq_patch_campaigns_hourly",
    default_args=default_args,
    schedule="0 * * * *",
    max_active_tasks=20,
    catchup=False,
    max_active_runs=1,
    user_defined_macros={"sql_cmd": get_sql_cmd},
    on_failure_callback=[slack_failure_media],
)

with dag:
    op = CreatorIQPatchOperator(
        task_id="patch_campaign_ids", sql_cmd="{{ sql_cmd(data_interval_end) }}"
    )
