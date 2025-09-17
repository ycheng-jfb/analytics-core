import pendulum

from datetime import timedelta
from airflow.models import DAG
from airflow.utils.log.logging_mixin import LoggingMixin

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.facebook import FacebookAdInsightsToS3
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support
from include.utils.external.facebook import ACCOUNT_CONFIG, HEAVY_ACCOUNTS


column_list = [
    Column("account_id", "VARCHAR"),
    Column("account_name", "VARCHAR"),
    Column("campaign_id", "VARCHAR"),
    Column("campaign_name", "VARCHAR"),
    Column("adset_id", "VARCHAR"),
    Column("adset_name", "VARCHAR"),
    Column("ad_id", "VARCHAR", uniqueness=True),
    Column("ad_name", "VARCHAR"),
    Column("impressions", "INT"),
    Column("outbound_clicks", "VARIANT"),
    Column("inline_link_clicks", "INT"),
    Column("spend", "DECIMAL(38, 8)"),
    Column("date_start", "DATE", uniqueness=True),
    Column("date_stop", "DATE", uniqueness=True),
    Column("hourly_stats_aggregated_by_advertiser_time_zone", "VARCHAR", uniqueness=True),
    Column("api_call_timestamp", "TIMESTAMP_LTZ(3)", delta_column=1),
]

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 12, 26, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "execution_timeout": timedelta(hours=4),
}

config = {
    "fields": [
        "account_id",
        "account_name",
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "impressions",
        "outbound_clicks",
        "inline_link_clicks",
        "spend",
        "date_start",
        "date_stop",
    ],
    "time_range": {
        "since": "{{ from_date(run_id, dag_run, data_interval_start) }}",
        "until": "{{ to_date(run_id, dag_run, ds) }}",
    },
    "time_increment": "1",
    "level": "ad",
    "breakdowns": [
        "hourly_stats_aggregated_by_advertiser_time_zone",
    ],
    "use_unified_attribution_setting": "true",
    "action_breakdowns": "action_type",
    "action_attribution_windows": "['7d_click','1d_view']",
    'filtering': [
        {
            'field': 'ad.effective_status',
            'operator': 'IN',
            'value': [
                'ACTIVE',
                'PAUSED',
                'DELETED',
                'PENDING_REVIEW',
                'DISAPPROVED',
                'PREAPPROVED',
                'PENDING_BILLING_INFO',
                'CAMPAIGN_PAUSED',
                'ARCHIVED',
                'ADSET_PAUSED',
                'IN_PROCESS',
                'WITH_ISSUES',
            ],
        },
    ],
}


def from_date(run_id, dag_run, data_interval_start):
    curr_time_pacific = pendulum.instance(data_interval_start).in_timezone("America/Los_Angeles")
    log = LoggingMixin().log
    if run_id.startswith('manual__'):
        log.info(f"This is a manual run : {run_id}")
        since = dag_run.conf.get('since') or curr_time_pacific.add(days=-1).to_date_string()
        log.info(f"Manual start_date is: {since}")
        return since
    log.info(f"curr_time_pacific: {curr_time_pacific}")
    curr_hour = curr_time_pacific.hour
    log.info(f"curr_hour: {curr_hour}")
    lookback_days = 1
    log.info(f"refreshing last {lookback_days} days")
    return curr_time_pacific.add(days=-lookback_days).to_date_string()


def to_date(run_id, dag_run, ds):
    log = LoggingMixin().log
    if run_id.startswith('manual__'):
        until = dag_run.conf.get('until') or ds
        log.info(f"Manual end_date is: {until}")
        return until
    log.info(f"end_date: {ds}")
    return ds


dag = DAG(
    dag_id="media_inbound_facebook_ad_insights_by_hour",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
    user_defined_macros={"from_date": from_date, "to_date": to_date},
)

with dag:
    schema = "facebook"
    table = "ad_insights_by_hour"
    s3_prefix = f"inbound/{schema}.{table}/daily_v1"
    date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="load_to_snowflake",
        database="lake",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=1),
        trigger_rule="all_done",
    )

    for region, account_list in ACCOUNT_CONFIG.items():
        for act_id in account_list:
            if act_id in HEAVY_ACCOUNTS:
                rows_per_req = 250
            else:
                rows_per_req = 500
            get_facebook = FacebookAdInsightsToS3(
                task_id=act_id,
                account_id=act_id,
                facebook_ads_conn_id=f"facebook_{region.lower()}",
                config=config,
                key=f"{s3_prefix}/ad_insights_{act_id}_{date_param}.tsv.gz",
                column_list=[x.source_name for x in column_list],
                bucket=s3_buckets.tsos_da_int_inbound,
                s3_conn_id=conn_ids.S3.tsos_da_int_prod,
                rows_per_req=rows_per_req,
            )

            get_facebook >> to_snowflake
