from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.facebook import FacebookAdsPixelStats
from include.airflow.operators.snowflake import SnowflakeAlertOperator, SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support, data_integration_support

schema = "facebook"
table = "ads_pixel_stats"
full_table_name = f"{schema}.{table}"

dag_id = "media_inbound_facebook_ads_pixel_stats"
s3_prefix = f"media/{full_table_name}/daily/v2"
date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"

PIXEL_CONFIG = {
    "na": [
        '528161084251376',
        '218843068308551',
        '537344443131231',
        '715289052842650',
        '686991808018907',
        '279505792226308',
        '212920452242866',
        '383363077795017',
    ],
    "eu": [
        '2194587504139935',
        '741318252734475',
        '1653034964810714',
    ],
}

column_list = [
    Column("pixel_id", "VARCHAR", uniqueness=True),
    Column("event", "VARCHAR", uniqueness=True),
    Column("start_time", "TIMESTAMP_TZ", uniqueness=True),
    Column("BROWSER", "INT"),
    Column("SERVER", "INT"),
]

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 5, 1, tz="America/Los_Angeles"),
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "execution_timeout": timedelta(hours=2),
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule="0 22 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)

with dag:
    conversions_event_stats_variance = SnowflakeProcedureOperator(
        priority_weight=100,
        snowflake_conn_id=conn_ids.Snowflake.edw,
        procedure='facebook.conversions_event_stats_variance.sql',
        database='reporting_media_base_prod',
    )
    media_facebook_pixel_stats_check = SnowflakeAlertOperator(
        task_id="conversions_event_stats_alert",
        sql_or_path="""
                              SELECT * FROM reporting_media_base_prod.facebook.conversions_event_stats_variance;
                         """,
        subject="Events with count discrepancies",
        body="Below are the Events details with count discrepancies.",
        distribution_list=data_integration_support,
        alert_type=['slack', 'mail'],
        slack_conn_id=conn_ids.SlackAlert.media,
        slack_channel_name="airflow-alerts-media",
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake",
        database="lake",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=1),
        trigger_rule="all_done",
    )
    for region, pixel_list in PIXEL_CONFIG.items():
        for pixel_id in pixel_list:
            get_facebook = FacebookAdsPixelStats(
                task_id=pixel_id,
                facebook_ads_conn_id=f"facebook_{region.lower()}",
                key=f"{s3_prefix}/ads_pixel_stats_{pixel_id}_{date_param}.tsv.gz",
                column_list=[x.source_name for x in column_list],
                bucket=s3_buckets.tsos_da_int_inbound,
                s3_conn_id=conn_ids.S3.tsos_da_int_prod,
                pixel_id=pixel_id,
            )
            get_facebook >> to_snowflake
    to_snowflake >> conversions_event_stats_variance >> media_facebook_pixel_stats_check
