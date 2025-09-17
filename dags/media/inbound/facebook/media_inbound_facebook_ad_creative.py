from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.facebook import FacebookAdCreativeNew
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support
from include.utils.external.facebook import ACCOUNT_CONFIG

schema = "facebook"
table = "ad_creative"
full_table_name = f"{schema}.{table}"

dag_id = "media_inbound_facebook_ad_creative"
s3_prefix = f"media/{full_table_name}/daily/v11"
date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"

column_list = [
    Column("account_id", "VARCHAR"),
    Column("campaign_id", "VARCHAR"),
    Column("ad_id", "INT", uniqueness=True),
    Column(
        "creative_id",
        "INT",
    ),
    Column("image_hash", "VARCHAR"),
    Column("image_url", "VARCHAR"),
    Column("configured_status", "VARCHAR"),
    Column("post_type", "VARCHAR"),
    Column("thumbnail_url", "VARCHAR"),
    Column("effective_object_story_id", "VARCHAR"),
    Column("object_story_spec", "VARCHAR"),
    Column("instagram_permalink_url", "VARCHAR"),
]

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 5, 1, tz="America/Los_Angeles"),
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "execution_timeout": timedelta(hours=8),
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule="0 9,16,22 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)

with dag:
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake",
        database="lake",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=0, skip_pct=1),
        trigger_rule="all_done",
    )
    for region, account_list in ACCOUNT_CONFIG.items():
        for act_id in account_list:
            get_facebook = FacebookAdCreativeNew(
                task_id=act_id,
                facebook_ads_conn_id=f"facebook_{region.lower()}",
                key=f"{s3_prefix}/adcreative_{act_id}_{date_param}.tsv.gz",
                column_list=[x.source_name for x in column_list],
                bucket=s3_buckets.tsos_da_int_inbound,
                s3_conn_id=conn_ids.S3.tsos_da_int_prod,
                account_id=act_id,
            )
            get_facebook >> to_snowflake
