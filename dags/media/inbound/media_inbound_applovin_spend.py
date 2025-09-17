import pendulum

from airflow.models import DAG
from collections import namedtuple
from datetime import timedelta

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.applovin import ApplovinToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import email_lists, owners, s3_buckets, stages, conn_ids


column_list = [
    Column("date", "VARCHAR", source_name="day", uniqueness=True),
    Column("hour", "VARCHAR", uniqueness=True),
    Column("country", "VARCHAR"),
    Column("advertiser_id", "VARCHAR", uniqueness=True),
    Column("ad", "VARCHAR"),
    Column("ad_id", "VARCHAR", uniqueness=True),
    Column("campaign", "VARCHAR"),
    Column("campaign_id_external", "VARCHAR"),
    Column("creative_set", "VARCHAR"),
    Column("creative_set_id", "VARCHAR"),
    Column("ad_creative_type", "VARCHAR", uniqueness=True),
    Column("ad_type", "VARCHAR", uniqueness=True),
    Column("impressions", "VARCHAR"),
    Column("clicks", "VARCHAR"),
    Column("ctr", "VARCHAR"),
    Column("cost", "VARCHAR"),
    Column("sales", "VARCHAR"),
    Column("average_cpc", "VARCHAR"),
    Column("campaign_bid_goal", "VARCHAR"),
    Column("api_call_timestamp", "TIMESTAMP_LTZ(3)", delta_column=1),
]


config = {
    "columns": ','.join(
        [
            col.source_name
            for col in column_list
            if col.name not in ['api_call_timestamp', 'advertiser_id']
        ]
    ),
    "report_type": "advertiser",
    "format": "json",
}

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": email_lists.airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    dag_id=f"media_inbound_applovin_spend",
    default_args=default_args,
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
)


Advertiser = namedtuple('Advertiser', ['brand', 'id'])
advertisers = [
    Advertiser('fl', '123456ALFL'),
    Advertiser('jf', '987654ALJF'),
    Advertiser('sx', '73625378ALSX'),
]

with dag:
    schema = "media"
    table = 'applovin_spend'
    date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%z')[0:-5] }}"
    s3_prefix = f"inbound/{schema}.{table}/daily_v1"
    update_advertiser_ids = """
                    update lake.media.applovin_spend_stg
                        set advertiser_id = IFF(lower(campaign) ilike '%fabletics_male%', '123456ALFLM', '123456ALFLW')
                    where advertiser_id = '123456ALFL';
                 """

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="load_to_snowflake",
        database="lake",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=1),
        trigger_rule="all_done",
        pre_merge_command=update_advertiser_ids,
    )
    for advertiser in advertisers:
        to_s3 = ApplovinToS3Operator(
            task_id=f"{advertiser.brand}_applovin_spend_to_s3",
            config=config,
            key=f"{s3_prefix}/{advertiser.brand}_applovin_spend_{date_param}.tsv.gz",
            column_list=[x.source_name for x in column_list],
            applovin_conn_id=f"applovin_{advertiser.brand}",
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            advertiser_id=advertiser.id,
            time_period={
                "start": "{{ (prev_data_interval_end_success or data_interval_start).strftime('%Y-%m-%d')}}",
                "end": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
            },
        )

        to_s3 >> to_snowflake
