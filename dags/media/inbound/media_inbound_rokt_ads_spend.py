from collections import namedtuple
from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media, slack_sla_miss_media
from include.airflow.operators.rokt import RoktToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support

schema = "rokt"
table = "ads_spend"
date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"
s3_prefix = f"media/{schema}.{table}/daily_v1"

column_list = [
    Column("groupByValue", "NUMBER", uniqueness=True),
    Column("grossCost", "DECIMAL(20,2)"),
    Column("netCost", "DECIMAL(20,2)"),
    Column("impressions", "NUMBER"),
    Column("referrals", "NUMBER"),
    Column("acquisitionsByConversionDate", "NUMBER"),
    Column("acquisitionsByReferralDate", "NUMBER"),
    Column("campaigns", "NUMBER"),
    Column("creatives", "NUMBER"),
    Column("audiences", "NUMBER"),
    Column("campaignCountries", "NUMBER"),
    Column("creativeName", "VARCHAR"),
    Column("dateStart", "DATE", uniqueness=True),
    Column("dateEnd", "DATE", uniqueness=True),
    Column("accountId", "NUMBER", uniqueness=True),
    Column("campaignId", "NUMBER", uniqueness=True),
]

Config = namedtuple("Config", ["store", "account_id", "campaign_id"])

items = [
    Config("FL", 3124040317978028205, 3266180594207425112),
]

default_args = {
    "start_date": pendulum.datetime(2021, 8, 23, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "sla": timedelta(hours=2),
    "priority_weight": 15,
    "execution_timeout": timedelta(hours=1),
}


def from_ds(data_interval_start):
    last_ed = pendulum.instance(data_interval_start)
    from_date = last_ed.add(days=-7)
    return from_date.to_date_string()


def to_ds(data_interval_start):
    exec_date = pendulum.instance(data_interval_start)
    exec_date = exec_date.add(days=+1)
    return exec_date.to_date_string()


dag = DAG(
    dag_id=f"media_inbound_{schema}_{table}",
    default_args=default_args,
    schedule="15 21,5 * * *",
    catchup=False,
    max_active_runs=1,
    user_defined_macros={"from_ds": from_ds, "to_ds": to_ds},
    sla_miss_callback=slack_sla_miss_media,
)


def build_request_params(account_id, campaign_id):
    return {
        "dateStart": "{{ from_ds(data_interval_start) }}",
        "dateEnd": "{{ to_ds(data_interval_start) }}",
        "accountId": account_id,
        "campaignId": campaign_id,
    }


with dag:
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake",
        database="lake",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=0, skip_pct=3),
        trigger_rule="all_done",
    )
    for item in items:
        get_ads_data = RoktToS3Operator(
            rokt_conn_id=conn_ids.Rokt.default,
            task_id=f"rokt_{item.store}_{item.campaign_id}_tos3",
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{s3_prefix}/rokt_{item.store}_{item.campaign_id}_{date_param}.tsv.gz",
            column_list=[x.source_name for x in column_list],
            request_params=build_request_params(item.account_id, item.campaign_id),
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        )
        get_ads_data >> to_snowflake
