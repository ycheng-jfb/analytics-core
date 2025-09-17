from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media, slack_sla_miss_media
from include.airflow.operators.commission_junction import CommissionJunctionToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support

schema = "cj"
table = "advertiser_spend"
date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"
s3_prefix = f"media/{schema}.{table}/daily_v6"

column_list = [
    Column("actionTrackerId", "NUMBER", uniqueness=True),
    Column("advertiserId", "NUMBER", uniqueness=True),
    Column("aid", "NUMBER"),
    Column("commissionId", "NUMBER", uniqueness=True),
    Column("orderId", "VARCHAR", uniqueness=True),
    Column("publisherId", "NUMBER", uniqueness=True),
    Column("postingDate", "TIMESTAMP_TZ(3)", uniqueness=True),
    Column("eventDate", "TIMESTAMP_TZ(3)"),
    Column("advCommissionAmountUsd", "DECIMAL(20,2)"),
    Column("pubCommissionAmountUsd", "DECIMAL(20,2)"),
    Column("cjFeeUsd", "DECIMAL(20,2)"),
    Column("actionType", "VARCHAR"),
    Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
    Column("custSegment", "VARCHAR"),
]
advertiser_ids = [5844330, 5889552, 5963319, 6323253]
# removed deactivated accounts from the advertiser_ids accounts
# advertiser_ids = [5844330, 5885341, 5886275, 5886276, 5886277, 5889552, 5963319, 6323253]

default_args = {
    "start_date": pendulum.datetime(2021, 8, 23, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "sla": timedelta(hours=2),
    "priority_weight": 15,
    "execution_timeout": timedelta(hours=3),
}


def from_ds(data_interval_start):
    exec_date = pendulum.instance(data_interval_start)
    exec_date = exec_date.add(days=-15).replace(tzinfo=None).format('YYYY-MM-DDTHH:MM:SS')
    return str(exec_date) + 'Z'


def to_ds(data_interval_start):
    exec_date = pendulum.instance(data_interval_start)
    exec_date = exec_date.add(days=+1).replace(tzinfo=None).format('YYYY-MM-DDTHH:MM:SS')
    return str(exec_date) + 'Z'


dag = DAG(
    dag_id=f"media_inbound_{schema}_{table}",
    default_args=default_args,
    schedule="15 21,5 * * *",
    catchup=False,
    max_active_runs=1,
    user_defined_macros={"from_ds": from_ds, "to_ds": to_ds},
    sla_miss_callback=slack_sla_miss_media,
)


def build_query(advertiser_id):
    lst = [x.source_name for x in column_list if x.source_name not in ["updated_at", "custSegment"]]

    return (
        f"{{ advertiserCommissions( forAdvertisers: [\"{advertiser_id}\"], "
        f"sincePostingDate:\"{{{{ from_ds(data_interval_start) }}}}\","
        f"beforePostingDate:\"{{{{ to_ds(data_interval_start) }}}}\") "
        f"{{ count payloadComplete records {{ {' '.join(lst)} verticalAttributes {{custSegment}} }} }} }}"
    )


with dag:
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake",
        database="lake",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=3),
        trigger_rule="all_done",
    )
    for advertiser_id in advertiser_ids:
        get_ads_data = CommissionJunctionToS3Operator(
            commissionjunction_conn_id=f"cj_{advertiser_id}",
            task_id=f"cj_{advertiser_id}_tos3",
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{s3_prefix}/cj_{advertiser_id}_{date_param}.tsv.gz",
            column_list=[x.source_name for x in column_list],
            request_params=build_query(advertiser_id),
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        )
        get_ads_data >> to_snowflake
