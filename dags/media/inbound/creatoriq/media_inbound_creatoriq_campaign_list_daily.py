import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.creatoriq import CreatorIQToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support
from include.utils.snowflake import CopyConfigCsv

TABLE = "campaign_list"
SCHEMA = "creatoriq"
S3_PREFIX = "media/creatoriq/campaign/campaign_list/v2"
date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"

column_list = [
    Column("CampaignId", "INT", uniqueness=True),
    Column("CampaignName", "VARCHAR"),
    Column("CampaignStatus", "VARCHAR"),
    Column("AdvertiserId", "INT"),
    Column("DateTimeCreated", "DATE"),
    Column("StartDate", "DATE"),
    Column("EndDate", "DATE"),
    Column("HashTag", "VARCHAR"),
    Column("AccountManagerName", "VARCHAR"),
    Column("LastModified", "TIMESTAMP_LTZ", delta_column=True),
]

DEFAULT_ARGS = {
    "start_date": pendulum.datetime(2020, 11, 13, 0, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}

dag = DAG(
    dag_id=f"media_inbound_{SCHEMA}_{TABLE}_daily",
    default_args=DEFAULT_ARGS,
    schedule="0 8 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)


class CreatorIQPullCampaignListToS3Operator(CreatorIQToS3Operator):
    """
    This operator expends CreatorIQToS3Operator to override get_rows method
    This operator pulls all the campaigns.
    """

    def get_rows(self):
        params = {"size": 1000, "page": 1}
        while True:
            response = self.hook.make_request(
                url="https://api.creatoriq.com/api/campaigns", params=params
            )
            data = response.json()
            for row in data["CampaignCollection"]:
                yield row["Campaign"]
            if data["total"] >= params["page"] * params["size"]:
                print(
                    f"Pulled {params['page'] * params['size']} records of {data['total']}"
                )
                params["page"] += 1
            else:
                break


with dag:
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="load_to_snowflake",
        database="LAKE",
        schema=SCHEMA,
        table=TABLE,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{S3_PREFIX}",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=0, skip_pct=1),
        trigger_rule="all_done",
    )
    op = CreatorIQPullCampaignListToS3Operator(
        task_id="get_ciq_data",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{S3_PREFIX}/{SCHEMA}_{TABLE}_{date_param}.tsv.gz",
        column_list=[x.name for x in column_list],
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )
    op >> to_snowflake
