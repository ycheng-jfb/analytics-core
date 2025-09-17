from dataclasses import dataclass
from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media, slack_sla_miss_media
from include.airflow.operators.roku import RokuToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support


@dataclass
class RokuAds:
    table: str
    relative_path: str
    column_list: list
    date_param: str = str(
        "{{ macros.datetime.now().strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"
    )

    @property
    def files_path(self):
        return f"{stages.tsos_da_int_inbound}/media/roku/{self.relative_path}"

    @property
    def s3_prefix(self):
        return f"media/roku/v2/{self.relative_path}"

    @property
    def report_params(self):
        return {
            "reports": [
                {
                    "name": f"{self.table}_{self.date_param}",
                    "organization_uid": "0GOQy14TED",
                    "start_at": "{{ macros.ds_add(ds, -14) }}",
                    "end_at": "{{ macros.ds_add(ds, 1) }}",
                    "fields": [x.source_name for x in self.column_list],
                    "delivery_type": "s3",
                }
            ]
        }


items = [
    RokuAds(
        table="daily_spend",
        relative_path="daily_spend",
        column_list=[
            Column("date", "DATE", uniqueness=True),
            Column("agency_group", "VARCHAR"),
            Column("agency_group_uid", "VARCHAR"),
            Column("agency", "VARCHAR"),
            Column("agency_uid", "VARCHAR"),
            Column("advertiser", "VARCHAR"),
            Column("advertiser_uid", "VARCHAR"),
            Column("campaign", "VARCHAR"),
            Column("campaign_uid", "VARCHAR", uniqueness=True),
            Column("creative_name", "VARCHAR", uniqueness=True),
            Column("tactic", "VARCHAR"),
            Column("flight", "VARCHAR"),
            Column("flight_uid", "VARCHAR", uniqueness=True),
            Column("impressions", "INT"),
            Column("total_spend", "DECIMAL(20,2)"),
            Column("campaign_clicks", "INT"),
        ],
    ),
    RokuAds(
        table="daily_conversion",
        relative_path="daily_conversion",
        column_list=[
            Column("date", "DATE", uniqueness=True),
            Column("agency_group", "VARCHAR"),
            Column("agency_group_uid", "VARCHAR"),
            Column("agency", "VARCHAR"),
            Column("agency_uid", "VARCHAR"),
            Column("advertiser", "VARCHAR"),
            Column("advertiser_uid", "VARCHAR"),
            Column("campaign", "VARCHAR"),
            Column("campaign_uid", "VARCHAR", uniqueness=True),
            Column("creative_name", "VARCHAR", uniqueness=True),
            Column("tactic", "VARCHAR"),
            Column("flight", "VARCHAR"),
            Column("flight_uid", "VARCHAR", uniqueness=True),
            Column("activity_name", "VARCHAR"),
            Column("campaign_view_actions", "INT", delta_column=True),
            Column("campaign_click_actions", "DECIMAL(20,2)"),
            Column("campaign_total_actions", "INT"),
        ],
    ),
]


default_args = {
    "start_date": pendulum.datetime(2021, 12, 7, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "sla": timedelta(hours=2),
    "priority_weight": 15,
    "execution_timeout": timedelta(hours=3),
}

dag = DAG(
    dag_id="media_inbound_roku_ads_daily",
    default_args=default_args,
    schedule="15 5 * * *",
    catchup=False,
    max_active_tasks=7,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_media,
)

with dag:
    for item in items:
        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f"load_{item.table}_to_snowflake",
            database="lake",
            schema="roku",
            table=item.table,
            column_list=item.column_list,
            files_path=item.files_path,
            copy_config=CopyConfigCsv(field_delimiter=",", header_rows=1, skip_pct=1),
            trigger_rule="all_done",
        )
        get_ads_data = RokuToS3Operator(
            task_id=f"roku_{item.table}",
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{item.s3_prefix}/roku_{item.table}_{RokuAds.date_param}.csv",
            report_params=item.report_params,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        )
        get_ads_data >> to_snowflake
