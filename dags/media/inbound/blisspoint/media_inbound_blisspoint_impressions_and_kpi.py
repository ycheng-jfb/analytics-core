from dataclasses import dataclass

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import email_lists, owners, stages

between_column_list = [
    Column("date", "DATE", uniqueness=True),
    Column("brand", "VARCHAR", uniqueness=True),
    Column("country", "VARCHAR", uniqueness=True),
    Column("isci", "VARCHAR", uniqueness=True),
    Column("creative_name", "VARCHAR", uniqueness=True),
    Column("parent_creative_name", "VARCHAR", uniqueness=True),
    Column("network", "VARCHAR", uniqueness=True),
    Column(
        "lead_count_1_day_view",
        "VARCHAR",
    ),
    Column(
        "lead_count_3_day_view",
        "VARCHAR",
    ),
    Column(
        "lead_count_7_day_view",
        "VARCHAR",
    ),
    Column(
        "vip_count_1_day_view",
        "VARCHAR",
    ),
    Column(
        "vip_count_3_day_view",
        "VARCHAR",
    ),
    Column(
        "vip_count_7_day_view",
        "VARCHAR",
    ),
    Column(
        "lead_count_1_day_view_halo",
        "VARCHAR",
    ),
    Column(
        "lead_count_3_day_view_halo",
        "VARCHAR",
    ),
    Column(
        "lead_count_7_day_view_halo",
        "VARCHAR",
    ),
    Column(
        "vip_count_1_day_view_halo",
        "VARCHAR",
    ),
    Column(
        "vip_count_3_day_view_halo",
        "VARCHAR",
    ),
    Column(
        "vip_count_7_day_view_halo",
        "VARCHAR",
    ),
    Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
]


@dataclass
class Blisspoint:
    table: str
    column_list: list
    relative_path: str
    date_col: str

    @property
    def files_path(self):
        return f"{stages.tsos_da_int_vendor}/inbound/svc_blisspoint_prod_s3/{self.relative_path}"

    @property
    def pre_merge_cmd(self):
        return f"""
                    DELETE FROM lake.blisspoint.{self.table}
                    WHERE to_date({self.date_col}) >= (
                        SELECT min(to_date({self.date_col}))
                        FROM lake.blisspoint.{self.table}_stg
                            );
                """


items = [
    Blisspoint(
        table="kpi_between",
        column_list=between_column_list,
        relative_path="blisspoint_reporting_kpi_between_v2",
        date_col="date",
    ),
    Blisspoint(
        table="impressions_between",
        column_list=between_column_list,
        relative_path="blisspoint_reporting_impressions_between_v2",
        date_col="date",
    ),
]

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 6, 10, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": email_lists.airflow_media_support,
    "on_failure_callback": slack_failure_media,
}


dag = DAG(
    dag_id="media_inbound_blisspoint_impressions_and_kpi",
    default_args=default_args,
    schedule="0 17 * * *",
    catchup=False,
    max_active_tasks=50,
    max_active_runs=1,
)

with dag:
    for item in items:
        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f"load_{item.table}_to_snowflake",
            database="lake",
            schema="blisspoint",
            table=item.table,
            column_list=item.column_list,
            files_path=item.files_path,
            copy_config=CopyConfigCsv(field_delimiter=",", header_rows=1, skip_pct=1),
            pre_merge_command=item.pre_merge_cmd,
        )
