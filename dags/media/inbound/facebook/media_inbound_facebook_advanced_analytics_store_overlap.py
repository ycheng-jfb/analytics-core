from dataclasses import dataclass
from datetime import timedelta
import re

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.facebook import FacebookAdvancedAnalyticsToS3OperatorNew
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support

schema = "facebook"
table = "advanced_analytics_store_overlap"

dag_id = f"media_inbound_{schema}_{table}"
s3_prefix = f"media/{schema}.{table}/weekly_v2"

column_list = [
    Column("string_list_store_name", "VARCHAR", uniqueness=True),
    Column("date_start", "DATE", uniqueness=True),
    Column("date_end", "DATE", uniqueness=True),
    Column("fabletics", "BOOLEAN", uniqueness=True),
    Column("fableticsmen", "BOOLEAN", uniqueness=True),
    Column("fableticsscrubs", "BOOLEAN", uniqueness=True),
    Column("yitty", "BOOLEAN", uniqueness=True),
    Column("savagex", "BOOLEAN", uniqueness=True),
    Column("justfab", "BOOLEAN", uniqueness=True),
    Column("shoedazzle", "BOOLEAN", uniqueness=True),
    Column("fabkids", "BOOLEAN", uniqueness=True),
    Column("spend", "DECIMAL(38, 8)"),
    Column("reach", "INT"),
    Column("impressions", "INT"),
    Column("api_call_timestamp", "TIMESTAMP_LTZ", delta_column=1),
]

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 4, 1, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "execution_timeout": timedelta(hours=2),
}


@dataclass
class Config:
    stores: str
    date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"
    date_start: str = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
    date_end: str = "{{ macros.ds_add(data_interval_end.strftime('%Y-%m-%d'), -1) }}"

    @property
    def template_id(self):
        return 1136486037417122

    @property
    def store_group(self):
        return f'{config.stores.replace(",", "_").replace(" ", "")}'.replace("'", "")

    @staticmethod
    def sql_cmd(stores):
        return f"""
            select distinct lower(store_brand_name) as store_brand_name,
                case when lower(store_brand_name) = 'fabletics' then 'LISTID_ACCOUNT_IDS_1'
                     when lower(store_brand_name) = 'fabletics men' then 'LISTID_ACCOUNT_IDS_2'
                     when lower(store_brand_name) = 'fabletics scrubs' then 'LISTID_ACCOUNT_IDS_3'
                     when lower(store_brand_name) = 'yitty' then 'LISTID_ACCOUNT_IDS_4'
                     when lower(store_brand_name) = 'savage x' then 'LISTID_ACCOUNT_IDS_5'
                     when lower(store_brand_name) = 'justfab' then 'LISTID_ACCOUNT_IDS_6'
                     when lower(store_brand_name) = 'shoedazzle' then 'LISTID_ACCOUNT_IDS_7'
                     when lower(store_brand_name) = 'fabkids' then 'LISTID_ACCOUNT_IDS_8'
                     else null end as listid
                ,account_id
            from reporting_media_prod.facebook.facebook_optimization_dataset_country
            where date between '{Config.date_start}' and '{Config.date_end}'
                and lower(store_brand_name) in ({stores})
                and region = 'NA'  and account_name not ilike '%organic%'
            order by 1,2;
        """

    @staticmethod
    def get_macros(stores):
        return {
            "DATE_START": Config.date_start,
            "DATE_END": Config.date_end,
            "STRING_LIST_STORE_BRAND_NAME": f"{stores}",
        }


configs = [
    Config(stores="'justfab','shoedazzle'"),
    Config(stores="'fabletics','fabletics men','fabletics scrubs','yitty'"),
]

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule="0 11 * * 1",
    catchup=False,
    max_active_tasks=10,
    max_active_runs=1,
)

with dag:
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="load_to_snowflake",
        database="lake",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=1, skip_pct=1),
        trigger_rule="all_done",
    )
    for config in configs:
        get_facebook = FacebookAdvancedAnalyticsToS3OperatorNew(
            task_id=f"load_{config.template_id}_{config.store_group}_to_s3",
            facebook_conn_id=conn_ids.Facebook.na,
            macros=config.get_macros(config.stores),
            template_id=config.template_id,
            key=f"{s3_prefix}/aa_{config.store_group}_{config.date_param}.tsv.gz",
            column_list=[x.source_name for x in column_list],
            bucket=s3_buckets.tsos_da_int_inbound,
            write_header=True,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            sql_cmd=config.sql_cmd(config.stores),
        )

        get_facebook >> to_snowflake
