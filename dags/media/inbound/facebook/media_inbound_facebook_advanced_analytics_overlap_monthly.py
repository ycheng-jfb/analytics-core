from dataclasses import dataclass
from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.facebook import FacebookAdvancedAnalyticsToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support

schema = "facebook"
table = "advanced_analytics_overlap"

dag_id = f"media_inbound_{schema}_{table}_monthly"
s3_prefix = f"media/{schema}.{table}/monthly_v2"

column_list = [
    Column("brand_name", "VARCHAR", source_name="_col0", uniqueness=True),
    Column("date_start", "DATE", uniqueness=True),
    Column("date_end", "DATE", uniqueness=True),
    Column("account1", "VARCHAR", source_name="Account1", uniqueness=True),
    Column("account2", "VARCHAR", source_name="Account2", uniqueness=True),
    Column("account3", "VARCHAR", source_name="Account3", uniqueness=True),
    Column("account4", "VARCHAR", source_name="Account4", uniqueness=True),
    Column("account5", "VARCHAR", source_name="Account5", uniqueness=True),
    Column("account6", "VARCHAR", source_name="Account6", uniqueness=True),
    Column("account7", "VARCHAR", source_name="Account7", uniqueness=True),
    Column("account8", "VARCHAR", source_name="Account8", uniqueness=True),
    Column("account9", "VARCHAR", source_name="Account9", uniqueness=True),
    Column("account10", "VARCHAR", source_name="Account10", uniqueness=True),
    Column("reach", "INT"),
    Column("total_impressions", "INT"),
    Column("total_spend_usd", "DECIMAL(38, 8)"),
    Column("api_call_timestamp", "TIMESTAMP_LTZ", delta_column=1),
]

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 10, 10, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "execution_timeout": timedelta(hours=2),
}


@dataclass
class Config:
    store: str
    date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"
    date_start: str = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
    date_end: str = "{{ macros.ds_add(data_interval_end.strftime('%Y-%m-%d'), -1) }}"

    @property
    def template_id(self):
        return 173142458808341

    @staticmethod
    def sql_cmd(store):
        return f"""
            with cte as (
                select store_brand_name,account_id, SUM(ZEROIFNULL(spend_usd)) as spend
                from reporting_media_prod.facebook.facebook_optimization_dataset_country
                where date between '{Config.date_start}' and '{Config.date_end}'
                group by store_brand_name,account_id
                )
            select distinct account_id
            from cte
            where lower(store_brand_name) = lower('{store}')
            QUALIFY ROW_NUMBER() OVER(PARTITION BY store_brand_name order by spend desc) <= 10;
        """

    @staticmethod
    def get_macros(store):
        return {
            "DATE_START": Config.date_start,
            "DATE_END": Config.date_end,
            "STRING_VALUE_STORE_BRAND_NAME": f"{store}",
        }


configs = [
    Config(store="Fabkids"),
    Config(store="Fabletics"),
    Config(store="Fabletics Men"),
    Config(store="JustFab"),
    Config(store="Savage X"),
    Config(store="Yitty"),
]

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule="0 10 1 * *",
    catchup=True,
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
        get_facebook = FacebookAdvancedAnalyticsToS3Operator(
            task_id=f'load_{config.template_id}_{config.store.replace(" ","_")}_to_s3',
            facebook_conn_id=conn_ids.Facebook.na,
            macros=config.get_macros(config.store),
            template_id=config.template_id,
            key=f"{s3_prefix}/aa_{config.store.replace(' ','_')}_{config.date_param}.tsv.gz",
            column_list=[x.source_name for x in column_list],
            bucket=s3_buckets.tsos_da_int_inbound,
            write_header=True,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            sql_cmd=config.sql_cmd(config.store),
        )

        get_facebook >> to_snowflake
