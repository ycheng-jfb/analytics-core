from dataclasses import dataclass
from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media, slack_sla_miss_media
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import SnowflakeCopyOperator
from include.airflow.operators.tatari import TatariToS3Operator
from include.config import owners, s3_buckets, snowflake_roles, stages
from include.config.email_lists import airflow_media_support
from include.config import conn_ids

schema = 'tatari'
database = 'lake'


@dataclass
class TatariAds:
    table: str
    prefix: str

    @property
    def dst_prefix(self):
        return f"media/{schema}/{self.table}"

    @property
    def sql_cmd(self):
        if self.table == "streaming_conversions":
            return f"""

COPY INTO {database}.{schema}.{self.table}_stg
FROM (
SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,split_part(split_part(metadata$filename, '/', -1),'_',1) as account_name,
to_date(substr(split_part(split_part(metadata$filename, '/', -1),'_',-1),1,10)) as filename_date,
to_date(substr(split_part(split_part(metadata$filename, '/', -1),'_',-1),12,10)) as end_date
FROM '{stages.tsos_da_int_inbound}/{self.dst_prefix}/{{}}/'
)
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%'
;
"""
        else:
            return f"""

COPY INTO {database}.{schema}.{self.table}_stg
FROM (
SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,split_part(split_part(metadata$filename, '/', -1),'_',1) as account_name,
to_date(substr(split_part(split_part(metadata$filename, '/', -1),'_',-1),1,10)) as filename_date,
to_date(substr(split_part(split_part(metadata$filename, '/', -1),'_',-1),12,10)) as end_date
FROM '{stages.tsos_da_int_inbound}/{self.dst_prefix}/{{}}/'
)
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%'
;
"""


items = [
    TatariAds(table="linear_spend_and_impressions", prefix="linear_spend_and_impressions"),
    TatariAds(table="streaming_spend_and_impression", prefix="streaming_spend_and_impression"),
    TatariAds(table="linear_conversions", prefix="spot_level_metrics"),
    TatariAds(table="streaming_conversions", prefix="streaming_metrics"),
]

default_args = {
    "start_date": pendulum.datetime(2021, 8, 23, 7, tz="America/Los_Angeles"),
    "retries": 0,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "sla": timedelta(hours=2),
    "priority_weight": 15,
    "execution_timeout": timedelta(hours=3),
}


dag = DAG(
    dag_id="media_inbound_tatari_ads",
    default_args=default_args,
    schedule="15 21,5,9 * * *",
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_media,
)

accounts = ['fabletics', 'savage_x', 'fab_kids', 'justfabcom', 'shoedazzle']


with dag:
    date_param = "{{ ds }}"
    merge = {
        'linear_spend_and_impressions': SnowflakeProcedureOperator(
            procedure='tatari.linear_spend_and_impressions.sql',
            database=database,
            autocommit=False,
            warehouse='DA_WH_ETL_LIGHT',
        ),
        'streaming_spend_and_impression': SnowflakeProcedureOperator(
            procedure='tatari.streaming_spend_and_impression.sql',
            database=database,
            autocommit=False,
            warehouse='DA_WH_ETL_LIGHT',
        ),
        'linear_conversions': SnowflakeProcedureOperator(
            procedure='tatari.linear_conversions.sql',
            database=database,
            autocommit=False,
            warehouse='DA_WH_ETL_LIGHT',
        ),
        'streaming_conversions': SnowflakeProcedureOperator(
            procedure='tatari.streaming_conversions.sql',
            database=database,
            autocommit=False,
            warehouse='DA_WH_ETL_LIGHT',
        ),
    }

    for item in items:
        to_snowflake = SnowflakeCopyOperator(
            task_id=f'tatari_{item.table}_load',
            snowflake_conn_id=conn_ids.Snowflake.default,
            role=snowflake_roles.etl_service_account,
            sql_or_path=item.sql_cmd.format(date_param),
            database=database,
            schema=schema,
        )

        for account in accounts:
            if 'streaming' in item.prefix:
                src_path = f'{account}/{item.prefix}/v3'
            else:
                src_path = f'{account}/{item.prefix}'
            get_ads_data = TatariToS3Operator(
                task_id=f'tatari_{account}_{item.table}',
                slug=account,
                src_file_path=src_path,
                dst_file_path=f"{item.dst_prefix}/{date_param}",
                s3_bucket=s3_buckets.tsos_da_int_inbound,
                s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            )
            get_ads_data >> to_snowflake >> merge[item.table]
