from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.storeforce import StoreforceToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import email_lists, owners, s3_buckets, stages, conn_ids

schema = "storeforce"
table = "export_kpis"
s3_prefix = f"lake/{schema}.{table}/daily_v1"

column_list = [
    Column(
        'name',
        "VARCHAR",
    ),
    Column(
        'description',
        "VARCHAR",
    ),
    Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
]


default_args = {
    "start_date": pendulum.datetime(2021, 8, 23, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
    "execution_timeout": timedelta(hours=1),
}


dag = DAG(
    dag_id=f"edm_inbound_storeforce_export_kpis",
    default_args=default_args,
    schedule="30 12 * * *",
    catchup=False,
    max_active_runs=1,
)


with dag:
    date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%z')[0:-3] }}"

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="storeforce_export_kpis_to_snowflake",
        database="lake",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=3),
    )
    get_data = StoreforceToS3Operator(
        task_id=f"storeforce_export_kpis_to_s3",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{s3_prefix}/storeforce_export_kpis_{date_param}.tsv.gz",
        column_list=[x.source_name for x in column_list],
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        endpoint='GetExportKPIs',
    )
    get_data >> to_snowflake
