from pathlib import Path

import pendulum
from airflow.models import DAG

from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_media
from include.config import owners, conn_ids, s3_buckets
from include.config.email_lists import airflow_media_support
from include.airflow.operators.snowflake_export import SnowflakeToS3Operator

table = "acquisition_data"
schema = "blisspoint"
table_name = f"{schema}.{table}"
date_param = "{{ ts_nodash }}"
dag_id = f"media_outbound_{schema}_{table}"
from_date = "{{ ( prev_data_interval_start_success or macros.datetime.utcnow() ) + macros.timedelta(days=-14) }}"

sql_parameters = {"from_date": from_date}

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 4, 3, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule="0 13 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

with dag:
    filename = "blisspoint.acquisition_data.sql"

    acquisition_data = SnowflakeToS3Operator(
        task_id=f"export_{table_name}",
        sql_or_path=Path(SQL_DIR, "lake", "procedures", filename),
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        bucket=s3_buckets.tsos_da_int_vendor,
        key=f"outbound/svc_blisspoint/acquisition/{table_name}_{date_param}.txt.gz",
        parameters=sql_parameters,
        header=True,
        field_delimiter="|",
        compression="gzip",
    )
