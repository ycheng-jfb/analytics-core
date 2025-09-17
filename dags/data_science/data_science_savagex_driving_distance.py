import pendulum
from airflow.models import DAG
from datetime import timedelta, datetime
from include.airflow.callbacks.slack import SlackFailureCallback
from include.airflow.operators.google_drive_time import DrivingTime
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

bucket_name = s3_buckets.tsos_da_int_inbound
file_key = "data_science/sxf_retail_driving_distance.csv.gz"
number_of_threads = 3
sql_query = """SELECT DISTINCT
                store_state,
                store_zip,
                store_state_zip,
                vip_state,
                vip_zip,
                vip_state_zip,
                distance,
                duration
                FROM
                (SELECT store_state,
                   store_zip,
                   store_state_zip,
                   customer_state AS vip_state,
                   customer_zip AS vip_zip,
                   customer_state_zip AS vip_state_zip,
                   miles AS distance,
                   9999 AS duration
             FROM reporting_base_prod.data_science.zip_50_miles_sxf_retail
             WHERE DAY(CURRENT_DATE) BETWEEN 1 AND 8
             UNION ALL
             (SELECT DISTINCT
                store_state,
                store_zip,
                store_state_zip,
                vip_state,
                vip_zip,
                vip_state_zip,
                distance,
                9999 AS duration
            FROM (
                SELECT
                    store_state,
                    store_zip,
                    store_state_zip,
                    vip_state,
                    vip_zip,
                    vip_state_zip,
                    distance
                FROM reporting_base_prod.data_science.sxf_retail_driving_distance
                WHERE duration = 9999
                UNION
                SELECT
                    store_state,
                    store_zip,
                    store_state_zip,
                    customer_state AS vip_state,
                    customer_zip AS vip_zip,
                    customer_state_zip AS vip_state_zip,
                    miles AS distance
                FROM reporting_base_prod.data_science.zip_50_miles_sxf_retail
                WHERE CONCAT(store_state_zip, vip_state_zip) NOT IN (
                    SELECT CONCAT(store_state_zip, vip_state_zip)
                    FROM reporting_base_prod.data_science.sxf_retail_driving_distance
                )
            ) AS combined_data
            WHERE DAY(CURRENT_DATE) NOT BETWEEN 1 AND 8));"""

default_args = {
    "start_date": pendulum.datetime(2020, 1, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.data_science,
    "email": "datascience@techstyle.com",
    "on_failure_callback": SlackFailureCallback("slack_alert_data_science"),
    "execution_timeout": timedelta(hours=2),
}

dag = DAG(
    dag_id="data_science_savagex_driving_distance",
    default_args=default_args,
    schedule="0 18 * * 4",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
)

column_list = [
    Column("store_state", "VARCHAR(50)", uniqueness=True),
    Column("store_zip", "VARCHAR(50)", uniqueness=True),
    Column("store_state_zip", "VARCHAR(50)", uniqueness=True),
    Column("vip_state", "VARCHAR(50)", uniqueness=True),
    Column("vip_zip", "VARCHAR(50)", uniqueness=True),
    Column("vip_state_zip", "VARCHAR(50)", uniqueness=True),
    Column("distance", "DOUBLE"),
    Column("duration", "DOUBLE"),
]


with dag:
    zip_50_miles_sxf_retail = SnowflakeProcedureOperator(
        procedure="data_science.zip_50_miles_sxf_retail.sql",
        database="reporting_base_prod",
    )

    sxf_google_map_api_driving_time = DrivingTime(
        task_id="google_map_api_driving_time_sxf",
        number_of_threads=number_of_threads,
        file_key=file_key,
        bucket_name=bucket_name,
        sql_query=sql_query,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )

    update_file_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="snowflake_load_sxf_retail_driving_distance",
        files_path=f"{stages.tsos_da_int_inbound}/{file_key}",
        database="reporting_base_prod",
        schema="data_science",
        table="sxf_retail_driving_distance",
        column_list=column_list,
        copy_config=CopyConfigCsv(field_delimiter=",", header_rows=1, skip_pct=1),
    )

    (
        zip_50_miles_sxf_retail
        >> sxf_google_map_api_driving_time
        >> update_file_to_snowflake
    )
