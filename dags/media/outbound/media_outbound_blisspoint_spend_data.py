import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.snowflake_export import SnowflakeToS3Operator
from include.config import conn_ids, owners, s3_buckets
from include.config.email_lists import airflow_media_support

table = "spend_data"
schema = "blisspoint"
table_name = f"{schema}.{table}"
data_interval_start = "{{ ts_nodash }}"

sql_cmd = """
    SELECT
        CAST(fmc.media_cost_date AS DATE) AS date,
        s.store_brand ||' ' || s.store_country AS store,
        fmc.channel AS channel,
        fmc.subchannel AS subchannel,
        fmc.vendor AS vendor,
        fmc.targeting,
        fmc.spend_iso_currency_code AS currency,
        fmc.cost AS spend,
        fmc.impressions,
        fmc.clicks,
        fmc.spend_date_usd_conv_rate AS to_usd_curr_rate,
        fmc.cost * fmc.spend_date_usd_conv_rate AS spend_usd,
        fmc.is_mens_flag
    FROM reporting_media_prod.dbo.vw_fact_media_cost fmc
    JOIN edw_prod.data_model.dim_store s ON s.store_id = fmc.store_id
      AND fmc.media_cost_date BETWEEN DATEADD(D,-45,CURRENT_DATE) AND CURRENT_DATE
"""

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 4, 3, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}

dag = DAG(
    dag_id=f"media_outbound_{schema}_{table}",
    default_args=default_args,
    schedule="0 15 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

with dag:
    spend_data = SnowflakeToS3Operator(
        task_id=f"export_{table_name}",
        sql_or_path=sql_cmd,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        bucket=s3_buckets.tsos_da_int_vendor,
        key=f"outbound/svc_blisspoint/spend/{table_name}_{data_interval_start}.txt.gz",
        field_delimiter="|",
        compression="gzip",
        header=True,
    )
