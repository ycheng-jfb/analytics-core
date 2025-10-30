import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.snowflake_export import SnowflakeToS3Operator
from include.config import conn_ids, owners, s3_buckets
from include.config.email_lists import airflow_media_support


data_interval_start = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
data_interval_end = '{{ (data_interval_end - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'

start_date_nodash = "{{ data_interval_start.strftime('%Y%m%d') }}"
end_date_nodash = '{{ (data_interval_end - macros.timedelta(days=1)).strftime("%Y%m%d") }}'

sql_cmd = f"""
    SELECT o.ORDER_PLACED_LOCAL_DATETIME AS order_date
        ,o.ORDER_ID
        ,o.CUSTOMER_ID
        ,o.PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_LOCAL_AMOUNT AS order_revenue
        ,NULL AS promo_code_used
        ,c.FIRST_NAME
        ,c.LAST_NAME
        ,a1.STREET_ADDRESS_1 AS shipping_address_1
        ,NULLIF(a1.STREET_ADDRESS_2, 'Unknown') AS shipping_address_2
        ,a1.CITY AS shipping_city
        ,a1.STATE AS shipping_state
        ,a1.ZIP_CODE AS shipping_zip
        ,a2.STREET_ADDRESS_1 AS billing_address_1
        ,NULLIF(a2.STREET_ADDRESS_2, 'Unknown') AS billing_address_2
        ,a2.CITY AS billing_city
        ,a2.STATE AS billing_state
        ,a2.ZIP_CODE AS billing_zip
        ,CASE WHEN o.ORDER_MEMBERSHIP_CLASSIFICATION_KEY = 2 THEN TRUE ELSE FALSE END AS first_time_customer_flag
    FROM EDW_PROD.DATA_MODEL_FL.FACT_ORDER o
    INNER JOIN EDW_PROD.DATA_MODEL_FL.DIM_ORDER_SALES_CHANNEL os ON o.ORDER_SALES_CHANNEL_KEY = os.ORDER_SALES_CHANNEL_KEY
    INNER JOIN EDW_PROD.DATA_MODEL_FL.DIM_STORE s ON o.STORE_ID = s.STORE_ID
    INNER JOIN EDW_PROD.DATA_MODEL_FL.DIM_ORDER_STATUS dos ON o.ORDER_STATUS_KEY = dos.ORDER_STATUS_KEY
    INNER JOIN EDW_PROD.DATA_MODEL_FL.DIM_CUSTOMER c ON o.CUSTOMER_ID = c.CUSTOMER_ID
    INNER JOIN EDW_PROD.DATA_MODEL_FL.DIM_ADDRESS a1 ON o.SHIPPING_ADDRESS_ID = a1.ADDRESS_ID
    INNER JOIN EDW_PROD.DATA_MODEL_FL.DIM_ADDRESS a2 ON o.BILLING_ADDRESS_ID = a2.ADDRESS_ID
    WHERE os.ORDER_CLASSIFICATION_L1 = 'Product Order' AND s.STORE_BRAND_ABBR = 'FL' AND s.STORE_COUNTRY = 'US'
        AND dos.ORDER_STATUS = 'Success'
        and order_date between '{data_interval_start}' and '{data_interval_end}';
"""

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 10, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}

dag = DAG(
    dag_id=f"media_outbound_slm_orders",
    default_args=default_args,
    schedule="0 8 15 * *",
    catchup=True,
    max_active_tasks=1,
    max_active_runs=1,
)

with dag:
    spend_data = SnowflakeToS3Operator(
        task_id=f"export_to_slm",
        sql_or_path=sql_cmd,
        s3_conn_id='aws_s3_slm_export',
        bucket=s3_buckets.tsos_da_slm_outbound,
        key=f"orders/slm_flus_orders_{start_date_nodash}_{end_date_nodash}.csv.gz",
        field_delimiter=",",
        compression="gzip",
        header=True,
    )
