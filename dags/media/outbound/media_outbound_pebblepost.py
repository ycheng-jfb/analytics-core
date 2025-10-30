from dataclasses import dataclass

import pendulum
from airflow.models import DAG
from datetime import timedelta

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.snowflake import TableDependencyNtz
from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator
from include.config import owners, conn_ids
from include.config.email_lists import airflow_media_support

data_interval_start = "{{ ts_nodash }}"

customer = """
SET low_watermark = %(low_watermark)s :: TIMESTAMP_LTZ;

SELECT edw_prod.stg.udf_unconcat_brand(dc.customer_id) AS BRCID
    ,first_name AS first_name
    ,last_name AS LAST_NAME
    ,address AS ADDR1
    ,city as CITY
    ,STATE_PROVINCE AS REGION
    ,POSTAL_CODE AS POSTAL_CODE
    ,email
    ,IFF(lvm.cumulative_cash_gross_profit_decile = 10 and lvm.customer_id is not null,'High_LTV_Q3_2022','') as labels
FROM reporting_media_base_prod.dbo.vips_audience dc
LEFT JOIN (
    SELECT *
    FROM edw_prod.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY qualify row_number() OVER (
            PARTITION BY customer_id ORDER BY month_date DESC
            ) = 1
        ) lvm ON lvm.customer_id = dc.customer_id
WHERE dc.store_id = 52
    AND dc.gender = 'M'
    AND cancel_type IS NULL
    AND activation_local_datetime_utc > $LOW_WATERMARK;
"""

transaction = """
SET low_watermark = %(low_watermark)s;

SELECT '2744' AS brandid
    ,ORDER_LOCAL_DATETIME::date AS TransactionDate
    ,edw_prod.stg.udf_unconcat_brand(fo.customer_id) AS customerid
    ,first_name AS firstname
    ,last_name AS lastname
    ,address AS PrimaryAddress
    ,city
    ,STATE_PROVINCE AS STATE
    ,POSTAL_CODE AS zipcode
    ,email
    ,edw_prod.stg.udf_unconcat_brand(fo.order_id) AS orderid
    ,coalesce(((fo.SUBTOTAL_EXCL_TARIFF_LOCAL_AMOUNT * fo.ORDER_DATE_USD_CONVERSION_RATE) -
        (fo.PRODUCT_DISCOUNT_LOCAL_AMOUNT * fo.ORDER_DATE_USD_CONVERSION_RATE) +
        (fo.SHIPPING_REVENUE_LOCAL_AMOUNT * fo.ORDER_DATE_USD_CONVERSION_RATE)), 0
            )::number(38,2) AS ordervalue
    ,IFF(store_type = 'STORE_TYPE', 'retail', 'online') AS channel
    ,'VIP' AS ConversionType
FROM reporting_media_base_prod.dbo.vips_audience va
JOIN edw_prod.data_model.fact_membership_event fme ON va.customer_id = fme.customer_id
    and fme.membership_state='VIP'
JOIN edw_prod.data_model.fact_order fo on fme.MEMBERSHIP_EVENT_KEY=fo.MEMBERSHIP_EVENT_KEY
LEFT JOIN edw_prod.data_model.dim_store ds ON va.sub_store_id = ds.store_id
WHERE va.store_id = 52
    AND va.gender = 'M'
    AND cancel_type IS NULL
    AND va.activation_local_datetime_utc > $LOW_WATERMARK;
"""

blacklist = """
SET low_watermark = %(low_watermark)s;

SELECT first_name AS first_name
    ,last_name AS LAST_NAME
    ,email
    ,address AS ADDR1
    ,city as CITY
    ,STATE_PROVINCE AS REGION
    ,POSTAL_CODE AS POSTAL_CODE
FROM reporting_media_base_prod.dbo.vips_audience v
JOIN (SELECT customer_id,request_type,status
FROM reporting_media_prod.dbo.ccpa_gdpr_requests
qualify row_number() over (partition by customer_id order by date_requested desc) = 1) r
    ON v.customer_id = r.customer_id
WHERE r.request_type != 'Access'
    AND r.STATUS = 'Completed'
    AND v.store_id = 52
    AND v.gender = 'M'
    AND email NOT ilike '%%removed%%'
    AND activation_local_datetime_utc > $LOW_WATERMARK;
"""

graph_mail = """
SET low_watermark = %(low_watermark)s;

SELECT l.FIRST_NAME as first_name
    ,l.last_name
    ,l.phone
     ,l.email
    ,IFF(ds.store_retail_zip_code is not null,'leads_in_retail_zip','') as labels
FROM reporting_media_base_prod.dbo.leads_audience l
JOIN edw_prod.data_model.dim_customer c ON l.customer_id = c.customer_id
left join edw_prod.data_model.dim_store ds on c.quiz_zip = ds.store_retail_zip_code
left outer join reporting_media_base_prod.dbo.vips_audience v on l.customer_id = v.customer_id
WHERE l.store_id = 52
    AND l.gender = 'M'
    AND l.is_active_vip = 0
    AND v.cancel_type is NULL
    AND datediff(month, l.REGISTRATION_LOCAL_DATETIME, CURRENT_DATE ()) < 3
    AND l.registration_local_datetime_utc > $LOW_WATERMARK;
"""

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 4, 3, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    dag_id="media_outbound_pebblepost",
    default_args=default_args,
    schedule="0 15 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)


@dataclass
class ListInfo:
    listname: str
    sql: str
    filename: str
    watermark_table: str
    watermark_column: str
    delimiter: str


lists = [
    ListInfo(
        'customers',
        customer,
        f'2744_pebblepost_customer_{data_interval_start}.txt',
        'reporting_media_base_prod.dbo.vips_audience',
        'activation_local_datetime_utc',
        "|",
    ),
    ListInfo(
        'transactions',
        transaction,
        f'2744_Fabletics_transactions_{data_interval_start}.csv',
        'reporting_media_base_prod.dbo.vips_audience',
        'activation_local_datetime_utc',
        ",",
    ),
    ListInfo(
        'blacklists',
        blacklist,
        f'2744_pebblepost_blacklist_{data_interval_start}.txt',
        'reporting_media_base_prod.dbo.vips_audience',
        'activation_local_datetime_utc',
        ",",
    ),
    ListInfo(
        'graph_mail',
        graph_mail,
        f'2744_pebblepost_graphmail_{data_interval_start}.txt',
        'reporting_media_base_prod.dbo.leads_audience',
        'registration_local_datetime_utc',
        ",",
    ),
]

with dag:
    for list in lists:
        spend_data = SnowflakeToSFTPOperator(
            task_id=f"export_{list.listname}",
            sql_or_path=f"{list.sql}",
            sftp_conn_id=conn_ids.SFTP.sftp_pebblepost,
            filename=f"{list.filename}",
            sftp_dir=f"/incoming/{list.listname}",
            field_delimiter=f"{list.delimiter}",
            header=True,
            quoting=2,
            initial_load_value='2021-09-01',
            watermark_tables=[
                TableDependencyNtz(
                    table_name=list.watermark_table,
                    column_name=list.watermark_column,
                    timezone='UTC',
                ),
            ],
        )
