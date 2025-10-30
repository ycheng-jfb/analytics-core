import pendulum

from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.amazon import AmazonRefundEventListToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import email_lists, owners, s3_buckets, stages, conn_ids


column_list = [
    Column("amazon_order_id", "VARCHAR", source_name="AmazonOrderId", uniqueness=True),
    Column("seller_order_id", "VARCHAR", source_name="SellerOrderId"),
    Column("market_place_name", "VARCHAR", source_name="MarketplaceName"),
    Column("posted_date", "TIMESTAMP_NTZ", source_name="PostedDate"),
    Column("shipment_item_adjustment_list", "VARIANT", source_name="ShipmentItemAdjustmentList"),
    Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
]

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}


dag = DAG(
    dag_id="edm_inbound_amazon_selling_partner_refund_events",
    default_args=default_args,
    schedule="30 1 * * *",
    catchup=False,
    max_active_runs=1,
)

with dag:
    date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"
    s3_prefix = "lake/amazon_selling_partner_refund_events"
    to_s3 = AmazonRefundEventListToS3Operator(
        task_id="refund_events_to_s3",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{s3_prefix}/refund_events_{date_param}.gz",
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        column_list=[col.source_name for col in column_list],
        namespace='amazon',
        process_name="selling_partner_refund_events",
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id='load_to_snowflake',
        database="lake",
        schema="amazon",
        table="selling_partner_refund_events",
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=3),
    )

    to_s3 >> to_snowflake
