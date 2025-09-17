import pendulum

from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.amazon import AmazonCatalogToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners, s3_buckets, stages, conn_ids


column_list = [
    Column("asin", "VARCHAR", uniqueness=True),
    Column('attributes', 'VARIANT'),
    Column('dimensions', 'VARIANT'),
    Column('identifiers', 'VARIANT'),
    Column('images', 'VARIANT'),
    Column('productTypes', 'VARIANT'),
    Column('relationships', 'VARIANT'),
    Column('salesRanks', 'VARIANT'),
    Column('summaries', 'VARIANT'),
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
    dag_id=f"edm_inbound_amazon_selling_partner_catalog",
    default_args=default_args,
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
)

"""
Below are the market_place_ids of the North America(NA) region.
For additional details, please visit the following link:
    `https://developer-docs.amazon.com/amazon-shipping/docs/marketplace-ids`
"""
na_market_place_ids = ["ATVPDKIKX0DER", "A2EUQ1WTGCTBG2", "A1AM78C64UM0Y8", "A2Q3Y263D00KWC"]

with dag:
    date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"
    s3_prefix = "lake/amazon_selling_partner_catalog"
    process_catalog_data = SnowflakeProcedureOperator(
        procedure='amazon.catalog.sql',
        database='lake',
        watermark_tables=["lake.amazon.amazon_catalog_items"],
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id=f"load_to_snowflake",
        database="lake",
        schema="amazon",
        table="amazon_catalog_items",
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=3),
    )
    to_s3 = AmazonCatalogToS3Operator(
        task_id=f"catalog_to_s3",
        market_place_ids=na_market_place_ids,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{s3_prefix}/catalog_items_{date_param}.gz",
        column_list=[col.source_name for col in column_list],
    )

    to_s3 >> to_snowflake >> process_catalog_data
