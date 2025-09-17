import pendulum
from airflow.models import DAG

from include.airflow.operators.salesfloor import SalesfloorExportToSFTP
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator
from include.config import conn_ids, owners
from include.config.email_lists import data_integration_support

SFTP_CONFIG_MAP: dict[str, dict[str, str]] = {
    "dev": dict(
        sftp_conn_id=conn_ids.SFTP.sftp_salesfloor_qa,
        base_path="/fabletics-dev/inbound/stg",
        customer_feed_path="ci-customers",
        product_feed_path="products",
        transaction_feed_path="ci-transactions",
        statistics_feed_path="ci-stats",
    ),
    "prd": dict(
        sftp_conn_id=conn_ids.SFTP.sftp_salesfloor,
        base_path="/fabletics-prd/inbound/prod",
        customer_feed_path="ci-customers",
        product_feed_path="products",
        transaction_feed_path="ci-transactions",
        statistics_feed_path="ci-stats",
    ),
}
SFTP_ENV = "prd"
SFTP_CONFIG = SFTP_CONFIG_MAP[SFTP_ENV]

sftp_conn_id = SFTP_CONFIG["sftp_conn_id"]
sftp_path = SFTP_CONFIG["base_path"]

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2022, 12, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": data_integration_support,
}

dag = DAG(
    dag_id="edm_outbound_sftp_salesfloor_feed",
    default_args=default_args,
    schedule="0 5 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

with dag:
    timestamp = "{{ ts_nodash }}"

    customer_feed_snowflake = SnowflakeProcedureOperator(
        procedure="salesfloor.customer.sql", database="reporting_prod"
    )

    customer_feed_sftp = SalesfloorExportToSFTP(
        task_id="salesfloor_customer_feed",
        sql_or_path="""select edw_prod.stg.udf_unconcat_brand(customer_id) as customer_id,
            * exclude (customer_id) from reporting_prod.salesfloor.customer_feed where meta_update_datetime =
            (select max(meta_update_datetime) from reporting_prod.salesfloor.customer_feed)
            order by status asc;""",
        sftp_conn_id=sftp_conn_id,
        sftp_dir=f"{sftp_path}/{SFTP_CONFIG['customer_feed_path']}",
        filename="fabletics-customers.csv",
        timestamp=timestamp,
        field_delimiter=",",
        batch_size=5_000_000,
        header=True,
    )
    product_feed_snowflake = SnowflakeProcedureOperator(
        procedure="salesfloor.product.sql", database="reporting_prod"
    )
    product_feed_sftp = SnowflakeToSFTPOperator(
        task_id="salesfloor_product_feed",
        sql_or_path="select * from reporting_prod.salesfloor.product_feed",
        sftp_conn_id=sftp_conn_id,
        sftp_dir=f"{sftp_path}/{SFTP_CONFIG['product_feed_path']}",
        filename="fabletics-products-{{ ts_nodash.replace('T', '-') }}.csv",
        field_delimiter=",",
        header=True,
    )
    product_feed_snowflake >> product_feed_sftp
    transaction_feed_snowflake = SnowflakeProcedureOperator(
        procedure="salesfloor.transaction.sql", database="reporting_prod"
    )
    transaction_feed_sftp = SalesfloorExportToSFTP(
        task_id="salesfloor_transaction_feed",
        sql_or_path="""select
                        edw_prod.stg.udf_unconcat_brand(PARENT_ID) as PARENT_ID,
                        edw_prod.stg.udf_unconcat_brand(ID) as ID,
                        TYPE,
                        DATE,
                        edw_prod.stg.udf_unconcat_brand(CUSTOMER_ID) as CUSTOMER_ID,
                        STORE_ID,
                        FULFILLMENT,
                        CURRENCY,
                        SKU,
                        PRODUCT_ATTRIBUTE_1,
                        PRODUCT_ATTRIBUTE_2,
                        UNIT_PRICE,
                        UNITS,
                        EMPLOYEE_ID,
                        META_ROW_HASH,
                        META_CREATE_DATETIME,
                        META_UPDATE_DATETIME
                        from reporting_prod.salesfloor.transaction_feed
                        where meta_update_datetime =
                        (select max(meta_update_datetime) from reporting_prod.salesfloor.transaction_feed)""",
        sftp_conn_id=sftp_conn_id,
        sftp_dir=f"{sftp_path}/{SFTP_CONFIG['transaction_feed_path']}",
        filename="fabletics-transactions.csv",
        timestamp=timestamp,
        field_delimiter=",",
        batch_size=4_000_000,
        header=True,
    )
    transaction_feed_snowflake >> transaction_feed_sftp

    statistics_feed_snowflake = SnowflakeProcedureOperator(
        procedure="salesfloor.statistics.sql", database="reporting_prod"
    )
    statistics_feed_sftp = SalesfloorExportToSFTP(
        task_id="salesfloor_statistics_feed",
        sql_or_path="select edw_prod.stg.udf_unconcat_brand(customer_id) as customer_id, "
        "* exclude (customer_id) from reporting_prod.salesfloor.statistics_feed "
        " where meta_update_datetime = (select max(meta_update_datetime) "
        " from reporting_prod.salesfloor.statistics_feed) ",
        sftp_conn_id=sftp_conn_id,
        sftp_dir=f"{sftp_path}/{SFTP_CONFIG['statistics_feed_path']}",
        filename="fabletics-statistics.csv",
        timestamp=timestamp,
        field_delimiter=",",
        batch_size=5_000_000,
        header=True,
    )
    statistics_feed_snowflake >> statistics_feed_sftp
    (
        customer_feed_snowflake
        >> customer_feed_sftp
        >> [
            statistics_feed_snowflake,
            product_feed_snowflake,
            transaction_feed_snowflake,
        ]
    )
