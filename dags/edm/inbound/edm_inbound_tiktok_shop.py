import pendulum
from airflow.models import DAG
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import CopyConfigCsv, SnowflakeIncrementalLoadOperator
from include.airflow.operators.tiktok_shop import (
    TiktokShopEndpointToS3Operator,
    TiktokShopInventoryToS3Operator,
    TiktokShopProductToS3Operator,
)
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from task_configs.dag_config.tiktok_shop_config import (
    inventory_column_list,
    product_column_list,
    tiktok_shop_config_list,
    tiktok_shop_connection_list,
)

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_tiktok_shop",
    default_args=default_args,
    schedule="30 1 * * *",
    catchup=False,
    max_active_runs=1,
)

with dag:
    date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"

    inventory_s3_prefix = "lake/tiktok_shop/inventory"

    inventory_to_s3_tasks = []
    for shop in tiktok_shop_connection_list:
        inventory_to_s3 = TiktokShopInventoryToS3Operator(
            task_id=f"{shop.name}_tiktok_shop_inventory_to_s3",
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{inventory_s3_prefix}/{shop.name}/inventory_{date_param}.gz",
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            column_list=[col.name for col in inventory_column_list],
            tiktok_shop_conn_id=shop.conn_id,
        )
        inventory_to_s3_tasks.append(inventory_to_s3)

    inventory_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id='tiktok_shop_inventory_to_snowflake',
        database="lake",
        staging_database="lake_stg",
        schema="tiktok_shop",
        table="inventory",
        column_list=inventory_column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{inventory_s3_prefix}/",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=3),
        initial_load=True,
    )

    transform_inventory = SnowflakeProcedureOperator(
        database='lake',
        procedure='tiktok_shop.inventory_report.sql',
        watermark_tables=['lake.tiktok_shop.inventory'],
    )

    inventory_to_s3_tasks >> inventory_to_snowflake >> transform_inventory

    product_s3_prefix = "lake/tiktok_shop/product"
    product_to_s3_tasks = []

    for shop in tiktok_shop_connection_list:
        product_to_s3 = TiktokShopProductToS3Operator(
            task_id=f"{shop.name}_tiktok_shop_product_to_s3",
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{product_s3_prefix}/{shop.name}/product_{date_param}.gz",
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            column_list=[col.name for col in product_column_list],
            namespace="tiktok_shop",
            process_name=f"{shop.name}_product",
            initial_load_value="None",
            tiktok_shop_conn_id=shop.conn_id,
        )
        product_to_s3_tasks.append(product_to_s3)

    product_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id='tiktok_shop_product_to_snowflake',
        database="lake",
        staging_database="lake_stg",
        schema="tiktok_shop",
        table="product",
        column_list=product_column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{product_s3_prefix}/",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=3),
        initial_load=True,
    )

    transform_product = SnowflakeProcedureOperator(
        database='lake',
        procedure='tiktok_shop.product_report.sql',
        watermark_tables=['lake.tiktok_shop.product'],
    )

    product_to_s3_tasks >> product_to_snowflake >> transform_product

    for cfg in tiktok_shop_config_list:

        s3_prefix = f"lake/tiktok_shop/{cfg.table}"
        to_s3_tasks = []

        for shop in tiktok_shop_connection_list:

            to_s3 = TiktokShopEndpointToS3Operator(
                task_id=f"{shop.name}_tiktok_shop_{cfg.table}_to_s3",
                bucket=s3_buckets.tsos_da_int_inbound,
                key=f"{s3_prefix}/{shop.name}/{cfg.table}_{date_param}.gz",
                s3_conn_id=conn_ids.S3.tsos_da_int_prod,
                column_list=[col.name for col in cfg.column_list],
                namespace=cfg.schema,
                process_name=f"{shop.name}_{cfg.table}",
                initial_load_value="None",
                tiktok_shop_conn_id=shop.conn_id,
                endpoint=cfg.endpoint,
                response_obj=cfg.response_obj,
            )
            to_s3_tasks.append(to_s3)

        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f'tiktok_shop_{cfg.table}_load_to_snowflake',
            database=cfg.database,
            staging_database=cfg.staging_database,
            schema=cfg.schema,
            table=cfg.table,
            column_list=cfg.column_list,
            files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
            copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=3),
            initial_load=True,
        )

        transform = SnowflakeProcedureOperator(
            database='lake',
            procedure=cfg.transform_procedure,
            watermark_tables=[f"{cfg.database}.{cfg.schema}.{cfg.table}"],
        )

        if cfg.table == "orders":
            transform_order_line = SnowflakeProcedureOperator(
                database='lake',
                procedure="tiktok_shop.order_line.sql",
                watermark_tables=[f"{cfg.database}.{cfg.schema}.{cfg.table}"],
            )

            to_s3_tasks >> to_snowflake >> [transform, transform_order_line]

        else:
            to_s3_tasks >> to_snowflake >> transform
