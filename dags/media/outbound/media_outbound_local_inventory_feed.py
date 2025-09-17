import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator
from include.config import owners
from include.config.conn_ids import SFTP
from include.config.email_lists import airflow_media_support


default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 9, 16, tz="America/Los_Angeles"),
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}


dag = DAG(
    dag_id="media_outbound_local_inventory_feed",
    default_args=default_args,
    schedule="30 8 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

sql = """
            SELECT DISTINCT ds.store_retail_location_code AS store_code,
                inv.sku AS id,
                IFF(onhand_quantity = 0, 'out of stock', 'in stock') AS availability,
                onhand_quantity AS quantity
            FROM edw_prod.data_model_fl.fact_inventory inv
            JOIN edw_prod.data_model_fl.dim_warehouse dw ON dw.warehouse_id = inv.warehouse_id
            JOIN edw_prod.reference.store_warehouse sw ON sw.warehouse_id = inv.warehouse_id
            JOIN edw_prod.data_model_fl.dim_store ds ON ds.store_id = sw.store_id
            JOIN reporting_base_prod.fabletics.ubt_max_showroom ubt ON ubt.sku = inv.product_sku
            WHERE inv.is_retail
                AND LOWER(sw.store_type) = 'retail'
                AND LOWER(ds.store_sub_type) = 'store'
                AND LOWER(ds.store_retail_status) = 'open - no issues'
                AND LOWER(ds.store_division) = 'fabletics'
                AND LOWER(ds.store_country) = 'us'
                AND onhand_quantity > 0;
"""
with dag:
    post_to_sftp = SnowflakeToSFTPOperator(
        task_id=f"fl_local_inventory_feed",
        sql_or_path=sql,
        sftp_conn_id=SFTP.sftp_fl_local_feed,
        filename="fl_local_inventory_data.csv",
        sftp_dir="/",
        field_delimiter=",",
        header=True,
    )
