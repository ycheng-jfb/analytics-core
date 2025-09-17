import pendulum
from airflow.models import DAG
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.omnishop import OmnishopInventoryS3ObjectToS3
from include.airflow.operators.snowflake_load import SnowflakeTruncateAndLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

inventory_column_list = [
    Column("epc", "VARCHAR", source_name="epc", uniqueness=True),
    Column("lpn_code", "VARCHAR", source_name="lpnCode"),
    Column("store_id", "VARCHAR", source_name="storeId", uniqueness=True),
    Column(
        "interpretted_instore_location",
        "VARCHAR",
        source_name="interprettedInStoreLocation",
    ),
    Column("gstore_location_for_lpn", "VARCHAR", source_name="gStoreLocationForLpn"),
]

store_file_config = {
    "107": "inventory-snapshot/107.csv",
    "235": "inventory-snapshot/235.csv",
    "104": "inventory-snapshot/104.csv",
    "115": "inventory-snapshot/115.csv",
    "257": "inventory-snapshot/257.csv",
    "280": "inventory-snapshot/280.csv",
    "290": "inventory-snapshot/290.csv",
    "103": "inventory-snapshot/103.csv",
    "106": "inventory-snapshot/106.csv",
    "118": "inventory-snapshot/118.csv",
    "174": "inventory-snapshot/174.csv",
    "175": "inventory-snapshot/175.csv",
    "224": "inventory-snapshot/224.csv",
    "247": "inventory-snapshot/247.csv",
    "253": "inventory-snapshot/253.csv",
    "240": "inventory-snapshot/240.csv",
    "111": "inventory-snapshot/111.csv",
    "109": "inventory-snapshot/109.csv",
    "208": "inventory-snapshot/208.csv",
    "217": "inventory-snapshot/217.csv",
    "239": "inventory-snapshot/239.csv",
    "226": "inventory-snapshot/226.csv",
    "252": "inventory-snapshot/252.csv",
    "256": "inventory-snapshot/256.csv",
    "270": "inventory-snapshot/270.csv",
    "283": "inventory-snapshot/283.csv",
    "281": "inventory-snapshot/281.csv",
    "318": "inventory-snapshot/318.csv",
    "320": "inventory-snapshot/320.csv",
    "105": "inventory-snapshot/105.csv",
    "110": "inventory-snapshot/110.csv",
    # '143': 'inventory-snapshot/143.csv',
    "180": "inventory-snapshot/180.csv",
    "172": "inventory-snapshot/172.csv",
    "227": "inventory-snapshot/227.csv",
    "243": "inventory-snapshot/243.csv",
    "255": "inventory-snapshot/255.csv",
    "278": "inventory-snapshot/278.csv",
}

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_omnishop_rfid_inventory",
    default_args=default_args,
    schedule="30 1 * * *",
    catchup=False,
    max_active_runs=1,
)

with dag:
    date_param = "{{ data_interval_start.strftime('%Y-%m-%d') }}"

    s3_prefix = "lake/omnishop/rfid_inventory"

    inventory_to_snowflake = SnowflakeTruncateAndLoadOperator(
        task_id="omnishop_rfid_inventory_to_snowflake",
        database="lake",
        view_database="lake_view",
        staging_database="lake_stg",
        schema="omnishop",
        table="rfid_inventory",
        column_list=inventory_column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        initial_load=True,
        copy_config=CopyConfigCsv(field_delimiter=",", header_rows=1, skip_pct=1),
        pattern=f".*/store_[0-9]+/inventory_{date_param}[.]csv[.]gz",
    )

    for store_id, store_file_location in store_file_config.items():
        inventory_to_s3 = OmnishopInventoryS3ObjectToS3(
            task_id=f"omnishop_rfid_store_{store_id}_inventory_to_s3",
            bucket=s3_buckets.tsos_da_int_inbound,
            key=f"{s3_prefix}/store_{store_id}/inventory_{date_param}.csv.gz",
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            store_file_location=store_file_location,
        )

        inventory_to_s3 >> inventory_to_snowflake
