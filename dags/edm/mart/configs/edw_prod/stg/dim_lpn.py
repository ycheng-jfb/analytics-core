from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_lpn",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_lpn.sql"],
        column_list=[
            Column("lpn_id", "INT", uniqueness=True),
            Column("lpn_code", "VARCHAR(255)"),
            Column("warehouse_id", "INT"),
            Column("item_id_uw", "INT"),
            Column("item_number", "VARCHAR(255)"),
            Column("receipt_id", "INT"),
            Column("receipt_received_datetime", "TIMESTAMP_TZ"),
            Column("po_number", "VARCHAR(255)"),
        ],
        watermark_tables=[
            "lake.ultra_warehouse.lpn",
            "lake.ultra_warehouse.receipt",
            "lake.ultra_warehouse.item",
        ],
    )
