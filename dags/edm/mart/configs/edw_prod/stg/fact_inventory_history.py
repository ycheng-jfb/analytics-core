from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table="fact_inventory_history",
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_fact_inventory_history.sql"],
        use_surrogate_key=False,
        column_list=[
            Column("item_id", "NUMBER(38,0)", uniqueness=True),
            Column("warehouse_id", "NUMBER(38,0)", uniqueness=True),
            Column("rollup_datetime_added", "DATETIME", uniqueness=True),
            Column("local_date", "DATE"),
            Column("region_id", "NUMBER(38,0)"),
            Column("brand", "VARCHAR(255)"),
            Column("sku", "VARCHAR(30)"),
            Column("onhand_quantity", "NUMBER(38,0)"),
            Column("replen_quantity", "NUMBER(38,0)"),
            Column("ghost_quantity", "NUMBER(38,0)"),
            Column("reserve_quantity", "NUMBER(38,0)"),
            Column("special_pick_quantity", "NUMBER(38,0)"),
            Column("manual_stock_reserve_quantity", "NUMBER(38,0)"),
            Column("available_to_sell_quantity", "NUMBER(38,0)"),
            Column("receipt_inspection_quantity", "NUMBER(38,0)"),
            Column("return_quantity", "NUMBER(38,0)"),
            Column("damaged_quantity", "NUMBER(38,0)"),
            Column("damaged_returns_quantity", "NUMBER(38,0)"),
            Column("allocated_quantity", "NUMBER(38,0)"),
            Column("intransit_quantity", "NUMBER(38,0)"),
            Column("staging_quantity", "NUMBER(38,0)"),
            Column("pick_staging_quantity", "NUMBER(38,0)"),
            Column("lost_quantity", "NUMBER(38,0)"),
            Column("open_to_buy_quantity", "NUMBER(38,0)"),
            Column("landed_cost_per_unit", "NUMBER(38,10)"),
            Column("dsw_dropship_quantity", "NUMBER(38,0)"),
            Column("is_deleted", "BOOLEAN"),
        ],
        watermark_tables=[
            "edw_prod.stg.fact_inventory",
            "lake.ultra_warehouse.inventory_rollup",
            "lake.ultra_warehouse.item",
            "edw_prod.reference.dropship_inventory_log",
        ],
    )
