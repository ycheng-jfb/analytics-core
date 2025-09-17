from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_warehouse",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_warehouse.sql"],
        column_list=[
            Column("warehouse_id", "NUMBER(38,0)", uniqueness=True),
            Column("warehouse_code", "VARCHAR(30)"),
            Column("warehouse_name", "VARCHAR(255)"),
            Column("warehouse_type", "VARCHAR(255)"),
            Column("wms_type", "VARCHAR(255)"),
            Column("is_active", "BOOLEAN"),
            Column("address1", "VARCHAR(50)"),
            Column("address2", "VARCHAR(35)"),
            Column("city", "VARCHAR(35)"),
            Column("state", "VARCHAR(25)"),
            Column("zip_code", "VARCHAR(25)"),
            Column("country_code", "VARCHAR(10)"),
            Column("phone", "VARCHAR(25)"),
            Column("coast", "VARCHAR(10)"),
            Column("region", "VARCHAR(10)"),
            Column("is_retail", "BOOLEAN"),
            Column("is_consignment", "BOOLEAN"),
            Column("is_wholesale", "BOOLEAN"),
            Column("carrier_service", "VARCHAR(255)"),
            Column("time_zone", "VARCHAR(100)"),
        ],
        watermark_tables=[
            "lake.ultra_warehouse.code",
            "lake.ultra_warehouse.address",
            "lake.ultra_warehouse.carrier_service",
            "lake.ultra_warehouse.warehouse",
        ],
    )
