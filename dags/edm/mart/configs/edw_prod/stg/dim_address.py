from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_address",
        use_surrogate_key=True,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_address.sql"],
        column_list=[
            Column("address_id", "INT", uniqueness=True),
            Column("meta_original_address_id", "INT"),
            Column("street_address_1", "VARCHAR(50)"),
            Column("street_address_2", "VARCHAR(35)"),
            Column("city", "VARCHAR(35)"),
            Column("state", "VARCHAR(25)"),
            Column("zip_code", "VARCHAR(25)"),
            Column("country_code", "VARCHAR(7)"),
            Column("is_state_valid", "BOOLEAN"),
            Column("is_zip_code_valid", "BOOLEAN"),
        ],
        watermark_tables=["lake_consolidated.ultra_merchant.address"],
    )
