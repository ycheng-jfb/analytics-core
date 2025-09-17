from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_dim import SnowflakeMartDimOperator


def get_mart_operator():
    return SnowflakeMartDimOperator(
        table="dim_chargeback_status",
        use_surrogate_key=True,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_dim_chargeback_status.sql"],
        column_list=[
            Column("chargeback_status_code", "INT", uniqueness=True),
            Column("chargeback_status", "VARCHAR(50)", uniqueness=True),
        ],
        watermark_tables=["lake_consolidated.ultra_merchant.statuscode"],
    )
