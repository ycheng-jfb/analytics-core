from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="rma_refund",
    initial_load_value="1",
    watermark_column="rma_id",
    schema_version_prefix="v2",
    column_list=[
        Column("rma_id", "INT", uniqueness=True, delta_column=0),
        Column("refund_id", "INT", uniqueness=True, delta_column=1),
    ],
)
