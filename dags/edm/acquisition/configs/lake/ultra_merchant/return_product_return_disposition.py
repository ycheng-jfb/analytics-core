from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="return_product_return_disposition",
    watermark_column="datetime_added",
    enable_archive=False,
    schema_version_prefix="v2",
    column_list=[
        Column("return_product_id", "INT", uniqueness=True),
        Column("return_disposition_id", "INT", uniqueness=True),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
