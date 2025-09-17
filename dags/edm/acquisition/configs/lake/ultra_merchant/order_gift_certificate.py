from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="order_gift_certificate",
    watermark_column="order_gift_certificate_id",
    initial_load_value="0",
    strict_inequality=True,
    enable_archive=False,
    schema_version_prefix="v2",
    column_list=[
        Column("order_gift_certificate_id", "INT", uniqueness=True, delta_column=True),
        Column("order_line_id", "INT"),
        Column("gift_certificate_id", "INT"),
    ],
)
