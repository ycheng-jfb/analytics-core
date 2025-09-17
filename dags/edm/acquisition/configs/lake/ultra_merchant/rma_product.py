from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="rma_product",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("rma_product_id", "INT", uniqueness=True),
        Column("rma_id", "INT"),
        Column("order_line_id", "INT"),
        Column("product_id", "INT"),
        Column("item_id", "INT"),
        Column("return_action_id", "INT"),
        Column("expected_return_condition_id", "INT"),
        Column("restocking_fee_id", "INT"),
        Column("return_reason_id", "INT"),
        Column("reason_comment", "VARCHAR(255)"),
        Column("lpn_code", "VARCHAR(25)"),
        Column("amount_paid", "NUMBER(19, 4)"),
        Column("refund_amount", "NUMBER(19, 4)"),
        Column("store_credit_amount", "NUMBER(19, 4)"),
        Column("restocking_fee_amount", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
