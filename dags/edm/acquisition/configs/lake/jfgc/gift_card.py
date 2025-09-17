from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jfgc",
    schema="dbo",
    table="gift_card",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("gift_card_id", "INT", uniqueness=True),
        Column("merchant_id", "INT"),
        Column("credit_source_type_id", "INT"),
        Column("code", "VARCHAR(50)"),
        Column("amount", "NUMBER(19, 4)"),
        Column("balance", "NUMBER(19, 4)"),
        Column("email", "VARCHAR(100)"),
        Column("reference_number", "VARCHAR(50)"),
        Column("reference_source_type", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_redeemed", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("datetime_originally_issued", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
    ],
)
