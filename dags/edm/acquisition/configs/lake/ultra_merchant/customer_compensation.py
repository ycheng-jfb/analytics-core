from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="customer_compensation",
    watermark_column="datetime_modified",
    schema_version_prefix="v4",
    column_list=[
        Column("customer_compensation_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("customer_compensation_type_id", "INT"),
        Column("promo_id", "INT"),
        Column("points", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column("label", "VARCHAR(50)"),
        Column("tier", "VARCHAR(50)"),
        Column("description", "VARCHAR"),
        Column("requires_approval", "INT"),
        Column("expiration_days", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("customer_compensation_code", "VARCHAR(255)"),
        Column("customer_compensation_system_id", "INT"),
        Column("customer_compensation_expiration_type_id", "INT"),
        Column("membership_type_id", "INT"),
    ],
)
