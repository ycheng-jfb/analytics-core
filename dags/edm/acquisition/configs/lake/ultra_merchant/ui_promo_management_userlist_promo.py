from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="ui_promo_management_userlist_promo",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("ui_promo_management_userlist_promo_id", "INT", uniqueness=True),
        Column("ui_promo_management_userlist_id", "INT"),
        Column("promo_id", "INT"),
        Column("membership_promo_type_id", "INT"),
        Column("membership_table", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("promo_expiration_days", "INT"),
    ],
)
