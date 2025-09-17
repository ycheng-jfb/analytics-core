from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="stylist",
    watermark_column="datetime_modified",
    strict_inequality=True,
    schema_version_prefix="v2",
    column_list=[
        Column("stylist_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("administrator_id", "INT"),
        Column("firstname", "VARCHAR(25)"),
        Column("lastname", "VARCHAR(25)"),
        Column("bio", "VARCHAR(2000)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("statuscode", "INT"),
        Column("display_to_public", "INT"),
        Column("public_bio", "VARCHAR(2000)"),
        Column("active_stylist_discount", "INT"),
        Column("email", "VARCHAR(100)"),
        Column("parent_stylist_id", "INT"),
        Column("datetime_last_login", "TIMESTAMP_NTZ(3)"),
    ],
)
