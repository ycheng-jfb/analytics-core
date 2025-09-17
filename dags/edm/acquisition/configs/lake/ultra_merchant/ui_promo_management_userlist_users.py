from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="ui_promo_management_userlist_users",
    watermark_column="datetime_modified",
    initial_load_value="2020-05-08 20:00:00.000",
    schema_version_prefix="v2",
    column_list=[
        Column(
            "ui_promo_management_users_id",
            "INT",
            uniqueness=True,
        ),
        Column("ui_promo_management_userlist_id", "INT"),
        Column("membership_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
