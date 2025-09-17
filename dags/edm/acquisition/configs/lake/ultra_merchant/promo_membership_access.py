from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="promo_membership_access",
    watermark_column="datetime_added",
    initial_load_value="1900-01-01",
    schema_version_prefix="v2",
    column_list=[
        Column("promo_membership_access_id", "INT", uniqueness=True),
        Column("membership_id", "INT"),
        Column("promo_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
