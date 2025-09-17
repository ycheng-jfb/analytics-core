from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracms",
    schema="dbo",
    table="ui_promo_management_promo_coupon_batch",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("ui_promo_management_promo_coupon_batch_id", "INT", uniqueness=True),
        Column("ui_promo_management_promo_id", "INT"),
        Column("coupon_batch_id", "INT"),
        Column("coupon_code_amount", "INT"),
        Column("statuscode", "INT"),
        Column("filename", "VARCHAR(50)"),
        Column("datetme_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
