from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="vat_rate",
    schema_version_prefix="v3",
    watermark_column="datetime_modified",
    column_list=[
        Column("vat_rate_id", "INT", uniqueness=True),
        Column("country_code", "VARCHAR(2)"),
        Column("rate", "DOUBLE"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
