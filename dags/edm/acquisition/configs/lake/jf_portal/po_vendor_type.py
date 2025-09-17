from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="JF_Portal",
    schema="dbo",
    table="PoVendorType",
    schema_version_prefix="v2",
    column_list=[
        Column("po_vendor_type_id", "INT", source_name="PoVendorTypeId"),
        Column("name", "VARCHAR(20)", source_name="NAME"),
    ],
)
