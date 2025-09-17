from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="PoMiscCost",
    is_full_upsert=True,
    enable_archive=True,
    schema_version_prefix="v2",
    column_list=[
        Column("po_misc_cost_id", "INT", uniqueness=True, source_name="PoMiscCostId"),
        Column("po_id", "INT", source_name="PoId"),
        Column("description", "VARCHAR(500)", source_name="Description"),
        Column("misc_cost_id", "INT", source_name="MiscCostId"),
        Column("allocation_id", "INT", source_name="AllocationId"),
        Column("misc_cost_type_id", "INT", source_name="MiscCostTypeId"),
        Column("cost_type", "INT", source_name="CostType"),
        Column("color", "VARCHAR(50)", source_name="Color"),
        Column("vendor_id", "VARCHAR(15)", source_name="VendorId"),
        Column("vendor_name", "VARCHAR(64)", source_name="VendorName"),
        Column("percentage", "NUMBER(4, 4)", source_name="Percentage"),
        Column("fixed_value", "NUMBER(19, 4)", source_name="FixedValue"),
        Column("hidden", "BOOLEAN", source_name="Hidden"),
        Column("user_create", "VARCHAR(100)", source_name="UserCreate"),
        Column("user_update", "VARCHAR(100)", source_name="UserUpdate"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
    ],
)
