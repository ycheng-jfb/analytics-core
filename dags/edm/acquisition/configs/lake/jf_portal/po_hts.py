from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="JF_Portal",
    schema="dbo",
    table="PoHts",
    schema_version_prefix="v3",
    is_full_upsert=True,
    enable_archive=True,
    column_list=[
        Column("po_hts_id", "INT", uniqueness=True, source_name="PoHtsId"),
        Column("hts_id", "INT", source_name="HtsId"),
        Column("hts_code", "VARCHAR(30)", source_name="HtsCode"),
        Column("po_id", "INT", source_name="PoId"),
        Column("color", "VARCHAR(50)", source_name="Color"),
        Column("duty", "NUMBER(4, 4)", source_name="Duty"),
        Column("fixed_value", "NUMBER(19, 4)", source_name="FixedValue"),
        Column("read_only", "BOOLEAN", source_name="ReadOnly"),
        Column("description", "VARCHAR(200)", source_name="Description"),
        Column("user_create", "VARCHAR(100)", source_name="UserCreate"),
        Column("user_update", "VARCHAR(100)", source_name="UserUpdate"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
        Column("use_to_calc_duty", "BOOLEAN", source_name="UseToCalcDuty"),
        Column("tariff", "NUMBER(4, 4)", source_name="Tariff"),
        Column("overwritten", "BOOLEAN", source_name="Overwritten"),
    ],
)
