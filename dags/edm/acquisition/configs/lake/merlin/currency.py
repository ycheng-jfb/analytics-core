from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="Merlin",
    schema="dbo",
    table="Currency",
    watermark_column="DateUpdate",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="Oid"),
        Column("name", "VARCHAR(50)", source_name="Name"),
        Column("culture", "VARCHAR(100)", source_name="Culture"),
        Column("optimistic_lock_field", "INT", source_name="OptimisticLockField"),
        Column("gc_record", "INT", source_name="GCRecord"),
        Column("iso", "VARCHAR(3)", source_name="ISO"),
        Column("gpid", "VARCHAR(10)", source_name="GPID"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
        Column("create_fx_rates_in_gp", "BOOLEAN", source_name="CreateFXRatesInGP"),
    ],
)
