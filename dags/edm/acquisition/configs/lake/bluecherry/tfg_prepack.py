from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    linked_server='bluecherry',
    database='datafbpr',
    schema='dbo',
    table='tfg_prepack',
    watermark_column='last_mod',
    schema_version_prefix='v1',
    column_list=[
        Column("prepack_sku", "VARCHAR(50)", uniqueness=True),
        Column("component_sku", "VARCHAR(50)", uniqueness=True),
        Column("prepack_qty", "INT"),
        Column("comp_size_qty", "INT"),
        Column("last_mod", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("last_mod_by", "VARCHAR(15)"),
        Column("date_create", "TIMESTAMP_NTZ(3)"),
    ],
)
