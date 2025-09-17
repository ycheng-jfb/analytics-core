from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='jf_portal',
    schema='dbo',
    table='PoType',
    schema_version_prefix='v2',
    column_list=[
        Column('po_type_id', 'INT', source_name='PoTypeId'),
        Column('po_vendor_type_id', 'INT', source_name='PoVendorTypeId'),
        Column('name', 'VARCHAR(30)', source_name='NAME'),
        Column('po_number_suffix', 'VARCHAR(3)', source_name='PONumberSuffix'),
    ],
)
