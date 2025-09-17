from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='featured_product_location_type',
    schema_version_prefix='v2',
    column_list=[
        Column('featured_product_location_type_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(50)'),
    ],
)
