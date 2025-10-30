from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='product_bundle_component_delete_log',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('product_bundle_component_delete_log_id', 'INT'),
        Column('product_bundle_component_id', 'INT', uniqueness=True),
        Column('bundle_product_id', 'INT'),
        Column('component_product_id', 'INT'),
        Column('price_contribution_percentage', 'DOUBLE'),
        Column('is_free', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_deleted', 'TIMESTAMP_NTZ(3)'),
    ],
)
