from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='product_feedback',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('product_feedback_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('is_positive', 'BOOLEAN'),
        Column('sequence_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('hidden', 'BOOLEAN'),
        Column('auto_remove_from_cart', 'BOOLEAN'),
    ],
)
