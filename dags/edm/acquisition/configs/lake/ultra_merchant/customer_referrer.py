from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='customer_referrer',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v5',
    column_list=[
        Column('customer_referrer_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('sort', 'INT'),
        Column('active', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('referrer_ref_id', 'INT', source_name='Referrer_ref_id'),
        Column('customer_referrer_type_id', 'INT'),
        Column('group_name', 'VARCHAR(100)'),
        Column('is_influencer', 'BOOLEAN'),
    ],
)
