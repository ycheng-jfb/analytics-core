from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='customer_tag',
    watermark_column='datetime_modified',
    initial_load_value='2020-01-14',
    schema_version_prefix='v2',
    column_list=[
        Column('customer_tag_id', 'INT'),
        Column('customer_id', 'INT', uniqueness=True),
        Column('tag_id', 'INT', uniqueness=True),
        Column('weight', 'DOUBLE'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
