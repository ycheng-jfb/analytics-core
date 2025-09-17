from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='media_publisher',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('media_publisher_id', 'INT', uniqueness=True),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('enable_entry_popups', 'INT'),
        Column('enable_exit_popups', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
