from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_level_group_modification_log',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_level_group_modification_log_id', 'INT', uniqueness=True),
        Column('membership_id', 'INT'),
        Column('old_value', 'INT'),
        Column('new_value', 'INT'),
        Column('passive_downgrade', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
