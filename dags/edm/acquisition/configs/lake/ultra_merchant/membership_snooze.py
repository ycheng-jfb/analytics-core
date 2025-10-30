from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    schema_version_prefix='v3',
    table='membership_snooze',
    watermark_column='datetime_modified',
    column_list=[
        Column('membership_snooze_id', 'INT', uniqueness=True),
        Column('membership_id', 'INT'),
        Column('administrator_id', 'INT'),
        Column('extended_by_administrator_id', 'INT'),
        Column('periods_to_skip', 'INT'),
        Column('extended', 'INT'),
        Column('comment', 'VARCHAR(255)'),
        Column('date_start', 'TIMESTAMP_NTZ(0)'),
        Column('date_end', 'TIMESTAMP_NTZ(0)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('datetime_cancelled', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('membership_snooze_type_id', 'INT'),
    ],
)
