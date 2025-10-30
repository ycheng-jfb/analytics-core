from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='marin_tracking',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('marin_tracking_id', 'INT', uniqueness=True),
        Column('session_id', 'INT'),
        Column('mkwid', 'VARCHAR(25)'),
        Column('pcrid', 'VARCHAR(25)'),
        Column('pkw', 'VARCHAR(500)'),
        Column('pmt', 'VARCHAR(10)'),
        Column('pdv', 'VARCHAR(10)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
