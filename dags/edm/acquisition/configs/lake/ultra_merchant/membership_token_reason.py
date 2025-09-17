from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_token_reason',
    watermark_column='datetime_added',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_token_reason_id', 'INT', uniqueness=True),
        Column('label', 'VARCHAR(50)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('cash', 'INT'),
        Column('access', 'VARCHAR(15)'),
    ],
)
