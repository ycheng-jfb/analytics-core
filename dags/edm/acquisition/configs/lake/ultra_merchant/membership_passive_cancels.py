from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_passive_cancels',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_passive_cancels_id', 'INT', uniqueness=True),
        Column('membership_id', 'INT'),
        Column('membership_type_id', 'INT'),
        Column('membership_plan_id', 'INT'),
        Column('membership_level_id', 'INT'),
        Column('membership_statuscode', 'INT'),
        Column('curr_period_id', 'INT'),
        Column('prev_period_id', 'INT'),
        Column('source_table', 'VARCHAR(255)'),
        Column('downgrade_status', 'VARCHAR(20)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
