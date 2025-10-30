from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='gaming_prevention_log',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('gaming_prevention_log_id', 'INT', uniqueness=True),
        Column('order_id', 'INT'),
        Column('review_threshold', 'NUMBER(18, 4)'),
        Column('void_threshold', 'NUMBER(18, 4)'),
        Column('total_score', 'NUMBER(18, 4)'),
        Column('gaming_prevention_action', 'VARCHAR(50)'),
        Column('gms_agent_action', 'VARCHAR(50)'),
        Column('gms_agent_action_datetime_updated', 'TIMESTAMP_NTZ(3)'),
        Column('processing_time', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
