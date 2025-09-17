from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='case_intent_log',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('case_intent_log_id', 'INT', uniqueness=True),
        Column('case_id', 'INT'),
        Column('case_intent_id', 'INT'),
        Column('object', 'VARCHAR(255)'),
        Column('object_id', 'VARCHAR(255)'),
        Column('reason_object_id', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
