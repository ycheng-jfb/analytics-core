from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='fraud_quarantine_queue_detail',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('fraud_quarantine_queue_detail_id', 'INT', uniqueness=True),
        Column('fraud_quarantine_queue_id', 'INT'),
        Column('object', 'VARCHAR(255)'),
        Column('object_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
