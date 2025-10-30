from include.airflow.operators.mssql_acquisition import HighWatermarkMaxRowVersion
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='um_replicated',
    schema='dbo',
    table='ultramerchant__session_detail__last',
    watermark_column='repl_timestamp',
    initial_load_value='0x0',
    high_watermark_cls=HighWatermarkMaxRowVersion,
    schema_version_prefix='v2',
    column_list=[
        Column('session_detail_hash_id', 'INT'),
        Column('session_detail_id', 'INT', uniqueness=True),
        Column('session_id', 'INT'),
        Column('name', 'VARCHAR(50)'),
        Column('value', 'VARCHAR(4000)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
