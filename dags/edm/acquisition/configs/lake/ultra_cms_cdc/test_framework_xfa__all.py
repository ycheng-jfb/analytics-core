from include.airflow.operators.mssql_acquisition import HighWatermarkMaxRowVersion
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='um_replicated',
    schema='dbo',
    table='ultracms__test_framework_xfa__all',
    watermark_column='repl_timestamp',
    initial_load_value='0x0',
    high_watermark_cls=HighWatermarkMaxRowVersion,
    schema_version_prefix='v2',
    column_list=[
        Column('test_framework_xfa_id', 'INT'),
        Column('store_group_id', 'INT'),
        Column('label', 'VARCHAR(50)'),
        Column('xfa', 'VARCHAR(50)'),
        Column('xfa_sort', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('repl_action', 'VARCHAR(1)'),
        Column('repl_timestamp', 'BINARY(8)', uniqueness=True),
    ],
)
