from include.airflow.operators.mssql_acquisition import HighWatermarkMaxRowVersion
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='um_replicated',
    schema='dbo',
    table='ultramerchant__membership_reward_transaction__del',
    watermark_column='repl_timestamp',
    initial_load_value='0x0',
    high_watermark_cls=HighWatermarkMaxRowVersion,
    column_list=[
        Column('membership_reward_transaction_id', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('repl_action', 'VARCHAR(1)'),
        Column('repl_timestamp', 'BINARY(8)', uniqueness=True),
    ],
)
