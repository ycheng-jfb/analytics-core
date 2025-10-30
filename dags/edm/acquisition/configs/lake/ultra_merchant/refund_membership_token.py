from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='refund_membership_token',
    watermark_column='datetime_modified',
    schema_version_prefix='v3',
    column_list=[
        Column('refund_id', 'INT', uniqueness=True),
        Column('membership_token_id', 'INT', uniqueness=True),
        Column('order_line_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
