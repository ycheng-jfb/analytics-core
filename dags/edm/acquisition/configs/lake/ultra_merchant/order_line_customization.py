from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='order_line_customization',
    watermark_column='datetime_modified',
    column_list=[
        Column('order_line_customization_id', 'INT', uniqueness=True),
        Column('order_id', 'INT'),
        Column('order_line_id', 'INT'),
        Column('customization_configuration_id', 'INT'),
        Column('error_message', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('statuscode', 'INT'),
    ],
)
