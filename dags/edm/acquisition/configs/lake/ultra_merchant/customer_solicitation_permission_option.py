from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='customer_solicitation_permission_option',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('customer_solicitation_permission_option_id', 'INT', uniqueness=True),
        Column('customer_solicitation_permission_option_type_id', 'INT'),
        Column('label', 'VARCHAR(100)'),
        Column('description', 'VARCHAR(250)'),
        Column('value', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
