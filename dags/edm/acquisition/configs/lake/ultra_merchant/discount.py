from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='discount',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('discount_id', 'INT', uniqueness=True),
        Column('applied_to', 'VARCHAR(15)'),
        Column('calculation_method', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(50)'),
        Column('percentage', 'DOUBLE'),
        Column('rate', 'NUMBER(19, 4)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('date_expires', 'TIMESTAMP_NTZ(0)'),
        Column('statuscode', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
