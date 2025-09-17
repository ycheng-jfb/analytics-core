from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='dropship_retailer',
    watermark_column='datetime_modified',
    column_list=[
        Column('dropship_retailer_id', 'INT', uniqueness=True),
        Column('code', 'VARCHAR(255)'),
        Column('label', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
