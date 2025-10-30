from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='company_carrier_service',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('company_carrier_service_id', 'INT', uniqueness=True),
        Column('company_carrier_id', 'INT'),
        Column('carrier_service_id', 'INT'),
        Column('code', 'VARCHAR(25)'),
        Column('label_format_id_no_address2', 'INT'),
        Column('label_format_id_address2', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
