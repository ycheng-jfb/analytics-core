from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='in_transit_milestone_cross_reference',
    watermark_column='datetime_modified',
    strict_inequality=True,
    schema_version_prefix='v2',
    column_list=[
        Column('in_transit_milestone_cross_reference_id', 'INT', uniqueness=True),
        Column('carrier_id', 'INT'),
        Column('status_code', 'VARCHAR(255)'),
        Column('milestone_code_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
