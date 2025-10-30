from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='gaming_prevention_log_detail',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('gaming_prevention_log_detail_id', 'INT', uniqueness=True),
        Column('gaming_prevention_log_id', 'INT'),
        Column('gaming_prevention_check_id', 'INT'),
        Column('gaming_prevention_query_id', 'INT'),
        Column('order_id', 'INT'),
        Column('hits', 'INT'),
        Column('base_score', 'NUMBER(18, 4)'),
        Column('boost_factor', 'NUMBER(18, 4)'),
        Column('score', 'NUMBER(18, 4)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
