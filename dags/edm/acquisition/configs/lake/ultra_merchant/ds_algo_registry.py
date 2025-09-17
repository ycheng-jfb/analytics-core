from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='ds_algo_registry',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('ds_algo_registry_id', 'INT', uniqueness=True),
        Column('ds_algo_environment_id', 'INT'),
        Column('store_group_id', 'INT'),
        Column('algo_name', 'VARCHAR(100)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('ds_algo_registry_group_id', 'INT'),
        Column('active', 'BOOLEAN'),
        Column('weight', 'INT'),
        Column('membership_suggestion_method_id', 'INT'),
    ],
)
