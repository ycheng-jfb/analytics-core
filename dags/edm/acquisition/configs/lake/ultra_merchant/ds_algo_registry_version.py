from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='ds_algo_registry_version',
    watermark_column='datetime_modified',
    schema_version_prefix='v2',
    column_list=[
        Column('ds_algo_registry_version_id', 'INT', uniqueness=True),
        Column('ds_algo_registry_id', 'INT'),
        Column('version_label', 'VARCHAR(20)'),
        Column('version_major', 'INT'),
        Column('version_minor', 'INT'),
        Column('version_build', 'INT'),
        Column('uri_base', 'VARCHAR'),
        Column('datetime_trained', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('active', 'BOOLEAN'),
        Column('ds_algo_environment_id', 'INT'),
        Column('sagemaker_endpoint_name', 'VARCHAR(1000)'),
        Column('previous_ds_algo_registry_version_id', 'INT'),
    ],
)
