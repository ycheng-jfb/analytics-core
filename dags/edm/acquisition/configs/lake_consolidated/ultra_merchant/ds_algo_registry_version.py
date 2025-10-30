from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='ds_algo_registry_version',
    company_join_sql="""
        SELECT DISTINCT
            L.DS_ALGO_REGISTRY_VERSION_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.DS_ALGO_REGISTRY AS DAR
            ON DS.STORE_GROUP_ID = DAR.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.ds_algo_registry_version AS L
            ON DAR.DS_ALGO_REGISTRY_ID = L.DS_ALGO_REGISTRY_ID """,
    column_list=[
        Column('ds_algo_registry_version_id', 'INT', uniqueness=True, key=True),
        Column('ds_algo_registry_id', 'INT', key=True),
        Column('version_label', 'VARCHAR(20)'),
        Column('version_major', 'INT'),
        Column('version_minor', 'INT'),
        Column('version_build', 'INT'),
        Column('uri_base', 'VARCHAR'),
        Column('datetime_trained', 'TIMESTAMP_NTZ(3)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('active', 'BOOLEAN'),
        Column('ds_algo_environment_id', 'INT'),
        Column('sagemaker_endpoint_name', 'VARCHAR(1000)'),
        Column('previous_ds_algo_registry_version_id', 'INT', key=True),
    ],
    watermark_column='datetime_modified',
)
