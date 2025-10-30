from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='session_media_data',
    company_join_sql="""
           SELECT DISTINCT
               L.session_id,
               ds.company_id
           FROM {database}.reference.dim_store AS ds
           INNER JOIN {database}.{schema}.session AS s
               ON ds.store_id = s.store_id
           INNER JOIN {database}.{source_schema}.session_media_data AS L
           on L.session_id=s.session_id""",
    column_list=[
        Column('session_id', 'INT', uniqueness=True, key=True),
        Column('placement_media_code_id', 'INT', key=True),
        Column('creative_media_code_id', 'INT', key=True),
        Column('ad_media_code_id', 'INT', key=True),
        Column('sub_media_code_id', 'INT', key=True),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
