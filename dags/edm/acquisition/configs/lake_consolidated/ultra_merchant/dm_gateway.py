from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table='dm_gateway',
    table_type=TableType.REGULAR,
    company_join_sql="""
     SELECT DISTINCT
         L.dm_gateway_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.dm_gateway AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('dm_gateway_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('dm_gateway_type_id', 'INT'),
        Column('store_id', 'INT'),
        Column('offer_category_id', 'INT', key=True),
        Column('code', 'VARCHAR(25)'),
        Column('label', 'VARCHAR(50)'),
        Column('description', 'VARCHAR(255)'),
        Column('redirect_dm_gateway_id', 'INT', key=True),
        Column('statuscode', 'INT'),
        Column('dm_gateway_sub_type_id', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('membership_brand_id', 'INT'),
    ],
    watermark_column='datetime_modified',
)
