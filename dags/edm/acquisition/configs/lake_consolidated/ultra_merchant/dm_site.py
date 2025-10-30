from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='dm_site',
    company_join_sql="""
     SELECT DISTINCT
         L.dm_site_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.dm_site AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('dm_site_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('store_id', 'INT'),
        Column('offer_category_id', 'INT'),
        Column('offer_id', 'INT'),
        Column('cart_derivative_id', 'INT'),
        Column('dm_site_type_id', 'INT'),
        Column('label', 'VARCHAR(100)'),
        Column('description', 'VARCHAR(255)'),
        Column('title', 'VARCHAR(255)'),
        Column('css_file', 'VARCHAR(255)'),
        Column('js_file', 'VARCHAR(255)'),
        Column('html_header', 'VARCHAR'),
        Column('html_footer', 'VARCHAR'),
        Column('html_home', 'VARCHAR'),
        Column('redirect_dm_gateway_id', 'INT', key=True),
        Column('redirect_url', 'VARCHAR(255)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
        Column('site_asset', 'VARCHAR(100)'),
        Column('cdn_link', 'VARCHAR(255)'),
        Column('cdn_content_datetime', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
