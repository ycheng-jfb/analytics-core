from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='review',
    company_join_sql="""
    SELECT DISTINCT
        L.REVIEW_ID,
        DS.COMPANY_ID
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}.CUSTOMER AS C
        ON DS.STORE_ID = C.STORE_ID
    INNER JOIN {database}.{source_schema}.REVIEW AS L
        ON C.CUSTOMER_ID = L.CUSTOMER_ID """,
    column_list=[
        Column('review_id', 'INT', uniqueness=True, key=True),
        Column('product_id', 'INT', key=True),
        Column('customer_id', 'INT', key=True),
        Column('review_template_id', 'INT', key=True),
        Column('order_id', 'INT', key=True),
        Column('administrator_id', 'INT'),
        Column('body', 'VARCHAR'),
        Column('recommended', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_claimed', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('rating', 'DOUBLE'),
        Column('review_title', 'VARCHAR(2000)'),
        Column('review_source_id', 'INT'),
    ],
    watermark_column='datetime_modified',
)
