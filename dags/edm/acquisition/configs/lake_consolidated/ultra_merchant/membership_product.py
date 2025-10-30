from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_product',
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_PRODUCT_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS C
            ON DS.STORE_ID = C.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_product AS L
            ON L.MEMBERSHIP_ID = C.MEMBERSHIP_ID """,
    column_list=[
        Column('membership_product_id', 'INT', uniqueness=True, key=True),
        Column('membership_id', 'INT', key=True),
        Column('product_id', 'INT', key=True),
        Column('membership_product_type_id', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('active', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
