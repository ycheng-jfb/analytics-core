from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='cart_warehouse',
    company_join_sql="""
     SELECT DISTINCT
         L.CART_WAREHOUSE_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.Session  AS S
     ON DS.STORE_ID = S.STORE_ID
     INNER JOIN {database}.{schema}.CART AS C
     ON S.SESSION_ID = C.SESSION_ID
     INNER JOIN {database}.{source_schema}.cart_warehouse AS L
     ON L.CART_ID=C.CART_ID """,
    column_list=[
        Column('cart_warehouse_id', 'INT', uniqueness=True, key=True),
        Column('cart_id', 'INT', key=True),
        Column('warehouse_id', 'INT'),
        Column('statuscode', 'INT'),
        Column('administrator_id', 'INT'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_expired', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
)
