from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='cart',
    schema='ultra_cart',
    company_join_sql="""
          SELECT DISTINCT
              L.CART_ID,
              L.CART_HASH_ID,
              DS.COMPANY_ID
          FROM {database}.REFERENCE.DIM_STORE AS DS
          INNER JOIN {database}.{source_schema}.cart AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column('cart_id', 'INT', uniqueness=True, key=True),
        Column('cart_hash_id', 'INT', uniqueness=True),
        Column('cart_source_type_id', 'INT'),
        Column('store_group_id', 'INT'),
        Column('session_id', 'INT', key=True),
        Column('customer_id', 'INT', key=True),
        Column('code', 'VARCHAR(36)'),
        Column('cart_object', 'VARCHAR'),
        Column('statuscode', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
    watermark_column='datetime_modified',
)
