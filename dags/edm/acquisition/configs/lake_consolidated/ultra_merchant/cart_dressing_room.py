from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='cart_dressing_room',
    company_join_sql="""
      SELECT DISTINCT
          L.CART_DRESSING_ROOM_ID,
          DS.COMPANY_ID
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{schema}.Session  AS S
      ON DS.STORE_ID = S.STORE_ID
      INNER JOIN {database}.{schema}.CART AS C
      ON S.SESSION_ID = C.SESSION_ID
      INNER JOIN {database}.{source_schema}.cart_dressing_room AS L
      ON C.CART_ID = L.CART_ID """,
    column_list=[
        Column('cart_dressing_room_id', 'INT', uniqueness=True, key=True),
        Column('cart_id', 'INT', key=True),
        Column('store_id', 'INT'),
        Column('dressing_room_id', 'INT', key=True),
        Column('datetime_exited', 'TIMESTAMP_NTZ(3)'),
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
