from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR,
    table='order_tracking_detail',
    company_join_sql="""
       SELECT DISTINCT
           L.ORDER_TRACKING_DETAIL_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS C
           ON DS.STORE_ID = C.STORE_ID
       INNER JOIN {database}.{source_schema}.order_tracking_detail AS L
           ON L.ORDER_TRACKING_ID = C.ORDER_TRACKING_ID """,
    column_list=[
        Column('order_tracking_detail_id', 'INT', uniqueness=True),
        Column('order_tracking_id', 'INT'),
        Column('object', 'VARCHAR(50)'),
        Column('object_id', 'INT'),
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
