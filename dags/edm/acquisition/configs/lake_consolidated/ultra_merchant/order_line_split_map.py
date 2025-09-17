from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='order_line_split_map',
    company_join_sql="""
    SELECT DISTINCT
        L.order_line_split_map_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}."ORDER" AS O
        ON DS.STORE_ID = O.STORE_ID
    INNER JOIN {database}.{schema}.order_line AS OL
            ON OL.ORDER_ID = O.ORDER_ID
    INNER JOIN {database}.{source_schema}.order_line_split_map AS L
        ON OL.order_line_id = L.order_line_id""",
    watermark_column='datetime_modified',
    column_list=[
        Column('order_line_split_map_id', 'INT', uniqueness=True, key=True),
        Column('master_order_id', 'INT', key=True),
        Column('master_order_line_id', 'INT', key=True),
        Column('order_id', 'INT', key=True),
        Column('order_line_id', 'INT', key=True),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
