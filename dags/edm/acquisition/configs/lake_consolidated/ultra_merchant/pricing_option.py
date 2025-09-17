from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table='pricing_option',
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
         SELECT
            L.pricing_option_id,
            DS.company_id
           FROM {database}.{source_schema}.pricing_option AS L
            JOIN (
        SELECT DISTINCT company_id
        FROM {database}.REFERENCE.DIM_STORE
        WHERE company_id IS NOT NULL
        ) AS DS   """,
    column_list=[
        Column('pricing_option_id', 'INT', uniqueness=True, key=True),
        Column('pricing_id', 'INT', key=True),
        Column('break_quantity', 'INT'),
        Column('unit_price', 'NUMBER(19, 4)'),
        Column('shipping_price', 'NUMBER(19, 4)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
    ],
    watermark_column='datetime_modified',
    post_sql="""
    DELETE FROM {database}.ultra_merchant.pricing_option
    WHERE pricing_option_id IN (SELECT pricing_option_id
        FROM {database}.ultra_merchant.pricing_option
        GROUP BY pricing_option_id
        HAVING COUNT(1) > 1)
    AND (data_source_id = 10 AND meta_company_id != 10)
      OR (data_source_id = 40 AND meta_company_id NOT IN (20, 30, 40));
    """,
)
