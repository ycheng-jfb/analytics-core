from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR_GLOBAL,
    table='product_category',
    company_join_sql="""
     SELECT product_category_id, ds.company_id
     FROM {database}.{source_schema}.product_category l
     JOIN (
        SELECT DISTINCT company_id
        FROM {database}.REFERENCE.DIM_STORE
        WHERE company_id IS NOT NULL
        ) AS DS
  """,
    column_list=[
        Column('product_category_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('parent_product_category_id', 'INT', key=True),
        Column('label', 'VARCHAR(50)'),
        Column('short_description', 'VARCHAR(255)'),
        Column('medium_description', 'VARCHAR(2000)'),
        Column('long_description', 'VARCHAR'),
        Column('full_image_content_id', 'INT'),
        Column('thumbnail_image_content_id', 'INT'),
        Column('active', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('product_category_type', 'VARCHAR(50)'),
        Column('archive', 'BOOLEAN'),
    ],
    watermark_column='datetime_modified',
    post_sql="""
    DELETE FROM {database}.ultra_merchant.product_category
    WHERE product_category_id IN (SELECT product_category_id
        FROM {database}.ultra_merchant.product_category
        GROUP BY product_category_id
        HAVING COUNT(1) > 1)
    AND (data_source_id = 10 AND meta_company_id != 10)
      OR (data_source_id = 40 AND meta_company_id NOT IN (20, 30, 40));
    """,
)
