from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='tag',
    company_join_sql="""
            SELECT DISTINCT
                L.tag_id,
                ds.company_id
            FROM {database}.reference.dim_store AS ds
            INNER JOIN {database}.{source_schema}.tag AS L
                ON ds.store_group_id = L.store_group_id """,
    column_list=[
        Column('tag_id', 'INT', uniqueness=True, key=True),
        Column('store_group_id', 'INT'),
        Column('tag_category_id', 'INT'),
        Column('parent_tag_id', 'INT', key=True),
        Column('brand_id', 'INT', key=True),
        Column('label', 'VARCHAR(100)'),
        Column('active', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('hide', 'BOOLEAN'),
    ],
    watermark_column='datetime_modified',
)
