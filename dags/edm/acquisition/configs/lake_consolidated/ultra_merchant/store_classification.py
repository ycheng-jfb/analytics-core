from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='store_classification',
    company_join_sql="""
            SELECT DISTINCT
                L.store_classification_id,
                ds.company_id
            FROM {database}.reference.dim_store AS ds
            INNER JOIN {database}.{source_schema}.store_classification AS L
                ON ds.store_id = L.store_id """,
    column_list=[
        Column('store_classification_id', 'INT', uniqueness=True, key=True),
        Column('store_id', 'INT'),
        Column('store_type_id', 'INT'),
    ],
)
