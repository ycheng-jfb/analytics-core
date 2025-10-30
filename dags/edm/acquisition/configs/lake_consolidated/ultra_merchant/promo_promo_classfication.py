from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='promo_promo_classfication',
    company_join_sql="""
        SELECT DISTINCT
            L.PROMO_PROMO_CLASSFICATION_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE DS
        INNER JOIN {database}.{schema}.PROMO P
            ON DS.STORE_GROUP_ID=P.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.promo_promo_classfication L
            ON L.PROMO_ID=P.PROMO_ID """,
    column_list=[
        Column(
            'promo_promo_classfication_id',
            'INT',
            uniqueness=True,
            key=True,
        ),
        Column('promo_id', 'INT', key=True),
        Column('promo_classification_id', 'INT'),
    ],
    watermark_column='promo_promo_classfication_id',
)
