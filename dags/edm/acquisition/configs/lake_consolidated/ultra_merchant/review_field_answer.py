from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.VALUE_COLUMN,
    table='review_field_answer',
    company_join_sql="""
    SELECT DISTINCT
        L.REVIEW_FIELD_ANSWER_ID,
        P.COMPANY_ID
    FROM (SELECT  l.product_id,
                ps.company_id
         FROM {database}.{source_schema}.product l
         LEFT JOIN (SELECT p.product_id,
                                    ds.company_id
                    FROM {database}.{source_schema}.product p
                             /* Grab store_group_id of Master Products */
                             LEFT JOIN (SELECT DISTINCT product_id,
                                                        store_group_id,
                                                        default_store_id
                                        FROM {database}.{source_schema}.product
                                        WHERE master_product_id IS NULL) AS ms
                                       ON p.master_product_id = ms.product_id
                             LEFT JOIN {database}.reference.dim_store AS ds
                                       ON ds.store_group_id = COALESCE(p.store_group_id, ms.store_group_id)
                    WHERE ds.company_id != 40
                    UNION
                    SELECT p.product_id,
                                    ds.company_id
                    FROM {database}.{source_schema}.product p
                             /* Grab store_group_id of Master Products */
                             LEFT JOIN (SELECT DISTINCT product_id,
                                                        store_group_id,
                                                        default_store_id
                                        FROM {database}.{source_schema}.product
                                        WHERE master_product_id IS NULL) AS ms
                                       ON p.master_product_id = ms.product_id
                             LEFT JOIN {database}.reference.dim_store AS ds
                                       ON ds.store_id = COALESCE(p.default_store_id, ms.default_store_id)
                   WHERE ds.company_id != 40
                    ) AS ps
                   ON ps.product_id = l.product_id) AS P
    INNER JOIN {database}.{schema}.REVIEW AS R
        ON P.PRODUCT_ID = R.PRODUCT_ID
    INNER JOIN {database}.{source_schema}.review_field_answer AS L
        ON L.REVIEW_ID = R.REVIEW_ID
    UNION
    SELECT DISTINCT
        L.REVIEW_FIELD_ANSWER_ID,
        DS.COMPANY_ID
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{schema}."ORDER" AS P
        ON P.STORE_ID = DS.STORE_ID
    INNER JOIN {database}.{schema}.REVIEW AS R
        ON P.ORDER_ID = R.ORDER_ID
    INNER JOIN {database}.{source_schema}.review_field_answer AS L
        ON L.REVIEW_ID = R.REVIEW_ID

         """,
    column_list=[
        Column('review_field_answer_id', 'INT', uniqueness=True, key=True),
        Column('review_id', 'INT', key=True),
        Column('review_template_field_id', 'INT', key=True),
        Column('review_template_field_answer_id', 'INT', key=True),
        Column('value', 'VARCHAR(2000)'),
        Column('comment', 'VARCHAR(512)'),
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
