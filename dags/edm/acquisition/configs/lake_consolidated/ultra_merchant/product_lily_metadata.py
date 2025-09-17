from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table="product_lily_metadata",
    company_join_sql="""
       SELECT DISTINCT
        L.product_lily_metadata_id,
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
    INNER JOIN {database}.{source_schema}.product_lily_metadata AS L
        ON L.product_id = P.product_id
        """,
    column_list=[
        Column("product_lily_metadata_id", "INT", uniqueness=True, key=True),
        Column("product_id", "INT", key=True),
        Column("category_name", "VARCHAR(100)"),
        Column("category_value", "VARCHAR"),
        Column("value_synonyms", "VARCHAR"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
