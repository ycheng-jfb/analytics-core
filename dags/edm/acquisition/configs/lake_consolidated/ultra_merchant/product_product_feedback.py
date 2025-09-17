from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="product_product_feedback",
    company_join_sql="""
    SELECT l.product_product_feedback_id, ds.company_id
        FROM {database}.{source_schema}.product_product_feedback l
        JOIN {database}.{schema}.order_line ol
            ON l.product_id = ol.product_id
             JOIN {database}.{schema}."ORDER" o
                  ON ol.order_id = o.order_id
             JOIN {database}.reference.dim_store ds
                  ON o.store_id = ds.store_id
                  UNION
        SELECT  l.product_product_feedback_id,
                ps.company_id
         FROM {database}.{source_schema}.product_product_feedback l
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
                    ) AS ps
                   ON ps.product_id = l.product_id""",
    column_list=[
        Column("product_product_feedback_id", "INT", uniqueness=True, key=True),
        Column("cart_id", "INT", key=True),
        Column("product_feedback_id", "INT", key=True),
        Column("product_id", "INT", key=True),
        Column("lpn_code", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
