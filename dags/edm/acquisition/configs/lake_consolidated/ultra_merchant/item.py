from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
    SELECT l.item_id, ds.company_id FROM {database}.{schema}.item l
    JOIN (
    SELECT l.item_id, ds.company_id
    FROM {database}.{schema}.item l
        JOIN (SELECT DISTINCT store_group_id, company_id
            FROM {database}.reference.dim_store) ds
            ON l.primary_store_group_id = ds.store_group_id
    UNION
    SELECT l.item_id, ds.company_id
    FROM {database}.{schema}.order_line l
        JOIN {database}.{schema}."ORDER" o
            ON l.order_id = o.order_id
        JOIN {database}.reference.dim_store ds
            ON o.store_id = ds.store_id
    UNION
    SELECT p.item_id,
       ds.company_id
    FROM {database}.{schema}.product p
         /* grab store_group_id of master products */
         JOIN (SELECT DISTINCT product_id,
                               store_group_id,
                               default_store_id
                    FROM {database}.{schema}.product
                    WHERE master_product_id IS NULL) AS MS
            ON p.master_product_id = MS.product_id
         JOIN {database}.reference.dim_store AS ds
                ON ds.store_group_id = coalesce(p.store_group_id, MS.store_group_id)
    UNION
    SELECT p.item_id,
          ds.company_id
    FROM {database}.{schema}.product p
         /* grab store_group_id of master products */
         JOIN (SELECT DISTINCT product_id,
                                    store_group_id,
                                    default_store_id
                    FROM {database}.{schema}.product
                    WHERE master_product_id IS NULL) AS MS
            ON p.master_product_id = MS.product_id
         JOIN {database}.reference.dim_store AS ds
            ON ds.store_id = coalesce(p.default_store_id, MS.default_store_id)
    UNION
    SELECT p.item_id,
                    ds.company_id
    FROM {database}.{schema}.product p
         JOIN {database}.{schema}.product mp
              ON p.master_product_id = mp.product_id
         JOIN (SELECT DISTINCT store_group_id, company_id FROM {database}.reference.dim_store) AS ds
              ON ds.store_group_id = mp.store_group_id
    UNION SELECT p.item_id, ds.company_id
    FROM {database}.{schema}.product p
        JOIN (SELECT DISTINCT store_group_id, company_id FROM {database}.reference.dim_store) AS ds
            ON ds.store_group_id = p.store_group_id
          WHERE p.master_product_id IS NULL AND p.item_id IS NOT NULL
    UNION
    SELECT i.item_id,
           ds.company_id
    FROM {database}.{schema}.item i
         JOIN lake.ultra_warehouse.item wi
              ON i.item_number = wi.item_number
         JOIN lake.ultra_warehouse.company c
              ON wi.company_id = c.company_id
         JOIN (SELECT DISTINCT company_name, company_id FROM {database}.reference.dim_store) ds
              ON lower(ds.company_name) = (CASE
                                        WHEN lower(c.label) IN ('shoe dazzle', 'fabkids', 'justfab') THEN 'jfb'
                                        WHEN lower(c.label) = 'fabletics' THEN 'fbl'
                                        WHEN lower(c.label) = 'savage x' THEN 'sxf' END)
    ) AS ds
    ON l.item_id = ds.item_id and ds.company_id <> 40
    """,
    table="item",
    column_list=[
        Column("item_id", "INT", uniqueness=True, key=True),
        Column("item_type_id", "INT"),
        Column("item_number", "VARCHAR(30)"),
        Column("description", "VARCHAR(255)"),
        Column("current_cost", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("coo_iso2", "VARCHAR(2)"),
        Column("market", "BOOLEAN"),
        Column("primary_store_group_id", "INT"),
        Column("fulfillment_partner_id", "INT"),
        Column("partner_item_number", "varchar(50)"),
    ],
    watermark_column="datetime_modified",
    post_sql="""
    DELETE FROM {database}.ultra_merchant.item
    WHERE item_id IN (SELECT item_id
        FROM {database}.ultra_merchant.item
        GROUP BY item_id
        HAVING COUNT(1) > 1)
    AND (data_source_id = 10 AND meta_company_id != 10)
        OR (data_source_id = 40 AND meta_company_id NOT IN (20, 30, 40));
    """,
)
