from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='item_preallocation',
    company_join_sql="""
        SELECT DISTINCT
            L.ITEM_ID,
            L.WAREHOUSE_ID,
            DS.COMPANY_ID
        FROM {database}.{source_schema}.item_preallocation AS L
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
        ON l.item_id = ds.item_id
    """,
    column_list=[
        Column('item_id', 'INT', uniqueness=True, key=True),
        Column('warehouse_id', 'INT', uniqueness=True),
        Column('quantity', 'INT'),
    ],
)
