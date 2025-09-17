from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table="product",
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
      SELECT l.product_id, ds.company_id
        FROM {database}.{schema}.order_line l
             JOIN {database}.{schema}."ORDER" o
                  ON l.order_id = o.order_id
             JOIN {database}.reference.dim_store ds
                  ON o.store_id = ds.store_id
                  UNION
        SELECT  l.product_id,
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
                   ON ps.product_id = l.product_id""",
    column_list=[
        Column("product_id", "INT", uniqueness=True, key=True),
        Column("master_product_id", "INT", key=True),
        Column("store_group_id", "INT"),
        Column("item_id", "INT", key=True),
        Column("packaging_item_id", "INT"),
        Column("product_type_id", "INT"),
        Column("brand_id", "INT", key=True),
        Column("brand_line_tag_id", "INT"),
        Column("manufacturer_id", "INT"),
        Column("vendor_delivery_type_id", "INT"),
        Column("is_master", "INT"),
        Column("option_signature", "VARCHAR(100)"),
        Column("label", "VARCHAR(100)"),
        Column("alias", "VARCHAR(100)"),
        Column("group_code", "VARCHAR(50)"),
        Column("short_description", "VARCHAR(255)"),
        Column("medium_description", "VARCHAR(2000)"),
        Column("long_description", "VARCHAR"),
        Column("meta_keywords", "VARCHAR(255)"),
        Column("meta_description", "VARCHAR(255)"),
        Column("default_store_id", "INT"),
        Column("default_pricing_id", "INT", key=True),
        Column("default_product_category_id", "INT", key=True),
        Column("default_autoship_frequency_id", "INT"),
        Column("default_warehouse_id", "INT"),
        Column("retail_pricing_id", "INT", key=True),
        Column("sale_pricing_id", "INT", key=True),
        Column("full_image_content_id", "INT"),
        Column("thumbnail_image_content_id", "INT"),
        Column("retail_unit_price", "NUMBER(19, 4)"),
        Column("measurement", "DOUBLE"),
        Column("measurement_unit", "VARCHAR(25)"),
        Column("usage_life_days", "INT"),
        Column("is_taxable", "INT"),
        Column("is_dangerous_goods", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("date_expected", "TIMESTAMP_NTZ(0)"),
        Column("date_sale_expires", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
        Column("product_returnable", "BOOLEAN"),
        Column("active", "INT"),
        Column("vip_variant_pricing_id_1", "INT"),
        Column("vip_variant_pricing_id_2", "INT"),
        Column("vip_variant_pricing_id_3", "INT"),
        Column("datetime_preorder_expires", "TIMESTAMP_NTZ(3)"),
        Column("token_redemption_quantity", "INT"),
        Column("sum_pricing", "INT"),
        Column("membership_brand_id", "INT"),
        Column("is_token", "INT"),
        Column("customization_configuration_id", "INT"),
        Column("is_warehouse_product", "INT"),
        Column("warehouse_unit_price", "INT"),
        Column("retail_outlet_unit_price", "INT"),
        Column("fulfillment_partner_id", "INT"),
    ],
    watermark_column="datetime_modified",
    post_sql="""
        DELETE FROM {database}.{schema}.product p
        USING {database}.reference.product_exclusion_list e
        WHERE p.meta_original_product_id = e.meta_original_product_id
        AND p.meta_company_id = e.meta_company_id;

        DELETE FROM {database}.ultra_merchant_history.product p
        USING {database}.reference.product_exclusion_list e
        WHERE p.meta_original_product_id = e.meta_original_product_id
        AND p.meta_company_id = e.meta_company_id;

        DELETE FROM {database}.ultra_merchant.product
        WHERE product_id IN (SELECT product_id
            FROM {database}.ultra_merchant.product
            GROUP BY product_id
            HAVING COUNT(1) > 1)
        AND (data_source_id = 10 AND meta_company_id != 10)
          OR (data_source_id = 40 AND meta_company_id NOT IN (20, 30, 40));
    """,
)
