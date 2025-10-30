CREATE or replace TRANSIENT TABLE lake_consolidated.ultra_merchant_history.PRODUCT (
    data_source_id INT,
    meta_company_id INT,
    product_id INT,
    master_product_id INT,
    store_group_id INT,
    item_id INT,
    packaging_item_id INT,
    product_type_id INT,
    brand_id INT,
    brand_line_tag_id INT,
    manufacturer_id INT,
    vendor_delivery_type_id INT,
    is_master INT,
    option_signature VARCHAR(100),
    label VARCHAR(100),
    alias VARCHAR(100),
    group_code VARCHAR(50),
    short_description VARCHAR,
    medium_description VARCHAR,
    long_description VARCHAR,
    meta_keywords VARCHAR(255),
    meta_description VARCHAR(255),
    default_store_id INT,
    default_pricing_id INT,
    default_product_category_id INT,
    default_autoship_frequency_id INT,
    default_warehouse_id INT,
    retail_pricing_id INT,
    sale_pricing_id INT,
    full_image_content_id INT,
    thumbnail_image_content_id INT,
    retail_unit_price NUMBER(19, 4),
    measurement DOUBLE,
    measurement_unit VARCHAR(25),
    usage_life_days INT,
    is_taxable INT,
    is_dangerous_goods INT,
    datetime_added TIMESTAMP_NTZ(3),
    datetime_modified TIMESTAMP_NTZ(3),
    date_expected TIMESTAMP_NTZ(0),
    date_sale_expires TIMESTAMP_NTZ(0),
    statuscode INT,
    product_returnable BOOLEAN,
    active INT,
    vip_variant_pricing_id_1 INT,
    vip_variant_pricing_id_2 INT,
    vip_variant_pricing_id_3 INT,
    datetime_preorder_expires TIMESTAMP_NTZ(3),
    token_redemption_quantity INT,
    sum_pricing INT,
    membership_brand_id INT,
    is_token INT,
    customization_configuration_id INT,
    is_warehouse_product INT,
    warehouse_unit_price INT,
    retail_outlet_unit_price INT,
    fulfillment_partner_id INT,
    meta_original_product_id INT,
    meta_original_master_product_id INT,
    meta_row_source VARCHAR(40),
    hvr_change_op INT,
    effective_start_datetime TIMESTAMP_LTZ(3),
    effective_end_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);


SET lake_jfb_watermark = (
    SELECT MIN(last_update)
    FROM (
        SELECT CAST(min(META_SOURCE_CHANGE_DATETIME) AS TIMESTAMP_LTZ(3)) AS last_update
        FROM lake_jfb.ultra_merchant_history.PRODUCT
    ) AS A
);


CREATE OR REPLACE TEMP TABLE _lake_jfb_product_history AS
SELECT DISTINCT

    product_id,
    master_product_id,
    store_group_id,
    item_id,
    packaging_item_id,
    product_type_id,
    brand_id,
    brand_line_tag_id,
    manufacturer_id,
    vendor_delivery_type_id,
    is_master,
    option_signature,
    label,
    alias,
    group_code,
    short_description,
    medium_description,
    long_description,
    meta_keywords,
    meta_description,
    default_store_id,
    default_pricing_id,
    default_product_category_id,
    default_autoship_frequency_id,
    default_warehouse_id,
    retail_pricing_id,
    sale_pricing_id,
    full_image_content_id,
    thumbnail_image_content_id,
    retail_unit_price,
    measurement,
    measurement_unit,
    usage_life_days,
    is_taxable,
    is_dangerous_goods,
    datetime_added,
    datetime_modified,
    date_expected,
    date_sale_expires,
    statuscode,
    product_returnable,
    active,
    vip_variant_pricing_id_1,
    vip_variant_pricing_id_2,
    vip_variant_pricing_id_3,
    datetime_preorder_expires,
    token_redemption_quantity,
    sum_pricing,
    membership_brand_id,
    is_token,
    customization_configuration_id,
    is_warehouse_product,
    warehouse_unit_price,
    retail_outlet_unit_price,
    fulfillment_partner_id,
    meta_row_source,
    hvr_change_op,
    CAST(META_SOURCE_CHANGE_DATETIME AS TIMESTAMP_LTZ(3))  as effective_start_datetime,
    10 as data_source_id
FROM lake_jfb.ultra_merchant_history.PRODUCT
WHERE META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
QUALIFY ROW_NUMBER() OVER(PARTITION BY product_id, META_SOURCE_CHANGE_DATETIME
    ORDER BY HVR_CHANGE_SEQUENCE DESC) = 1;


 CREATE OR REPLACE TEMP TABLE _lake_jfb_company_product AS
SELECT product_id, company_id as meta_company_id
FROM (
      SELECT l.product_id, ds.company_id
        FROM lake_jfb.ultra_merchant.order_line l
             JOIN lake_jfb.ultra_merchant."ORDER" o
                  ON l.order_id = o.order_id
             JOIN lake_jfb.reference.dim_store ds
                  ON o.store_id = ds.store_id
                  UNION
        SELECT  l.product_id,
                ps.company_id
         FROM lake_jfb.ultra_merchant_history.product l
         LEFT JOIN (SELECT p.product_id,
                                    ds.company_id
                    FROM lake_jfb.ultra_merchant_history.product p

                             LEFT JOIN (SELECT DISTINCT product_id,
                                                        store_group_id,
                                                        default_store_id
                                        FROM lake_jfb.ultra_merchant_history.product
                                        WHERE master_product_id IS NULL) AS ms
                                       ON p.master_product_id = ms.product_id
                             LEFT JOIN lake_jfb.reference.dim_store AS ds
                                       ON ds.store_group_id = COALESCE(p.store_group_id, ms.store_group_id)
                    WHERE ds.company_id != 40
                    UNION
                    SELECT p.product_id,
                                    ds.company_id
                    FROM lake_jfb.ultra_merchant_history.product p

                             LEFT JOIN (SELECT DISTINCT product_id,
                                                        store_group_id,
                                                        default_store_id
                                        FROM lake_jfb.ultra_merchant_history.product
                                        WHERE master_product_id IS NULL) AS ms
                                       ON p.master_product_id = ms.product_id
                             LEFT JOIN lake_jfb.reference.dim_store AS ds
                                       ON ds.store_id = COALESCE(p.default_store_id, ms.default_store_id)
                   WHERE ds.company_id != 40
                    ) AS ps
                   ON ps.product_id = l.product_id
    WHERE l.META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
    ) AS l ;


 INSERT INTO lake_consolidated.ultra_merchant_history.PRODUCT (
    data_source_id,
    meta_company_id,
    product_id,
    master_product_id,
    store_group_id,
    item_id,
    packaging_item_id,
    product_type_id,
    brand_id,
    brand_line_tag_id,
    manufacturer_id,
    vendor_delivery_type_id,
    is_master,
    option_signature,
    label,
    alias,
    group_code,
    short_description,
    medium_description,
    long_description,
    meta_keywords,
    meta_description,
    default_store_id,
    default_pricing_id,
    default_product_category_id,
    default_autoship_frequency_id,
    default_warehouse_id,
    retail_pricing_id,
    sale_pricing_id,
    full_image_content_id,
    thumbnail_image_content_id,
    retail_unit_price,
    measurement,
    measurement_unit,
    usage_life_days,
    is_taxable,
    is_dangerous_goods,
    datetime_added,
    datetime_modified,
    date_expected,
    date_sale_expires,
    statuscode,
    product_returnable,
    active,
    vip_variant_pricing_id_1,
    vip_variant_pricing_id_2,
    vip_variant_pricing_id_3,
    datetime_preorder_expires,
    token_redemption_quantity,
    sum_pricing,
    membership_brand_id,
    is_token,
    customization_configuration_id,
    is_warehouse_product,
    warehouse_unit_price,
    retail_outlet_unit_price,
    fulfillment_partner_id,
    meta_original_product_id,
    meta_row_source,
    hvr_change_op,
    effective_start_datetime,
    effective_end_datetime,
    meta_update_datetime
)
SELECT DISTINCT
    s.data_source_id,
    s.meta_company_id,

    CASE WHEN NULLIF(s.product_id, '0') IS NOT NULL THEN CONCAT(s.product_id, s.meta_company_id) ELSE NULL END as product_id,
    CASE WHEN NULLIF(s.master_product_id, '0') IS NOT NULL THEN CONCAT(s.master_product_id, s.meta_company_id) ELSE NULL END as master_product_id,
    s.store_group_id,
    CASE WHEN NULLIF(s.item_id, '0') IS NOT NULL THEN CONCAT(s.item_id, s.meta_company_id) ELSE NULL END as item_id,
    s.packaging_item_id,
    s.product_type_id,
    CASE WHEN NULLIF(s.brand_id, '0') IS NOT NULL THEN CONCAT(s.brand_id, s.meta_company_id) ELSE NULL END as brand_id,
    s.brand_line_tag_id,
    s.manufacturer_id,
    s.vendor_delivery_type_id,
    s.is_master,
    s.option_signature,
    s.label,
    s.alias,
    s.group_code,
    s.short_description,
    s.medium_description,
    s.long_description,
    s.meta_keywords,
    s.meta_description,
    s.default_store_id,
    CASE WHEN NULLIF(s.default_pricing_id, '0') IS NOT NULL THEN CONCAT(s.default_pricing_id, s.meta_company_id) ELSE NULL END as default_pricing_id,
    CASE WHEN NULLIF(s.default_product_category_id, '0') IS NOT NULL THEN CONCAT(s.default_product_category_id, s.meta_company_id) ELSE NULL END as default_product_category_id,
    s.default_autoship_frequency_id,
    s.default_warehouse_id,
    CASE WHEN NULLIF(s.retail_pricing_id, '0') IS NOT NULL THEN CONCAT(s.retail_pricing_id, s.meta_company_id) ELSE NULL END as retail_pricing_id,
    CASE WHEN NULLIF(s.sale_pricing_id, '0') IS NOT NULL THEN CONCAT(s.sale_pricing_id, s.meta_company_id) ELSE NULL END as sale_pricing_id,
    s.full_image_content_id,
    s.thumbnail_image_content_id,
    s.retail_unit_price,
    s.measurement,
    s.measurement_unit,
    s.usage_life_days,
    s.is_taxable,
    s.is_dangerous_goods,
    s.datetime_added,
    s.datetime_modified,
    s.date_expected,
    s.date_sale_expires,
    s.statuscode,
    s.product_returnable,
    s.active,
    s.vip_variant_pricing_id_1,
    s.vip_variant_pricing_id_2,
    s.vip_variant_pricing_id_3,
    s.datetime_preorder_expires,
    s.token_redemption_quantity,
    s.sum_pricing,
    s.membership_brand_id,
    s.is_token,
    s.customization_configuration_id,
    s.is_warehouse_product,
    s.warehouse_unit_price,
    s.retail_outlet_unit_price,
    s.fulfillment_partner_id,
    s.product_id as meta_original_product_id,
    s.meta_row_source,
    s.hvr_change_op,
    s.effective_start_datetime,
    NULL AS effective_end_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime
FROM (
    SELECT
        A.*,
        COALESCE(CJ.meta_company_id, 40) AS meta_company_id
    FROM _lake_jfb_product_history AS A
    LEFT JOIN _lake_jfb_company_product AS CJ
        ON cj.product_id = a.product_id
    WHERE COALESCE(CJ.meta_company_id, 40) IN (10)
    ) AS s
WHERE NOT EXISTS (
    SELECT
        1
    FROM lake_consolidated.ultra_merchant_history.PRODUCT AS t
    WHERE
        s.data_source_id = t.data_source_id
        AND  s.meta_company_id = t.meta_company_id
        AND          s.product_id = t.meta_original_product_id
        AND s.effective_start_datetime = t.effective_start_datetime
    )
ORDER BY s.effective_start_datetime ASC;


CREATE OR REPLACE temp table _product_updates as
SELECT s.*,
    row_number() over(partition by s.data_source_id,
s.meta_company_id,

        s.meta_original_product_id
ORDER BY s.effective_start_datetime ASC) AS rnk
FROM (
SELECT DISTINCT
    s.data_source_id,
s.meta_company_id,

        s.meta_original_product_id,
    s.effective_start_datetime,
    s.effective_end_datetime
FROM lake_consolidated.ultra_merchant_history.PRODUCT as s
INNER JOIN (
    SELECT
        data_source_id,
meta_company_id,

        meta_original_product_id,
        effective_start_datetime
    FROM lake_consolidated.ultra_merchant_history.PRODUCT
    WHERE effective_end_datetime is NULL
    ) AS t
    ON s.data_source_id = t.data_source_id
    AND s.meta_company_id = t.meta_company_id
    AND     s.meta_original_product_id = t.meta_original_product_id
WHERE (
    s.effective_start_datetime >= t.effective_start_datetime
    OR s.effective_end_datetime = '9999-12-31 00:00:00.000 -0800'
)) s;


CREATE OR REPLACE TEMP TABLE _product_delta AS
SELECT
    s.data_source_id,
s.meta_company_id,

        s.meta_original_product_id,
    s.effective_start_datetime,
    COALESCE(dateadd(MILLISECOND, -1, t.effective_start_datetime), '9999-12-31 00:00:00.000 -0800') AS new_effective_end_datetime
FROM _product_updates AS s
    LEFT JOIN _product_updates AS t
    ON s.data_source_id = t.data_source_id
    AND s.meta_company_id = t.meta_company_id
    AND     s.meta_original_product_id = t.meta_original_product_id
    AND S.rnk = T.rnk - 1
WHERE (S.effective_end_datetime <> dateadd(MILLISECOND, -1, T.effective_start_datetime)
    OR S.effective_end_datetime IS NULL) ;


 UPDATE lake_consolidated.ultra_merchant_history.PRODUCT AS t
SET t.effective_end_datetime = s.new_effective_end_datetime,
    META_UPDATE_DATETIME = current_timestamp()
FROM _product_delta AS s
WHERE s.data_source_id = t.data_source_id
    AND s.meta_company_id = t.meta_company_id
    AND     s.meta_original_product_id = t.meta_original_product_id
    AND s.effective_start_datetime = t.effective_start_datetime
    AND COALESCE(t.effective_end_datetime, '1900-01-01 00:00:00 -0800') <> s.new_effective_end_datetime;
