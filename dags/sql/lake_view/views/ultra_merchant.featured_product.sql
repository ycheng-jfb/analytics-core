CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.FEATURED_PRODUCT AS
SELECT
    featured_product_id,
    featured_product_location_id,
    object,
    object_id,
    product_id,
    sort,
    active,
    datetime_added,
    datetime_start,
    datetime_end,
    sale_price,
    datetime_modified,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.featured_product s
WHERE NOT exists(
        SELECT
            1
        FROM lake.ultra_merchant.featured_product_delete_log d
        WHERE d.featured_product_id = s.featured_product_id
    )
    AND NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.featured_product__del cd
        WHERE cd.featured_product_id = s.featured_product_id
    )
    ;
