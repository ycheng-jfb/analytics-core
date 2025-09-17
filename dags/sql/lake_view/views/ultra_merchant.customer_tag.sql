CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.CUSTOMER_TAG AS
SELECT
    customer_tag_id,
    customer_id,
    tag_id,
    weight,
    datetime_added,
    datetime_modified,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.customer_tag s
WHERE NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.customer_tag__del cd
        WHERE cd.customer_id = s.customer_id
            and cd.tag_id = s.tag_id
    );
