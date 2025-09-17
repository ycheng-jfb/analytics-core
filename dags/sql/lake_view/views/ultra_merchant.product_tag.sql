CREATE OR REPLACE VIEW lake_view.ultra_merchant.product_tag COPY GRANTS AS
SELECT
    product_id,
    tag_id,
    datetime_added,
    datetime_modified,
    product_tag_id,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.product_tag s
WHERE NOT exists(
        SELECT
            1
        FROM lake.ultra_merchant.product_tag_delete_log d
        WHERE s.product_id = d.product_id
          AND s.tag_id = d.tag_id
    );
