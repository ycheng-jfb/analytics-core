CREATE OR REPLACE VIEW lake_view.ultra_merchant.variant_pricing COPY GRANTS AS
SELECT
    variant_pricing_id,
    test_metadata_id,
    product_id,
    pricing_id,
    control_pricing_id,
    variant_number,
    statuscode,
    datetime_added,
    datetime_modified,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.variant_pricing s
WHERE NOT exists(
        SELECT
            1
        FROM lake.ultra_merchant.variant_pricing_delete_log d
        WHERE d.variant_pricing_id = s.variant_pricing_id
    )
;
