CREATE OR REPLACE VIEW lake_view.ultra_merchant.product_bundle_component COPY GRANTS AS
SELECT
    product_bundle_component_id,
    bundle_product_id,
    component_product_id,
    price_contribution_percentage,
    is_free,
    datetime_added,
    datetime_modified,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.product_bundle_component s
WHERE NOT exists(
        SELECT
            1
        FROM lake.ultra_merchant.product_bundle_component_delete_log l
        WHERE l.product_bundle_component_id = s.product_bundle_component_id
    );
