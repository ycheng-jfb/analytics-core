
CREATE OR REPLACE VIEW data_model.dim_bundle_component_history
(
    bundle_component_history_key,
    product_bundle_component_id,
    bundle_product_id,
    bundle_component_product_id,
    bundle_name,
    bundle_alias,
    bundle_default_product_category,
    bundle_component_default_product_category,
    bundle_component_color,
    bundle_component_name,
    bundle_price_contribution_percent,
    bundle_component_vip_unit_price,
    bundle_component_retail_unit_price,
    bundle_is_free,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    bundle_component_history_key,
    product_bundle_component_id,
    bundle_product_id,
    bundle_component_product_id,
    bundle_name,
    bundle_alias,
    bundle_default_product_category,
    bundle_component_default_product_category,
    bundle_component_color,
    bundle_component_name,
    bundle_price_contribution_percent,
    bundle_component_vip_unit_price,
    bundle_component_retail_unit_price,
    bundle_is_free,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_bundle_component_history
WHERE NOT NVL(is_deleted, FALSE);
