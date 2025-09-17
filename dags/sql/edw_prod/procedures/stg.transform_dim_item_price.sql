SET target_table = 'stg.dim_item_price';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET wm_lake_history_ultra_merchant_product = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.product'));
SET wm_lake_ultra_merchant_item = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.item'));
SET wm_lake_history_ultra_merchant_pricing_option = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.pricing_option'));
SET wm_edw_stg_dim_product_price_history = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_product_price_history'));

CREATE OR REPLACE TEMP TABLE _dim_item_price__base (product_id INT, item_id INT);

INSERT INTO _dim_item_price__base (product_id, item_id)
SELECT product_id, item_id
FROM lake_consolidated.ultra_merchant_history.product
WHERE item_id IS NOT NULL
AND $is_full_refresh = TRUE
QUALIFY ROW_NUMBER() OVER(PARTITION BY product_id, item_id ORDER BY effective_start_datetime DESC) = 1
ORDER BY product_id, item_id;

INSERT INTO _dim_item_price__base (product_id, item_id)
SELECT DISTINCT product_id, item_id
FROM (SELECT p.product_id, i.item_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
               JOIN lake_consolidated.ultra_merchant.item AS i ON i.item_id = p.item_id
      WHERE p.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'
        AND (p.meta_update_datetime > $wm_lake_history_ultra_merchant_product OR
             i.meta_update_datetime > $wm_lake_ultra_merchant_item)

      UNION ALL

      SELECT p.product_id, p.item_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
               JOIN lake_consolidated.ultra_merchant_history.product AS mp
                    ON mp.product_id = p.master_product_id
      WHERE p.item_id IS NOT NULL
        AND mp.meta_update_datetime > $wm_lake_history_ultra_merchant_product
        AND mp.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT p.product_id, p.item_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
               JOIN lake_consolidated.ultra_merchant_history.product AS mp
                    ON mp.product_id = p.master_product_id
               JOIN stg.dim_product_price_history AS dpph
                    ON dpph.product_id = mp.product_id
      WHERE dpph.meta_update_datetime > $wm_edw_stg_dim_product_price_history
        AND dpph.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'
        AND p.item_id IS NOT NULL

      UNION ALL

      SELECT p.product_id, p.item_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
               JOIN stg.dim_product_price_history AS dpph
                    ON dpph.product_id = p.product_id
      WHERE dpph.meta_update_datetime > $wm_edw_stg_dim_product_price_history
        AND dpph.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'
        AND p.item_id IS NOT NULL

      UNION ALL

      SELECT p.product_id, p.item_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
               JOIN lake_consolidated.ultra_merchant_history.product AS mp
                    ON mp.product_id = p.master_product_id
               JOIN lake_consolidated.ultra_merchant_history.pricing_option AS po
                    ON po.pricing_id = mp.default_pricing_id
      WHERE po.meta_update_datetime > $wm_lake_history_ultra_merchant_pricing_option
        AND po.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'
        AND p.item_id IS NOT NULL

      UNION ALL

      SELECT p.product_id, p.item_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
              JOIN lake_consolidated.ultra_merchant_history.pricing_option AS po
                    ON po.pricing_id = p.default_pricing_id
      WHERE po.meta_update_datetime > $wm_lake_history_ultra_merchant_pricing_option
        AND po.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'
        AND p.item_id IS NOT NULL

      UNION ALL

      SELECT product_id, item_id
      FROM excp.dim_item_price
      WHERE meta_is_current_excp
        AND meta_data_quality = 'error') AS incr
WHERE $is_full_refresh = FALSE
ORDER BY product_id, item_id;

CREATE OR REPLACE TEMP TABLE _dim_item_price__sku AS
SELECT
    base.product_id,
    base.item_id,
    p.master_product_id,
    i.item_number as sku,
    i.hvr_is_deleted as is_item_deleted,
    p.effective_start_datetime,
    p.effective_end_datetime,
    p.meta_original_product_id,
    i.meta_original_item_id
FROM  _dim_item_price__base AS base
    JOIN lake_consolidated.ultra_merchant_history.product as p
        ON base.product_id = p.product_id
        AND base.item_id = p.item_id
    JOIN lake_consolidated.ultra_merchant.item AS i
        ON i.item_id = p.item_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY p.product_id, p.item_id ORDER BY effective_start_datetime DESC) = 1;
-- SELECT * FROM _dim_item_price__sku

CREATE OR REPLACE TEMP TABLE _dim_item_price__master_attributes AS
SELECT
    sku.product_id,
    sku.item_id,
    sku.master_product_id,
    sku.sku,
    p.retail_unit_price,
    COALESCE(p.default_pricing_id, p2.default_pricing_id) AS default_pricing_id,
    COALESCE(p.membership_brand_id, p2.membership_brand_id) AS membership_brand_id,
    COALESCE(p.default_store_id, p2.default_store_id) AS store_id,
    COALESCE(INITCAP(p.label), INITCAP(p2.label), 'Unknown') AS product_name,
    CASE
        WHEN sku.is_item_deleted = TRUE THEN FALSE
        WHEN p.active = 1 OR p2.active = 1 THEN TRUE
        ELSE FALSE
    END AS is_active,
    sku.meta_original_product_id,
    sku.meta_original_item_id,
    COALESCE(p2.effective_start_datetime, p.effective_start_datetime) AS master_effective_start_datetime
FROM _dim_item_price__sku as sku
    /* wrapping the child datetime with the effective start/end of master */
    JOIN lake_consolidated.ultra_merchant_history.product AS p
        ON p.product_id = COALESCE(sku.master_product_id, sku.product_id)
        AND sku.effective_start_datetime BETWEEN p.effective_start_datetime AND p.effective_end_datetime
    /* wrapping the master datetime with the effective start/end of child and getting the latest master record changed */
    LEFT JOIN (
        SELECT
            product_id,
            master_product_id,
            effective_start_datetime,
            effective_end_datetime,
            retail_unit_price,
            default_pricing_id,
            membership_brand_id,
            default_store_id,
            label,
            active,
            ROW_NUMBER()OVER(PARTITION BY product_id ORDER BY effective_start_datetime DESC) as rnk
        FROM lake_consolidated.ultra_merchant_history.product
    ) AS p2
        ON p2.product_id = COALESCE(sku.master_product_id, sku.product_id)
        AND p2.effective_start_datetime BETWEEN sku.effective_start_datetime AND sku.effective_end_datetime
        AND p2.rnk = 1;

CREATE OR REPLACE TEMP TABLE _dim_item_price__stg AS
SELECT
    ma.product_id,
    ma.item_id,
    COALESCE(ma.master_product_id, -1) AS master_product_id,
    ma.store_id,
    ma.sku,
    sku.product_sku,
    sku.base_sku,
    ma.product_name,
    COALESCE(po.unit_price, dpph.vip_unit_price, 0.00) as vip_unit_price,
    COALESCE(ma.retail_unit_price, dpph.retail_unit_price, 0) AS retail_unit_price,
    ma.is_active,
    COALESCE(ma.membership_brand_id, -1) AS membership_brand_id,
    ma.meta_original_product_id,
    ma.meta_original_item_id,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
from _dim_item_price__master_attributes AS ma
    LEFT JOIN lake_consolidated.ultra_merchant_history.pricing_option AS po
        ON po.pricing_id = ma.default_pricing_id
        AND ma.master_effective_start_datetime BETWEEN po.effective_start_datetime AND po.effective_end_datetime
    LEFT JOIN stg.dim_product_price_history AS dpph
        ON dpph.product_id = ma.product_id
        AND ma.master_effective_start_datetime BETWEEN dpph.effective_start_datetime AND dpph.effective_end_datetime
    LEFT JOIN stg.dim_sku AS sku
        ON sku.sku = ma.sku;
-- SELECT * FROM _dim_item_price__stg;

INSERT INTO stg.dim_item_price_stg
(
     product_id,
     item_id,
     master_product_id,
     store_id,
     sku,
     product_sku,
     base_sku,
     product_name,
     vip_unit_price,
     retail_unit_price,
     is_active,
     membership_brand_id,
     meta_original_product_id,
     meta_original_item_id,
     meta_create_datetime,
     meta_update_datetime
)

SELECT
    product_id,
    item_id,
    master_product_id,
    store_id,
    sku,
    product_sku,
    base_sku,
    product_name,
    vip_unit_price,
    retail_unit_price,
    is_active,
    membership_brand_id,
    meta_original_product_id,
    meta_original_item_id,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_item_price__stg;
