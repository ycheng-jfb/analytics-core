SET target_table = 'stg.fact_order_line_embroidery';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

/*
-- Initial load / full refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_edw_stg_fact_order_line = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_line'));
SET wm_edw_stg_fact_order_line_product_cost = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_line_product_cost'));
SET lake_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));

CREATE OR REPLACE TEMP TABLE _fact_order_line_embroidery__order_line_base (order_line_id INT, meta_original_order_line_id INT);
-- full refresh
INSERT INTO _fact_order_line_embroidery__order_line_base (order_line_id, meta_original_order_line_id)
SELECT fol.order_line_id, fol.meta_original_order_line_id
FROM stg.fact_order_line AS fol
    JOIN stg.dim_product_type AS dpt
        ON dpt.product_type_key = fol.product_type_key
WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
AND dpt.product_type_name = 'Customization'
ORDER BY fol.order_line_id;

-- incremental refresh
INSERT INTO _fact_order_line_embroidery__order_line_base (order_line_id, meta_original_order_line_id)
SELECT DISTINCT fol.order_line_id, fol.meta_original_order_line_id
FROM stg.fact_order_line AS fol
    JOIN stg.dim_product_type AS dpt
        ON dpt.product_type_key = fol.product_type_key
    LEFT JOIN stg.fact_order_line_product_cost AS folpc
        ON fol.order_line_id = folpc.order_line_id
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS odl
        ON odl.order_id = fol.order_id
WHERE NOT $is_full_refresh
AND dpt.product_type_name = 'Customization'
AND (fol.meta_update_datetime > $wm_edw_stg_fact_order_line
    OR folpc.meta_update_datetime > $wm_edw_stg_fact_order_line_product_cost
    OR odl.meta_update_datetime > $lake_ultra_merchant_order);

-- mapping the embroidery order_line_id to the order_line_id of the item being embroidered
CREATE OR REPLACE TEMP TABLE _fact_order_line_embroidery__order_line_id AS
SELECT
    fol2.order_line_id,
    fol2.meta_original_order_line_id,
    base.order_line_id AS embroidery_order_line_id,
    base.meta_original_order_line_id AS meta_original_embroidery_order_line_id,
    fol.order_id
FROM _fact_order_line_embroidery__order_line_base AS base
    JOIN stg.fact_order_line AS fol
        ON fol.order_line_id = base.order_line_id
    JOIN stg.fact_order_line AS fol2
        ON fol2.order_id = fol.order_id
        AND fol2.sub_group_key = fol.sub_group_key
    JOIN stg.dim_product_type AS pt
        ON pt.product_type_key = fol2.product_type_key
WHERE pt.product_type_name <> 'Customization';

INSERT INTO stg.fact_order_line_embroidery_stg
(
 order_line_id,
 embroidery_order_line_id,
 meta_original_order_line_id,
 meta_original_embroidery_order_line_id,
 order_id,
 product_id,
 order_line_status_key,
 order_status_key,
 item_quantity,
 payment_transaction_local_amount,
 subtotal_excl_tariff_local_amount,
 tariff_revenue_local_amount,
 tax_local_amount,
 tax_cash_local_amount,
 tax_credit_local_amount,
 product_discount_local_amount,
 cash_credit_local_amount,
 cash_membership_credit_local_amount,
 cash_refund_credit_local_amount,
 cash_giftcard_credit_local_amount,
 non_cash_credit_local_amount,
 estimated_landed_cost_local_amount,
 reporting_landed_cost_local_amount,
 actual_landed_cost_local_amount,
 oracle_cost_local_amount,
 price_offered_local_amount,
 bounceback_endowment_local_amount,
 vip_endowment_local_amount,
 air_vip_price,
 retail_unit_price,
 product_price_history_key,
 sub_group_key,
 is_test_customer,
 is_deleted,
 meta_create_datetime,
 meta_update_datetime
)
SELECT
    oli.order_line_id,
    oli.embroidery_order_line_id,
    oli.meta_original_order_line_id,
    oli.meta_original_embroidery_order_line_id,
    fol.order_id,
    fol.product_id,
    fol.order_line_status_key,
    fol.order_status_key,
    fol.item_quantity,
    fol.payment_transaction_local_amount,
    fol.subtotal_excl_tariff_local_amount,
    fol.tariff_revenue_local_amount,
    fol.tax_local_amount,
    fol.tax_cash_local_amount,
    fol.tax_credit_local_amount,
    fol.product_discount_local_amount,
    fol.cash_credit_local_amount,
    fol.cash_membership_credit_local_amount,
    fol.cash_refund_credit_local_amount,
    fol.cash_giftcard_credit_local_amount,
    fol.non_cash_credit_local_amount,
    fol.estimated_landed_cost_local_amount,
    COALESCE(folpc.reporting_landed_cost_local_amount, 0) AS reporting_landed_cost_local_amount,
    COALESCE(folpc.actual_landed_cost_local_amount, 0) AS actual_landed_cost_local_amount,
    COALESCE(fol.oracle_cost_local_amount, 0) AS oracle_cost_local_amount,
    fol.price_offered_local_amount,
    fol.bounceback_endowment_local_amount,
    fol.vip_endowment_local_amount,
    fol.air_vip_price,
    fol.retail_unit_price,
    fol.product_price_history_key,
    fol.sub_group_key,
    fol.is_test_customer,
    IFF(hvr_is_deleted = 1 , TRUE, FALSE) AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_udpate_datetime
FROM _fact_order_line_embroidery__order_line_id AS oli
    JOIN stg.fact_order_line AS fol
        ON fol.order_line_id = oli.embroidery_order_line_id
    LEFT JOIN stg.fact_order_line_product_cost AS folpc
        ON folpc.order_line_id = fol.order_line_id
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS odl
        ON odl.order_id = fol.order_id;
