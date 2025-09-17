CREATE OR REPLACE VIEW data_model_jfb.fact_return_line
AS
SELECT
    frl.meta_original_return_line_id AS return_line_id,
    frl.meta_original_rma_product_id AS rma_product_id,
    stg.udf_unconcat_brand(frl.return_id) AS return_id,
    stg.udf_unconcat_brand(frl.order_line_id) AS order_line_id,
    stg.udf_unconcat_brand(frl.order_id) AS order_id,
    stg.udf_unconcat_brand(frl.customer_id) AS customer_id,
    frl.activation_key,
    frl.first_activation_key,
    stg.udf_unconcat_brand(frl.product_id) AS product_id,
    frl.item_price_key,
    frl.store_id,
    stg.udf_unconcat_brand(frl.rma_id) as rma_id,
    frl.administrator_id,
    frl.return_category_id,
    frl.return_carrier,
    frl.return_source_id,
    frl.retail_return_store_id,
    frl.warehouse_id,
    frl.return_status_key,
    frl.return_condition_key,
    frl.return_reason_id,
    frl.is_exchange,
    frl.return_item_quantity,
    frl.return_product_cost_local_amount,
    frl.estimated_return_shipping_cost_local_amount,
    frl.return_restocking_fee_local_amount,
    frl.return_receipt_date_eur_conversion_rate,
    frl.return_receipt_date_usd_conversion_rate,
    frl.return_request_local_datetime,
    frl.return_receipt_local_datetime,
    frl.return_completion_local_datetime,
    frl.rma_transit_local_datetime,
    frl.rma_resolution_local_datetime,
    frl.effective_vat_rate,
    frl.return_tax_local_amount,
    frl.return_subtotal_local_amount,
    frl.return_cash_credit_local_amount,
    frl.return_discount_local_amount,
    frl.return_non_cash_credit_local_amount,
    estimated_returned_product_cost_local_amount AS estimated_returned_product_local_cost,
    frl.estimated_returned_product_resaleable_pct,
        frl.estimated_returned_product_cost_local_amount *
        (1 - estimated_returned_product_resaleable_pct) AS estimated_returned_product_cost_local_amount_damaged,
        frl.estimated_returned_product_cost_local_amount *
        estimated_returned_product_resaleable_pct AS estimated_returned_product_cost_local_amount_resaleable,
    frl.meta_create_datetime,
    frl.meta_update_datetime
FROM stg.fact_return_line AS frl
    LEFT JOIN stg.dim_store AS ds
        ON ds.store_id = frl.store_id
WHERE NOT frl.is_deleted
    AND NOT NVL(frl.is_test_customer, FALSE)
    AND ds.store_brand NOT IN ('Legacy')
    AND (substring(frl.return_line_id, -2) = '10' OR frl.return_line_id = -1);
