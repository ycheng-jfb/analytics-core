CREATE OR REPLACE VIEW data_model.fact_return_line
AS
SELECT
    frl.return_line_id,
    frl.rma_product_id,
    frl.return_id,
    frl.order_line_id,
    frl.order_id,
    frl.customer_id,
    frl.activation_key,
    frl.first_activation_key,
    frl.product_id,
    frl.item_price_key,
    frl.store_id,
    frl.rma_id,
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
    frl.rma_resolution_local_datetime,
    frl.rma_transit_local_datetime,
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
    AND ds.store_brand NOT IN ('Legacy');
