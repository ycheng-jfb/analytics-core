SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

INSERT INTO snapshot.finance_assumption (
    bu,
    financial_date,
    brand,
    gender,
    region_type,
    region_type_mapping,
    store_type,
    local_currency,
    shipping_supplies_cost_per_order,
    shipping_cost_per_order,
    variable_warehouse_cost_per_order,
    variable_gms_cost_per_order,
    variable_payment_processing_pct_cash_revenue,
    return_shipping_cost_per_order,
    product_markdown_percent,
    returned_product_resaleable_percent,
    snapshot_datetime
)
SELECT
    bu,
    financial_date,
    brand,
    gender,
    region_type,
    region_type_mapping,
    store_type,
    local_currency,
    shipping_supplies_cost_per_order,
    shipping_cost_per_order,
    variable_warehouse_cost_per_order,
    variable_gms_cost_per_order,
    variable_payment_processing_pct_cash_revenue,
    return_shipping_cost_per_order,
    product_markdown_percent,
    returned_product_resaleable_percent,
    $execution_start_time AS snapshot_datetime
FROM reference.finance_assumption;
