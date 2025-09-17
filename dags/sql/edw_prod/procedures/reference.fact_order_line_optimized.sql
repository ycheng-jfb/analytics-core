BEGIN TRANSACTION;

DELETE FROM reference.fact_order_line_optimized;

INSERT INTO reference.fact_order_line_optimized (
    order_line_id,
    order_id,
    store_id,
    customer_id,
    product_id,
    master_product_id,
    bundle_product_id,
    bundle_component_product_id,
    group_code,
    bundle_component_history_key,
    product_type_key,
    bundle_order_line_id,
    order_membership_classification_key,
    order_sales_channel_key,
    order_line_status_key,
    order_status_key,
    order_product_source_key,
    currency_key,
    administrator_id,
    warehouse_id,
    shipping_address_id,
    billing_address_id,
    bounceback_endowment_id,
    cost_source_id,
    cost_source,
    order_local_datetime,
    payment_transaction_local_datetime,
    shipped_local_datetime,
    order_completion_local_datetime,
    order_date_usd_conversion_rate,
    order_date_eur_conversion_rate,
    payment_transaction_date_usd_conversion_rate,
    payment_transaction_date_eur_conversion_rate,
    shipped_date_usd_conversion_rate,
    shipped_date_eur_conversion_rate,
    reporting_usd_conversion_rate,
    reporting_eur_conversion_rate,
    effective_vat_rate,
    item_quantity,
    payment_transaction_local_amount,
    subtotal_excl_tariff_local_amount,
    tariff_revenue_local_amount,
    product_subtotal_local_amount,
    tax_local_amount,
    product_discount_local_amount,
    shipping_cost_local_amount,
    shipping_discount_local_amount,
    shipping_revenue_before_discount_local_amount,
    shipping_revenue_local_amount,
    cash_credit_local_amount,
    cash_membership_credit_local_amount,
    cash_refund_credit_local_amount,
    cash_giftco_credit_local_amount,
    cash_giftcard_credit_local_amount,
    token_count,
    token_local_amount,
    cash_token_count,
    cash_token_local_amount,
    non_cash_token_count,
    non_cash_token_local_amount,
    non_cash_credit_local_amount,
    estimated_landed_cost_local_amount,
    oracle_cost_local_amount,
    lpn_po_cost_local_amount,
    po_cost_local_amount,
    misc_cogs_local_amount,
    estimated_shipping_supplies_cost_local_amount,
    estimated_shipping_cost_local_amount,
    estimated_variable_warehouse_cost_local_amount,
    estimated_variable_gms_cost_local_amount,
    estimated_variable_payment_processing_pct_cash_revenue,
    price_offered_local_amount,
    air_vip_price,
    retail_unit_price,
    bundle_price_offered_local_amount,
    product_price_history_key,
    bundle_product_price_history_key,
    item_price_key,
    amount_to_pay,
    cash_gross_revenue_local_amount,
    product_gross_revenue_local_amount,
    product_gross_revenue_excl_shipping_local_amount,
    product_margin_pre_return_local_amount,
    product_margin_pre_return_excl_shipping_local_amount,
    group_key,
    sub_group_key,
    lpn_code,
    custom_printed_text,
    reporting_landed_cost_local_amount,
    is_actual_landed_cost,
    is_fully_landed,
    fully_landed_conversion_date,
    actual_landed_cost_local_amount,
    actual_po_cost_local_amount,
    actual_cmt_cost_local_amount,
    actual_tariff_duty_cost_local_amount,
    actual_freight_cost_local_amount,
    actual_other_cost_local_amount,
    bounceback_endowment_local_amount,
    vip_endowment_local_amount,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    fol.order_line_id,
    fol.order_id,
    fol.store_id,
    fol.customer_id,
    fol.product_id,
    fol.master_product_id,
    fol.bundle_product_id,
    fol.bundle_component_product_id,
    fol.group_code,
    fol.bundle_component_history_key,
    fol.product_type_key,
    fol.bundle_order_line_id,
    fol.order_membership_classification_key,
    fol.order_sales_channel_key,
    fol.order_line_status_key,
    fol.order_status_key,
    fol.order_product_source_key,
    fol.currency_key,
    fol.administrator_id,
    fol.warehouse_id,
    fol.shipping_address_id,
    fol.billing_address_id,
    fol.bounceback_endowment_id,
    fol.cost_source_id,
    fol.cost_source,
    fol.order_local_datetime,
    fol.payment_transaction_local_datetime,
    fol.shipped_local_datetime,
    fol.order_completion_local_datetime,
    fol.order_date_usd_conversion_rate,
    fol.order_date_eur_conversion_rate,
    fol.payment_transaction_date_usd_conversion_rate,
    fol.payment_transaction_date_eur_conversion_rate,
    fol.shipped_date_usd_conversion_rate,
    fol.shipped_date_eur_conversion_rate,
    fol.reporting_usd_conversion_rate,
    fol.reporting_eur_conversion_rate,
    fol.effective_vat_rate,
    fol.item_quantity,
    fol.payment_transaction_local_amount,
    fol.subtotal_excl_tariff_local_amount,
    fol.tariff_revenue_local_amount,
    IFNULL(fol.subtotal_excl_tariff_local_amount,0) + IFNULL(fol.tariff_revenue_local_amount,0) AS product_subtotal_local_amount,
    fol.tax_local_amount,
    fol.product_discount_local_amount,
    IFF(fol.shipping_cost_local_amount =0,fol.estimated_shipping_cost_local_amount,fol.shipping_cost_local_amount) AS shipping_cost_local_amount,
    fol.shipping_discount_local_amount,
    fol.shipping_revenue_before_discount_local_amount,
    IFNULL(fol.shipping_revenue_before_discount_local_amount,0) - IFNULL(fol.shipping_discount_local_amount,0) AS shipping_revenue_local_amount,
    fol.cash_credit_local_amount,
    fol.cash_membership_credit_local_amount,
    fol.cash_refund_credit_local_amount,
    fol.cash_giftco_credit_local_amount,
    fol.cash_giftcard_credit_local_amount,
    fol.token_count,
    fol.token_local_amount,
    fol.cash_token_count,
    fol.cash_token_local_amount,
    fol.non_cash_token_count,
    fol.non_cash_token_local_amount,
    fol.non_cash_credit_local_amount,
    fol.estimated_landed_cost_local_amount,
    fol.oracle_cost_local_amount,
    fol.lpn_po_cost_local_amount,
    fol.po_cost_local_amount,
    fol.misc_cogs_local_amount,
    fol.estimated_shipping_supplies_cost_local_amount,
    fol.estimated_shipping_cost_local_amount,
    fol.estimated_variable_warehouse_cost_local_amount,
    fol.estimated_variable_gms_cost_local_amount,
    fol.estimated_variable_payment_processing_pct_cash_revenue,
    fol.price_offered_local_amount,
    fol.air_vip_price,
    fol.retail_unit_price,
    fol.bundle_price_offered_local_amount,
    fol.product_price_history_key,
    fol.bundle_product_price_history_key,
    fol.item_price_key,
    IFNULL(fol.subtotal_excl_tariff_local_amount,0)
        + IFNULL(fol.tariff_revenue_local_amount,0)
        - IFNULL(fol.product_discount_local_amount,0)
        + IFNULL(fol.tax_local_amount,0)
        + IFNULL(fol.shipping_revenue_before_discount_local_amount,0)
        - IFNULL(fol.shipping_discount_local_amount,0)
        AS amount_to_pay,
    IFF(dos.order_status = 'Pending',
        IFNULL(product_subtotal_local_amount,0)
                - IFNULL(fol.product_discount_local_amount,0)
                + IFNULL(shipping_revenue_local_amount,0)
                - IFNULL(fol.non_cash_credit_local_amount,0)
                - IFNULL(fol.cash_credit_local_amount,0)
        ,IFNULL(fol.payment_transaction_local_amount,0) - IFNULL(fol.tax_local_amount,0)) AS cash_gross_revenue_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
        IFNULL(fol.subtotal_excl_tariff_local_amount,0)
        + IFNULL(fol.tariff_revenue_local_amount,0)
        - IFNULL(fol.product_discount_local_amount,0)
        + IFNULL(fol.shipping_revenue_before_discount_local_amount,0)
        - IFNULL(fol.shipping_discount_local_amount,0)
        - IFNULL(fol.non_cash_credit_local_amount,0)
        ,0) AS product_gross_revenue_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
        IFNULL(fol.subtotal_excl_tariff_local_amount,0)
        + IFNULL(fol.tariff_revenue_local_amount,0)
        - IFNULL(fol.product_discount_local_amount,0)
        - IFNULL(fol.non_cash_credit_local_amount,0)
        ,0) AS product_gross_revenue_excl_shipping_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
        IFNULL(fol.subtotal_excl_tariff_local_amount,0)
        + IFNULL(fol.tariff_revenue_local_amount,0)
        - IFNULL(fol.product_discount_local_amount,0)
        + IFNULL(fol.shipping_revenue_before_discount_local_amount,0)
        - IFNULL(fol.shipping_discount_local_amount,0)
        - IFNULL(fol.non_cash_credit_local_amount,0)
        - IFNULL(pc.reporting_landed_cost_local_amount,0)
        - IFNULL(fol.estimated_shipping_supplies_cost_local_amount,0)
        - IFF(fol.shipping_cost_local_amount =0,fol.estimated_shipping_cost_local_amount,fol.shipping_cost_local_amount)
        ,0) AS product_margin_pre_return_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
        IFNULL(fol.subtotal_excl_tariff_local_amount,0)
        + IFNULL(fol.tariff_revenue_local_amount,0)
        - IFNULL(fol.product_discount_local_amount,0)
        - IFNULL(fol.non_cash_credit_local_amount,0)
        - IFNULL(pc.reporting_landed_cost_local_amount,0)
        ,0) AS product_margin_pre_return_excl_shipping_local_amount,
    fol.group_key,
    fol.sub_group_key,
    fol.lpn_code,
    fol.custom_printed_text,
    pc.reporting_landed_cost_local_amount,
    pc.is_actual_landed_cost,
    pc.is_fully_landed,
    pc.fully_landed_conversion_date,
    pc.actual_landed_cost_local_amount,
    pc.actual_po_cost_local_amount,
    pc.actual_cmt_cost_local_amount,
    pc.actual_tariff_duty_cost_local_amount,
    pc.actual_freight_cost_local_amount,
    pc.actual_other_cost_local_amount,
    fol.bounceback_endowment_local_amount,
    fol.vip_endowment_local_amount,
    fol.meta_create_datetime,
    fol.meta_update_datetime
FROM stg.fact_order_line fol
    LEFT JOIN stg.dim_store AS ds
        ON fol.store_id = ds.store_id
    LEFT JOIN stg.dim_order_sales_channel AS sc
        ON fol.order_sales_channel_key = sc.order_sales_channel_key
    LEFT JOIN stg.dim_order_status AS dos
        ON dos.order_status_key = fol.order_status_key
    LEFT JOIN stg.fact_order_line_product_cost pc
        ON pc.order_line_id = fol.order_line_id
WHERE NOT fol.is_deleted
    AND ds.store_brand NOT IN ('Legacy')
    AND NOT NVL(fol.is_test_customer, FALSE)
    AND NOT NVL(sc.is_test_order, FALSE)
    AND fol.product_type_key <> 67; -- embroidery

COMMIT;
