CREATE OR REPLACE VIEW data_model_sxf.fact_order (
    order_id,
    membership_event_key,
    activation_key,
    first_activation_key,
    currency_key,
    customer_id,
    store_id,
    cart_store_id,
    membership_brand_id,
    administrator_id,
    master_order_id,
    order_payment_status_key,
    order_processing_status_key,
    order_membership_classification_key,
    is_cash,
    is_credit_billing_on_retry,
    shipping_address_id,
    billing_address_id,
    order_status_key,
    order_sales_channel_key,
    payment_key,
    order_customer_selected_shipping_key,
    session_id,
    bops_store_id,
    order_local_datetime,
    payment_transaction_local_datetime,
    shipped_local_datetime,
    order_completion_local_datetime,
    bops_pickup_local_datetime,
    order_placed_local_datetime,
    unit_count,
    loyalty_unit_count,
    third_party_unit_count,
    order_date_usd_conversion_rate,
    order_date_eur_conversion_rate,
    payment_transaction_date_usd_conversion_rate,
    payment_transaction_date_eur_conversion_rate,
    shipped_date_usd_conversion_rate,
    shipped_date_eur_conversion_rate,
    reporting_usd_conversion_rate,
    reporting_eur_conversion_rate,
    effective_vat_rate,
    payment_transaction_local_amount,
    subtotal_excl_tariff_local_amount,
    tariff_revenue_local_amount,
    product_subtotal_local_amount,
    tax_local_amount,
    delivery_fee_local_amount,
    cash_credit_count,
    cash_credit_local_amount,
    cash_membership_credit_local_amount,
    cash_membership_credit_count,
    cash_refund_credit_local_amount,
    cash_refund_credit_count,
    cash_giftco_credit_local_amount,
    cash_giftco_credit_count,
    cash_giftcard_credit_local_amount,
    cash_giftcard_credit_count,
    token_local_amount,
    token_count,
    cash_token_local_amount,
    cash_token_count,
    non_cash_token_local_amount,
    non_cash_token_count,
    non_cash_credit_local_amount,
    non_cash_credit_count,
    shipping_revenue_before_discount_local_amount,
    shipping_revenue_local_amount,
    shipping_cost_local_amount,
    shipping_cost_source,
    product_discount_local_amount,
    shipping_discount_local_amount,
    amount_to_pay,
    cash_gross_revenue_local_amount,
    product_gross_revenue_local_amount,
    product_gross_revenue_excl_shipping_local_amount,
    product_margin_pre_return_local_amount,
    product_margin_pre_return_excl_shipping_local_amount,
    product_order_cash_margin_pre_return_local_amount,
    estimated_landed_cost_local_amount,
    misc_cogs_local_amount,
    reporting_landed_cost_local_amount,
    is_actual_landed_cost,
    estimated_variable_gms_cost_local_amount,
    estimated_variable_warehouse_cost_local_amount,
    estimated_variable_payment_processing_pct_cash_revenue,
    estimated_shipping_supplies_cost_local_amount,
    bounceback_endowment_local_amount,
    vip_endowment_local_amount,
    meta_create_datetime,
    meta_update_datetime
)
AS
SELECT
    stg.udf_unconcat_brand(fo.order_id) AS order_id,
    fo.membership_event_key,
    fo.activation_key,
    fo.first_activation_key,
    fo.currency_key,
    stg.udf_unconcat_brand(fo.customer_id) AS customer_id,
    fo.store_id,
    fo.cart_store_id,
    fo.membership_brand_id,
    fo.administrator_id,
    stg.udf_unconcat_brand(fo.master_order_id) AS master_order_id,
    fo.order_payment_status_key,
    fo.order_processing_status_key,
    fo.order_membership_classification_key,
    fo.is_cash,
    fo.is_credit_billing_on_retry,
    stg.udf_unconcat_brand(fo.shipping_address_id) AS shipping_address_id,
    stg.udf_unconcat_brand(fo.billing_address_id) AS billing_address_id,
    fo.order_status_key,
    fo.order_sales_channel_key,
    fo.payment_key,
    fo.order_customer_selected_shipping_key,
    stg.udf_unconcat_brand(fo.session_id) AS session_id,
    fo.bops_store_id,
    fo.order_local_datetime,
    fo.payment_transaction_local_datetime,
    fo.shipped_local_datetime,
    fo.order_completion_local_datetime,
    fo.bops_pickup_local_datetime,
    fo.order_placed_local_datetime,
    fo.unit_count,
    fo.loyalty_unit_count,
    fo.third_party_unit_count,
    fo.order_date_usd_conversion_rate,
    fo.order_date_eur_conversion_rate,
    fo.payment_transaction_date_usd_conversion_rate,
    fo.payment_transaction_date_eur_conversion_rate,
    fo.shipped_date_usd_conversion_rate,
    fo.shipped_date_eur_conversion_rate,
    fo.reporting_usd_conversion_rate,
    fo.reporting_eur_conversion_rate,
    fo.effective_vat_rate,
    fo.payment_transaction_local_amount,
    fo.subtotal_excl_tariff_local_amount,
    fo.tariff_revenue_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',IFNULL(fo.subtotal_excl_tariff_local_amount,0) + IFNULL(tariff_revenue_local_amount,0),0) AS product_subtotal_local_amount,
    fo.tax_local_amount,
    fo.delivery_fee_local_amount,
    fo.cash_credit_count,
    fo.cash_credit_local_amount,
    fo.cash_membership_credit_local_amount,
    fo.cash_membership_credit_count,
    fo.cash_refund_credit_local_amount,
    fo.cash_refund_credit_count,
    fo.cash_giftco_credit_local_amount,
    fo.cash_giftco_credit_count,
    fo.cash_giftcard_credit_local_amount,
    fo.cash_giftcard_credit_count,
    fo.token_local_amount,
    fo.token_count,
    fo.cash_token_local_amount,
    fo.cash_token_count,
    fo.non_cash_token_local_amount,
    fo.non_cash_token_count,
    fo.non_cash_credit_local_amount,
    fo.non_cash_credit_count,
    fo.shipping_revenue_before_discount_local_amount,
    IFNULL(fo.shipping_revenue_before_discount_local_amount,0) - IFNULL(fo.shipping_discount_local_amount,0) AS shipping_revenue_local_amount,
    IFF(shipping_cost_local_amount =0,fo.estimated_shipping_cost_local_amount,fo.shipping_cost_local_amount) AS shipping_cost_local_amount,
    CASE WHEN ds.store_type ='Retail' AND is_retail_ship_only_order = FALSE THEN 'Actual'
        WHEN shipping_cost_local_amount = 0 THEN 'Estimate'
        ELSE 'Actual' END AS shipping_cost_source,
    fo.product_discount_local_amount,
    fo.shipping_discount_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
    IFNULL(fo.subtotal_excl_tariff_local_amount,0)
            + IFNULL(fo.tariff_revenue_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            + IFNULL(fo.tax_local_amount,0)
            + IFNULL(fo.shipping_revenue_before_discount_local_amount,0)
            - IFNULL(fo.shipping_discount_local_amount,0)
    ,0) AS amount_to_pay,
    IFF(dos.order_status = 'Pending',
    IFNULL(product_subtotal_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            + IFNULL(shipping_revenue_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
            - IFNULL(fo.cash_credit_local_amount,0)
    ,IFNULL(fo.payment_transaction_local_amount,0) - IFNULL(fo.tax_local_amount,0)) AS cash_gross_revenue_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
    IFNULL(fo.subtotal_excl_tariff_local_amount,0)
            + IFNULL(fo.tariff_revenue_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            + IFNULL(fo.shipping_revenue_before_discount_local_amount,0)
            - IFNULL(fo.shipping_discount_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
    ,0) AS product_gross_revenue_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
    IFNULL(fo.subtotal_excl_tariff_local_amount,0)
            + IFNULL(fo.tariff_revenue_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
    ,0) AS product_gross_revenue_excl_shipping_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
    IFNULL(fo.subtotal_excl_tariff_local_amount,0)
            + IFNULL(fo.tariff_revenue_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            + IFNULL(fo.shipping_revenue_before_discount_local_amount,0)
            - IFNULL(fo.shipping_discount_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
            - IFNULL(fo.reporting_landed_cost_local_amount,0)
            - IFNULL(fo.estimated_shipping_supplies_cost_local_amount,0)
            - IFNULL(IFF(shipping_cost_local_amount =0,fo.estimated_shipping_cost_local_amount,fo.shipping_cost_local_amount),0)
    ,0) AS product_margin_pre_return_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
    IFNULL(fo.subtotal_excl_tariff_local_amount,0)
            + IFNULL(fo.tariff_revenue_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
            - IFNULL(fo.reporting_landed_cost_local_amount,0)
    ,0) AS product_margin_pre_return_excl_shipping_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order' AND dos.order_status = 'Pending',
    IFNULL(product_subtotal_local_amount,0)
            - IFNULL(fo.shipping_discount_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            + IFNULL(shipping_revenue_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
            - IFNULL(fo.cash_credit_local_amount,0)
            - IFNULL(fo.estimated_landed_cost_local_amount,0)
            - IFNULL(fo.estimated_shipping_supplies_cost_local_amount,0)
            - IFF(shipping_cost_local_amount =0,fo.estimated_shipping_cost_local_amount,fo.shipping_cost_local_amount),
    IFF(sc.order_classification_l1 = 'Product Order' AND dos.order_status = 'Success',
    cash_gross_revenue_local_amount
            - IFNULL(fo.reporting_landed_cost_local_amount,0)
            - IFNULL(fo.estimated_shipping_supplies_cost_local_amount,0)
            - IFF(shipping_cost_local_amount =0,fo.estimated_shipping_cost_local_amount,fo.shipping_cost_local_amount),0)) AS product_order_cash_margin_pre_return_local_amount,
    fo.estimated_landed_cost_local_amount,
    fo.misc_cogs_local_amount,
    fo.reporting_landed_cost_local_amount,
    fo.is_actual_landed_cost,
    fo.estimated_variable_gms_cost_local_amount,
    fo.estimated_variable_warehouse_cost_local_amount,
    fo.estimated_variable_payment_processing_pct_cash_revenue,
    fo.estimated_shipping_supplies_cost_local_amount,
    fo.bounceback_endowment_local_amount,
    fo.vip_endowment_local_amount,
    fo.meta_create_datetime,
    fo.meta_update_datetime
FROM stg.fact_order AS fo
    LEFT JOIN stg.dim_store AS ds
        ON fo.store_id = ds.store_id
    LEFT JOIN stg.dim_order_sales_channel AS sc
        ON fo.order_sales_channel_key = sc.order_sales_channel_key
    JOIN data_model_sxf.dim_order_status AS dos
       ON fo.order_status_key = dos.order_status_key
WHERE NOT fo.is_deleted
    AND ds.store_brand NOT IN ('Legacy')
    AND NOT NVL(fo.is_test_customer,False)
    AND NOT NVL(sc.is_test_order,False)
    AND (substring(order_id, -2) = '30' OR order_id = -1) ;
