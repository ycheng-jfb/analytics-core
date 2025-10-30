truncate table EDW_PROD.NEW_STG.fact_inventory_in_transit;

insert into EDW_PROD.NEW_STG.fact_inventory_in_transit
SELECT
     od.sku AS item_id, -- 可替换为真实商品ID映射
     od.sku,
     od.fromPhysicalWarehouseCode AS from_warehouse_id,
     od.toPhysicalWarehouseCode AS to_warehouse_id,
     SUM(od.expectedInboundQuantity - od.actualInboundQuantity) AS units,
     TRUE AS is_current
   FROM
     lake.mmt.ODS_IMS_T_ON_WAY_INFO_DETAIL_DF od
   WHERE
     od.delFlag = 0 and od.PT_DAY = current_date
   GROUP BY
     od.sku,
     od.fromPhysicalWarehouseCode,
     od.toPhysicalWarehouseCode;











































select * from EDW_PROD.DATA_MODEL_JFB.FACT_ORDER;





create or replace view EDW_PROD.DATA_MODEL_JFB.FACT_ORDER(
    ORDER_ID,
    MEMBERSHIP_EVENT_KEY,
    ACTIVATION_KEY,
    FIRST_ACTIVATION_KEY,
    CURRENCY_KEY,
    CUSTOMER_ID,
    STORE_ID,
    CART_STORE_ID,
    MEMBERSHIP_BRAND_ID,
    ADMINISTRATOR_ID,
    MASTER_ORDER_ID,
    ORDER_PAYMENT_STATUS_KEY,
    ORDER_PROCESSING_STATUS_KEY,
    ORDER_MEMBERSHIP_CLASSIFICATION_KEY,
    IS_CASH,
    IS_CREDIT_BILLING_ON_RETRY,
    SHIPPING_ADDRESS_ID,
    BILLING_ADDRESS_ID,
    ORDER_STATUS_KEY,
    ORDER_SALES_CHANNEL_KEY,
    PAYMENT_KEY,
    ORDER_CUSTOMER_SELECTED_SHIPPING_KEY,
    SESSION_ID,
    BOPS_STORE_ID,
    ORDER_LOCAL_DATETIME,
    PAYMENT_TRANSACTION_LOCAL_DATETIME,
    SHIPPED_LOCAL_DATETIME,
    ORDER_COMPLETION_LOCAL_DATETIME,
    BOPS_PICKUP_LOCAL_DATETIME,
    ORDER_PLACED_LOCAL_DATETIME,
    UNIT_COUNT,
    LOYALTY_UNIT_COUNT,
    THIRD_PARTY_UNIT_COUNT,
    ORDER_DATE_USD_CONVERSION_RATE,
    ORDER_DATE_EUR_CONVERSION_RATE,
    PAYMENT_TRANSACTION_DATE_USD_CONVERSION_RATE,
    PAYMENT_TRANSACTION_DATE_EUR_CONVERSION_RATE,
    SHIPPED_DATE_USD_CONVERSION_RATE,
    SHIPPED_DATE_EUR_CONVERSION_RATE,
    REPORTING_USD_CONVERSION_RATE,
    REPORTING_EUR_CONVERSION_RATE,
    EFFECTIVE_VAT_RATE,
    PAYMENT_TRANSACTION_LOCAL_AMOUNT,
    SUBTOTAL_EXCL_TARIFF_LOCAL_AMOUNT,
    TARIFF_REVENUE_LOCAL_AMOUNT,
    PRODUCT_SUBTOTAL_LOCAL_AMOUNT,
    TAX_LOCAL_AMOUNT,
    DELIVERY_FEE_LOCAL_AMOUNT,
    CASH_CREDIT_COUNT,
    CASH_CREDIT_LOCAL_AMOUNT,
    CASH_MEMBERSHIP_CREDIT_LOCAL_AMOUNT,
    CASH_MEMBERSHIP_CREDIT_COUNT,
    CASH_REFUND_CREDIT_LOCAL_AMOUNT,
    CASH_REFUND_CREDIT_COUNT,
    CASH_GIFTCO_CREDIT_LOCAL_AMOUNT,
    CASH_GIFTCO_CREDIT_COUNT,
    CASH_GIFTCARD_CREDIT_LOCAL_AMOUNT,
    CASH_GIFTCARD_CREDIT_COUNT,
    TOKEN_LOCAL_AMOUNT,
    TOKEN_COUNT,
    CASH_TOKEN_LOCAL_AMOUNT,
    CASH_TOKEN_COUNT,
    NON_CASH_TOKEN_LOCAL_AMOUNT,
    NON_CASH_TOKEN_COUNT,
    NON_CASH_CREDIT_LOCAL_AMOUNT,
    NON_CASH_CREDIT_COUNT,
    SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,
    SHIPPING_REVENUE_LOCAL_AMOUNT,
    SHIPPING_COST_LOCAL_AMOUNT,
    SHIPPING_COST_SOURCE,
    PRODUCT_DISCOUNT_LOCAL_AMOUNT,
    SHIPPING_DISCOUNT_LOCAL_AMOUNT,
    AMOUNT_TO_PAY,
    CASH_GROSS_REVENUE_LOCAL_AMOUNT,
    PRODUCT_GROSS_REVENUE_LOCAL_AMOUNT,
    PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_LOCAL_AMOUNT,
    PRODUCT_MARGIN_PRE_RETURN_LOCAL_AMOUNT,
    PRODUCT_MARGIN_PRE_RETURN_LOCAL_AMOUNT_ACCOUNTING,
    PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_LOCAL_AMOUNT,
    PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_LOCAL_AMOUNT_ACCOUNTING,
    PRODUCT_ORDER_CASH_MARGIN_PRE_RETURN_LOCAL_AMOUNT,
    PRODUCT_ORDER_CASH_MARGIN_PRE_RETURN_LOCAL_AMOUNT_ACCOUNTING,
    ESTIMATED_LANDED_COST_LOCAL_AMOUNT,
    ESTIMATED_LANDED_COST_LOCAL_AMOUNT_ACCOUNTING,
    MISC_COGS_LOCAL_AMOUNT,
    REPORTING_LANDED_COST_LOCAL_AMOUNT,
    REPORTING_LANDED_COST_LOCAL_AMOUNT_ACCOUNTING,
    IS_ACTUAL_LANDED_COST,
    ESTIMATED_VARIABLE_GMS_COST_LOCAL_AMOUNT,
    ESTIMATED_VARIABLE_WAREHOUSE_COST_LOCAL_AMOUNT,
    ESTIMATED_VARIABLE_PAYMENT_PROCESSING_PCT_CASH_REVENUE,
    ESTIMATED_SHIPPING_SUPPLIES_COST_LOCAL_AMOUNT,
    BOUNCEBACK_ENDOWMENT_LOCAL_AMOUNT,
    VIP_ENDOWMENT_LOCAL_AMOUNT,
    META_CREATE_DATETIME,
    META_UPDATE_DATETIME
) as
SELECT
    EDW_PROD.stg.udf_unconcat_brand(fo.order_id) AS order_id,
    fo.membership_event_key,
    fo.activation_key,
    fo.first_activation_key,
    fo.currency_key,
    EDW_PROD.stg.udf_unconcat_brand(fo.customer_id) AS customer_id,
    fo.store_id,
    fo.cart_store_id,
    fo.membership_brand_id,
    fo.administrator_id,
    EDW_PROD.stg.udf_unconcat_brand(fo.master_order_id) AS master_order_id,
    fo.order_payment_status_key,
    fo.order_processing_status_key,
    fo.order_membership_classification_key,
    fo.is_cash,
    fo.is_credit_billing_on_retry,
    EDW_PROD.stg.udf_unconcat_brand(fo.shipping_address_id) AS shipping_address_id,
    EDW_PROD.stg.udf_unconcat_brand(fo.billing_address_id) AS billing_address_id,
    fo.order_status_key,
    fo.order_sales_channel_key,
    fo.payment_key,
    fo.order_customer_selected_shipping_key,
    EDW_PROD.stg.udf_unconcat_brand(fo.session_id) AS session_id,
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
            + IFNULL(fo.shipping_revenue_before_discount_local_amount,0)
            - IFNULL(fo.shipping_discount_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
            - IFNULL(fo.reporting_landed_cost_local_amount_accounting,0)
            - IFNULL(fo.estimated_shipping_supplies_cost_local_amount,0)
            - IFNULL(IFF(shipping_cost_local_amount =0,fo.estimated_shipping_cost_local_amount,fo.shipping_cost_local_amount),0)
    ,0) AS product_margin_pre_return_local_amount_accounting,
    IFF(sc.order_classification_l1 = 'Product Order',
    IFNULL(fo.subtotal_excl_tariff_local_amount,0)
            + IFNULL(fo.tariff_revenue_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
            - IFNULL(fo.reporting_landed_cost_local_amount,0)
    ,0) AS product_margin_pre_return_excl_shipping_local_amount,
    IFF(sc.order_classification_l1 = 'Product Order',
    IFNULL(fo.subtotal_excl_tariff_local_amount,0)
            + IFNULL(fo.tariff_revenue_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
            - IFNULL(fo.reporting_landed_cost_local_amount_accounting,0)
    ,0) AS product_margin_pre_return_excl_shipping_local_amount_accounting,
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
    IFF(sc.order_classification_l1 = 'Product Order' AND dos.order_status = 'Pending',
    IFNULL(product_subtotal_local_amount,0)
            - IFNULL(fo.shipping_discount_local_amount,0)
            - IFNULL(fo.product_discount_local_amount,0)
            + IFNULL(shipping_revenue_local_amount,0)
            - IFNULL(fo.non_cash_credit_local_amount,0)
            - IFNULL(fo.cash_credit_local_amount,0)
            - IFNULL(fo.estimated_landed_cost_local_amount_accounting,0)
            - IFNULL(fo.estimated_shipping_supplies_cost_local_amount,0)
            - IFF(shipping_cost_local_amount =0,fo.estimated_shipping_cost_local_amount,fo.shipping_cost_local_amount),
    IFF(sc.order_classification_l1 = 'Product Order' AND dos.order_status = 'Success',
    cash_gross_revenue_local_amount
            - IFNULL(fo.reporting_landed_cost_local_amount_accounting,0)
            - IFNULL(fo.estimated_shipping_supplies_cost_local_amount,0)
            - IFF(shipping_cost_local_amount =0,fo.estimated_shipping_cost_local_amount,fo.shipping_cost_local_amount),0)) AS product_order_cash_margin_pre_return_local_amount_accounting,
    fo.estimated_landed_cost_local_amount,
    fo.estimated_landed_cost_local_amount_accounting,
    fo.misc_cogs_local_amount,
    fo.reporting_landed_cost_local_amount,
    fo.reporting_landed_cost_local_amount_accounting,
    fo.is_actual_landed_cost,
    fo.estimated_variable_gms_cost_local_amount,
    fo.estimated_variable_warehouse_cost_local_amount,
    fo.estimated_variable_payment_processing_pct_cash_revenue,
    fo.estimated_shipping_supplies_cost_local_amount,
    fo.bounceback_endowment_local_amount,
    fo.vip_endowment_local_amount,
    fo.meta_create_datetime,
    fo.meta_update_datetime
FROM EDW_PROD.stg.fact_order AS fo
    LEFT JOIN EDW_PROD.stg.dim_store AS ds
        ON fo.store_id = ds.store_id
    LEFT JOIN EDW_PROD.stg.dim_order_sales_channel AS sc
        ON fo.order_sales_channel_key = sc.order_sales_channel_key
    JOIN data_model_jfb.dim_order_status AS dos
       ON fo.order_status_key = dos.order_status_key
WHERE NOT fo.is_deleted
    AND ds.store_brand NOT IN ('Legacy')
    AND NOT NVL(fo.is_test_customer,False)
    AND NOT NVL(sc.is_test_order,False)
    AND (substring(order_id, -2) = '10' OR order_id = -1)
    and order_local_datetime<'2025-09-15'

union all

select
    ORDER_ID,
    MEMBERSHIP_EVENT_KEY,
    ACTIVATION_KEY,
    FIRST_ACTIVATION_KEY,
    1 as CURRENCY_KEY,
    CUSTOMER_ID,
    STORE_ID,
    CART_STORE_ID,
    MEMBERSHIP_BRAND_ID,
    ADMINISTRATOR_ID,
    MASTER_ORDER_ID,
    ORDER_PAYMENT_STATUS_KEY,
    ORDER_PROCESSING_STATUS_KEY,
    ORDER_MEMBERSHIP_CLASSIFICATION_KEY,
    IS_CASH,
    IS_CREDIT_BILLING_ON_RETRY,
    SHIPPING_ADDRESS_ID,
    BILLING_ADDRESS_ID,
    ORDER_STATUS_KEY,
    ORDER_SALES_CHANNEL_KEY,
    PAYMENT_KEY,
    ORDER_CUSTOMER_SELECTED_SHIPPING_KEY,
    SESSION_ID,
    BOPS_STORE_ID,
    -- cast(ORDER_LOCAL_DATETIME as TIMESTAMP_LTZ(9)),
    ORDER_LOCAL_DATETIME,
    PAYMENT_TRANSACTION_LOCAL_DATETIME,
    SHIPPED_LOCAL_DATETIME,
    ORDER_COMPLETION_LOCAL_DATETIME,
    BOPS_PICKUP_LOCAL_DATETIME,
    ORDER_PLACED_LOCAL_DATETIME,
    UNIT_COUNT,
    LOYALTY_UNIT_COUNT,
    THIRD_PARTY_UNIT_COUNT,
    ORDER_DATE_USD_CONVERSION_RATE,
    ORDER_DATE_EUR_CONVERSION_RATE,
    PAYMENT_TRANSACTION_DATE_USD_CONVERSION_RATE,
    PAYMENT_TRANSACTION_DATE_EUR_CONVERSION_RATE,
    SHIPPED_DATE_USD_CONVERSION_RATE,
    SHIPPED_DATE_EUR_CONVERSION_RATE,
    REPORTING_USD_CONVERSION_RATE,
    REPORTING_EUR_CONVERSION_RATE,
    EFFECTIVE_VAT_RATE,
    PAYMENT_TRANSACTION_LOCAL_AMOUNT,
    SUBTOTAL_EXCL_TARIFF_LOCAL_AMOUNT,
    TARIFF_REVENUE_LOCAL_AMOUNT,
    PRODUCT_SUBTOTAL_LOCAL_AMOUNT,
    TAX_LOCAL_AMOUNT,
    DELIVERY_FEE_LOCAL_AMOUNT,
    CASH_CREDIT_COUNT,
    CASH_CREDIT_LOCAL_AMOUNT,
    CASH_MEMBERSHIP_CREDIT_LOCAL_AMOUNT,
    CASH_MEMBERSHIP_CREDIT_COUNT,
    CASH_REFUND_CREDIT_LOCAL_AMOUNT,
    CASH_REFUND_CREDIT_COUNT,
    CASH_GIFTCO_CREDIT_LOCAL_AMOUNT,
    CASH_GIFTCO_CREDIT_COUNT,
    CASH_GIFTCARD_CREDIT_LOCAL_AMOUNT,
    CASH_GIFTCARD_CREDIT_COUNT,
    TOKEN_LOCAL_AMOUNT,
    TOKEN_COUNT,
    CASH_TOKEN_LOCAL_AMOUNT,
    CASH_TOKEN_COUNT,
    NON_CASH_TOKEN_LOCAL_AMOUNT,
    NON_CASH_TOKEN_COUNT,
    NON_CASH_CREDIT_LOCAL_AMOUNT,
    NON_CASH_CREDIT_COUNT,
    SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,
    SHIPPING_REVENUE_LOCAL_AMOUNT,
    SHIPPING_COST_LOCAL_AMOUNT,
    SHIPPING_COST_SOURCE,
    PRODUCT_DISCOUNT_LOCAL_AMOUNT,
    SHIPPING_DISCOUNT_LOCAL_AMOUNT,
    AMOUNT_TO_PAY,
    CASH_GROSS_REVENUE_LOCAL_AMOUNT,
    PRODUCT_GROSS_REVENUE_LOCAL_AMOUNT,
    PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_LOCAL_AMOUNT,
    PRODUCT_MARGIN_PRE_RETURN_LOCAL_AMOUNT,
    null as PRODUCT_MARGIN_PRE_RETURN_LOCAL_AMOUNT_ACCOUNTING,
    PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_LOCAL_AMOUNT,
    null as PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_LOCAL_AMOUNT_ACCOUNTING,
    PRODUCT_ORDER_CASH_MARGIN_PRE_RETURN_LOCAL_AMOUNT,
    null as PRODUCT_ORDER_CASH_MARGIN_PRE_RETURN_LOCAL_AMOUNT_ACCOUNTING,
    ESTIMATED_LANDED_COST_LOCAL_AMOUNT,
    null as ESTIMATED_LANDED_COST_LOCAL_AMOUNT_ACCOUNTING,
    MISC_COGS_LOCAL_AMOUNT,
    REPORTING_LANDED_COST_LOCAL_AMOUNT,
    null as REPORTING_LANDED_COST_LOCAL_AMOUNT_ACCOUNTING,
    IS_ACTUAL_LANDED_COST,
    ESTIMATED_VARIABLE_GMS_COST_LOCAL_AMOUNT,
    ESTIMATED_VARIABLE_WAREHOUSE_COST_LOCAL_AMOUNT,
    ESTIMATED_VARIABLE_PAYMENT_PROCESSING_PCT_CASH_REVENUE,
    ESTIMATED_SHIPPING_SUPPLIES_COST_LOCAL_AMOUNT,
    BOUNCEBACK_ENDOWMENT_LOCAL_AMOUNT,
    VIP_ENDOWMENT_LOCAL_AMOUNT,
    cast(META_CREATE_DATETIME as TIMESTAMP_LTZ(9)),
    cast(META_UPDATE_DATETIME as TIMESTAMP_LTZ(9))
    from EDW_PROD.NEW_STG.fact_order
    where order_local_datetime>='2025-09-15'
    ;

