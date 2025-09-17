CREATE OR REPLACE TEMP TABLE _PAYMENT_METHOD_BY_DAY_stg AS
SELECT
    order_local_date
     ,ship_local_date
     ,brand
     ,region
     ,country
     ,STORE_ID
     ,store
     ,store_type_sales_channel AS STORE_TYPE
     ,original_activating_store_id
     ,original_activating_store
     ,original_activating_store_type
     ,current_membership_type
     ,customer_gender
     ,session_start_membership_state
     ,PLATFORM
     ,os
     ,browser
     ,order_status
     ,CASE WHEN ORDER_STATUS in ('Success','Pending') THEN TRUE ELSE FALSE END is_successful_pending_order
     ,ORDER_SALES_CHANNEL_L1
     ,ORDER_SALES_CHANNEL_L2
     ,ORDER_CLASSIFICATION_L1
     ,ORDER_CLASSIFICATION_L2
     ,MEMBERSHIP_ORDER_TYPE_L3
     ,IS_CASH
     ,IS_PREPAID_CREDITCARD
     ,payment_type_adjusted AS payment_type
     ,digital_wallet_payment_type_adjusted AS digital_wallet_payment_type
     ,payment_method_type_adjusted AS payment_method_type
     ,credit_card_type_adjusted AS credit_card_type
     ,CUSTOMER_SELECTED_SHIPPING_TYPE
     ,CUSTOMER_SELECTED_SHIPPING_SERVICE

     ,count(distinct ORDER_ID) AS orders_total
     ,sum(ttl_units) AS units_total
     ,sum(ttl_product_gross_revenue) AS product_revenue_total
     ,sum(ttl_product_gross_revenue_excl_shipping) AS product_revenue_excl_shipping_total
     ,sum(ttl_product_margin_pre_return) AS product_margin_pre_return_total
     ,sum(ttl_product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping_total
     ,sum(ttl_cash_gross_revenue) AS cash_gross_revenue_total

     ,count(distinct CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' THEN ORDER_ID END) AS orders_activating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' THEN ttl_units END) AS units_activating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' THEN ttl_product_gross_revenue END)
            AS product_revenue_activating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' THEN ttl_product_gross_revenue_excl_shipping END)
            AS product_revenue_excl_shipping_activating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' THEN ttl_product_margin_pre_return END)
            AS product_margin_pre_return_activating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' THEN ttl_product_margin_pre_return_excl_shipping END)
            AS product_margin_pre_return_excl_shipping_activating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' THEN ttl_cash_gross_revenue END)
            AS cash_gross_revenue_activating

     ,count(distinct CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 != 'Activating VIP' THEN ORDER_ID END) AS orders_nonactivating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 != 'Activating VIP' THEN ttl_units END) AS units_nonactivating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 != 'Activating VIP' THEN ttl_product_gross_revenue END)
            AS product_revenue_nonactivating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 != 'Activating VIP' THEN ttl_product_gross_revenue_excl_shipping END)
            AS product_revenue_excl_shipping_nonactivating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 != 'Activating VIP' THEN ttl_product_margin_pre_return END)
            AS product_margin_pre_return_nonactivating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 != 'Activating VIP' THEN ttl_product_margin_pre_return_excl_shipping END)
            AS product_margin_pre_return_excl_shipping_nonactivating
     ,sum(CASE WHEN MEMBERSHIP_ORDER_TYPE_L3 !='Activating VIP'  THEN ttl_cash_gross_revenue END)
            AS cash_gross_revenue_nonactivating
     ,current_timestamp AS datetime_added
     ,current_timestamp AS datetime_modified
FROM REPORTING_BASE_PROD.SHARED.order_line_psource_payment
WHERE
    ttl_order_rnk = 1
    and order_local_date >= dateadd(Day,-30,current_date)
GROUP BY
        order_local_date,
        ship_local_date,
        brand,region,
        country,
        STORE_ID,
        store,
        store_type_sales_channel ,
        original_activating_store_id,
        original_activating_store,
        original_activating_store_type,
        current_membership_type,
        customer_gender,
        session_start_membership_state,
        PLATFORM,
        os,
        browser,
        order_status,
        CASE WHEN ORDER_STATUS in ('Success','Pending') THEN TRUE ELSE FALSE END,
        ORDER_SALES_CHANNEL_L1,
        ORDER_SALES_CHANNEL_L2,
        ORDER_CLASSIFICATION_L1,
        ORDER_CLASSIFICATION_L2,
        MEMBERSHIP_ORDER_TYPE_L3,
        IS_CASH,
        IS_PREPAID_CREDITCARD,
        payment_type_adjusted ,
        digital_wallet_payment_type_adjusted ,
        payment_method_type_adjusted ,
        credit_card_type_adjusted ,
        CUSTOMER_SELECTED_SHIPPING_TYPE,
        CUSTOMER_SELECTED_SHIPPING_SERVICE;

DELETE FROM REPORTING_PROD.SHARED.PAYMENT_METHOD_BY_DAY WHERE order_local_date >= dateadd(Day,-30,current_date);

INSERT INTO REPORTING_PROD.SHARED.PAYMENT_METHOD_BY_DAY (
        ORDER_LOCAL_DATE
        ,SHIP_LOCAL_DATE
        ,BRAND
        ,REGION
        ,COUNTRY
        ,STORE_ID
        ,STORE
        ,STORE_TYPE
        ,ORIGINAL_ACTIVATING_STORE_ID
        ,ORIGINAL_ACTIVATING_STORE
        ,ORIGINAL_ACTIVATING_STORE_TYPE
        ,CURRENT_MEMBERSHIP_TYPE
        ,CUSTOMER_GENDER
        ,SESSION_START_MEMBERSHIP_STATE
        ,PLATFORM
        ,OS
        ,BROWSER
        ,ORDER_STATUS
        ,IS_SUCCESSFUL_PENDING_ORDER
        ,ORDER_SALES_CHANNEL_L1
        ,ORDER_SALES_CHANNEL_L2
        ,ORDER_CLASSIFICATION_L1
        ,ORDER_CLASSIFICATION_L2
        ,MEMBERSHIP_ORDER_TYPE_L3
        ,IS_CASH
        ,IS_PREPAID_CREDITCARD
        ,PAYMENT_TYPE
        ,DIGITAL_WALLET_PAYMENT_TYPE
        ,PAYMENT_METHOD_TYPE
        ,CREDIT_CARD_TYPE
        ,CUSTOMER_SELECTED_SHIPPING_TYPE
        ,CUSTOMER_SELECTED_SHIPPING_SERVICE
        ,ORDERS_TOTAL
        ,UNITS_TOTAL
        ,PRODUCT_REVENUE_TOTAL
        ,PRODUCT_REVENUE_EXCL_SHIPPING_TOTAL
        ,PRODUCT_MARGIN_PRE_RETURN_TOTAL
        ,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_TOTAL
        ,CASH_GROSS_REVENUE_TOTAL
        ,ORDERS_ACTIVATING
        ,UNITS_ACTIVATING
        ,PRODUCT_REVENUE_ACTIVATING
        ,PRODUCT_REVENUE_EXCL_SHIPPING_ACTIVATING
        ,PRODUCT_MARGIN_PRE_RETURN_ACTIVATING
        ,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_ACTIVATING
        ,CASH_GROSS_REVENUE_ACTIVATING
        ,ORDERS_NONACTIVATING
        ,UNITS_NONACTIVATING
        ,PRODUCT_REVENUE_NONACTIVATING
        ,PRODUCT_REVENUE_EXCL_SHIPPING_NONACTIVATING
        ,PRODUCT_MARGIN_PRE_RETURN_NONACTIVATING
        ,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_NONACTIVATING
        ,CASH_GROSS_REVENUE_NONACTIVATING
        ,DATETIME_ADDED
        ,DATETIME_MODIFIED )
SELECT ORDER_LOCAL_DATE
        ,SHIP_LOCAL_DATE
        ,BRAND
        ,REGION
        ,COUNTRY
        ,STORE_ID
        ,STORE
        ,STORE_TYPE
        ,ORIGINAL_ACTIVATING_STORE_ID
        ,ORIGINAL_ACTIVATING_STORE
        ,ORIGINAL_ACTIVATING_STORE_TYPE
        ,CURRENT_MEMBERSHIP_TYPE
        ,CUSTOMER_GENDER
        ,SESSION_START_MEMBERSHIP_STATE
        ,PLATFORM
        ,OS
        ,BROWSER
        ,ORDER_STATUS
        ,IS_SUCCESSFUL_PENDING_ORDER
        ,ORDER_SALES_CHANNEL_L1
        ,ORDER_SALES_CHANNEL_L2
        ,ORDER_CLASSIFICATION_L1
        ,ORDER_CLASSIFICATION_L2
        ,MEMBERSHIP_ORDER_TYPE_L3
        ,IS_CASH
        ,IS_PREPAID_CREDITCARD
        ,PAYMENT_TYPE
        ,DIGITAL_WALLET_PAYMENT_TYPE
        ,PAYMENT_METHOD_TYPE
        ,CREDIT_CARD_TYPE
        ,CUSTOMER_SELECTED_SHIPPING_TYPE
        ,CUSTOMER_SELECTED_SHIPPING_SERVICE
        ,ORDERS_TOTAL
        ,UNITS_TOTAL
        ,PRODUCT_REVENUE_TOTAL
        ,PRODUCT_REVENUE_EXCL_SHIPPING_TOTAL
        ,PRODUCT_MARGIN_PRE_RETURN_TOTAL
        ,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_TOTAL
        ,CASH_GROSS_REVENUE_TOTAL
        ,ORDERS_ACTIVATING
        ,UNITS_ACTIVATING
        ,PRODUCT_REVENUE_ACTIVATING
        ,PRODUCT_REVENUE_EXCL_SHIPPING_ACTIVATING
        ,PRODUCT_MARGIN_PRE_RETURN_ACTIVATING
        ,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_ACTIVATING
        ,CASH_GROSS_REVENUE_ACTIVATING
        ,ORDERS_NONACTIVATING
        ,UNITS_NONACTIVATING
        ,PRODUCT_REVENUE_NONACTIVATING
        ,PRODUCT_REVENUE_EXCL_SHIPPING_NONACTIVATING
        ,PRODUCT_MARGIN_PRE_RETURN_NONACTIVATING
        ,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_NONACTIVATING
        ,CASH_GROSS_REVENUE_NONACTIVATING
        ,DATETIME_ADDED
        ,DATETIME_MODIFIED  FROM _PAYMENT_METHOD_BY_DAY_stg;
