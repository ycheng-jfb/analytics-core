CREATE OR REPLACE temporary TABLE _order_base AS
SELECT DISTINCT fo.ORDER_ID,fo.STORE_ID,fo.CUSTOMER_ID,PAYMENT_KEY,ACTIVATION_KEY
from edw_prod.DATA_MODEL.FACT_ORDER AS fo
join edw_prod.DATA_MODEL.DIM_STORE AS s on S.STORE_ID = fo.STORE_ID
join edw_prod.DATA_MODEL.DIM_ORDER_STATUS AS t on t.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
join edw_prod.DATA_MODEL.DIM_ORDER_SALES_CHANNEL AS c on c.ORDER_SALES_CHANNEL_KEY = fo.ORDER_SALES_CHANNEL_KEY
where
    t.order_status IN ('Success','Pending','Cancelled','Failure')
    AND ORDER_CLASSIFICATION_L1 IN ('Billing Order','Product Order')
    AND cast(fo.ORDER_LOCAL_DATETIME AS date) >=dateadd(Day,-30,current_date); --comment
  --     and cast(fo.ORDER_LOCAL_DATETIME AS date) >= '2022-01-01'; --comment

ALTER TABLE _order_base SET DATA_RETENTION_TIME_IN_DAYS = 0;--comment

CREATE OR REPLACE TEMPORARY TABLE _fl_ubt_product_attributes as
SELECT DISTINCT
    product_id
   ,CASE WHEN ubt.item_status ILIKE '%CORE' THEN 'CORE' ELSE 'FASHION' END as product_lifestyle_type
    ,ubt.sub_brand as product_sub_brand
    ,gender as product_gender
    ,CASE WHEN sub_brand = 'Scrubs' and gender ILIKE 'MEN%' THEN 'SC-M-PROD'
        WHEN sub_brand = 'Scrubs'  THEN 'SC-W-PROD'
        WHEN sub_brand = 'Fabletics' and gender ILIKE 'MEN%' THEN 'FL-M-PROD'
        WHEN sub_brand = 'Fabletics'  THEN 'FL-W-PROD'
        WHEN SUB_BRAND = 'Yitty' THEN 'YT-PROD' END as product_segment

    ,ubt.CURRENT_SHOWROOM as product_ubt_showroom
    ,ubt.CURRENT_NAME as product_name
    ,ubt.COLOR as product_color
    ,ubt.category as product_category
    ,ubt.class as product_class
    ,ubt.subclass as product_subclass
    ,ubt.INITIAL_LAUNCH as product_special_collection
FROM (select distinct product_id, dp.product_sku,IMAGE_URL from edw_prod.data_model.dim_product dp) dp
LEFT JOIN
    (SELECT DISTINCT ubt.*
        from lake.excel.fl_items_ubt ubt
join
    (select sku, max(current_showroom) as max_current_showroom
        from lake.excel.fl_items_ubt group by 1) as ms on ubt.sku = ms.sku and ubt.current_showroom = ms.max_current_showroom
            ) ubt on dp.product_sku = ubt.sku;

CREATE OR REPLACE TEMPORARY TABLE _byo_bundles AS -- to get SXF BYO bundles only, not 'regular' bundles
SELECT DISTINCT
    PRODUCT_ID AS byo_bundle_product_id
    ,p.LABEL AS byo_bundle_name
FROM LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.product AS p
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.product_type AS t on t.product_type_id = p.product_type_id
WHERE
    t.LABEL = 'BYO'
    AND p.LABEL NOT ilike 'FND%';

CREATE OR REPLACE TEMPORARY TABLE _eu_payment_base AS
SELECT
    o.order_id
     ,STORE_COUNTRY
     ,ptp.datetime_added
     ,creditcard_type
     ,p.type AS psp_creditcard_type
     ,rank() over (partition by o.customer_id, o.order_id order by ptp.datetime_added desc) AS rnk
FROM _order_base AS o
JOIN EDW_prod.DATA_MODEL.DIM_PAYMENT AS pay on pay.PAYMENT_KEY = o.PAYMENT_KEY
JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.payment_transaction_psp AS ptp on o.order_id = ptp.order_ID
JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.psp AS p on ptp.psp_id = p.psp_id
JOIN EDW_prod.DATA_MODEL.DIM_STORE AS st on st.STORE_ID = o.STORE_ID
where STORE_REGION = 'EU';

DELETE FROM _eu_payment_base WHERE rnk <> 1;

CREATE OR REPLACE TEMPORARY TABLE _eu_payment_adjusted AS
SELECT
    STORE_COUNTRY
     ,order_id
     ,case
     WHEN psp_creditcard_type ilike '%sepa%' THEN 'Sepa'
     WHEN psp_creditcard_type ilike '%ideal%' THEN 'Ideal'
     ELSE INITCAP(creditcard_type) END AS creditcard_type_clean
FROM _eu_payment_base;

CREATE OR REPLACE temp table _promo_code AS
SELECT DISTINCT
    listagg(distinct promo_code,'|') WITHIN GROUP (ORDER BY promo_code) as promo_code
    ,listagg(distinct promo_name,'|') WITHIN GROUP (ORDER BY promo_name) AS PROMO_NAME
    ,order_line_id
FROM _order_base as o
join EDW_PROD.DATA_MODEL.FACT_ORDER_LINE_DISCOUNT old on old.order_id = o.order_id
LEFT JOIN EDW_PROD.DATA_MODEL.DIM_PROMO_HISTORY ph ON old.promo_history_key = ph.promo_history_key
GROUP BY order_line_id;

CREATE OR REPLACE TEMP TABLE _order_line_psource_payment_stg AS --comment
-- CREATE OR REPLACE TRANSIENT TABLE REPORTING_BASE_PROD.SHARED.order_line_psource_payment as --comment
select
    ds.STORE_BRAND AS brand
    ,ds.STORE_COUNTRY AS country
    ,ds.STORE_REGION AS region
    ,ds.STORE_FULL_NAME AS store
    ,ds.STORE_TYPE AS store_type_sales_channel
    ,b.STORE_ID AS store_id

    ,CASE WHEN sc.ORDER_SALES_CHANNEL_L2 = 'Web Order' THEN 'Online'
        WHEN sc.ORDER_SALES_CHANNEL_L2 = 'Mobile App Order' THEN 'Mobile App'
        WHEN sc.ORDER_SALES_CHANNEL_L2 = 'Retail Order' THEN 'Retail' ELSE 'N/A' END AS product_order_store_type

     ,dss.STORE_ID AS original_activating_store_id
     ,dss.STORE_FULL_NAME AS original_activating_store
     ,dss.STORE_TYPE AS original_activating_store_type

--     ,convert_timezone('America/Los_Angeles',o.ORDER_LOCAL_DATETIME)::datetime as order_hq_datetime
     ,date_trunc('month',f.ORDER_LOCAL_DATETIME)::date AS local_month_order_date
     ,CAST(o.order_local_datetime AS DATE) AS order_local_date
     ,CAST(o.SHIPPED_LOCAL_DATETIME AS DATE) AS ship_local_date
     ,o.ORDER_LOCAL_DATETIME
     ,o.SHIPPED_LOCAL_DATETIME
     ,REGISTRATION_LOCAL_DATETIME
     ,o.SESSION_ID
     ,o.CUSTOMER_ID
     ,dc.current_membership_type
     ,CASE WHEN ds.STORE_BRAND = 'Fabletics' AND lower(dc.gender) = 'm' THEN 'Mens'
        WHEN ds.STORE_BRAND = 'Fabletics' THEN 'Womens'
        ELSE 'None' END  AS customer_gender
     ,sesh.MEMBERSHIP_STATE AS session_start_membership_state
     ,sesh.PLATFORM
     ,sesh.os
     ,sesh.browser

     ,o.ORDER_ID
     ,f.ORDER_LINE_ID
     ,f.BUNDLE_ORDER_LINE_ID
     ,ORDER_LINE_STATUS

     ,ORDER_STATUS
     ,ORDER_PROCESSING_STATUS_CODE
     ,ORDER_PROCESSING_STATUS
     ,sc.ORDER_SALES_CHANNEL_L1
     ,sc.ORDER_SALES_CHANNEL_L2
     ,ORDER_CLASSIFICATION_L1
     ,ORDER_CLASSIFICATION_L2
     ,MEMBERSHIP_ORDER_TYPE_L1
     ,MEMBERSHIP_ORDER_TYPE_L2
     ,MEMBERSHIP_ORDER_TYPE_L3
     ,ph.promo_code AS promo_code
     ,ph.PROMO_NAME
     ,css.CUSTOMER_SELECTED_SHIPPING_TYPE
     ,css.CUSTOMER_SELECTED_SHIPPING_SERVICE

--payment info
     ,IS_CASH
     ,pay.IS_PREPAID_CREDITCARD
     ,CASE WHEN pre.session_id IS NOT NULL THEN TRUE ELSE FALSE END AS IS_PREPAID_CREDITCARD_FAILURE
     ,IS_APPLEPAY
     ,IS_MASTERCARD_CHECKOUT
     ,IS_VISA_CHECKOUT
     ,PAYMENT_METHOD AS payment_method_unadjusted
     ,CREDITCARD_TYPE AS creditcard_type_unadjusted
     ,creditcard_type_clean
    ,CASE WHEN PAYMENT_METHOD = 'Cash' THEN 'Cash'
          when coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Afterpay' then 'Afterpay'
          when coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Klarna' then 'Klarna'
          WHEN IS_APPLEPAY = TRUE or IS_MASTERCARD_CHECKOUT = TRUE OR (IS_VISA_CHECKOUT = TRUE and
                                                                       CREDITCARD_TYPE <> 'Afterpay') OR CREDITCARD_TYPE = 'Paypal'
              OR CREDITCARD_TYPE = 'Cashapppay' THEN 'Digital Wallet'-- addition to when statement---
          WHEN (coalesce(creditcard_type_clean,CREDITCARD_TYPE) IN ('Unknown','Not Applicable') AND PAYMENT_METHOD <> 'Cash') OR
               PAYMENT_METHOD = 'None' then 'Unknown'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Other' then 'Other'
          ELSE 'Credit Card' END AS payment_type_adjusted

    ,CASE WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Afterpay' THEN 'Afterpay'
          WHEN IS_APPLEPAY = TRUE THEN 'Apple Pay'
          WHEN IS_MASTERCARD_CHECKOUT = TRUE THEN 'Masterpass'
          WHEN(IS_VISA_CHECKOUT = TRUE and CREDITCARD_TYPE <> 'Afterpay') THEN 'Visa Checkout'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Paypal' THEN 'Paypal'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Afterpay' THEN 'Afterpay'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Klarna' then 'Klarna'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Cashapppay' then 'Cash App' --new when statement----
          WHEN (PAYMENT_METHOD <> 'Cash' AND coalesce(creditcard_type_clean,CREDITCARD_TYPE) in ('Unknown','Not Applicable')) THEN 'Unknown' ---adjusted
          WHEN PAYMENT_METHOD = 'Cash' then 'Not Applicable'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Other' THEN 'Other'
          ELSE 'Credit Card' END AS digital_wallet_payment_type_adjusted

    ,CASE WHEN PAYMENT_METHOD = 'Cash' then 'Cash'
          WHEN IS_APPLEPAY = TRUE then 'Apple Pay'
          WHEN IS_MASTERCARD_CHECKOUT = TRUE THEN 'Masterpass'
          WHEN (IS_VISA_CHECKOUT = TRUE and CREDITCARD_TYPE <> 'Afterpay') THEN 'Visa Checkout'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Paypal' THEN 'Paypal'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Afterpay' THEN 'Afterpay'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Cashapppay' then 'Cash App' --new when statement
          WHEN (PAYMENT_METHOD<>'Cash' AND coalesce(creditcard_type_clean,CREDITCARD_TYPE) in ('Unknown','Not Applicable')) THEN 'Unknown'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Other' THEN 'Other'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Vpay' THEN 'Visa'
          ELSE coalesce(creditcard_type_clean,CREDITCARD_TYPE) END AS payment_method_type_adjusted

     ,CASE WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Vpay' THEN 'Visa'
          WHEN coalesce(creditcard_type_clean,CREDITCARD_TYPE) = 'Afterpay' THEN 'Afterpay'
          ELSE coalesce(creditcard_type_clean,CREDITCARD_TYPE) END AS credit_card_type_adjusted
--product & psource info
     ,dp.PRODUCT_SKU
     ,dp.SKU
     ,dp.MASTER_PRODUCT_ID
     ,f.PRODUCT_ID
    ,byo_bundle_product_id
     ,BUNDLE_PRODUCT_ID
     ,CASE WHEN ty.PRODUCT_TYPE_NAME = 'Bundle Component' THEN lower(dp2.PRODUCT_NAME) END AS bundle_name
     ,byo_bundle_name
     ,coalesce(lower(ubt.product_name),lower(dp.product_name)) AS product_name
     ,CASE WHEN ds.STORE_BRAND = 'Savage X' AND byo_bundle_product_id is not null THEN 'BYO'
           WHEN ds.STORE_BRAND = 'Savage X' AND lower (dp2.PRODUCT_NAME) ilike '%box' and NOT
                contains(lower(dp2.PRODUCT_NAME),'boxer') THEN 'VIP Box'
           WHEN ty.PRODUCT_TYPE_NAME = 'Bundle Component' then 'Bundle'
           ELSE ty.PRODUCT_TYPE_NAME end AS product_type

     --FL/YTY UBT
    ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN product_lifestyle_type
           ELSE 'None' END product_lifestyle_type
    ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN product_sub_brand
           ELSE 'None' END product_sub_brand
     ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN product_gender
           ELSE 'None' END product_gender
     ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN product_segment
           ELSE 'None' END product_segment
     ,CASE WHEN sc.ORDER_SALES_CHANNEL_L2 = 'Billing Order' THEN NULL ELSE (CASE WHEN ds.STORE_BRAND in ('Fabletics') and (lower(dp.department) = 'mens' OR lower(dp2.department) = 'mens') THEN 'Mens'
           WHEN ds.STORE_BRAND in ('Fabletics') THEN 'Womens'
           ELSE dp.DEPARTMENT END) END  AS product_department
    ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN product_ubt_showroom
           ELSE NULL END product_ubt_showroom
     ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN product_color
           ELSE dp.color END product_color
     ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN ubt.product_category
           ELSE dp.CATEGORY END AS  product_category
     ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN product_class
           ELSE dp.class END product_class
     ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN product_subclass
           ELSE 'None' END product_subclass
     ,CASE WHEN ds.STORE_BRAND in ('Fabletics','Yitty') THEN product_special_collection
           ELSE 'None' END product_special_collection
    ,CASE WHEN sc.ORDER_SALES_CHANNEL_L2 = 'Billing Order' THEN NULL ELSE (CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || dp.product_sku || '/' || dp.product_sku ||
           '-1_271x407.jpg')) END AS product_image_url

     ,lower(ps1.value) AS psource_pieced
     ,lower(ps2.value) AS psource_bundle

     ,CASE WHEN sc.ORDER_SALES_CHANNEL_L2 = 'Billing Order' THEN NULL ELSE (CASE WHEN lower(ops.ORDER_PRODUCT_SOURCE_NAME) = 'unknown' THEN 'none' ELSE lower(ops.ORDER_PRODUCT_SOURCE_NAME)
            END) END AS psource_raw
     ,CASE WHEN sc.ORDER_SALES_CHANNEL_L2 = 'Billing Order' THEN NULL ELSE (CASE WHEN ds.STORE_BRAND <> 'Savage X' then coalesce(ps2.value,ps1.value,'None')
           WHEN ds.STORE_BRAND = 'Savage X' AND PRODUCT_TYPE_NAME = 'Normal' then coalesce(ps2.value,ps1.value,'None') -- SXF normal items
           WHEN ds.STORE_BRAND = 'Savage X' AND (lower(bundle_name) not ilike '%box' and not
                contains(lower(bundle_name),'boxer')) AND byo_bundle_product_id is null THEN coalesce(ps2.value,'browse_sets_')  --sxf bundles that are null
           WHEN ds.STORE_BRAND = 'Savage X' AND byo_bundle_name IS NOT NULL AND lower(ps1.value) ilike 'byo%' then
                coalesce(ps1.value,ps2.value,'byo_') --byo
           WHEN ds.STORE_BRAND = 'Savage X' AND byo_bundle_name IS NOT NULL AND (lower(ps1.value) not ilike 'byo%' or
                ps1.value IS NULL) THEN coalesce(ps2.value,'byo_') --byo
           WHEN ds.STORE_BRAND = 'Savage X' AND (lower(bundle_name) ilike '%box' AND NOT contains(lower(bundle_name),'boxer'))
                AND byo_bundle_product_id is null THEN coalesce(ps2.value,'xtrashop_')
           ELSE 'None' END) END AS psource

    ,CASE WHEN sc.ORDER_SALES_CHANNEL_L2 = 'Billing Order' THEN NULL ELSE (CASE WHEN f.TOKEN_COUNT > 0 THEN 'Token' ELSE 'Cash' END) END as token_usage --NEW

     ,CASE WHEN sc.ORDER_SALES_CHANNEL_L2 = 'Billing Order' THEN NULL ELSE (CASE WHEN ds.STORE_BRAND = 'Savage X' AND byo_bundle_product_id is not null THEN 'BYO'
           WHEN ds.STORE_BRAND = 'Savage X' AND lower (dp2.PRODUCT_NAME) ilike '%box' and NOT
                contains(lower(dp2.PRODUCT_NAME),'boxer') THEN 'VIP Box'
           WHEN ty.PRODUCT_TYPE_NAME = 'Bundle Component' then 'Bundle'
           ELSE ty.PRODUCT_TYPE_NAME end || ' - ' || token_usage) END as sales_type

     ,ITEM_QUANTITY AS units
     ,iff(PRODUCT_TYPE_NAME = 'Bundle Component',ITEM_QUANTITY,0) AS outfit_component_units
     ,iff(PRODUCT_TYPE_NAME = 'Normal',ITEM_QUANTITY,0) AS pieced_units

     ,f.SUBTOTAL_EXCL_TARIFF_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS subtotal_excl_tariff
     ,f.TARIFF_REVENUE_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS tariff_revenue

     ,f.PRODUCT_SUBTOTAL_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS product_subtotal
     ,f.PRODUCT_DISCOUNT_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS product_discount
     ,f.SHIPPING_DISCOUNT_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS shipping_discount
     ,f.SHIPPING_REVENUE_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS shipping_revenue
     ,f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS shipping_revenue_before_discount

     ,f.PRODUCT_GROSS_REVENUE_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS product_gross_revenue
     ,f.PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS product_gross_revenue_excl_shipping

     ,f.PRODUCT_MARGIN_PRE_RETURN_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS product_margin_pre_return
     ,f.PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE
            AS product_margin_pre_return_excl_shipping

     ,f.estimated_shipping_supplies_cost_local_amount * f.ORDER_DATE_USD_CONVERSION_RATE AS product_cost
     ,(IFF(f.shipping_cost_local_amount = 0,f.estimated_shipping_cost_local_amount,f.shipping_cost_local_amount) *
            f.ORDER_DATE_USD_CONVERSION_RATE) AS shipping_cost
     ,f.ESTIMATED_LANDED_COST_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS landed_cost

     ,f.NON_CASH_CREDIT_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS non_cash_credit
     ,f.CASH_CREDIT_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS cash_credit
     ,f.CASH_GROSS_REVENUE_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS cash_gross_revenue
     ,f.PAYMENT_TRANSACTION_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS payment_transaction
     ,f.TAX_LOCAL_AMOUNT * f.ORDER_DATE_USD_CONVERSION_RATE AS tax

     --order level metrics, RNK = 1
    ,o.UNIT_COUNT AS ttl_units
     ,o.SUBTOTAL_EXCL_TARIFF_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_subtotal_excl_tariff --excludes tariff
     ,o.TARIFF_REVENUE_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_tariff

     ,o.PRODUCT_SUBTOTAL_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_product_subtotal_incl_tariff --includes tariff
     ,o.PRODUCT_DISCOUNT_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_product_discount
     ,o.SHIPPING_DISCOUNT_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_shipping_discount
     ,o.SHIPPING_REVENUE_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_shipping_revenue --includes shipping discount
     ,o.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_shipping_revenue_before_discount

     ,o.PRODUCT_GROSS_REVENUE_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_product_gross_revenue --gaap gross revenue
     ,o.PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_product_gross_revenue_excl_shipping

     ,o.PRODUCT_MARGIN_PRE_RETURN_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_product_margin_pre_return --gaap gross margin
     ,o.PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_LOCAL_AMOUNT  * o.order_date_usd_conversion_rate AS
            ttl_product_margin_pre_return_excl_shipping --gaap gross margin

     ,o.ESTIMATED_SHIPPING_SUPPLIES_COST_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_product_cost
     ,o.SHIPPING_COST_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_shipping_cost

     ,o.ESTIMATED_LANDED_COST_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_landed_cost

     ,o.NON_CASH_CREDIT_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_non_cash_credit
     ,o.NON_CASH_CREDIT_COUNT as ttl_non_cash_credit_count
     ,o.CASH_CREDIT_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_cash_credit

     ,o.AMOUNT_TO_PAY * o.order_date_usd_conversion_rate AS ttl_amount_to_pay
     ,o.PAYMENT_TRANSACTION_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_payment_transaction
     ,o.TAX_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_tax
     ,o.CASH_GROSS_REVENUE_LOCAL_AMOUNT * o.order_date_usd_conversion_rate AS ttl_cash_gross_revenue --cash collected
     ,ifnull(cr.CREDIT_REDEEMED_LOCAL_AMOUNT * o.order_date_usd_conversion_rate,0) AS ttl_membership_credit_redeemed
     ,ifnull(round(BILLED_CASH_CREDIT_REDEEMED_EQUIVALENT_COUNT),0) AS ttl_credits_token_redeemed_count

     ,o.order_date_usd_conversion_rate
     ,rank() over (partition by o.order_id ORDER BY f.ORDER_LINE_ID asc) AS ttl_order_rnk
     ,current_timestamp AS datetime_added
     ,current_timestamp AS datetime_modified

--      ,$refresh_datetime_hq AS refresh_datetime_hq
FROM _order_base AS b
JOIN EDW_prod.DATA_MODEL.FACT_ORDER AS o on o.ORDER_ID = b.ORDER_ID
JOIN EDW_prod.DATA_MODEL.DIM_ORDER_PROCESSING_STATUS AS opss on opss.ORDER_PROCESSING_STATUS_KEY = o.ORDER_PROCESSING_STATUS_KEY
LEFT JOIN REPORTING_BASE_prod.SHARED.SESSION AS sesh on sesh.SESSION_ID = o.SESSION_ID --left join just in case the session table hasn't refreshed yet
JOIN EDW_prod.DATA_MODEL.DIM_ORDER_STATUS AS dos on dos.ORDER_STATUS_KEY = o.ORDER_STATUS_KEY
JOIN EDW_prod.DATA_MODEL.DIM_ORDER_MEMBERSHIP_CLASSIFICATION AS ch
    on ch.ORDER_MEMBERSHIP_CLASSIFICATION_KEY = o.ORDER_MEMBERSHIP_CLASSIFICATION_KEY
JOIN EDW_prod.DATA_MODEL.DIM_ORDER_SALES_CHANNEL as sc on sc.ORDER_SALES_CHANNEL_KEY = o.ORDER_SALES_CHANNEL_KEY
JOIN EDW_prod.DATA_MODEL.DIM_STORE AS ds on ds.STORE_ID = o.STORE_ID
JOIN EDW_prod.DATA_MODEL.DIM_CUSTOMER AS dc on dc.CUSTOMER_ID = o.CUSTOMER_ID
LEFT JOIN EDW_prod.data_model.fact_activation act ON act.ACTIVATION_KEY = o.ACTIVATION_KEY
LEFT JOIN EDW_prod.data_model.dim_store dss ON dss.store_id = act.SUB_STORE_ID
LEFT JOIN EDW_prod.DATA_MODEL.DIM_PAYMENT AS pay on pay.PAYMENT_KEY = o.PAYMENT_KEY
LEFT JOIN _eu_payment_adjusted AS adj on adj.order_id = o.ORDER_ID
LEFT JOIN (SELECT DISTINCT session_id from lake_consolidated_view.ultra_merchant.payment_transaction_creditcard WHERE STATUSCODE = 4035) AS pre
    on pre.SESSION_ID = o.session_id
LEFT JOIN EDW_prod.DATA_MODEL.FACT_ORDER_CREDIT AS cr on cr.ORDER_ID = o.ORDER_ID
-- order line
JOIN EDW_prod.DATA_MODEL.FACT_ORDER_LINE AS f on f.ORDER_ID = o.ORDER_ID
JOIN EDW_prod.DATA_MODEL.DIM_PRODUCT_TYPE AS ty on ty.PRODUCT_TYPE_KEY = f.PRODUCT_TYPE_KEY
    AND ty.PRODUCT_TYPE_NAME in ('Normal','Bundle Component') --excluding 'Membership Reward Points Item' so counts will slightly be off from fact_order
    AND ty.IS_FREE = FALSE
    AND ty.IS_SOURCE_FREE = FALSE
JOIN EDW_prod.DATA_MODEL.DIM_ORDER_LINE_STATUS AS os on os.ORDER_LINE_STATUS_KEY = f.ORDER_LINE_STATUS_KEY
    and os.ORDER_LINE_STATUS <> 'Cancelled'
--new dim psource table
LEFT JOIN EDW_prod.DATA_MODEL.DIM_ORDER_PRODUCT_SOURCE AS ops on ops.ORDER_PRODUCT_SOURCE_KEY = f.ORDER_PRODUCT_SOURCE_KEY
-- pieced items
JOIN EDW_prod.DATA_MODEL.DIM_PRODUCT AS dp on dp.PRODUCT_ID = f.PRODUCT_ID
LEFT JOIN (select regexp_replace(value,'\\?.*', '') as value,ORDER_ID,PRODUCT_ID FROM LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.ORDER_PRODUCT_SOURCE) AS ps1
    on ps1.ORDER_ID = f.ORDER_ID
    AND ps1.product_id = f.PRODUCT_ID
--bundled + byo components
LEFT JOIN EDW_prod.DATA_MODEL.DIM_PRODUCT AS dp2 on dp2.PRODUCT_ID = f.BUNDLE_PRODUCT_ID
LEFT JOIN (select regexp_replace(value,'\\?.*', '') as value,ORDER_ID,PRODUCT_ID FROM LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.ORDER_PRODUCT_SOURCE) AS ps2
    on ps2.ORDER_ID = f.ORDER_ID
    AND ps2.PRODUCT_ID = dp2.PRODUCT_ID --bundle product id
LEFT JOIN _byo_bundles as byo on byo_bundle_product_id = f.BUNDLE_PRODUCT_ID
LEFT JOIN _promo_code ph on ph.order_line_id=f.order_line_id
LEFT JOIN EDW_prod.DATA_MODEL.dim_order_customer_selected_shipping css
    on o.ORDER_CUSTOMER_SELECTED_SHIPPING_KEY=css.ORDER_CUSTOMER_SELECTED_SHIPPING_KEY
left join _fl_ubt_product_attributes as ubt on f.product_id = ubt.product_id
        and ds.STORE_BRAND in ('Fabletics','Yitty') and ORDER_CLASSIFICATION_L1 = 'Product Order'
;

DELETE FROM REPORTING_BASE_PROD.SHARED.order_line_psource_payment WHERE order_id in (select order_id from _order_line_psource_payment_stg); --comment

INSERT INTO REPORTING_BASE_PROD.SHARED.order_line_psource_payment ( --comment
        BRAND
        ,COUNTRY
        ,REGION
        ,STORE
        ,STORE_TYPE_SALES_CHANNEL
        ,STORE_ID
        ,PRODUCT_ORDER_STORE_TYPE
        ,ORIGINAL_ACTIVATING_STORE_ID
        ,ORIGINAL_ACTIVATING_STORE
        ,ORIGINAL_ACTIVATING_STORE_TYPE
        ,LOCAL_MONTH_ORDER_DATE
        ,ORDER_LOCAL_DATE
        ,SHIP_LOCAL_DATE
        ,ORDER_LOCAL_DATETIME
        ,SHIPPED_LOCAL_DATETIME
        ,REGISTRATION_LOCAL_DATETIME
        ,SESSION_ID
        ,CUSTOMER_ID
        ,CURRENT_MEMBERSHIP_TYPE
        ,CUSTOMER_GENDER
        ,SESSION_START_MEMBERSHIP_STATE
        ,PLATFORM
        ,OS
        ,BROWSER
        ,ORDER_ID
        ,ORDER_LINE_ID
        ,BUNDLE_ORDER_LINE_ID
        ,ORDER_LINE_STATUS
        ,ORDER_STATUS
        ,ORDER_PROCESSING_STATUS_CODE
        ,ORDER_PROCESSING_STATUS
        ,ORDER_SALES_CHANNEL_L1
        ,ORDER_SALES_CHANNEL_L2
        ,ORDER_CLASSIFICATION_L1
        ,ORDER_CLASSIFICATION_L2
        ,MEMBERSHIP_ORDER_TYPE_L1
        ,MEMBERSHIP_ORDER_TYPE_L2
        ,MEMBERSHIP_ORDER_TYPE_L3
        ,PROMO_CODE
        ,PROMO_NAME
        ,CUSTOMER_SELECTED_SHIPPING_TYPE
        ,CUSTOMER_SELECTED_SHIPPING_SERVICE
        ,IS_CASH
        ,IS_PREPAID_CREDITCARD
        ,IS_PREPAID_CREDITCARD_FAILURE
        ,IS_APPLEPAY
        ,IS_MASTERCARD_CHECKOUT
        ,IS_VISA_CHECKOUT
        ,PAYMENT_METHOD_UNADJUSTED
        ,CREDITCARD_TYPE_UNADJUSTED
        ,CREDITCARD_TYPE_CLEAN
        ,PAYMENT_TYPE_ADJUSTED
        ,DIGITAL_WALLET_PAYMENT_TYPE_ADJUSTED
        ,PAYMENT_METHOD_TYPE_ADJUSTED
        ,CREDIT_CARD_TYPE_ADJUSTED
        ,PRODUCT_SKU
        ,SKU
        ,MASTER_PRODUCT_ID
        ,PRODUCT_ID
        ,BYO_BUNDLE_PRODUCT_ID
        ,BUNDLE_PRODUCT_ID
        ,BUNDLE_NAME
        ,BYO_BUNDLE_NAME
        ,PRODUCT_NAME
        ,PRODUCT_TYPE
        ,product_lifestyle_type
        ,product_sub_brand
        ,product_gender
        ,product_segment
        ,product_ubt_showroom
        ,product_color
        ,product_category
        ,product_class
        ,product_subclass
        ,product_special_collection
        ,product_image_url
        ,PRODUCT_DEPARTMENT
        ,PSOURCE_PIECED
        ,PSOURCE_BUNDLE
        ,PSOURCE_RAW
        ,PSOURCE
        ,token_usage
        ,sales_type
        ,UNITS
        ,OUTFIT_COMPONENT_UNITS
        ,PIECED_UNITS
        ,SUBTOTAL_EXCL_TARIFF
        ,TARIFF_REVENUE
        ,PRODUCT_SUBTOTAL
        ,PRODUCT_DISCOUNT
        ,SHIPPING_DISCOUNT
        ,SHIPPING_REVENUE
        ,SHIPPING_REVENUE_BEFORE_DISCOUNT
        ,PRODUCT_GROSS_REVENUE
        ,PRODUCT_GROSS_REVENUE_EXCL_SHIPPING
        ,PRODUCT_MARGIN_PRE_RETURN
        ,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING
        ,PRODUCT_COST
        ,SHIPPING_COST
        ,LANDED_COST
        ,NON_CASH_CREDIT
        ,CASH_CREDIT
        ,CASH_GROSS_REVENUE
        ,PAYMENT_TRANSACTION
        ,TAX
        ,TTL_UNITS
        ,TTL_SUBTOTAL_EXCL_TARIFF
        ,TTL_TARIFF
        ,TTL_PRODUCT_SUBTOTAL_INCL_TARIFF
        ,TTL_PRODUCT_DISCOUNT
        ,TTL_SHIPPING_DISCOUNT
        ,TTL_SHIPPING_REVENUE
        ,TTL_SHIPPING_REVENUE_BEFORE_DISCOUNT
        ,TTL_PRODUCT_GROSS_REVENUE
        ,TTL_PRODUCT_GROSS_REVENUE_EXCL_SHIPPING
        ,TTL_PRODUCT_MARGIN_PRE_RETURN
        ,TTL_PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING
        ,TTL_PRODUCT_COST
        ,TTL_SHIPPING_COST
        ,TTL_LANDED_COST
        ,TTL_NON_CASH_CREDIT
        ,TTL_NON_CASH_CREDIT_COUNT
        ,TTL_CASH_CREDIT
        ,TTL_AMOUNT_TO_PAY
        ,TTL_PAYMENT_TRANSACTION
        ,TTL_TAX
        ,TTL_CASH_GROSS_REVENUE
        ,TTL_MEMBERSHIP_CREDIT_REDEEMED
        ,TTL_CREDITS_TOKEN_REDEEMED_COUNT
        ,ORDER_DATE_USD_CONVERSION_RATE
        ,TTL_ORDER_RNK
        ,DATETIME_ADDED
        ,DATETIME_MODIFIED )
SELECT BRAND
        ,COUNTRY
        ,REGION
        ,STORE
        ,STORE_TYPE_SALES_CHANNEL
        ,STORE_ID
        ,PRODUCT_ORDER_STORE_TYPE
        ,ORIGINAL_ACTIVATING_STORE_ID
        ,ORIGINAL_ACTIVATING_STORE
        ,ORIGINAL_ACTIVATING_STORE_TYPE
        ,LOCAL_MONTH_ORDER_DATE
        ,ORDER_LOCAL_DATE
        ,SHIP_LOCAL_DATE
        ,ORDER_LOCAL_DATETIME
        ,SHIPPED_LOCAL_DATETIME
        ,REGISTRATION_LOCAL_DATETIME
        ,SESSION_ID
        ,CUSTOMER_ID
        ,CURRENT_MEMBERSHIP_TYPE
        ,CUSTOMER_GENDER
        ,SESSION_START_MEMBERSHIP_STATE
        ,PLATFORM
        ,OS
        ,BROWSER
        ,ORDER_ID
        ,ORDER_LINE_ID
        ,BUNDLE_ORDER_LINE_ID
        ,ORDER_LINE_STATUS
        ,ORDER_STATUS
        ,ORDER_PROCESSING_STATUS_CODE
        ,ORDER_PROCESSING_STATUS
        ,ORDER_SALES_CHANNEL_L1
        ,ORDER_SALES_CHANNEL_L2
        ,ORDER_CLASSIFICATION_L1
        ,ORDER_CLASSIFICATION_L2
        ,MEMBERSHIP_ORDER_TYPE_L1
        ,MEMBERSHIP_ORDER_TYPE_L2
        ,MEMBERSHIP_ORDER_TYPE_L3
        ,PROMO_CODE
        ,PROMO_NAME
        ,CUSTOMER_SELECTED_SHIPPING_TYPE
        ,CUSTOMER_SELECTED_SHIPPING_SERVICE
        ,IS_CASH
        ,IS_PREPAID_CREDITCARD
        ,IS_PREPAID_CREDITCARD_FAILURE
        ,IS_APPLEPAY
        ,IS_MASTERCARD_CHECKOUT
        ,IS_VISA_CHECKOUT
        ,PAYMENT_METHOD_UNADJUSTED
        ,CREDITCARD_TYPE_UNADJUSTED
        ,CREDITCARD_TYPE_CLEAN
        ,PAYMENT_TYPE_ADJUSTED
        ,DIGITAL_WALLET_PAYMENT_TYPE_ADJUSTED
        ,PAYMENT_METHOD_TYPE_ADJUSTED
        ,CREDIT_CARD_TYPE_ADJUSTED
        ,PRODUCT_SKU
        ,SKU
        ,MASTER_PRODUCT_ID
        ,PRODUCT_ID
        ,BYO_BUNDLE_PRODUCT_ID
        ,BUNDLE_PRODUCT_ID
        ,BUNDLE_NAME
        ,BYO_BUNDLE_NAME
        ,PRODUCT_NAME
        ,PRODUCT_TYPE
        ,product_lifestyle_type
        ,product_sub_brand
        ,product_gender
        ,product_segment
        ,product_ubt_showroom
        ,product_color
        ,product_category
        ,product_class
        ,product_subclass
        ,product_special_collection
        ,product_image_url
        ,PRODUCT_DEPARTMENT
        ,PSOURCE_PIECED
        ,PSOURCE_BUNDLE
        ,PSOURCE_RAW
        ,PSOURCE
        ,token_usage
        ,sales_type
        ,UNITS
        ,OUTFIT_COMPONENT_UNITS
        ,PIECED_UNITS
        ,SUBTOTAL_EXCL_TARIFF
        ,TARIFF_REVENUE
        ,PRODUCT_SUBTOTAL
        ,PRODUCT_DISCOUNT
        ,SHIPPING_DISCOUNT
        ,SHIPPING_REVENUE
        ,SHIPPING_REVENUE_BEFORE_DISCOUNT
        ,PRODUCT_GROSS_REVENUE
        ,PRODUCT_GROSS_REVENUE_EXCL_SHIPPING
        ,PRODUCT_MARGIN_PRE_RETURN
        ,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING
        ,PRODUCT_COST
        ,SHIPPING_COST
        ,LANDED_COST
        ,NON_CASH_CREDIT
        ,CASH_CREDIT
        ,CASH_GROSS_REVENUE
        ,PAYMENT_TRANSACTION
        ,TAX
        ,TTL_UNITS
        ,TTL_SUBTOTAL_EXCL_TARIFF
        ,TTL_TARIFF
        ,TTL_PRODUCT_SUBTOTAL_INCL_TARIFF
        ,TTL_PRODUCT_DISCOUNT
        ,TTL_SHIPPING_DISCOUNT
        ,TTL_SHIPPING_REVENUE
        ,TTL_SHIPPING_REVENUE_BEFORE_DISCOUNT
        ,TTL_PRODUCT_GROSS_REVENUE
        ,TTL_PRODUCT_GROSS_REVENUE_EXCL_SHIPPING
        ,TTL_PRODUCT_MARGIN_PRE_RETURN
        ,TTL_PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING
        ,TTL_PRODUCT_COST
        ,TTL_SHIPPING_COST
        ,TTL_LANDED_COST
        ,TTL_NON_CASH_CREDIT
        ,TTL_NON_CASH_CREDIT_COUNT
        ,TTL_CASH_CREDIT
        ,TTL_AMOUNT_TO_PAY
        ,TTL_PAYMENT_TRANSACTION
        ,TTL_TAX
        ,TTL_CASH_GROSS_REVENUE
        ,TTL_MEMBERSHIP_CREDIT_REDEEMED
        ,TTL_CREDITS_TOKEN_REDEEMED_COUNT
        ,ORDER_DATE_USD_CONVERSION_RATE
        ,TTL_ORDER_RNK
        ,DATETIME_ADDED
        ,DATETIME_MODIFIED
FROM _order_line_psource_payment_stg pp; --comment
