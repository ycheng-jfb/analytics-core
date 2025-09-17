set start_date = cast('2019-01-01' as date);


create or replace temporary table _failed_credit_billing as
select distinct
    cast(fol.ORDER_LOCAL_DATETIME as date) as Order_HQ_Day
    ,upper(st.STORE_BRAND) as STORE_BRAND_NAME
    ,upper(st.STORE_BRAND) as STORE_GROUP
    ,(case
        when dc.SPECIALTY_COUNTRY_CODE = 'GB' then 'UK'
        when dc.SPECIALTY_COUNTRY_CODE != 'Unknown' then dc.SPECIALTY_COUNTRY_CODE
        else st.STORE_COUNTRY end) as STORE_COUNTRY_ABBR
    ,'failed credit billing' as order_classification
    ,(case
        when domc.MEMBERSHIP_ORDER_TYPE_L2 = 'Guest' then 'ecom'
        when domc.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP' then 'vip activating'
        else 'vip repeat' end) as order_type
    ,dpm.IS_PREPAID_CREDITCARD
    ,(CASE WHEN dpm.RAW_CREDITCARD_TYPE ilike 'Ap_%' THEN 'Apple Pay'
        WHEN od.name in ('masterpass_checkout_transaction_id', 'masterpass_checkout_payment_id') THEN 'Masterpass'
        WHEN od.name = 'visa_checkout_call_id' THEN 'Visa Checkout'
        when lower(dpm.RAW_CREDITCARD_TYPE) like '%paypal%' then 'Paypal'
        ELSE dpm.RAW_CREDITCARD_TYPE
        END) as Payment_Method
    ,fol.ORDER_ID

    --Sales
    ,fol.UNIT_COUNT as Num_Items
    ,fol.SUBTOTAL_EXCL_TARIFF_LOCAL_AMOUNT * coalesce(fol.ORDER_DATE_USD_CONVERSION_RATE, 1) as Subotal_Amount
    ,fol.PRODUCT_DISCOUNT_LOCAL_AMOUNT * coalesce(fol.ORDER_DATE_USD_CONVERSION_RATE, 1) as Discount_Amount
    ,fol.CASH_CREDIT_LOCAL_AMOUNT * coalesce(fol.ORDER_DATE_USD_CONVERSION_RATE, 1) as Cash_Credit_Amount
    ,fol.NON_CASH_CREDIT_LOCAL_AMOUNT * coalesce(fol.ORDER_DATE_USD_CONVERSION_RATE, 1) as Non_Cash_Credit_Amount
    ,fol.SHIPPING_REVENUE_LOCAL_AMOUNT * coalesce(fol.ORDER_DATE_USD_CONVERSION_RATE, 1) as Shipping_Revenue_Amount
    ,0 as Refund_Amount
    ,0 as Chargeback_Amount
    ,(fol.ESTIMATED_LANDED_COST_LOCAL_AMOUNT) * coalesce(fol.ORDER_DATE_USD_CONVERSION_RATE, 1) as COGS
from EDW_PROD.DATA_MODEL_JFB.FACT_ORDER fol
join reporting_prod.gfb.vw_store st
    on st.STORE_ID = fol.STORE_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_STATUS dos
    on dos.ORDER_STATUS_KEY = fol.ORDER_STATUS_KEY
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_MEMBERSHIP_CLASSIFICATION domc
    on domc.ORDER_MEMBERSHIP_CLASSIFICATION_KEY = fol.ORDER_MEMBERSHIP_CLASSIFICATION_KEY
join EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER dc
    on dc.CUSTOMER_ID = fol.CUSTOMER_ID
    and dc.IS_TEST_CUSTOMER = 0
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_PROCESSING_STATUS dops
    on dops.ORDER_PROCESSING_STATUS_KEY = fol.ORDER_PROCESSING_STATUS_KEY
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_SALES_CHANNEL dosc
    on dosc.ORDER_SALES_CHANNEL_KEY = fol.ORDER_SALES_CHANNEL_KEY
    and dosc.IS_BORDER_FREE_ORDER = 0
    and dosc.IS_PS_ORDER = 0
    and dosc.IS_TEST_ORDER = 0
JOIN EDW_PROD.DATA_MODEL_JFB.DIM_PAYMENT dpm
    ON dpm.PAYMENT_KEY = fol.PAYMENT_KEY
LEFT JOIN LAKE_JFB_VIEW.ULTRA_MERCHANT.ORDER_DETAIL od
    on (od.ORDER_ID=fol.ORDER_ID and od.name in ('visa_checkout_call_id', 'masterpass_checkout_transaction_id'))
where
    dos.ORDER_STATUS in ('Cancelled', 'Failure')
    and dosc.ORDER_CLASSIFICATION_L1 in ('Billing Order')
    and cast(fol.ORDER_LOCAL_DATETIME as date) >= $start_date;


create or replace temporary table _ind_order as
select
    olp.ORDER_DATE as ORDER_HQ_DATETIME
    ,olp.BUSINESS_UNIT as STORE_BRAND_NAME
    ,olp.BUSINESS_UNIT as STORE_GROUP
    ,olp.COUNTRY as STORE_COUNTRY_ABBR
    ,olp.ORDER_CLASSIFICATION
    ,olp.ORDER_TYPE
    ,olp.IS_PREPAID_CREDITCARD
    ,(CASE WHEN olp.creditcard_type ilike 'Ap_%' THEN 'Apple Pay'
        WHEN od.name in ('masterpass_checkout_transaction_id', 'masterpass_checkout_payment_id') THEN 'Masterpass'
        WHEN od.name = 'visa_checkout_call_id' THEN 'Visa Checkout'
        when lower(olp.creditcard_type) like '%paypal%' then 'Paypal'
        ELSE olp.creditcard_type
        END) as Payment_Method
    ,olp.ORDER_ID
    ,sum(olp.TOTAL_QTY_SOLD) as Num_Items
    ,sum(olp.ORDER_LINE_SUBTOTAL) as Subotal_Amount
    ,sum(olp.TOTAL_DISCOUNT) as Discount_Amount
    ,sum(olp.TOTAL_CASH_CREDIT_AMOUNT) as Cash_Credit_Amount
    ,sum(olp.TOTAL_NON_CASH_CREDIT_AMOUNT) as Non_Cash_Credit_Amount
    ,sum(olp.TOTAL_SHIPPING_REVENUE) as Shipping_Revenue_Amount
    ,sum(olp.TOTAL_REFUND_AMOUNT) as Refund_Amount
    ,sum(olp.TOTAL_CHARGEBACK_AMOUNT) as Chargeback_Amount
    ,sum(olp.TOTAL_COGS) as COGS
from REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE olp
LEFT JOIN LAKE_JFB_VIEW.ULTRA_MERCHANT.ORDER_DETAIL od
    on (od.ORDER_ID=olp.ORDER_ID and od.name in ('visa_checkout_call_id', 'masterpass_checkout_transaction_id'))
WHERE
    olp.ORDER_DATE >= $start_date
group by
    olp.ORDER_DATE
    ,olp.BUSINESS_UNIT
    ,olp.BUSINESS_UNIT
    ,olp.COUNTRY
    ,olp.ORDER_CLASSIFICATION
    ,olp.ORDER_TYPE
    ,olp.IS_PREPAID_CREDITCARD
    ,(CASE WHEN olp.creditcard_type ilike 'Ap_%' THEN 'Apple Pay'
        WHEN od.name in ('masterpass_checkout_transaction_id', 'masterpass_checkout_payment_id') THEN 'Masterpass'
        WHEN od.name = 'visa_checkout_call_id' THEN 'Visa Checkout'
        when lower(olp.creditcard_type) like '%paypal%' then 'Paypal'
        ELSE olp.creditcard_type
        END)
    ,olp.ORDER_ID

union

select
    *
from _failed_credit_billing a;


create or replace temporary table _ind_order_finance as
SELECT
    DATE_TRUNC('DAY',IO.ORDER_HQ_DATETIME) Order_HQ_Day
    ,IO.STORE_BRAND_NAME
    ,IO.STORE_GROUP
    ,IO.STORE_COUNTRY_ABBR
    ,IO.ORDER_CLASSIFICATION
    ,IO.ORDER_TYPE
    ,IO.IS_PREPAID_CREDITCARD
    ,IO.Payment_Method
    ,(case
        when Payment_Method in ('Amex', 'Discover', 'Master Card','Visa') then 'Credit Card'
        when Payment_Method in ('Apple Pay', 'Masterpass', 'Visa Checkout', 'Braintreepaypal','Paypal', 'Vpay') then 'Digital Wallet'
        when Payment_Method in ('Afterpay') then 'Buy Now, Pay Later'
        when Payment_Method in ('Not Applicable', 'Unknown') then 'Unknown or N/A'
        when Payment_Method in ('Other') then 'Other'
        else 'DETERMINE'
        end) as Payment_Type
    ,IO.ORDER_ID
    ,IO.Num_Items
    ,(IO.Subotal_Amount + IO.Shipping_Revenue_Amount - IO.Cash_Credit_Amount - IO.Non_Cash_Credit_Amount - IO.Discount_Amount) as Cash_Gross_Revenue
    ,IO.Discount_Amount as Discount_Amount
    ,(IO.Subotal_Amount + IO.Shipping_Revenue_Amount - IO.Discount_Amount) as Order_Value
    ,(Cash_Gross_Revenue - IO.Refund_Amount - IO.Chargeback_Amount) as Cash_Net_Revenue
    ,(Cash_Net_Revenue - IO.COGS) as Cash_Gross_Margin
FROM _ind_order IO;


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb012_payment_method as
select
    IOF.Order_HQ_Day
    ,IOF.STORE_BRAND_NAME
    ,IOF.STORE_GROUP
    ,IOF.STORE_COUNTRY_ABBR
    ,IOF.ORDER_CLASSIFICATION
    ,IOF.ORDER_TYPE
    ,IOF.IS_PREPAID_CREDITCARD
    ,IOF.Payment_Method
    ,IOF.Payment_Type
    ,COUNT(DISTINCT IOF.ORDER_ID) Num_Orders
    ,Sum(IOF.Num_Items) Num_Items
    ,Sum(IOF.Cash_Gross_Revenue) Sum_Cash_Gross_Revenue
    ,Sum(IOF.Discount_Amount) Sum_Discount_Amount
    ,Sum(IOF.Order_Value) Sum_Order_Value
    ,SUM(IOF.Cash_Net_Revenue) Sum_Cash_Net_Revenue
    ,SUM(IOF.Cash_Gross_Margin) Sum_Cash_Gross_Margin
from _ind_order_finance iof
group by
    IOF.Order_HQ_Day
    ,IOF.STORE_BRAND_NAME
    ,IOF.STORE_GROUP
    ,IOF.STORE_COUNTRY_ABBR
    ,IOF.ORDER_CLASSIFICATION
    ,IOF.ORDER_TYPE
    ,IOF.IS_PREPAID_CREDITCARD
    ,IOF.Payment_Method
    ,IOF.Payment_Type;
