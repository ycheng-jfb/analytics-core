set start_date = cast('2019-01-01' as date);
set end_date = current_date();


create or replace temporary table _failed_orders as
select distinct
    fo.ORDER_ID
from EDW_PROD.DATA_MODEL_JFB.FACT_ORDER fo
join reporting_prod.gfb.vw_store st
    on st.STORE_ID = fo.STORE_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER dc
    on dc.CUSTOMER_ID = fo.CUSTOMER_ID
    and dc.IS_TEST_CUSTOMER = 0
where
    fo.ORDER_LOCAL_DATETIME > $start_date;


create or replace temporary table _failed_order_reason as
select
    a.*
from
(
    select
        a.RESPONSE_RESULT_TEXT
        ,upper(a.RESPONSE_REASON_TEXT) as RESPONSE_REASON_TEXT
        ,a.ORDER_ID
        ,rank() over (partition by a.ORDER_ID order by a.PAYMENT_TRANSACTION_ID desc) as payment_transaction_rank
    from LAKE_JFB_VIEW.ULTRA_MERCHANT.PAYMENT_TRANSACTION_CREDITCARD a
    where
        a.ORDER_ID in (
                select distinct
                    a.ORDER_ID
                from _failed_orders a
            )
        and a.RESPONSE_REASON_TEXT is not null
        and a.RESPONSE_RESULT_TEXT = 'DECLINED'

    union

    select
        a.RESPONSE_RESULT_TEXT
        ,upper(split_part(a.RESPONSE_REASON_TEXT, ': ', -1)) as RESPONSE_REASON_TEXT
        ,a.ORDER_ID
        ,rank() over (partition by a.ORDER_ID order by a.PAYMENT_TRANSACTION_ID desc) as payment_transaction_rank
    from LAKE_JFB_VIEW.ULTRA_MERCHANT.PAYMENT_TRANSACTION_PSP a
    where
        a.ORDER_ID in (
                select distinct
                    a.ORDER_ID
                from _failed_orders a
            )
        and a.RESPONSE_REASON_TEXT is not null
        and a.RESPONSE_RESULT_TEXT = 'DECLINED'
        and a.RESPONSE_REASON_TEXT like '%:%'
) a
where
    a.payment_transaction_rank = 1
;


create or replace temporary table _ind_order as
select
    cast(fo.ORDER_LOCAL_DATETIME as date) as ORDER_HQ_DATETIME
    ,upper(ds.STORE_BRAND) as STORE_BRAND_NAME
    ,upper(ds.STORE_BRAND) as STORE_GROUP
    ,(case
        when dc.SPECIALTY_COUNTRY_CODE = 'GB' then 'UK'
        when dc.SPECIALTY_COUNTRY_CODE != 'Unknown' then dc.SPECIALTY_COUNTRY_CODE
        else ds.STORE_COUNTRY end) as COUNTRY
    ,(case
        when dosc.ORDER_CLASSIFICATION_L1 = 'Product Order' then 'product order'
        when dosc.ORDER_CLASSIFICATION_L1 = 'Billing Order' then 'credit billing'
        when dosc.ORDER_CLASSIFICATION_L1 = 'Exchange' then 'exchange'
        when dosc.ORDER_CLASSIFICATION_L1 = 'Reship' then 'reship'
        end) as order_classification
    ,(case
        when domc.MEMBERSHIP_ORDER_TYPE_L2 = 'Guest' then 'ecom'
        when domc.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP' then 'vip activating'
        else 'vip repeat' end) as order_type
    ,dpm.IS_PREPAID_CREDITCARD
    ,(CASE WHEN dpm.RAW_CREDITCARD_TYPE ilike 'Ap_%' THEN 'Apple Pay'
        WHEN od.name in ('masterpass_checkout_transaction_id', 'masterpass_checkout_payment_id') THEN 'Masterpass'
        WHEN od.name = 'visa_checkout_call_id' THEN 'Visa Checkout'
        ELSE dpm.RAW_CREDITCARD_TYPE
        END) as Payment_Method
    ,fo.ORDER_ID
    ,fail.RESPONSE_REASON_TEXT as failed_transaction_reason
from EDW_PROD.DATA_MODEL_JFB.FACT_ORDER fo
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_MEMBERSHIP_CLASSIFICATION domc
    on domc.ORDER_MEMBERSHIP_CLASSIFICATION_KEY = fo.ORDER_MEMBERSHIP_CLASSIFICATION_KEY
JOIN EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_STATUS dos
    on dos.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_SALES_CHANNEL dosc
    on dosc.ORDER_SALES_CHANNEL_KEY = fo.ORDER_SALES_CHANNEL_KEY
    and dosc.IS_BORDER_FREE_ORDER = 0
    and dosc.IS_PS_ORDER = 0
    and dosc.IS_TEST_ORDER = 0
JOIN EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER dc
    ON dc.CUSTOMER_ID = fo.CUSTOMER_ID
JOIN reporting_prod.gfb.vw_store ds
    on ds.STORE_ID = fo.STORE_ID
JOIN EDW_PROD.DATA_MODEL_JFB.DIM_PAYMENT dpm
    ON dpm.PAYMENT_KEY = fo.PAYMENT_KEY
LEFT JOIN LAKE_JFB_VIEW.ULTRA_MERCHANT.ORDER_DETAIL od
    on (od.ORDER_ID=fo.ORDER_ID and od.name in ('visa_checkout_call_id', 'masterpass_checkout_transaction_id'))
JOIN _failed_order_reason fail
    on fail.ORDER_ID = fo.ORDER_ID;


create or replace temporary table _ind_order_finance as
SELECT
    DATE_TRUNC('DAY',IO.ORDER_HQ_DATETIME) Order_HQ_Day
    ,IO.STORE_BRAND_NAME
    ,IO.STORE_GROUP
    ,IO.COUNTRY as STORE_COUNTRY_ABBR
    ,IO.ORDER_CLASSIFICATION
    ,IO.order_type
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
    ,io.failed_transaction_reason
FROM _ind_order IO;


create or replace transient table reporting_prod.GFB.gfb012_01_failed_payment_method as
select
    IOF.Order_HQ_Day
    ,IOF.STORE_BRAND_NAME
    ,IOF.STORE_GROUP
    ,IOF.STORE_COUNTRY_ABBR
    ,IOF.ORDER_CLASSIFICATION
    ,IOF.order_type
    ,IOF.IS_PREPAID_CREDITCARD
    ,IOF.Payment_Method
    ,IOF.Payment_Type
    ,iof.failed_transaction_reason
    ,COUNT(DISTINCT IOF.ORDER_ID) Num_Orders
from _ind_order_finance iof
group by
    IOF.Order_HQ_Day
    ,IOF.STORE_BRAND_NAME
    ,IOF.STORE_GROUP
    ,IOF.STORE_COUNTRY_ABBR
    ,IOF.ORDER_CLASSIFICATION
    ,IOF.order_type
    ,IOF.IS_PREPAID_CREDITCARD
    ,IOF.Payment_Method
    ,IOF.Payment_Type
    ,iof.failed_transaction_reason
