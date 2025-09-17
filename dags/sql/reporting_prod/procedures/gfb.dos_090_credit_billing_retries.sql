create or replace temporary table _credit_billing_orders as
SELECT
    upper(st.STORE_BRAND) as STORE_BRAND_NAME
    ,upper(st.STORE_REGION) as STORE_REGION_ABBR
    ,date_trunc(day, coalesce(fo.PAYMENT_TRANSACTION_LOCAL_DATETIME, fo.ORDER_LOCAL_DATETIME)) as FULL_DATE
    ,fo.order_id
    ,(case
        when dos.ORDER_STATUS = 'Cancelled' then 'cancelled'
        when dos.ORDER_STATUS = 'Failure' then 'failed'
        else 'success' end) as is_success_credit_billing
    ,(case
        when fo.IS_CREDIT_BILLING_ON_RETRY = 1 then 'retry'
        else 'non-retry' end) as is_credit_billing_retry
    ,dpm.CREDITCARD_TYPE
    ,fo.customer_id
FROM EDW_PROD.DATA_MODEL_JFB.FACT_ORDER fo
JOIN reporting_prod.gfb.vw_store st
    ON st.STORE_ID = fo.STORE_ID
JOIN EDW_PROD.DATA_MODEL_JFB.DIM_PAYMENT dpm
    ON dpm.PAYMENT_KEY = fo.PAYMENT_KEY
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_SALES_CHANNEL dosc
    on dosc.ORDER_SALES_CHANNEL_KEY = fo.ORDER_SALES_CHANNEL_KEY
    and dosc.IS_BORDER_FREE_ORDER = 0
    and dosc.IS_PS_ORDER = 0
    and dosc.IS_TEST_ORDER = 0
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_STATUS dos
    on dos.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
WHERE
    dosc.ORDER_CLASSIFICATION_L1 = 'Billing Order'
    and dos.ORDER_STATUS in ('Success', 'Cancelled', 'Failure')
    AND date_trunc(day, coalesce(fo.PAYMENT_TRANSACTION_LOCAL_DATETIME, fo.ORDER_LOCAL_DATETIME)) >= '2020-01-01';


create or replace temporary table _retry_attempts as
select
    cbo.STORE_BRAND_NAME
    ,cbo.STORE_REGION_ABBR
    ,cbo.FULL_DATE
    ,cbo.ORDER_ID
    ,cbo.is_success_credit_billing
    ,cbo.is_credit_billing_retry
    ,cbo.CREDITCARD_TYPE
    ,count(cbo.ORDER_ID) as credit_billing_retry_attempts
from _credit_billing_orders cbo
join LAKE_JFB_VIEW.ULTRA_MERCHANT.payment_transaction_creditcard ptc
    on ptc.order_id = cbo.ORDER_ID
where
    cbo.is_credit_billing_retry = 'retry'
    and ptc.TRANSACTION_TYPE = 'AUTH_ONLY'
group by
    cbo.STORE_BRAND_NAME
    ,cbo.STORE_REGION_ABBR
    ,cbo.FULL_DATE
    ,cbo.ORDER_ID
    ,cbo.is_success_credit_billing
    ,cbo.is_credit_billing_retry
    ,cbo.CREDITCARD_TYPE;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.GFB.dos_090_credit_billing_retries as
select
    cbo.STORE_BRAND_NAME
    ,cbo.STORE_REGION_ABBR
    ,cbo.FULL_DATE
    ,cbo.is_success_credit_billing
    ,cbo.is_credit_billing_retry
    ,cbo.CREDITCARD_TYPE

    ,count(distinct cbo.customer_id) AS customer_count
    ,count(cbo.ORDER_ID) as credit_billing_orders
    ,sum(coalesce(ra.credit_billing_retry_attempts, 0)) as credit_billing_retry_attempts
from _credit_billing_orders cbo
left join _retry_attempts ra
    on ra.ORDER_ID = cbo.ORDER_ID
group by
    cbo.STORE_BRAND_NAME
    ,cbo.STORE_REGION_ABBR
    ,cbo.FULL_DATE
    ,cbo.is_success_credit_billing
    ,cbo.is_credit_billing_retry
    ,cbo.CREDITCARD_TYPE;
