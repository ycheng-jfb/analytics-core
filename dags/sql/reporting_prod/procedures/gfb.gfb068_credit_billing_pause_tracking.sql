create or replace temporary table _snoozed_vip as
select
    vd.BUSINESS_UNIT
    ,vd.REGION
    ,vd.CUSTOMER_ID
    ,min(ms.DATE_START) as first_pause_date
from REPORTING_PROD.GFB.GFB_DIM_VIP vd
join LAKE_JFB_VIEW.ULTRA_MERCHANT.MEMBERSHIP m
    on m.CUSTOMER_ID = vd.CUSTOMER_ID
join LAKE_JFB_VIEW.ULTRA_MERCHANT.MEMBERSHIP_SNOOZE ms
    on ms.MEMBERSHIP_ID = m.MEMBERSHIP_ID
where
    vd.FIRST_ACTIVATING_DATE >= '2020-05-01'
group by
    vd.BUSINESS_UNIT
    ,vd.REGION
    ,vd.CUSTOMER_ID;


create or replace transient table REPORTING_PROD.GFB.gfb068_credit_billing_pause_tracking as
select
    ma.*
    ,coalesce(sn.snoozed_count, 0) as snoozed_count
from
(
    select
        sv.BUSINESS_UNIT
        ,sv.REGION
        ,clvm.MONTH_DATE
        ,sv.first_pause_date

        ,count(distinct case
                when clvm.PRODUCT_ORDER_COUNT > 0
                then clvm.CUSTOMER_ID end) as product_purchaser
        ,count(distinct case
                when clvm.IS_SKIP = 1
                then clvm.CUSTOMER_ID end) as SKIPs
        ,count(distinct case
                when clvm.IS_SUCCESSFUL_BILLING = 1
                then clvm.CUSTOMER_ID end) as SUCCESSFUL_BILLINGs
        ,count(distinct case
                when clvm.IS_CANCEL = 1
                then clvm.CUSTOMER_ID end) as CANCELs
        ,count(distinct case
                when clvm.IS_BOP_VIP = 1
                then clvm.CUSTOMER_ID end) as bop_vips
        ,count(distinct case
                when clvm.MONTHLY_BILLED_CREDIT_CASH_REFUND_COUNT > 1
                then clvm.CUSTOMER_ID end) as credit_billing_refunds
    from EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY clvm
    join _snoozed_vip sv
        on sv.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
    where
        clvm.MONTH_DATE >= sv.first_pause_date
    group by
        sv.BUSINESS_UNIT
        ,sv.REGION
        ,clvm.MONTH_DATE
        ,sv.first_pause_date
) ma
left join
(
    select
        vd.BUSINESS_UNIT
        ,vd.REGION
        ,date_trunc(month, ms.DATETIME_ADDED) as month_date
        ,vd.first_pause_date

        ,count(vd.CUSTOMER_ID) as snoozed_count
    from _snoozed_vip vd
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.MEMBERSHIP m
        on m.CUSTOMER_ID = vd.CUSTOMER_ID
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.MEMBERSHIP_SNOOZE ms
        on ms.MEMBERSHIP_ID = m.MEMBERSHIP_ID
        and ms.DATE_START > vd.first_pause_date
    group by
        vd.BUSINESS_UNIT
        ,vd.REGION
        ,date_trunc(month, ms.DATETIME_ADDED)
        ,vd.first_pause_date
) sn on sn.BUSINESS_UNIT = ma.BUSINESS_UNIT
    and sn.REGION = ma.REGION
    and sn.month_date = ma.MONTH_DATE
    and sn.first_pause_date = ma.first_pause_date;
