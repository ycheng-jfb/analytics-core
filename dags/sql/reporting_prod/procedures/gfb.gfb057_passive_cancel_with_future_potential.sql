set start_date = date_trunc(year, dateadd(year, -2, current_date()));


create or replace temporary table _vip_cancellation as
select
    dv.BUSINESS_UNIT
    ,dv.REGION
    ,date_trunc(month, dv.RECENT_VIP_CANCELLATION_DATE) as cancel_month
    ,(case
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, cancel_month) + 1 < 15
        then 'M' || cast(datediff(month, dv.FIRST_ACTIVATING_COHORT, cancel_month) + 1 as varchar(100))
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, cancel_month) + 1 < 25
        then 'M15-M24'
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, cancel_month) + 1 < 36
        then 'M25-M36'
        else 'M36+' end) as tenure

    ,count(dv.CUSTOMER_ID) as vip_cancels
    ,count(case
            when dv.RECENT_CANCELLATION_TYPE = 'Passive'
            then dv.CUSTOMER_ID end) as vip_passive_cancels
from REPORTING_PROD.GFB.GFB_DIM_VIP dv
where
    dv.RECENT_VIP_CANCELLATION_DATE >= $start_date
group by
    dv.BUSINESS_UNIT
    ,dv.REGION
    ,date_trunc(month, dv.RECENT_VIP_CANCELLATION_DATE)
    ,(case
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, cancel_month) + 1 < 15
        then 'M' || cast(datediff(month, dv.FIRST_ACTIVATING_COHORT, cancel_month) + 1 as varchar(100))
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, cancel_month) + 1 < 25
        then 'M15-M24'
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, cancel_month) + 1 < 36
        then 'M25-M36'
        else 'M36+' end);


create or replace temporary table _current_vips as
select
    dv.BUSINESS_UNIT
    ,dv.REGION
    ,dv.CUSTOMER_ID
    ,dv.FIRST_ACTIVATING_COHORT
from REPORTING_PROD.GFB.GFB_DIM_VIP dv
where
    dv.CURRENT_MEMBERSHIP_STATUS = 'VIP';


create or replace temporary table _last_session as
select
    cv.CUSTOMER_ID
    ,max(cast(s.SESSION_LOCAL_DATETIME as date)) as last_session_date
from _current_vips cv
join REPORTING_BASE_PROD.SHARED.SESSION s
    on edw_prod.stg.udf_unconcat_brand(s.CUSTOMER_ID) = cv.CUSTOMER_ID
group by
    cv.CUSTOMER_ID;


create or replace temporary table _last_purchase as
select
    cv.CUSTOMER_ID
    ,max(olp.ORDER_DATE) as last_order_date
from _current_vips cv
join REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE olp
    on olp.CUSTOMER_ID = cv.CUSTOMER_ID
where
    olp.ORDER_CLASSIFICATION in ('product order', 'credit billing')
    and olp.ORDER_TYPE = 'vip repeat'
group by
    cv.CUSTOMER_ID;


create or replace temporary table _last_skip as
select
    cv.CUSTOMER_ID
    ,max(cast(fca.CUSTOMER_ACTION_LOCAL_DATETIME as date)) as last_skip_date
from _current_vips cv
join EDW_PROD.DATA_MODEL_JFB.FACT_CUSTOMER_ACTION fca
    on fca.CUSTOMER_ID = cv.CUSTOMER_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER_ACTION_TYPE cat
    on cat.CUSTOMER_ACTION_TYPE_KEY = fca.CUSTOMER_ACTION_TYPE_KEY
    and cat.CUSTOMER_ACTION_TYPE = 'Skipped Month'
group by
    cv.CUSTOMER_ID;


create or replace temporary table _future_potential_passive_cancel as
select
    cv.BUSINESS_UNIT
    ,cv.REGION
    ,cv.CUSTOMER_ID

    ,ls.last_session_date
    ,lp.last_order_date
    ,lsk.last_skip_date

    ,(case
        when coalesce(datediff(month, ls.last_session_date, current_date()), 12) >= 12
            and coalesce(datediff(month, lp.last_order_date, current_date()), 12) >= 12
            and coalesce(datediff(month, lsk.last_skip_date, current_date()), 12) >= 12
            then dateadd(month, 1, date_trunc(month, current_date()))
        when coalesce(datediff(month, ls.last_session_date, current_date()), 11) >= 11
            and coalesce(datediff(month, lp.last_order_date, current_date()), 11) >= 11
            and coalesce(datediff(month, lsk.last_skip_date, current_date()), 11) >= 11
            then dateadd(month, 2, date_trunc(month, current_date()))
        when coalesce(datediff(month, ls.last_session_date, current_date()), 10) >= 10
            and coalesce(datediff(month, lp.last_order_date, current_date()), 10) >= 10
            and coalesce(datediff(month, lsk.last_skip_date, current_date()), 10) >= 10
            then dateadd(month, 3, date_trunc(month, current_date()))
        end ) as potential_passive_cancel_month
    ,(case
        when coalesce(datediff(month, ls.last_session_date, current_date()), 12) >= 12
            and coalesce(datediff(month, lp.last_order_date, current_date()), 12) >= 12
            and coalesce(datediff(month, lsk.last_skip_date, current_date()), 12) >= 12
            then datediff(month, cv.FIRST_ACTIVATING_COHORT, potential_passive_cancel_month) + 1
        when coalesce(datediff(month, ls.last_session_date, current_date()), 11) >= 11
            and coalesce(datediff(month, lp.last_order_date, current_date()), 11) >= 11
            and coalesce(datediff(month, lsk.last_skip_date, current_date()), 11) >= 11
            then datediff(month, cv.FIRST_ACTIVATING_COHORT, potential_passive_cancel_month) + 1
        when coalesce(datediff(month, ls.last_session_date, current_date()), 10) >= 10
            and coalesce(datediff(month, lp.last_order_date, current_date()), 10) >= 10
            and coalesce(datediff(month, lsk.last_skip_date, current_date()), 10) >= 10
            then datediff(month, cv.FIRST_ACTIVATING_COHORT, potential_passive_cancel_month) + 1
        end ) as potential_passive_cancel_tenure
    ,(case
        when potential_passive_cancel_tenure < 15
        then 'M' || cast(potential_passive_cancel_tenure as varchar(100))
        when potential_passive_cancel_tenure < 25
        then 'M15-M24'
        when potential_passive_cancel_tenure < 36
        then 'M25-M36'
        else 'M36+' end) as tenure
from _current_vips cv
join LAKE_JFB_VIEW.ULTRA_MERCHANT.MEMBERSHIP m
    on m.CUSTOMER_ID = cv.CUSTOMER_ID
    and m.STATUSCODE = 3930
left join _last_session ls
    on ls.CUSTOMER_ID = cv.CUSTOMER_ID
left join _last_purchase lp
    on lp.CUSTOMER_ID = cv.CUSTOMER_ID
left join _last_skip lsk
    on lsk.CUSTOMER_ID = cv.CUSTOMER_ID
where
    potential_passive_cancel_month is not null;


create or replace transient table REPORTING_PROD.GFB.gfb057_passive_cancel_with_future_potential as
select
    coalesce(vc.BUSINESS_UNIT, fp.BUSINESS_UNIT) as BUSINESS_UNIT
    ,coalesce(vc.REGION, fp.REGION) as REGION
    ,coalesce(vc.cancel_month, fp.potential_passive_cancel_month) as cancel_month
    ,coalesce(vc.tenure, fp.tenure) as tenure

    ,vc.vip_cancels
    ,vc.vip_passive_cancels
    ,fp.potential_passive_cancels
from _vip_cancellation vc
full join
(
    select
        fp.BUSINESS_UNIT
        ,fp.REGION
        ,fp.potential_passive_cancel_month
        ,fp.tenure

        ,count(fp.CUSTOMER_ID) as potential_passive_cancels
    from _future_potential_passive_cancel fp
    group by
        fp.BUSINESS_UNIT
        ,fp.REGION
        ,fp.potential_passive_cancel_month
        ,fp.tenure
) fp on fp.BUSINESS_UNIT = vc.BUSINESS_UNIT
    and fp.REGION = vc.REGION
    and fp.tenure = vc.tenure
    and fp.potential_passive_cancel_month = vc.cancel_month;
