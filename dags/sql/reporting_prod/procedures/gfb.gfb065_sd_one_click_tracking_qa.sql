set start_date = '2021-01-01';


create or replace temporary table _cal_vip_qa as
select
    dv.BUSINESS_UNIT
    ,dv.REGION
    ,dv.COUNTRY
    ,dv.DEFAULT_STATE_PROVINCE as state
    ,dv.CUSTOMER_ID
    ,dv.FIRST_ACTIVATING_COHORT
    ,dv.FIRST_ACTIVATING_DATE
    ,dv.RECENT_VIP_CANCELLATION_DATE
from REPORTING_PROD.GFB.GFB_DIM_VIP dv
where
    dv.BUSINESS_UNIT = 'SHOEDAZZLE'
    and dv.COUNTRY = 'US'
    and dv.DEFAULT_STATE_PROVINCE = 'CA';


create or replace temporary table _membership_cancel_clicked_qa as
select distinct
     bc.properties_name
    ,bc.properties_customer_id AS customer_id
    ,date_trunc(month, bc.originaltimestamp::DATE) as month_date
from LAKE.SEGMENT_GFB.JAVASCRIPT_SHOEDAZZLE_BUTTON_CLICKED bc
join _cal_vip_qa cv
    on cv.CUSTOMER_ID = bc.properties_customer_id
where
    bc.properties_name in ('One Click Cancel - Cancel Membership','One Click Cancel - Skip The Month Confirmed')
    and bc.originaltimestamp::DATE >= $start_date;

create or replace temporary table _membership_cancel_viewed_qa as
select distinct
     bv.properties_name
    ,bv.properties_customer_id AS customer_id
    ,date_trunc(month, bv.originaltimestamp::DATE) as month_date
from LAKE.SEGMENT_GFB.JAVASCRIPT_SHOEDAZZLE_BUTTON_VIEWED bv
join _cal_vip_qa cv
    on cv.CUSTOMER_ID = bv.properties_customer_id
where
    bv.properties_name in ('One Click Cancel - Cancel Membership','One Click Cancel - Skip The Month')
    and bv.originaltimestamp::DATE >= $start_date;


create or replace temporary table _customer_online_cancellation_qa as
SELECT distinct
    cv.CUSTOMER_ID
    ,date_trunc(month, a.DATETIME_ADDED) as month_date
FROM lake_jfb_view.ultra_merchant.CUSTOMER_LOG a
join _cal_vip_qa cv
    on cv.CUSTOMER_ID = a.CUSTOMER_ID
WHERE
    lower(a.comment) like '%membership has been set to pay as you go by customer using online cancel%'
    and a.DATETIME_ADDED >= $start_date;


create or replace temporary table _bop_vip_qa as
select
    cv.BUSINESS_UNIT
    ,cv.REGION
    ,cv.COUNTRY
    ,cv.state
    ,clvm.MONTH_DATE
    ,cv.CUSTOMER_ID
    ,clvm.IS_CANCEL
    ,clvm.IS_SKIP

    ,(case
        when mcc.CUSTOMER_ID is not null then 'one click cancel confirmed'
        when coc.CUSTOMER_ID is not null then 'online cancel'
        when clvm.IS_CANCEL = 1 then 'other cancel'
        end) as cancellation_type
    ,(case
        when mcv.CUSTOMER_ID is not null then 'one click cancel viewed'
        else 'no one click cancel viewed'
        end) as one_click_cancel_viewed
    ,(case
        when sc.CUSTOMER_ID is not null then 'one click skip confirmed'
        when clvm.IS_SKIP = 1 then 'normal skip'
        end) as skip_type
    ,(case
        when sv.CUSTOMER_ID is not null then 'one click skip viewed'
        else 'no one click skip viewed'
        end) as one_click_skip_viewed
from EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY clvm
join _cal_vip_qa cv
    on cv.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
left join _membership_cancel_clicked_qa mcc
    on mcc.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
    and mcc.month_date = clvm.MONTH_DATE
    and mcc.properties_name = 'One Click Cancel - Cancel Membership'
left join _membership_cancel_viewed_qa mcv
    on mcv.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
    and mcv.month_date = clvm.MONTH_DATE
    and mcv.properties_name = 'One Click Cancel - Cancel Membership'
left join _membership_cancel_clicked_qa sc
    on sc.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
    and sc.month_date = clvm.MONTH_DATE
    and sc.properties_name = 'One Click Cancel - Skip The Month Confirmed'
left join _membership_cancel_viewed_qa sv
    on sv.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
    and sv.month_date = clvm.MONTH_DATE
    and sv.properties_name = 'One Click Cancel - Skip The Month'
left join _customer_online_cancellation_qa coc
    on coc.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
    and coc.month_date = clvm.MONTH_DATE
where
    clvm.MONTH_DATE >= $start_date
    and clvm.IS_BOP_VIP = 1;


create or replace transient table REPORTING_PROD.GFB.gfb065_sd_one_click_tracking_qa as
select
    bv.BUSINESS_UNIT
    ,bv.REGION
    ,bv.COUNTRY
    ,bv.state
    ,bv.MONTH_DATE

    ,count(bv.CUSTOMER_ID) as bop_vip
    ,count(case
            when bv.IS_CANCEL = 1
            then bv.CUSTOMER_ID end) as total_cancels
    ,count(case
            when bv.cancellation_type = 'online cancel'
            then bv.CUSTOMER_ID end) as online_cancels
    ,count(case
            when bv.cancellation_type = 'other cancel'
            then bv.CUSTOMER_ID end) as other_cancels
    ,count(case
            when bv.cancellation_type = 'one click cancel confirmed'
            then bv.CUSTOMER_ID end) as one_click_cancels
    ,count(case
            when bv.one_click_cancel_viewed = 'one click cancel viewed'
            then bv.CUSTOMER_ID end) as one_click_cancel_viewed
    ,count(case
            when bv.IS_SKIP = 1
            then bv.CUSTOMER_ID end) as total_skips
    ,count(case
            when bv.skip_type = 'normal skip'
            then bv.CUSTOMER_ID end) as normal_skips
    ,count(case
            when bv.skip_type = 'one click skip confirmed'
            then bv.CUSTOMER_ID end) as one_click_skips
    ,count(case
            when bv.one_click_skip_viewed = 'one click skip viewed'
            then bv.CUSTOMER_ID end) as one_click_skip_viewed
from _bop_vip_qa bv
group by
    bv.BUSINESS_UNIT
    ,bv.REGION
    ,bv.COUNTRY
    ,bv.state
    ,bv.MONTH_DATE;