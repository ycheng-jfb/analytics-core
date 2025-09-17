set end_date = date_trunc(month, dateadd(day, -1, current_date()));
set start_date = dateadd(year, -2, $end_date);


create or replace temporary table _month_date as
select distinct
    dd.MONTH_DATE
from EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
where
    dd.FULL_DATE >= $start_date
    and dd.FULL_DATE <= $end_date;


create or replace temporary table _bop_vip_by_tenure as
select distinct
    cap.MONTH_DATE as bop_month_date
    ,dv.BUSINESS_UNIT
    ,dv.COUNTRY
    ,dv.REGION
    ,dv.CUSTOMER_ID
    ,dv.FIRST_ACTIVATING_COHORT
    ,dv.RECENT_VIP_CANCELLATION_DATE
    ,(case
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, cap.MONTH_DATE) + 1 > 24 then '24+'
        else cast(datediff(month, dv.FIRST_ACTIVATING_COHORT, cap.MONTH_DATE) + 1 as varchar(20))
        end) as tenure
    ,dv.MEMBERSHIP_PRICE
from EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY cap
join REPORTING_PROD.GFB.GFB_DIM_VIP dv
    on dv.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(cap.CUSTOMER_ID)
where
    dv.BUSINESS_UNIT in ('JUSTFAB', 'SHOEDAZZLE')
    and dv.REGION = 'NA'
    and cap.MONTH_DATE >= $start_date
    and cap.MONTH_DATE <= $end_date
    and cap.IS_BOP_VIP = 1;


create or replace temporary table _credit_billing_rc as
select
    mca.BUSINESS_UNIT
    ,mca.REGION
    ,mca.country
    ,mca.CUSTOMER_ID
    ,md.MONTH_DATE
    ,(case
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, md.MONTH_DATE) + 1 > 24 then '24+'
        else cast(datediff(month, dv.FIRST_ACTIVATING_COHORT, md.MONTH_DATE) + 1 as varchar(20))
        end) as tenure
    ,sum(mca.TOTAL_REFUND_CASH_AMOUNT) as cancel_amount
from REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE mca
join _month_date md
    on md.MONTH_DATE = date_trunc(month, mca.REFUND_DATE)
    and md.MONTH_DATE < $end_date
join REPORTING_PROD.GFB.GFB_DIM_VIP dv
    on dv.CUSTOMER_ID = mca.CUSTOMER_ID
where
    mca.BUSINESS_UNIT in ('JUSTFAB', 'SHOEDAZZLE')
    and mca.REGION = 'NA'
    and mca.ORDER_CLASSIFICATION = 'credit billing'
group by
    mca.BUSINESS_UNIT
    ,mca.REGION
    ,mca.country
    ,mca.CUSTOMER_ID
    ,md.MONTH_DATE
    ,(case
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, md.MONTH_DATE) + 1 > 24 then '24+'
        else cast(datediff(month, dv.FIRST_ACTIVATING_COHORT, md.MONTH_DATE) + 1 as varchar(20))
        end)

union

select
    mca.BUSINESS_UNIT
    ,mca.REGION
    ,mca.country
    ,mca.CUSTOMER_ID
    ,md.MONTH_DATE
    ,(case
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, md.MONTH_DATE) + 1 > 24 then '24+'
        else cast(datediff(month, dv.FIRST_ACTIVATING_COHORT, md.MONTH_DATE) + 1 as varchar(20))
        end) as tenure
    ,sum(mca.TOTAL_CHARGEBACK_AMOUNT) as cancel_amount
from REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE mca
join _month_date md
    on md.MONTH_DATE = date_trunc(month, mca.CHARGEBACK_DATE)
    and md.MONTH_DATE < $end_date
join REPORTING_PROD.GFB.GFB_DIM_VIP dv
    on dv.CUSTOMER_ID = mca.CUSTOMER_ID
where
    mca.BUSINESS_UNIT in ('JUSTFAB', 'SHOEDAZZLE')
    and mca.REGION = 'NA'
    and mca.ORDER_CLASSIFICATION = 'credit billing'
group by
    mca.BUSINESS_UNIT
    ,mca.REGION
    ,mca.country
    ,mca.CUSTOMER_ID
    ,md.MONTH_DATE
    ,(case
        when datediff(month, dv.FIRST_ACTIVATING_COHORT, md.MONTH_DATE) + 1 > 24 then '24+'
        else cast(datediff(month, dv.FIRST_ACTIVATING_COHORT, md.MONTH_DATE) + 1 as varchar(20))
        end);


create or replace temporary table _cancel_rate as
select
    bop.BUSINESS_UNIT
    ,bop.REGION
    ,bop.COUNTRY
    ,bop.bop_month_date as month_date
    ,bop.tenure

    ,red.vip_count * 1.0/bop.bop_vip as vip_credit_cancel_rate
    ,red.credit_cancel_amount * 1.0/red.vip_count as credit_cancel_amount_per_vip
    ,bop.bop_vip
    ,red.vip_count
    ,red.credit_cancel_amount
from
(
    select
        bop.BUSINESS_UNIT
        ,bop.REGION
        ,bop.country
        ,bop.bop_month_date
        ,bop.tenure

        ,count(distinct bop.CUSTOMER_ID) as bop_vip
    from _bop_vip_by_tenure bop
    group by
        bop.BUSINESS_UNIT
        ,bop.REGION
        ,bop.country
        ,bop.bop_month_date
        ,bop.tenure
) bop
left join
(
    select
        cr.BUSINESS_UNIT
        ,cr.REGION
        ,cr.country
        ,cr.MONTH_DATE
        ,cr.tenure

        ,count(distinct cr.CUSTOMER_ID) as vip_count
        ,sum(cr.cancel_amount) as credit_cancel_amount
    from _credit_billing_rc cr
    group by
        cr.BUSINESS_UNIT
        ,cr.REGION
        ,cr.country
        ,cr.MONTH_DATE
        ,cr.tenure
) red on red.BUSINESS_UNIT = bop.BUSINESS_UNIT
    and red.REGION = bop.REGION
    and red.country = bop.COUNTRY
    and red.MONTH_DATE = bop.bop_month_date
    and red.tenure = bop.tenure;


create or replace temporary table _credit_cancel_coef as
select
    ly.BUSINESS_UNIT
    ,ly.REGION
    ,ly.COUNTRY
    ,ty.credit_cancel_amount_per_vip * 1.0/ly.credit_cancel_amount_per_vip as credit_cancel_amount_per_vip_coef
    ,ty.vip_credit_cancel_rate * 1.0/ly.vip_credit_cancel_rate as vip_credit_cancel_rate_coef
from
(
    select
        rr.BUSINESS_UNIT
        ,rr.REGION
        ,rr.country

        ,sum(rr.credit_cancel_amount) * 1.0/sum(rr.vip_count) as credit_cancel_amount_per_vip
        ,sum(rr.vip_count) * 1.0/sum(rr.bop_vip) as vip_credit_cancel_rate
    from _cancel_rate rr
    where
        rr.MONTH_DATE >= dateadd(month, -6, dateadd(year, -1, $end_date))
        and rr.MONTH_DATE < dateadd(year, -1, $end_date)
    group by
        rr.BUSINESS_UNIT
        ,rr.REGION
        ,rr.country
) ly
join
(
    select
        rr.BUSINESS_UNIT
        ,rr.REGION
        ,rr.country

        ,sum(rr.credit_cancel_amount) * 1.0/sum(rr.vip_count) as credit_cancel_amount_per_vip
        ,sum(rr.vip_count) * 1.0/sum(rr.bop_vip) as vip_credit_cancel_rate
    from _cancel_rate rr
    where
        rr.MONTH_DATE >= dateadd(month, -6, $end_date)
        and rr.MONTH_DATE < $end_date
    group by
        rr.BUSINESS_UNIT
        ,rr.REGION
        ,rr.country
) ty on ty.BUSINESS_UNIT = ly.BUSINESS_UNIT
    and ty.REGION = ly.REGION
    and ty.COUNTRY = ly.COUNTRY;


create or replace temporary table _credit_cancel_forecast as
select
    bvt.BUSINESS_UNIT
    ,bvt.REGION
    ,bvt.COUNTRY
    ,bvt.bop_month_date as month_date
    ,sum(
            rate.credit_cancel_amount_per_vip * cbrc.credit_cancel_amount_per_vip_coef
            * rate.vip_credit_cancel_rate * cbrc.vip_credit_cancel_rate_coef
        ) as forecast_credit_cancelled_amount
from _bop_vip_by_tenure bvt
left join
(
    select
        cbr.BUSINESS_UNIT
        ,cbr.REGION
        ,cbr.COUNTRY
        ,cbr.tenure
        ,avg(cbr.credit_cancel_amount_per_vip) as credit_cancel_amount_per_vip
        ,avg(cbr.vip_credit_cancel_rate) as vip_credit_cancel_rate
    from _cancel_rate cbr
    where
        cbr.MONTH_DATE = dateadd(year, -1, $end_date)
    group by
        cbr.BUSINESS_UNIT
        ,cbr.REGION
        ,cbr.COUNTRY
        ,cbr.tenure
) rate on rate.BUSINESS_UNIT = bvt.BUSINESS_UNIT
    and rate.REGION = bvt.REGION
    and rate.COUNTRY = bvt.COUNTRY
    and rate.tenure = bvt.tenure
left join _credit_cancel_coef cbrc
    on cbrc.BUSINESS_UNIT = bvt.BUSINESS_UNIT
    and cbrc.REGION = bvt.REGION
    and cbrc.COUNTRY = bvt.COUNTRY
where
    bvt.bop_month_date = $end_date
group by
    bvt.BUSINESS_UNIT
    ,bvt.REGION
    ,bvt.COUNTRY
    ,bvt.bop_month_date;


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb051_03_finance_forecast_credit_cancellation as
select
    cbf.BUSINESS_UNIT
    ,cbf.REGION
    ,dateadd(year, 1, r.DATE) as date
    ,sum(cbf.forecast_credit_cancelled_amount * r.CREDIT_CANCEL_RATE_DAILY) as forecast_credit_cancelled_amount
from _credit_cancel_forecast cbf
join REPORTING_PROD.GFB.GFB051_04_FINANCE_FORECAST_DAILY_RATE r
    on r.BUSINESS_UNIT = cbf.BUSINESS_UNIT
    and r.REGION = cbf.REGION
    and r.COUNTRY = cbf.COUNTRY
    and date_trunc(month, r.DATE) = dateadd(year, -1, cbf.month_date)
group by
    cbf.BUSINESS_UNIT
    ,cbf.REGION
    ,dateadd(year, 1, r.DATE);
