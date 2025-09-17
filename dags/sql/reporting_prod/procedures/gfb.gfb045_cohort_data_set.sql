set start_date = '2015-01-01';


create or replace temporary table _vips as
select
    dv.BUSINESS_UNIT || ' ' || dv.REGION as store_region
    ,dv.CUSTOMER_ID
    ,dv.FIRST_ACTIVATING_COHORT as vip_cohort
    ,dv.FIRST_ACTIVATING_DATE as vip_start_date
    ,coalesce(dv.RECENT_VIP_CANCELLATION_DATE, current_date()) as vip_end_date
from REPORTING_PROD.GFB.GFB_DIM_VIP dv
where
    dv.FIRST_ACTIVATING_COHORT >= $start_date;


create or replace temporary table _vip_count as
select
    v.store_region
    ,v.vip_cohort
    ,count(v.CUSTOMER_ID) as vip_count_by_cohort
from _vips v
group by
    v.store_region
    ,v.vip_cohort;


create or replace temporary table _ltv as
select distinct
    a.store_region
    ,a.vip_cohort
    ,a.MONTH_DATE

    ,a.CUMULATIVE_CASH_GROSS_MARGIN
    ,sum(a.cash_net_revenue) over (partition by a.store_region, a.vip_cohort order by a.MONTH_DATE) as cumulative_net_cash_revenue
from
(
    select
        v.store_region
        ,v.vip_cohort
        ,clvm.MONTH_DATE

        ,sum(clvm.CUMULATIVE_CASH_GROSS_PROFIT) as CUMULATIVE_CASH_GROSS_MARGIN
        ,sum(clvm.CASH_NET_REVENUE) as cash_net_revenue
    from EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY clvm
    join _vips v
        on v.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
        and clvm.MONTH_DATE >= v.vip_cohort
    where
        clvm.MONTH_DATE >= $start_date
    group by
        v.store_region
        ,v.vip_cohort
        ,clvm.MONTH_DATE
) a;


create or replace temporary table _orders as
select
    v.store_region
    ,v.vip_cohort
    ,date_trunc(month, olp.SHIP_DATE) as month_date

    ,count(distinct olp.ORDER_ID) as orders
    ,sum(olp.TOTAL_QTY_SOLD) as TOTAL_QTY_SOLD
    ,sum(olp.TOTAL_PRODUCT_REVENUE + olp.TOTAL_SHIPPING_REVENUE - olp.TOTAL_NON_CASH_CREDIT_AMOUNT) as gaap_gross_revenue
from REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_SHIP_DATE olp
join _vips v
    on v.CUSTOMER_ID = olp.CUSTOMER_ID
    and olp.SHIP_DATE >= v.vip_cohort
where
    olp.ORDER_CLASSIFICATION = 'product order'
    and olp.SHIP_DATE >= $start_date
group by
    v.store_region
    ,v.vip_cohort
    ,date_trunc(month, olp.SHIP_DATE);


create or replace transient table REPORTING_PROD.GFB.gfb045_cohort_data_set as
select
    a.*
    ,vc.vip_count_by_cohort
from
(
    select
        coalesce(o.store_region, l.store_region) as store_region
        ,coalesce(o.vip_cohort, l.vip_cohort) as vip_cohort
        ,coalesce(o.month_date, l.MONTH_DATE) as MONTH_DATE

        ,coalesce(o.orders, 0) as orders
        ,coalesce(o.TOTAL_QTY_SOLD, 0) as TOTAL_QTY_SOLD
        ,coalesce(o.gaap_gross_revenue, 0) as gaap_gross_revenue
        ,coalesce(l.CUMULATIVE_NET_CASH_REVENUE, 0) as CUMULATIVE_NET_CASH_REVENUE
        ,coalesce(l.CUMULATIVE_CASH_GROSS_MARGIN, 0) as  CUMULATIVE_CASH_GROSS_MARGIN
    from _orders o
    full join _ltv l
        on l.store_region = o.store_region
        and l.vip_cohort = o.vip_cohort
        and l.MONTH_DATE = o.month_date
) a
join _vip_count vc
    on vc.store_region = a.store_region
    and vc.vip_cohort = a.vip_cohort;
