set start_date = date_trunc(year, dateadd(year, -2, current_date()));

create or replace temporary table _activating_order_detail as
select distinct
    pd.BUSINESS_UNIT
    ,pd.REGION
    ,pd.COUNTRY
    ,date_trunc(month, pd.ORDER_DATE) as activating_month
    ,coalesce(pd.PROMO_CODE_1, 'No Promo') as PROMO_CODE_1
    ,pd.PROMO_1_PROMOTION_NAME
    ,pd.PROMO_1_OFFER
    ,pd.PROMO_ID_1
    ,coalesce(pd.PROMO_CODE_2, 'No Promo') as PROMO_CODE_2
    ,pd.PROMO_2_PROMOTION_NAME
    ,pd.PROMO_2_OFFER
    ,pd.PROMO_ID_2
    ,pd.ORDER_ID
    ,pd.CUSTOMER_ID
    ,pd.ACTIVATING_OFFERS
from REPORTING_PROD.GFB.GFB011_PROMO_DATA_SET pd
where
    pd.ORDER_TYPE = 'vip activating'
    and (
            pd.PROMO_ID_1 is not null
            or
            pd.PROMO_ID_2 is not null
        )
    and pd.ORDER_DATE >= $start_date;


create or replace temporary table _vip_cohort as
select
    aod.BUSINESS_UNIT
    ,aod.REGION
    ,aod.COUNTRY
    ,aod.activating_month
    ,aod.PROMO_CODE_1
    ,aod.PROMO_1_PROMOTION_NAME
    ,aod.PROMO_1_OFFER
    ,aod.PROMO_ID_1
    ,aod.PROMO_CODE_2
    ,aod.PROMO_2_PROMOTION_NAME
    ,aod.PROMO_2_OFFER
    ,aod.PROMO_ID_2
    ,aod.ACTIVATING_OFFERS
    ,count(aod.CUSTOMER_ID) as vip_count_by_cohort
from _activating_order_detail aod
group by
    aod.BUSINESS_UNIT
    ,aod.REGION
    ,aod.COUNTRY
    ,aod.activating_month
    ,aod.PROMO_CODE_1
    ,aod.PROMO_1_PROMOTION_NAME
    ,aod.PROMO_1_OFFER
    ,aod.PROMO_ID_1
    ,aod.PROMO_CODE_2
    ,aod.PROMO_2_PROMOTION_NAME
    ,aod.PROMO_2_OFFER
    ,aod.PROMO_ID_2
    ,aod.ACTIVATING_OFFERS;


create or replace temporary table _activating_promo_ltv as
select
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.activating_month
    ,a.PROMO_CODE_1
    ,a.PROMO_1_PROMOTION_NAME
    ,a.PROMO_1_OFFER
    ,a.PROMO_ID_1
    ,a.PROMO_CODE_2
    ,a.PROMO_2_PROMOTION_NAME
    ,a.PROMO_2_OFFER
    ,a.PROMO_ID_2
    ,a.ACTIVATING_OFFERS
    ,clvm.MONTH_DATE
    ,datediff(month, a.activating_month, clvm.MONTH_DATE) + 1 as tenure

    ,sum(clvm.CASH_GROSS_PROFIT) as PERIOD_CASH_GROSS_MARGIN
    ,sum(clvm.PRODUCT_GROSS_PROFIT) as period_product_gross_margin
    ,sum(clvm.CUMULATIVE_CASH_GROSS_PROFIT) as CUMULATIVE_CASH_GROSS_MARGIN
    ,sum(clvm.CUMULATIVE_PRODUCT_GROSS_PROFIT) as cumulative_product_gross_margin
    ,count(distinct case
            when clvm.IS_BOP_VIP = 1 then clvm.CUSTOMER_ID
            end) as vip_count_by_month
from EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY clvm
join _activating_order_detail a
    on a.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
    and a.activating_month <= clvm.MONTH_DATE
group by
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.activating_month
    ,a.PROMO_CODE_1
    ,a.PROMO_1_PROMOTION_NAME
    ,a.PROMO_1_OFFER
    ,a.PROMO_ID_1
    ,a.PROMO_CODE_2
    ,a.PROMO_2_PROMOTION_NAME
    ,a.PROMO_2_OFFER
    ,a.PROMO_ID_2
    ,a.ACTIVATING_OFFERS
    ,clvm.MONTH_DATE
    ,datediff(month, a.activating_month, clvm.MONTH_DATE) + 1;


create or replace transient table reporting_prod.GFB.gfb024_activating_promo_ltv as
select
    vc.*
    ,apl.MONTH_DATE
    ,apl.tenure
    ,coalesce(apl.PERIOD_CASH_GROSS_MARGIN, 0) as PERIOD_CASH_GROSS_MARGIN
    ,coalesce(apl.period_product_gross_margin, 0) as period_product_gross_margin
    ,coalesce(apl.CUMULATIVE_CASH_GROSS_MARGIN, 0) as CUMULATIVE_CASH_GROSS_MARGIN
    ,coalesce(apl.cumulative_product_gross_margin, 0) as cumulative_product_gross_margin
    ,coalesce(apl.vip_count_by_month, 0) as vip_count_by_month
from _vip_cohort vc
join _activating_promo_ltv apl
    on vc.BUSINESS_UNIT = apl.BUSINESS_UNIT
    and vc.REGION = apl.REGION
    and vc.COUNTRY = apl.COUNTRY
    and vc.activating_month = apl.activating_month
    and coalesce(vc.PROMO_ID_1, 0) = coalesce(apl.PROMO_ID_1, 0)
    and coalesce(vc.PROMO_ID_2, 0) = coalesce(apl.PROMO_ID_2, 0)
    and vc.ACTIVATING_OFFERS = apl.ACTIVATING_OFFERS;


create or replace transient table reporting_prod.GFB.gfb024_01_promo_code_attribute as
select distinct
    a.PROMO_CODE_1 as promo_code
    ,a.PROMO_1_OFFER as promo_offer
from reporting_prod.GFB.gfb024_activating_promo_ltv a
where
    a.PROMO_CODE_1 is not null

union

select distinct
    a.PROMO_CODE_2 as promo_code
    ,a.PROMO_2_OFFER as promo_offer
from reporting_prod.GFB.gfb024_activating_promo_ltv a
where
    a.PROMO_CODE_2 is not null;
