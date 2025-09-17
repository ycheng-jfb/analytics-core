create or replace temporary table _gfb_dim_vip_qa as
select BUSINESS_UNIT, REGION, CUSTOMER_ID  from REPORTING_PROD.GFB.GFB_DIM_VIP
group by 1,2,3;

create or replace temporary table _customer_lifetime_value_monthly_qa as 
select distinct MONTH_DATE,CUSTOMER_ID ,VIP_COHORT_MONTH_DATE, IS_CANCEL, CUMULATIVE_PRODUCT_GROSS_PROFIT,
CUMULATIVE_CASH_GROSS_PROFIT, IS_BOP_VIP
from EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY clvm
where
    (clvm.IS_BOP_VIP = 1 or clvm.VIP_COHORT_MONTH_DATE = clvm.MONTH_DATE);

create or replace temporary table _customer_monthly_value_qa as
select
    dv.BUSINESS_UNIT
    ,dv.REGION
    ,clvm.MONTH_DATE
    ,dv.CUSTOMER_ID
    ,clvm.VIP_COHORT_MONTH_DATE
    ,'M' || cast(datediff(month, clvm.VIP_COHORT_MONTH_DATE, clvm.MONTH_DATE) + 1 as varchar) as tenure
    ,datediff(month, clvm.VIP_COHORT_MONTH_DATE, clvm.MONTH_DATE) + 1 as tenure_num
    ,clvm.IS_CANCEL

    ,sum(clvm.CUMULATIVE_PRODUCT_GROSS_PROFIT) as CUMULATIVE_PRODUCT_GROSS_Margin
    ,sum(clvm.CUMULATIVE_CASH_GROSS_PROFIT) as CUMULATIVE_CASH_GROSS_Margin
from _customer_lifetime_value_monthly_qa clvm
join _gfb_dim_vip_qa dv
    on dv.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(clvm.CUSTOMER_ID)
where
    (clvm.IS_BOP_VIP = 1 or clvm.VIP_COHORT_MONTH_DATE = clvm.MONTH_DATE)
group by
    dv.BUSINESS_UNIT
    ,dv.REGION
    ,clvm.MONTH_DATE
    ,dv.CUSTOMER_ID
    ,clvm.VIP_COHORT_MONTH_DATE
    ,'M' || cast(datediff(month, clvm.VIP_COHORT_MONTH_DATE, clvm.MONTH_DATE) + 1 as varchar)
    ,datediff(month, clvm.VIP_COHORT_MONTH_DATE, clvm.MONTH_DATE) + 1
    ,clvm.IS_CANCEL;


create or replace temporary table _customer_tenure_segment_qa as
select distinct
    cmv.CUSTOMER_ID
    ,cmv.tenure as segment_name
    ,cmv.tenure_num
    ,cmv.VIP_COHORT_MONTH_DATE
from _customer_monthly_value_qa cmv
where
    cmv.tenure in ('M6', 'M12', 'M24', 'M36')

union

select distinct
    cmv.CUSTOMER_ID
    ,(case
        when cmv.tenure = 'M1' then 'M1 - Cancel'
        when cmv.tenure in ('M2', 'M3') then 'M3 - Cancel'
        else 'M6 - Cancel'
        end) as segment_name
    ,cmv.tenure_num
    ,cmv.VIP_COHORT_MONTH_DATE
from _customer_monthly_value_qa cmv
where
    cmv.IS_CANCEL = 1
    and cmv.tenure in ('M1', 'M2', 'M3', 'M4', 'M5', 'M6');


create or replace transient table REPORTING_PROD.GFB.gfb001_03_tenure_block_lifetime_value_qa as
select
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.tenure
--     ,a.MONTH_DATE
    ,cts.segment_name

    ,count(a.CUSTOMER_ID) as vip_count
    ,sum(a.CUMULATIVE_PRODUCT_GROSS_Margin) as CUMULATIVE_PRODUCT_GROSS_Margin
    ,sum(a.CUMULATIVE_CASH_GROSS_Margin) as CUMULATIVE_CASH_GROSS_Margin
from _customer_monthly_value_qa a
join _customer_tenure_segment_qa cts
    on cts.CUSTOMER_ID = a.CUSTOMER_ID
    and cts.VIP_COHORT_MONTH_DATE = a.VIP_COHORT_MONTH_DATE
    and cts.tenure_num >= a.tenure_num
group by
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.tenure
--     ,a.MONTH_DATE
    ,cts.segment_name
