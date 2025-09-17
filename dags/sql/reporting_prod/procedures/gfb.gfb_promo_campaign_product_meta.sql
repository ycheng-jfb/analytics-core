set start_date = date_trunc(year, dateadd(year, -2, current_date()));


create or replace temporary table _promo_meta as
select distinct
    prm.CAMPAIGN_NAME
    ,prm.PRODUCT_SKU
    ,prm.START_DATE
    ,(case
        when prm.END_DATE >= current_date()
        then current_date() else prm.END_DATE end) as END_DATE
    ,split(trim(prm.PROMO_CODES), ', ') as promo_code_list
from LAKE_VIEW.MERCH.PROMO_REPORTING_METADATA prm
where
    (prm.BUSINESS_UNIT like '%JF%' or prm.BUSINESS_UNIT like '%SD%' or prm.BUSINESS_UNIT like '%FK%')
    and prm.START_DATE >= $start_date;


create or replace temporary table _promo_champaign_product as
select distinct
    dp.BUSINESS_UNIT
    ,dp.REGION
    ,dp.COUNTRY
    ,dp.PROMOTION_CODE
    ,pm.CAMPAIGN_NAME
    ,pm.PRODUCT_SKU
    ,pm.START_DATE
    ,pm.END_DATE
from REPORTING_PROD.GFB.DIM_PROMO dp
join _promo_meta pm
    on array_contains(dp.PROMOTION_CODE::variant, pm.promo_code_list);


create or replace temporary table _promo_champaign_product_date as
select distinct
    pcp.BUSINESS_UNIT
    ,pcp.REGION
    ,pcp.COUNTRY
    ,pcp.PROMOTION_CODE
    ,pcp.CAMPAIGN_NAME
    ,pcp.PRODUCT_SKU
    ,pcp.START_DATE
    ,pcp.END_DATE
    ,dd.FULL_DATE as date
from _promo_champaign_product pcp
join EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
    on dd.FULL_DATE >= pcp.START_DATE
    and dd.FULL_DATE <= pcp.END_DATE
where
    dd.FULL_DATE >= $start_date
    and dd.FULL_DATE < current_date();


create or replace transient table REPORTING_PROD.GFB.gfb_promo_campaign_product_meta as
select
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.PROMOTION_CODE
    ,a.PRODUCT_SKU
    ,a.date

    ,listagg(a.CAMPAIGN_NAME, ', ') as campaigns
from _promo_champaign_product_date a
group by
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.PROMOTION_CODE
    ,a.PRODUCT_SKU
    ,a.date;
