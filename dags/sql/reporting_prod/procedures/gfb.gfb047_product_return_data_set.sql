set end_date = current_date();
set start_date = date_trunc(year, dateadd(year, -2, $end_date));


create or replace temporary table _product_shipped_return as
select
    ols.BUSINESS_UNIT
    ,ols.REGION
    ,ols.COUNTRY
    ,ols.PRODUCT_SKU
    ,ols.SHIP_DATE
    ,ols.RETURN_DATE
    ,ols.RETURN_REASON

    ,sum(ols.TOTAL_QTY_SOLD) as TOTAL_QTY_SOLD
    ,sum(coalesce(ols.TOTAL_RETURN_UNIT, 0)) as TOTAL_RETURN_UNIT
from REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_SHIP_DATE ols
where
    ols.ORDER_CLASSIFICATION = 'product order'
    and ols.SHIP_DATE >= $start_date
    and ols.SHIP_DATE < $end_date
group by
    ols.BUSINESS_UNIT
    ,ols.REGION
    ,ols.COUNTRY
    ,ols.PRODUCT_SKU
    ,ols.SHIP_DATE
    ,ols.RETURN_DATE
    ,ols.RETURN_REASON;


create or replace transient table REPORTING_PROD.GFB.GFB047_PRODUCT_RETURN_DATA_SET as
select
    psr.*

    ,mdp.DEPARTMENT_DETAIL
    ,mdp.SUBCATEGORY
from _product_shipped_return psr
join REPORTING_PROD.GFB.MERCH_DIM_PRODUCT mdp
    on mdp.BUSINESS_UNIT = psr.BUSINESS_UNIT
    and mdp.REGION = psr.REGION
    and mdp.COUNTRY = psr.COUNTRY
    and mdp.PRODUCT_SKU = psr.PRODUCT_SKU
