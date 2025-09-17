set start_date = cast('2020-01-01' as date);
set end_date = current_date();

create or replace temporary table _store as
select
    *
from EDW_PROD.DATA_MODEL_JFB.DIM_STORE ds
where
    ds.STORE_ID in (26, 55);


create or replace temporary table _active_vips as
select distinct
    upper(st.STORE_BRAND || ' ' || st.STORE_COUNTRY) as STORE_REPORTING_NAME
    ,dc.CUSTOMER_ID
    ,m.MEMBERSHIP_ID
    ,dc.EMAIL as CUSTOMER_EMAIL
    ,clv.FIRST_ACTIVATING_COHORT as FIRST_ACTIVATION_COHORT
from reporting_prod.GFB.GFB_DIM_VIP clv
join _store st
    on st.STORE_ID = clv.STORE_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER dc
    on dc.CUSTOMER_ID = clv.CUSTOMER_ID
    and dc.IS_TEST_CUSTOMER = 0
join LAKE_JFB_VIEW.ULTRA_MERCHANT.MEMBERSHIP m
    on m.CUSTOMER_ID = clv.CUSTOMER_ID
where
    clv.CURRENT_MEMBERSHIP_STATUS = 'VIP';


create or replace temporary table _sales_detail as
select
    fol.BUSINESS_UNIT || ' ' || fol.COUNTRY as STORE_REPORTING_NAME
    ,av.CUSTOMER_ID
    ,av.MEMBERSHIP_ID
    ,av.CUSTOMER_EMAIL
    ,av.FIRST_ACTIVATION_COHORT
    ,datediff(month, av.FIRST_ACTIVATION_COHORT, date_trunc(month, fol.ORDER_DATE)) + 1 as tenure
    ,fol.ORDER_ID
    ,fol.ORDER_LINE_ID
    ,(case
        when fol.ORDER_TYPE = 'vip activating' then 'vip_activating'
        else 'vip_repeat' end) as order_type
    ,fol.ORDER_DATE as order_date
    ,fol.PRODUCT_SKU
    ,fol.DP_SIZE as SIZE
    ,fol.PRODUCT_NAME
    ,mfd.DESCRIPTION
    ,fol.TOTAL_QTY_SOLD as qty_sold
    ,mfd.DEPARTMENT
    ,mfd.SUBCATEGORY
    ,mfd.STYLE_NAME
    ,(case
        when mfd.WW_WC like '%E%' then 'Extra Wide'
        else 'Regular Wide' end) as wide_type
from reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE fol
join _active_vips av
    on av.CUSTOMER_ID = fol.CUSTOMER_ID
join reporting_prod.GFB.MERCH_DIM_PRODUCT mfd
    on mfd.BUSINESS_UNIT = fol.BUSINESS_UNIT
    and mfd.COUNTRY = fol.COUNTRY
    and mfd.PRODUCT_SKU = fol.PRODUCT_SKU
where
    fol.ORDER_CLASSIFICATION = 'product order'
    and mfd.WW_WC is not null
    and fol.ORDER_DATE >= $start_date
    and fol.ORDER_DATE < $end_date;


create or replace temporary table _return_detail as
select
    olp.ORDER_LINE_ID
    ,olp.RETURN_DATE as return_date
    ,olp.RETURN_REASON
from reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE olp
where
    olp.ORDER_CLASSIFICATION = 'product order'
    and olp.RETURN_DATE >= $start_date
    and olp.RETURN_DATE < $end_date;


create or replace transient table reporting_prod.GFB.gfb013_wide_fit_purchase as
select
    sd.STORE_REPORTING_NAME as business_unit
    ,sd.CUSTOMER_EMAIL
    ,sd.CUSTOMER_ID
    ,sd.MEMBERSHIP_ID
    ,sd.PRODUCT_SKU
    ,sd.PRODUCT_NAME
    ,sd.DESCRIPTION
    ,sd.SIZE
    ,sd.order_date
    ,rd.return_date
    ,rd.RETURN_REASON
    ,sd.tenure
    ,sd.order_type
    ,sd.DEPARTMENT
    ,sd.SUBCATEGORY
    ,sd.STYLE_NAME
    ,sd.wide_type
    ,sd.ORDER_ID
    ,sum(sd.qty_sold) as qty_sold
from _sales_detail sd
left join _return_detail rd
    on rd.ORDER_LINE_ID = sd.ORDER_LINE_ID
group by
    sd.STORE_REPORTING_NAME
    ,sd.CUSTOMER_EMAIL
    ,sd.CUSTOMER_ID
    ,sd.MEMBERSHIP_ID
    ,sd.PRODUCT_SKU
    ,sd.PRODUCT_NAME
    ,sd.DESCRIPTION
    ,sd.SIZE
    ,sd.order_date
    ,rd.return_date
    ,rd.RETURN_REASON
    ,sd.tenure
    ,sd.order_type
    ,sd.DEPARTMENT
    ,sd.SUBCATEGORY
    ,sd.STYLE_NAME
    ,sd.wide_type
    ,sd.ORDER_ID;
