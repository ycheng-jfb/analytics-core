set start_date = '2018-01-01';


create or replace temporary table _activating_offer as
select distinct
    pds.ORDER_LINE_ID
    ,pds.ACTIVATING_OFFERS
from REPORTING_PROD.GFB.GFB011_PROMO_DATA_SET pds
where
    pds.ACTIVATING_OFFERS is not null
    and pds.ORDER_DATE >= $start_date;


create or replace temporary table _orders as
select
    olp.BUSINESS_UNIT
    ,olp.REGION
    ,olp.COUNTRY
    ,olp.ORDER_DATE
    ,olp.ORDER_ID
    ,olp.ORDER_TYPE
    ,ao.ACTIVATING_OFFERS
    ,olp.CUSTOMER_ID
    ,dv.DAYS_BETWEEN_REGISTRATION_ACTIVATION as lead_tenure

    ,sum(olp.TOTAL_QTY_SOLD) as TOTAL_QTY_SOLD
    ,sum(olp.TOTAL_PRODUCT_REVENUE) as TOTAL_PRODUCT_REVENUE
    ,sum(olp.TOTAL_COGS) as TOTAL_COGS
    ,sum(olp.TOTAL_SHIPPING_REVENUE) as TOTAL_SHIPPING_REVENUE
    ,sum(olp.TOTAL_DISCOUNT) as TOTAL_DISCOUNT
from REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE olp
left join _activating_offer ao
    on ao.ORDER_LINE_ID = olp.ORDER_LINE_ID
left join REPORTING_PROD.GFB.GFB_DIM_VIP dv
    on dv.CUSTOMER_ID = olp.CUSTOMER_ID
where
    olp.ORDER_CLASSIFICATION = 'product order'
    and olp.ORDER_DATE >= $start_date
    and olp.ORDER_TYPE in ('vip activating', 'vip repeat')
group by
    olp.BUSINESS_UNIT
    ,olp.REGION
    ,olp.COUNTRY
    ,olp.ORDER_DATE
    ,olp.ORDER_ID
    ,olp.ORDER_TYPE
    ,ao.ACTIVATING_OFFERS
    ,olp.CUSTOMER_ID
    ,dv.DAYS_BETWEEN_REGISTRATION_ACTIVATION;


create or replace transient table REPORTING_PROD.GFB.gfb030_orders_in_bucket_analysis as
select
    o.BUSINESS_UNIT
    ,o.REGION
    ,o.COUNTRY
    ,o.ORDER_DATE
    ,o.ORDER_TYPE
    ,(case
        when o.TOTAL_PRODUCT_REVENUE <= 20 then 'Under 20'
        when o.TOTAL_PRODUCT_REVENUE <= 40 then '21 to 40'
        when o.TOTAL_PRODUCT_REVENUE <= 60 then '41 to 60'
        when o.TOTAL_PRODUCT_REVENUE <= 80 then '61 to 80'
        else '80+' end) as aov_bucket
    ,(case
        when o.TOTAL_PRODUCT_REVENUE + o.TOTAL_SHIPPING_REVENUE <= 20 then 'Under 20'
        when o.TOTAL_PRODUCT_REVENUE + o.TOTAL_SHIPPING_REVENUE <= 40 then '21 to 40'
        when o.TOTAL_PRODUCT_REVENUE + o.TOTAL_SHIPPING_REVENUE <= 60 then '41 to 60'
        when o.TOTAL_PRODUCT_REVENUE + o.TOTAL_SHIPPING_REVENUE <= 80 then '61 to 80'
        else '80+' end) as aov_bucket_with_shipping
    ,(case
        when o.TOTAL_QTY_SOLD > 8 then '8+'
        else cast(o.TOTAL_QTY_SOLD as string) end) as upt_bucket
    ,o.ACTIVATING_OFFERS
    ,(case
        when o.TOTAL_QTY_SOLD > 1 then 'Orders With Multiple Items'
        else 'Orders With Single Item' end) as order_qty_filter
    ,(case
        when o.lead_tenure <= 7 then 'D1 - D7'
        when o.lead_tenure <= 29 then 'D8 - D29'
        when o.lead_tenure <= 59 then 'D30 - D59'
        when o.lead_tenure <= 89 then 'D60 - D89'
        when o.lead_tenure >= 90 then '90+'
        end) as lead_tenure

    ,count(o.ORDER_ID) as orders
    ,sum(o.TOTAL_PRODUCT_REVENUE) as TOTAL_PRODUCT_REVENUE
    ,sum(o.TOTAL_COGS) as TOTAL_COGS
    ,sum(o.TOTAL_QTY_SOLD) as TOTAL_QTY_SOLD
    ,sum(o.TOTAL_DISCOUNT) as TOTAL_DISCOUNT
from _orders o
group by
    o.BUSINESS_UNIT
    ,o.REGION
    ,o.COUNTRY
    ,o.ORDER_DATE
    ,o.ORDER_TYPE
    ,(case
        when o.TOTAL_PRODUCT_REVENUE <= 20 then 'Under 20'
        when o.TOTAL_PRODUCT_REVENUE <= 40 then '21 to 40'
        when o.TOTAL_PRODUCT_REVENUE <= 60 then '41 to 60'
        when o.TOTAL_PRODUCT_REVENUE <= 80 then '61 to 80'
        else '80+' end)
    ,(case
        when o.TOTAL_PRODUCT_REVENUE + o.TOTAL_SHIPPING_REVENUE <= 20 then 'Under 20'
        when o.TOTAL_PRODUCT_REVENUE + o.TOTAL_SHIPPING_REVENUE <= 40 then '21 to 40'
        when o.TOTAL_PRODUCT_REVENUE + o.TOTAL_SHIPPING_REVENUE <= 60 then '41 to 60'
        when o.TOTAL_PRODUCT_REVENUE + o.TOTAL_SHIPPING_REVENUE <= 80 then '61 to 80'
        else '80+' end)
    ,(case
        when o.TOTAL_QTY_SOLD > 8 then '8+'
        else cast(o.TOTAL_QTY_SOLD as string) end)
    ,o.ACTIVATING_OFFERS
    ,(case
        when o.TOTAL_QTY_SOLD > 1 then 'Orders With Multiple Items'
        else 'Orders With Single Item' end)
    ,(case
        when o.lead_tenure <= 7 then 'D1 - D7'
        when o.lead_tenure <= 29 then 'D8 - D29'
        when o.lead_tenure <= 59 then 'D30 - D59'
        when o.lead_tenure <= 89 then 'D60 - D89'
        when o.lead_tenure >= 90 then '90+'
        end)
