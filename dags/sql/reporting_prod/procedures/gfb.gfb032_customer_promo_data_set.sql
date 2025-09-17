set start_date = date_trunc(year, dateadd(year, -2, current_date()));


create or replace temporary table _customer_promo_data as
select
    pds.BUSINESS_UNIT
    ,pds.REGION
    ,pds.COUNTRY
    ,pds.ORDER_DATE
    ,pds.ORDER_TYPE
    ,pds.PROMO_ORDER_FLAG
    ,pds.CUSTOMER_ID
    ,pds.PROMO_CODE_1 as promo_code
    ,pds.PROMO_1_OFFER as promo_offer
    ,pds.PROMO_1_GROUP as promo_group
    ,pds.PROMO_1_GOAL as promo_goal
    ,pds.PROMO_1_PROMOTION_NAME as promo_name
    ,(case
        when lower(pds.PROMO_1_PARENT_PROMO_CLASSIFICATION) like '%upsell%' or lower(pds.PROMO_CODE_1) like '%upsell%'
        then 'Upsell Promo' else 'Non-Upsell Promo' end) as upsell_promo_flag

    ,count(distinct pds.ORDER_ID) as ORDERS
    ,sum(pds.TOTAL_QTY_SOLD) as TOTAL_QTY_SOLD
    ,sum(pds.TOTAL_PRODUCT_REVENUE) as TOTAL_PRODUCT_REVENUE
    ,sum(pds.TOTAL_COGS) as TOTAL_COGS
    ,sum(pds.TOTAL_CASH_COLLECTED) as TOTAL_CASH_COLLECTED
    ,sum(pds.TOTAL_CREDIT_REDEEMED_AMOUNT) as TOTAL_CREDIT_REDEEMED_AMOUNT
    ,sum(pds.TOTAL_SUBTOTAL_AMOUNT) as TOTAL_SUBTOTAL_AMOUNT
    ,sum(pds.TOTAL_DISCOUNT) as TOTAL_DISCOUNT
    ,sum(pds.total_shipping_cost) as total_shipping_cost
    ,sum(pds.TOTAL_SHIPPING_REVENUE) as TOTAL_SHIPPING_REVENUE
from REPORTING_PROD.GFB.GFB011_PROMO_DATA_SET pds
where
    pds.PROMO_CODE_1 is not null
    and pds.ORDER_DATE >= $start_date
group by
    pds.BUSINESS_UNIT
    ,pds.REGION
    ,pds.COUNTRY
    ,pds.ORDER_DATE
    ,pds.ORDER_TYPE
    ,pds.PROMO_ORDER_FLAG
    ,pds.CUSTOMER_ID
    ,pds.PROMO_CODE_1
    ,pds.PROMO_1_OFFER
    ,pds.PROMO_1_GROUP
    ,pds.PROMO_1_GOAL
    ,pds.PROMO_1_PROMOTION_NAME
    ,(case
        when lower(pds.PROMO_1_PARENT_PROMO_CLASSIFICATION) like '%upsell%' or lower(pds.PROMO_CODE_1) like '%upsell%'
        then 'Upsell Promo' else 'Non-Upsell Promo' end)

union

select
    pds.BUSINESS_UNIT
    ,pds.REGION
    ,pds.COUNTRY
    ,pds.ORDER_DATE
    ,pds.ORDER_TYPE
    ,pds.PROMO_ORDER_FLAG
    ,pds.CUSTOMER_ID
    ,pds.PROMO_CODE_2 as promo_code
    ,pds.PROMO_2_OFFER as promo_offer
    ,pds.PROMO_2_GROUP as promo_group
    ,pds.PROMO_2_GOAL as promo_goal
    ,pds.PROMO_2_PROMOTION_NAME as promo_name
    ,(case
        when lower(pds.PROMO_2_PARENT_PROMO_CLASSIFICATION) like '%upsell%' or lower(pds.PROMO_CODE_2) like '%upsell%'
        then 'Upsell Promo' else 'Non-Upsell Promo' end) as upsell_promo_flag

    ,count(distinct pds.ORDER_ID) as ORDERS
    ,sum(pds.TOTAL_QTY_SOLD) as TOTAL_QTY_SOLD
    ,sum(pds.TOTAL_PRODUCT_REVENUE) as TOTAL_PRODUCT_REVENUE
    ,sum(pds.TOTAL_COGS) as TOTAL_COGS
    ,sum(pds.TOTAL_CASH_COLLECTED) as TOTAL_CASH_COLLECTED
    ,sum(pds.TOTAL_CREDIT_REDEEMED_AMOUNT) as TOTAL_CREDIT_REDEEMED_AMOUNT
    ,sum(pds.TOTAL_SUBTOTAL_AMOUNT) as TOTAL_SUBTOTAL_AMOUNT
    ,sum(pds.TOTAL_DISCOUNT) as TOTAL_DISCOUNT
    ,sum(pds.total_shipping_cost) as total_shipping_cost
    ,sum(pds.TOTAL_SHIPPING_REVENUE) as TOTAL_SHIPPING_REVENUE
from REPORTING_PROD.GFB.GFB011_PROMO_DATA_SET pds
where
    pds.PROMO_CODE_2 is not null
    and pds.ORDER_DATE >= $start_date
group by
    pds.BUSINESS_UNIT
    ,pds.REGION
    ,pds.COUNTRY
    ,pds.ORDER_DATE
    ,pds.ORDER_TYPE
    ,pds.PROMO_ORDER_FLAG
    ,pds.CUSTOMER_ID
    ,pds.PROMO_CODE_2
    ,pds.PROMO_2_OFFER
    ,pds.PROMO_2_GROUP
    ,pds.PROMO_2_GOAL
    ,pds.PROMO_2_PROMOTION_NAME
    ,(case
        when lower(pds.PROMO_2_PARENT_PROMO_CLASSIFICATION) like '%upsell%' or lower(pds.PROMO_CODE_2) like '%upsell%'
        then 'Upsell Promo' else 'Non-Upsell Promo' end);


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb032_customer_promo_data_set as
select
    cpd.*
    ,v.FIRST_ACTIVATING_COHORT
    ,v.FIRST_ACTIVATING_DATE
    ,v.REGISTRATION_DATE
    ,datediff(month, v.FIRST_ACTIVATING_DATE, cpd.ORDER_DATE) + 1 as vip_tenure
    ,datediff(month, v.REGISTRATION_DATE, cpd.ORDER_DATE) + 1 as lead_tenure
from _customer_promo_data cpd
join REPORTING_PROD.GFB.GFB_DIM_VIP v
    on v.CUSTOMER_ID = cpd.CUSTOMER_ID
where
    vip_tenure > 0
    and lead_tenure > 0
