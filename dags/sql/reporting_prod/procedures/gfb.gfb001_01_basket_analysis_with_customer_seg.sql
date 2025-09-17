set start_date = date_trunc(year, dateadd(year, -2, current_date()));
set end_date = current_date();

create or replace temporary table _credit_redemption as
select
    oc.ORDER_ID
    ,count(oc.STORE_CREDIT_ID) as membership_credit_redemption_count
from LAKE_JFB_VIEW.ULTRA_MERCHANT.ORDER_CREDIT oc
join EDW_PROD.DATA_MODEL_JFB.DIM_CREDIT dc
    on dc.CREDIT_ID = oc.STORE_CREDIT_ID
    and dc.SOURCE_CREDIT_ID_TYPE = 'store_credit_id'
    and dc.CREDIT_TYPE = 'Fixed Credit'
    and dc.CREDIT_TYPE = 'Membership Credit'
join EDW_PROD.DATA_MODEL_JFB.FACT_ORDER fo
    on fo.ORDER_ID = oc.ORDER_ID
join reporting_prod.gfb.vw_store st
    on st.STORE_ID = fo.STORE_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
    on dd.FULL_DATE = cast(fo.ORDER_LOCAL_DATETIME as date)
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_STATUS dos
    on dos.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_PROCESSING_STATUS dops
    on dops.ORDER_PROCESSING_STATUS_KEY = fo.ORDER_PROCESSING_STATUS_KEY
join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_SALES_CHANNEL dosc
    on dosc.ORDER_SALES_CHANNEL_KEY = fo.ORDER_SALES_CHANNEL_KEY
    and dosc.IS_BORDER_FREE_ORDER = 0
    and dosc.IS_PS_ORDER = 0
    and dosc.IS_TEST_ORDER = 0
where
    (
        dos.ORDER_STATUS = 'Success'
        or
        (dos.ORDER_STATUS = 'Pending' and dops.ORDER_PROCESSING_STATUS in ('FulFillment (Batching)','FulFillment (In Progress)','Placed'))
    )
    and dosc.ORDER_CLASSIFICATION_L1 in ('Product Order')
    and dd.FULL_DATE >= $start_date
    and dd.FULL_DATE < $end_date
group by
    oc.ORDER_ID;


create or replace temporary table _order_info as
select
    fol.business_unit
    ,fol.region
    ,fol.COUNTRY
    ,date_trunc(month, fol.ORDER_DATE) as order_month
    ,fol.ORDER_DATE
    ,mfd.DEPARTMENT
    ,mfd.IS_PLUSSIZE
    ,(case
        when coalesce(cr.membership_credit_redemption_count, 0) >= 4 then '4+'
        else cast(coalesce(cr.membership_credit_redemption_count, 0) as varchar(10)) end) as membership_credit_redemption_count
    ,fol.ORDER_ID
    ,fol.CUSTOMER_ID
    ,fol.ORDER_LINE_ID
    ,(case
        when fol.ORDER_TYPE = 'vip activating' then 'Activating'
        else 'Repeat' end) as is_activating
    ,'Product Order' as order_type
    ,fol.CLEARANCE_FLAG as is_clearance_flag

    ,sum(fol.TOTAL_QTY_SOLD) as total_qty_sold
    ,sum(fol.TOTAL_PRODUCT_REVENUE) as total_product_revenue
    ,sum(fol.TOTAL_COGS) as total_cogs
    ,sum(fol.ORDER_LINE_SUBTOTAL) as subtotal
    ,sum(fol.TOTAL_DISCOUNT) as discount

from reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE fol
join REPORTING_PROD.GFB.MERCH_DIM_PRODUCT mfd
    on lower(mfd.BUSINESS_UNIT) = lower(fol.BUSINESS_UNIT)
    and lower(mfd.REGION) = lower(fol.REGION)
    and lower(mfd.COUNTRY) = lower(fol.COUNTRY)
    and mfd.PRODUCT_SKU = fol.PRODUCT_SKU
left join _credit_redemption cr
    on cr.ORDER_ID = fol.ORDER_ID
where
    fol.ORDER_CLASSIFICATION = 'product order'
    and fol.ORDER_DATE >= $start_date
    and fol.ORDER_DATE < $end_date
    and fol.ORDER_TYPE in ('vip activating', 'vip repeat')
group by
    fol.business_unit
    ,fol.region
    ,fol.COUNTRY
    ,date_trunc(month, fol.ORDER_DATE)
    ,fol.ORDER_DATE
    ,mfd.DEPARTMENT
    ,mfd.IS_PLUSSIZE
    ,(case
        when coalesce(cr.membership_credit_redemption_count, 0) >= 4 then '4+'
        else cast(coalesce(cr.membership_credit_redemption_count, 0) as varchar(10)) end)
    ,fol.ORDER_ID
    ,fol.CUSTOMER_ID
    ,fol.ORDER_LINE_ID
    ,(case
        when fol.ORDER_TYPE = 'vip activating' then 'Activating'
        else 'Repeat' end)
    ,fol.CLEARANCE_FLAG

union

select
    fol.business_unit
    ,fol.region
    ,fol.COUNTRY
    ,date_trunc(month, fol.ORDER_DATE) as order_month
    ,fol.ORDER_DATE
    ,'Credit Billing' as DEPARTMENT
    ,'N' as IS_PLUSSIZE
    ,(case
        when coalesce(cr.membership_credit_redemption_count, 0) >= 4 then '4+'
        else cast(coalesce(cr.membership_credit_redemption_count, 0) as varchar(10)) end) as membership_credit_redemption_count
    ,fol.ORDER_ID
    ,fol.CUSTOMER_ID
    ,fol.ORDER_LINE_ID
    ,(case
        when fol.ORDER_TYPE = 'vip activating' then 'Activating'
        else 'Repeat' end) as is_activating
    ,'Credit Billing Order' as order_type
    ,'N' as is_clearance_flag

    ,sum(fol.TOTAL_QTY_SOLD) as total_qty_sold
    ,sum(fol.TOTAL_PRODUCT_REVENUE) as total_product_revenue
    ,0 as total_cogs
    ,sum(fol.ORDER_LINE_SUBTOTAL) as subtotal
    ,sum(fol.TOTAL_DISCOUNT) as discount

from reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE fol
left join _credit_redemption cr
    on cr.ORDER_ID = fol.ORDER_ID
where
    fol.ORDER_CLASSIFICATION = 'credit billing'
    and fol.ORDER_DATE >= $start_date
    and fol.ORDER_DATE < $end_date
    and fol.ORDER_TYPE in ('vip activating', 'vip repeat')
group by
    fol.business_unit
    ,fol.region
    ,fol.COUNTRY
    ,date_trunc(month, fol.ORDER_DATE)
    ,fol.ORDER_DATE
    ,(case
        when coalesce(cr.membership_credit_redemption_count, 0) >= 4 then '4+'
        else cast(coalesce(cr.membership_credit_redemption_count, 0) as varchar(10)) end)
    ,fol.ORDER_ID
    ,fol.CUSTOMER_ID
    ,fol.ORDER_LINE_ID
    ,(case
        when fol.ORDER_TYPE = 'vip activating' then 'Activating'
        else 'Repeat' end);


create or replace temporary table _aov as
select
    oi.ORDER_ID
    ,sum(oi.total_product_revenue) as aov_total
from _order_info oi
group by
    oi.ORDER_ID;


create or replace temporary table _final_result as
select
    oi.BUSINESS_UNIT
    ,oi.REGION
    ,oi.COUNTRY
    ,oi.order_month
    ,oi.DEPARTMENT
    ,oi.IS_PLUSSIZE
    ,oi.membership_credit_redemption_count
    ,oi.is_activating
    ,oi.order_type
    ,(case
        when a.aov_total >=0 and a.aov_total <=10 then '$0 - $10'
        when a.aov_total>10 and a.aov_total<=20 then '$10 - $20'
        when a.aov_total>20 and a.aov_total<=30 then '$20 - $30'
        when a.aov_total>30 and a.aov_total<=40 then '$30 - $40'
        when a.aov_total>40 and a.aov_total<=50 then '$40 - $50'
        when a.aov_total>50 and a.aov_total<=60 then '$50 - $60'
        when a.aov_total>60 and a.aov_total<=70 then '$60 - $70'
        when a.aov_total>70 and a.aov_total<=80 then '$70 - $80'
        when a.aov_total>80 and a.aov_total<=90 then '$80 - $90'
        when a.aov_total>90 and a.aov_total<=100 then '$90 - $100'
        when a.aov_total>100 and a.aov_total<=110 then '$100 - $110'
        when a.aov_total>110 and a.aov_total<=120 then '$110 - $120'
        when a.aov_total>120 then '>$120'
        end) as aov_bucket
    ,cs.VIP_COHORT
    ,cs.CUSTOM_SEGMENT_CATEGORY
    ,cs.CUSTOM_SEGMENT
    ,(case
        when clv.CASH_GROSS_PROFIT <= 30 then 'below $30'
        when clv.CASH_GROSS_PROFIT <= 45 then '$30 - $45'
        when clv.CASH_GROSS_PROFIT <= 60 then '$45 - $60'
        when clv.CASH_GROSS_PROFIT <= 80 then '$60 - $80'
        when clv.CASH_GROSS_PROFIT <= 100 then '$80 - $100'
        when clv.CASH_GROSS_PROFIT <= 200 then '$100 - $200'
        when clv.CASH_GROSS_PROFIT <= 300 then '$200 - $300'
        when clv.CASH_GROSS_PROFIT <= 400 then '$300 - $400'
        when clv.CASH_GROSS_PROFIT > 400 then '$400+'
        end) as ltv_bucket
    ,(case
        when clv.PRODUCT_GROSS_PROFIT <= 30 then 'below $30'
        when clv.PRODUCT_GROSS_PROFIT <= 45 then '$30 - $45'
        when clv.PRODUCT_GROSS_PROFIT <= 60 then '$45 - $60'
        when clv.PRODUCT_GROSS_PROFIT <= 80 then '$60 - $80'
        when clv.PRODUCT_GROSS_PROFIT <= 100 then '$80 - $100'
        when clv.PRODUCT_GROSS_PROFIT <= 200 then '$100 - $200'
        when clv.PRODUCT_GROSS_PROFIT <= 300 then '$200 - $300'
        when clv.PRODUCT_GROSS_PROFIT <= 400 then '$300 - $400'
        when clv.PRODUCT_GROSS_PROFIT > 400 then '$400+'
        end) as ltv_gaap_bucket
    ,oi.ORDER_ID
    ,oi.CUSTOMER_ID
    ,oi.is_clearance_flag
    ,(case
        when oi.total_product_revenue - oi.total_cogs <= -100 then 'below -$100'
        when oi.total_product_revenue - oi.total_cogs <= -80 then '-$100 to -$80'
        when oi.total_product_revenue - oi.total_cogs <= -60 then '-$80 to -$60'
        when oi.total_product_revenue - oi.total_cogs <= -40 then '-$60 to -$40'
        when oi.total_product_revenue - oi.total_cogs <= -20 then '-$40 to -$20'
        when oi.total_product_revenue - oi.total_cogs <= 0 then '-$20 to $0'
        when oi.total_product_revenue - oi.total_cogs <= 20 then '$0 to $20'
        when oi.total_product_revenue - oi.total_cogs <= 40 then '$20 to $40'
        when oi.total_product_revenue - oi.total_cogs <= 60 then '$40 to $60'
        when oi.total_product_revenue - oi.total_cogs <= 80 then '$60 to $80'
        when oi.total_product_revenue - oi.total_cogs <= 100 then '$80 to $100'
        when oi.total_product_revenue - oi.total_cogs > 100 then '$100+'
    end) as product_margin_bucket

    ,sum(oi.total_qty_sold) as total_qty_sold
    ,sum(oi.total_product_revenue) as total_product_revenue
    ,sum(oi.total_cogs) as total_cogs
    ,sum(oi.total_product_revenue) - sum(oi.total_cogs) as total_product_margin
    ,sum(oi.subtotal) as subtotal_amount
    ,sum(oi.discount) as discount_amount
from _order_info oi
join _aov a
    on a.ORDER_ID = oi.ORDER_ID
join reporting_prod.gfb.GFB001_CUSTOMER_SEG cs
    on cs.CUSTOMER_ID = oi.CUSTOMER_ID
    and cs.CUSTOM_SEGMENT_CATEGORY != 'Count of Repeat Purchases since 2018-01-01'
    and cs.CUSTOM_SEGMENT_CATEGORY != 'Customer State'
    and cs.CUSTOM_SEGMENT_CATEGORY != 'Free Trial'
    and cs.CUSTOM_SEGMENT_CATEGORY != 'Gamers'
    and cs.CUSTOM_SEGMENT_CATEGORY != 'Membership Price'
    and cs.CUSTOM_SEGMENT_CATEGORY != 'Prepaid Credit Card'
join REPORTING_PROD.GFB.GFB_DIM_VIP clv
    on clv.CUSTOMER_ID = oi.CUSTOMER_ID
where
    oi.order_month >= date_trunc(year, dateadd(year, -1, current_date()))
group by
    oi.BUSINESS_UNIT
    ,oi.REGION
    ,oi.COUNTRY
    ,oi.order_month
    ,oi.DEPARTMENT
    ,oi.IS_PLUSSIZE
    ,oi.membership_credit_redemption_count
    ,oi.is_activating
    ,oi.order_type
    ,(case
        when a.aov_total >=0 and a.aov_total <=10 then '$0 - $10'
        when a.aov_total>10 and a.aov_total<=20 then '$10 - $20'
        when a.aov_total>20 and a.aov_total<=30 then '$20 - $30'
        when a.aov_total>30 and a.aov_total<=40 then '$30 - $40'
        when a.aov_total>40 and a.aov_total<=50 then '$40 - $50'
        when a.aov_total>50 and a.aov_total<=60 then '$50 - $60'
        when a.aov_total>60 and a.aov_total<=70 then '$60 - $70'
        when a.aov_total>70 and a.aov_total<=80 then '$70 - $80'
        when a.aov_total>80 and a.aov_total<=90 then '$80 - $90'
        when a.aov_total>90 and a.aov_total<=100 then '$90 - $100'
        when a.aov_total>100 and a.aov_total<=110 then '$100 - $110'
        when a.aov_total>110 and a.aov_total<=120 then '$110 - $120'
        when a.aov_total>120 then '>$120'
        end)
    ,cs.VIP_COHORT
    ,cs.CUSTOM_SEGMENT_CATEGORY
    ,cs.CUSTOM_SEGMENT
    ,(case
        when clv.CASH_GROSS_PROFIT <= 30 then 'below $30'
        when clv.CASH_GROSS_PROFIT <= 45 then '$30 - $45'
        when clv.CASH_GROSS_PROFIT <= 60 then '$45 - $60'
        when clv.CASH_GROSS_PROFIT <= 80 then '$60 - $80'
        when clv.CASH_GROSS_PROFIT <= 100 then '$80 - $100'
        when clv.CASH_GROSS_PROFIT <= 200 then '$100 - $200'
        when clv.CASH_GROSS_PROFIT <= 300 then '$200 - $300'
        when clv.CASH_GROSS_PROFIT <= 400 then '$300 - $400'
        when clv.CASH_GROSS_PROFIT > 400 then '$400+'
        end)
    ,(case
        when clv.PRODUCT_GROSS_PROFIT <= 30 then 'below $30'
        when clv.PRODUCT_GROSS_PROFIT <= 45 then '$30 - $45'
        when clv.PRODUCT_GROSS_PROFIT <= 60 then '$45 - $60'
        when clv.PRODUCT_GROSS_PROFIT <= 80 then '$60 - $80'
        when clv.PRODUCT_GROSS_PROFIT <= 100 then '$80 - $100'
        when clv.PRODUCT_GROSS_PROFIT <= 200 then '$100 - $200'
        when clv.PRODUCT_GROSS_PROFIT <= 300 then '$200 - $300'
        when clv.PRODUCT_GROSS_PROFIT <= 400 then '$300 - $400'
        when clv.PRODUCT_GROSS_PROFIT > 400 then '$400+'
        end)
    ,oi.ORDER_ID
    ,oi.CUSTOMER_ID
    ,oi.is_clearance_flag
    ,(case
        when oi.total_product_revenue - oi.total_cogs <= -100 then 'below -$100'
        when oi.total_product_revenue - oi.total_cogs <= -80 then '-$100 to -$80'
        when oi.total_product_revenue - oi.total_cogs <= -60 then '-$80 to -$60'
        when oi.total_product_revenue - oi.total_cogs <= -40 then '-$60 to -$40'
        when oi.total_product_revenue - oi.total_cogs <= -20 then '-$40 to -$20'
        when oi.total_product_revenue - oi.total_cogs <= 0 then '-$20 to $0'
        when oi.total_product_revenue - oi.total_cogs <= 20 then '$0 to $20'
        when oi.total_product_revenue - oi.total_cogs <= 40 then '$20 to $40'
        when oi.total_product_revenue - oi.total_cogs <= 60 then '$40 to $60'
        when oi.total_product_revenue - oi.total_cogs <= 80 then '$60 to $80'
        when oi.total_product_revenue - oi.total_cogs <= 100 then '$80 to $100'
        when oi.total_product_revenue - oi.total_cogs > 100 then '$100+'
    end);


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.GFB001_01_BASKET_ANALYSIS_WITH_CUSTOMER_SEG as
select
    a.*
    ,rank() over (partition by a.CUSTOMER_ID, a.order_type ,a.CUSTOM_SEGMENT_CATEGORY order by a.ORDER_ID asc) as customer_order_rank
from _final_result a;
