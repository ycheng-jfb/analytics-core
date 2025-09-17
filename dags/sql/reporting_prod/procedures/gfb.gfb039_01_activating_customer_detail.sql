create or replace temporary table _passive_cancel as
select
    dv.BUSINESS_UNIT
    ,dv.REGION
    ,dv.COUNTRY
    ,cast(fme.EVENT_START_LOCAL_DATETIME as date) as passive_cancel_date
    ,date_trunc(month, passive_cancel_date) as passive_cancel_month
    ,fme.CUSTOMER_ID
    ,(case
        when dv.ACTIVATING_PAYMENT_METHOD = 'ppcc' then 'PPCC Activation'
        else 'Non PPCC Activation' end) as ppcc_activation_flag
    ,(case
        when dv.RECENT_ACTIVATING_COHORT >= passive_cancel_month
        then dv.FIRST_ACTIVATING_COHORT else dv.RECENT_ACTIVATING_COHORT end) as activating_cohort
from EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT fme
join GFB.GFB_DIM_VIP dv
    on dv.CUSTOMER_ID = fme.CUSTOMER_ID
where
    fme.MEMBERSHIP_EVENT_TYPE = 'Cancellation'
    and fme.MEMBERSHIP_TYPE_DETAIL = 'Passive'
    and activating_cohort >= '2018-01-01';

create or replace temporary table _gamers as
select distinct
    ac.CUSTOMER_ID
from _passive_cancel ac
left join
(
    select distinct
        ac.CUSTOMER_ID
    from LAKE_JFB_VIEW.ULTRA_MERCHANT."ORDER" o
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.customer c
        on c.customer_id = o.customer_id
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.store st
        on c.store_id = st.store_id
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.order_classification oc
        on oc.order_id = o.order_id
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.order_detail od
        on o.order_id = od.order_id
        and od.name = 'gaming_prevention_log_id'
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.gaming_prevention_log gl
        on TRY_TO_NUMBER(od.value) = gl.gaming_prevention_log_id
        and gl.gaming_prevention_action = 'void'
    join _passive_cancel ac
        on ac.CUSTOMER_ID = c.CUSTOMER_ID
    where
        order_type_id = 23
        and o.date_placed >= cast('2019-06-01' as date)
) gamers on gamers.CUSTOMER_ID = ac.CUSTOMER_ID;


CREATE OR REPLACE TRANSIENT TABLE GFB.gfb039_01_activating_customer_detail as
select
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.activating_cohort
    ,a.CUSTOMER_ID

    ,v.PRODUCT_ORDER_COUNT
    ,v.AGE
    ,v.ACTIVATING_ITEM_COUNT
    ,v.REGISTRATION_TYPE
    ,v.ACTIVATING_BOGO_PROMO
    ,v.ACTIVATING_PRICE_BUCKET_PROMO
    ,v.IS_CROSS_BRAND
    ,v.DAYS_BETWEEN_REGISTRATION_ACTIVATION
    ,v.GATEWAY_TYPE
    ,(case
        when v.ACTIVATING_PAYMENT_METHOD = 'ppcc' then 'PPCC Activation'
        else 'Non PPCC Activation' end) as ppcc_activation_flag
    ,v.IS_FRIEND_REFERRAL
    ,(case
        when g.CUSTOMER_ID is not null then 'Gamer'
        else 'Non Gamer' end) as gamer_flag
from _passive_cancel a
join GFB.GFB_DIM_VIP v
    on v.CUSTOMER_ID = a.CUSTOMER_ID
left join _gamers g
    on g.CUSTOMER_ID = a.CUSTOMER_ID;
