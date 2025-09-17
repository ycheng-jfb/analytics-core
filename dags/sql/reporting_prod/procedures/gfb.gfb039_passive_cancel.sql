set start_date = date_trunc(year, dateadd(year, -2, current_date()));


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
join REPORTING_PROD.GFB.GFB_DIM_VIP dv
    on dv.CUSTOMER_ID = fme.CUSTOMER_ID
where
    fme.MEMBERSHIP_EVENT_TYPE = 'Cancellation'
    and fme.MEMBERSHIP_TYPE_DETAIL = 'Passive'
    and cast(fme.EVENT_START_LOCAL_DATETIME as date) >= $start_date;


create or replace temporary table _activation as
select
    dv.BUSINESS_UNIT
    ,dv.REGION
    ,dv.COUNTRY
    ,cast(fme.EVENT_START_LOCAL_DATETIME as date) as activating_date
    ,date_trunc(month, activating_date) as activating_cohort
    ,fme.CUSTOMER_ID
    ,(case
        when dv.ACTIVATING_PAYMENT_METHOD = 'ppcc' then 'PPCC Activation'
        else 'Non PPCC Activation' end) as ppcc_activation_flag
from EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT fme
join REPORTING_PROD.GFB.GFB_DIM_VIP dv
    on dv.CUSTOMER_ID = fme.CUSTOMER_ID
where
    fme.MEMBERSHIP_EVENT_TYPE = 'Activation'
    and activating_cohort >= (select min(pc.activating_cohort) from _passive_cancel pc);


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb039_passive_cancel as
select
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.activating_cohort
    ,pc.passive_cancel_month
    ,a.ppcc_activation_flag

    ,count(a.CUSTOMER_ID) as activations
    ,count(pc.CUSTOMER_ID) as passive_cancels
from _activation a
left join _passive_cancel pc
    on pc.CUSTOMER_ID = a.CUSTOMER_ID
group by
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.activating_cohort
    ,pc.passive_cancel_month
    ,a.ppcc_activation_flag;
