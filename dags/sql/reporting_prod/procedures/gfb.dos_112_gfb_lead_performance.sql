set start_date = date_trunc(month, dateadd(year, -2, current_date()));
set process_start_date = dateadd(day, -30, current_date());
set current_month = date_trunc(month, dateadd(day, -1, current_date()));

create or replace temporary table _lead_registration_only as
select distinct
    st.STORE_BRAND || ' ' || st.STORE_COUNTRY as STORE_REPORTING_NAME
    ,dd.FULL_DATE as lead_registration_date
    ,dd.MONTH_DATE as lead_registration_cohort
    ,dc.CUSTOMER_ID
from EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT fma
join reporting_prod.gfb.vw_store st
    on st.STORE_ID = fma.STORE_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
    on dd.FULL_DATE = cast(fma.EVENT_START_LOCAL_DATETIME as date)
join EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER dc
    on dc.CUSTOMER_ID = fma.CUSTOMER_ID
    and dc.IS_TEST_CUSTOMER = 0
where
    fma.MEMBERSHIP_EVENT_TYPE in ('Registration')
    and dd.MONTH_DATE != '1900-01-01';


create or replace temporary table _vip_failed_activation as
select distinct
    st.STORE_BRAND || ' ' || st.STORE_COUNTRY as STORE_REPORTING_NAME
    ,dd.FULL_DATE as failed_activating_date
    ,lro.CUSTOMER_ID
from EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT fma
join reporting_prod.gfb.vw_store st
    on st.STORE_ID = fma.STORE_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
    on dd.FULL_DATE = cast(fma.EVENT_START_LOCAL_DATETIME as date)
join EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER dc
    on dc.CUSTOMER_ID = fma.CUSTOMER_ID
    and dc.IS_TEST_CUSTOMER = 0
join _lead_registration_only lro
    on lro.CUSTOMER_ID = fma.CUSTOMER_ID
where
    fma.MEMBERSHIP_EVENT_TYPE in ('Failed Activation');


create or replace temporary table _vip_activation as
select distinct
    st.STORE_BRAND || ' ' || st.STORE_COUNTRY as STORE_REPORTING_NAME
    ,dd.FULL_DATE as vip_activation_date
    ,dd.MONTH_DATE as vip_activation_cohort
    ,lro.CUSTOMER_ID
from EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT fma
join reporting_prod.gfb.vw_store st
    on st.STORE_ID = fma.STORE_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
    on dd.FULL_DATE = cast(fma.EVENT_START_LOCAL_DATETIME as date)
join EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER dc
    on dc.CUSTOMER_ID = fma.CUSTOMER_ID
    and dc.IS_TEST_CUSTOMER = 0
join _lead_registration_only lro
    on lro.CUSTOMER_ID = fma.CUSTOMER_ID
left join _vip_failed_activation vfa
    on vfa.CUSTOMER_ID = fma.CUSTOMER_ID
where
    fma.MEMBERSHIP_EVENT_TYPE in ('Activation')
    and vfa.CUSTOMER_ID is null;


create or replace temporary table _lead_registration as
select
    lro.STORE_REPORTING_NAME
    ,lro.lead_registration_date
    ,lro.lead_registration_cohort
    ,va.vip_activation_date
    ,va.vip_activation_cohort
    ,lro.CUSTOMER_ID
from _lead_registration_only lro
left join _vip_activation va
    on va.CUSTOMER_ID = lro.CUSTOMER_ID
    and va.vip_activation_date >= lro.lead_registration_date;


create table if not exists reporting_prod.GFB.dos_112_gfb_lead_performance as
select
    dd.FULL_DATE
    ,dd.MONTH_DATE
    ,lr.STORE_REPORTING_NAME
    ,(case
        when (dd.MONTH_DATE between lr.lead_registration_cohort and coalesce(lr.vip_activation_date, current_date())) and datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 < 14
        then 'M' || cast(datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 as varchar(20))
        when (dd.MONTH_DATE between lr.lead_registration_cohort and coalesce(lr.vip_activation_date, current_date())) and datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 >= 14
        then 'M14+'
        end) as lead_tenure
    ,(case
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 = 1
        then 'D1'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 7
        then 'D2-7'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 30
        then 'D8-30'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 60
        then 'D31-60'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 90
        then 'D61-90'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 180
        then 'D91-180'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 365
        then 'D181-365'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 730
        then 'D365-730'
        else 'D730+'
        end) as lead_tenure_by_day

    ,count(case
            when lr.lead_registration_date = dd.FULL_DATE then lr.CUSTOMER_ID end) as lead_registrations
    ,count(case
            when lr.vip_activation_date = dd.FULL_DATE then lr.CUSTOMER_ID end) as vip_activations
    ,count(case
            when dd.FULL_DATE >= lr.lead_registration_date and dd.FULL_DATE <= coalesce(lr.vip_activation_date, current_date()) then lr.CUSTOMER_ID end) as daily_leads_count
from _lead_registration lr
join EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
    on dd.FULL_DATE >= $start_date
    and dd.FULL_DATE >= lr.lead_registration_date
    and dd.FULL_DATE <= coalesce(lr.vip_activation_date, current_date())
where
    coalesce(lr.vip_activation_date, current_date()) >= $start_date
group by
    dd.FULL_DATE
    ,dd.MONTH_DATE
    ,lr.STORE_REPORTING_NAME
    ,(case
        when (dd.MONTH_DATE between lr.lead_registration_cohort and coalesce(lr.vip_activation_date, current_date())) and datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 < 14
        then 'M' || cast(datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 as varchar(20))
        when (dd.MONTH_DATE between lr.lead_registration_cohort and coalesce(lr.vip_activation_date, current_date())) and datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 >= 14
        then 'M14+'
        end)
    ,(case
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 = 1
        then 'D1'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 7
        then 'D2-7'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 30
        then 'D8-30'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 60
        then 'D31-60'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 90
        then 'D61-90'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 180
        then 'D91-180'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 365
        then 'D181-365'
        when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 730
        then 'D365-730'
        else 'D730+'
        end);


merge into reporting_prod.GFB.dos_112_gfb_lead_performance t
using
(
    select
        dd.FULL_DATE
        ,dd.MONTH_DATE
        ,lr.STORE_REPORTING_NAME
        ,(case
            when (dd.MONTH_DATE between lr.lead_registration_cohort and coalesce(lr.vip_activation_date, current_date())) and datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 < 14
            then 'M' || cast(datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 as varchar(20))
            when (dd.MONTH_DATE between lr.lead_registration_cohort and coalesce(lr.vip_activation_date, current_date())) and datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 >= 14
            then 'M14+'
            end) as lead_tenure
        ,(case
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 = 1
            then 'D1'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 7
            then 'D2-7'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 30
            then 'D8-30'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 60
            then 'D31-60'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 90
            then 'D61-90'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 180
            then 'D91-180'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 365
            then 'D181-365'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 730
            then 'D365-730'
            else 'D730+'
            end) as lead_tenure_by_day

        ,count(case
                when lr.lead_registration_date = dd.FULL_DATE then lr.CUSTOMER_ID end) as lead_registrations
        ,count(case
                when lr.vip_activation_date = dd.FULL_DATE then lr.CUSTOMER_ID end) as vip_activations
        ,count(case
                when dd.FULL_DATE >= lr.lead_registration_date and dd.FULL_DATE <= coalesce(lr.vip_activation_date, current_date()) then lr.CUSTOMER_ID end) as daily_leads_count
    from _lead_registration lr
    join EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
        on dd.FULL_DATE >= $process_start_date
        and dd.FULL_DATE >= lr.lead_registration_date
        and dd.FULL_DATE <= coalesce(lr.vip_activation_date, current_date())
    where
        coalesce(lr.vip_activation_date, current_date()) >= $process_start_date
    group by
        dd.FULL_DATE
        ,dd.MONTH_DATE
        ,lr.STORE_REPORTING_NAME
        ,(case
            when (dd.MONTH_DATE between lr.lead_registration_cohort and coalesce(lr.vip_activation_date, current_date())) and datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 < 14
            then 'M' || cast(datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 as varchar(20))
            when (dd.MONTH_DATE between lr.lead_registration_cohort and coalesce(lr.vip_activation_date, current_date())) and datediff(month, lr.lead_registration_cohort, dd.MONTH_DATE) + 1 >= 14
            then 'M14+'
            end)
        ,(case
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 = 1
            then 'D1'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 7
            then 'D2-7'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 30
            then 'D8-30'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 60
            then 'D31-60'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 90
            then 'D61-90'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 180
            then 'D91-180'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 365
            then 'D181-365'
            when (dd.FULL_DATE between lr.lead_registration_date and coalesce(lr.vip_activation_date, current_date())) and datediff(day, lr.lead_registration_date, dd.FULL_DATE) + 1 <= 730
            then 'D365-730'
            else 'D730+'
            end)
) s on s.STORE_REPORTING_NAME = t.STORE_REPORTING_NAME
    and s.FULL_DATE = t.FULL_DATE
    and s.MONTH_DATE = t.MONTH_DATE
    and s.lead_tenure = t.lead_tenure
    and s.lead_tenure_by_day = t.lead_tenure_by_day
when not matched then insert (
                                full_date
                                ,month_date
                                ,store_reporting_name
                                ,lead_tenure
                                ,lead_tenure_by_day
                                ,lead_registrations
                                ,vip_activations
                                ,daily_leads_count
                            )
values (
            s.FULL_DATE
            ,s.MONTH_DATE
            ,s.STORE_REPORTING_NAME
            ,s.lead_tenure
            ,lead_tenure_by_day
            ,s.lead_registrations
            ,s.vip_activations
            ,s.daily_leads_count
       )
when matched then update
set
    t.FULL_DATE = s.FULL_DATE
    ,t.MONTH_DATE = s.MONTH_DATE
    ,t.STORE_REPORTING_NAME = s.STORE_REPORTING_NAME
    ,t.lead_tenure = s.lead_tenure
    ,t.lead_tenure_by_day = s.lead_tenure_by_day
    ,t.lead_registrations = s.lead_registrations
    ,t.vip_activations = s.vip_activations
    ,t.daily_leads_count = s.daily_leads_count;


create or replace temporary table _lead_site_visit as
select
    lr.STORE_REPORTING_NAME
    ,date_trunc(month, smd.SESSIONLOCALDATETIME) as session_month
    ,(case
        when datediff(month, lr.lead_registration_cohort, date_trunc(month, smd.SESSIONLOCALDATETIME)) + 1 < 14
        then 'M' || cast(datediff(month, lr.lead_registration_cohort, date_trunc(month, smd.SESSIONLOCALDATETIME)) + 1 as varchar(20))
        when datediff(month, lr.lead_registration_cohort, date_trunc(month, smd.SESSIONLOCALDATETIME)) + 1 >= 14
        then 'M14+'
        end) as lead_tenure
    ,count(distinct lr.CUSTOMER_ID) as revisited_lead_count
from REPORTING_BASE_PROD.SHARED.SESSION_SINGLE_VIEW_MEDIA smd
join _lead_registration lr
    on lr.CUSTOMER_ID = edw_prod.stg.udf_unconcat_brand(smd.CUSTOMER_ID)
    and date_trunc(day, smd.SESSIONLOCALDATETIME) < lr.vip_activation_date
    and cast(smd.SESSIONLOCALDATETIME as date) > lr.lead_registration_date
    and cast(smd.SESSIONLOCALDATETIME as date) > $start_date
    and cast(smd.SESSIONLOCALDATETIME as date) < current_date()
group by
    lr.STORE_REPORTING_NAME
    ,date_trunc(month, smd.SESSIONLOCALDATETIME)
    ,(case
        when datediff(month, lr.lead_registration_cohort, date_trunc(month, smd.SESSIONLOCALDATETIME)) + 1 < 14
        then 'M' || cast(datediff(month, lr.lead_registration_cohort, date_trunc(month, smd.SESSIONLOCALDATETIME)) + 1 as varchar(20))
        when datediff(month, lr.lead_registration_cohort, date_trunc(month, smd.SESSIONLOCALDATETIME)) + 1 >= 14
        then 'M14+'
        end);


create or replace temporary table _bop_lead as
select
    lr.STORE_REPORTING_NAME
    ,month_date.MONTH_DATE
    ,(case
        when datediff(month, lr.lead_registration_cohort, month_date.MONTH_DATE) + 1 < 14
        then 'M' || cast(datediff(month, lr.lead_registration_cohort, month_date.MONTH_DATE) + 1 as varchar(20))
        when datediff(month, lr.lead_registration_cohort, month_date.MONTH_DATE) + 1 >= 14
        then 'M14+'
        end) as lead_tenure
    ,count(lr.CUSTOMER_ID) as leads
from _lead_registration lr
join
(
    select distinct
        dd.MONTH_DATE
    from EDW_PROD.DATA_MODEL_JFB.DIM_DATE dd
    where
        dd.FULL_DATE >= $start_date
        and dd.FULL_DATE < current_date()
) month_date on month_date.MONTH_DATE >= lr.lead_registration_cohort
            and month_date.MONTH_DATE < lr.vip_activation_date
group by
    lr.STORE_REPORTING_NAME
    ,month_date.MONTH_DATE
    ,(case
        when datediff(month, lr.lead_registration_cohort, month_date.MONTH_DATE) + 1 < 14
        then 'M' || cast(datediff(month, lr.lead_registration_cohort, month_date.MONTH_DATE) + 1 as varchar(20))
        when datediff(month, lr.lead_registration_cohort, month_date.MONTH_DATE) + 1 >= 14
        then 'M14+'
        end);


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.GFB.dos_112_01_gfb_lead_site_visit as
select
    cl.*
    ,lsv.revisited_lead_count
from _bop_lead cl
left join _lead_site_visit lsv
    on lsv.STORE_REPORTING_NAME = cl.STORE_REPORTING_NAME
    and lsv.lead_tenure = cl.lead_tenure
    and lsv.session_month = cl.MONTH_DATE;



CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.dos_112_02_gfb_lead_to_vip_by_cohort as
select
    lr.STORE_REPORTING_NAME
    ,lr.lead_registration_cohort
    ,count(distinct case
            when datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 = 1
            then lr.CUSTOMER_ID end) as d1
    ,count(distinct case
            when datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 > 1 and datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 <= 7
            then lr.CUSTOMER_ID end) as "D2-7"
    ,count(distinct case
            when datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 > 7 and datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 <= 30
            then lr.CUSTOMER_ID end) as "D8-30"
    ,count(distinct case
            when datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 > 30 and datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 <= 60
            then lr.CUSTOMER_ID end) as "D31-60"
    ,count(distinct case
            when datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 > 60 and datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 <= 90
            then lr.CUSTOMER_ID end) as "D61-90"
    ,count(distinct case
            when datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 > 90 and datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 <= 180
            then lr.CUSTOMER_ID end) as "D91-180"
    ,count(distinct case
            when datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 > 180 and datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 <= 365
            then lr.CUSTOMER_ID end) as "D181-365"
    ,count(distinct case
            when datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 > 365 and datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 <= 730
            then lr.CUSTOMER_ID end) as "D365-730"
    ,count(distinct case
            when datediff(day, lr.lead_registration_date, lr.vip_activation_date) + 1 > 730
            then lr.CUSTOMER_ID end) as "D730+"
    ,count(distinct lr.CUSTOMER_ID) as total_leads
    ,count(distinct case
            when lr.vip_activation_date is not null then lr.CUSTOMER_ID end) as total_vip_activations
from _lead_registration lr
where
    lr.lead_registration_date >= $start_date
group by
    lr.STORE_REPORTING_NAME
    ,lr.lead_registration_cohort
