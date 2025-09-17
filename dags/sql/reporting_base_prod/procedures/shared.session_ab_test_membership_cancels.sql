
-------------------------------------------------------
-- /*  MEMBERSHIP CANCELLATIONS + ONLINE CANCELS  */
CREATE OR REPLACE TEMPORARY TABLE _membership_cancels as
select distinct
    v.customer_id
    ,STORE_BRAND
    ,TEST_KEY
    ,test_label
    ,CMS_MIN_TEST_START_DATETIME_HQ
    ,AB_TEST_SEGMENT
    ,v.min_session_id --not the actual session the customer cancelled. this is just to get distinct cancellations per customer
    ,sum(case when a.CUSTOMER_ID is not null then 1 else 0 end) as cancellation
from (select distinct
        customer_id
        ,STORE_BRAND
        ,TEST_KEY
        ,test_label
        ,CMS_MIN_TEST_START_DATETIME_HQ
        ,AB_TEST_SEGMENT
        ,AB_TEST_START_LOCAL_DATETIME
        ,min(session_id) as min_session_id
        ,min(SESSION_LOCAL_DATETIME) as min_session_datetime_hq
        ,max(SESSION_LOCAL_DATETIME) + interval '3 hour' as max_session_datetime_hq
      from reporting_base_prod.shared.SESSION_AB_TEST_METADATA
       group by 1,2,3,4,5,6,7) as v
join EDW_PROD.DATA_MODEL.FACT_MEMBERSHIP_EVENT AS a on a.CUSTOMER_ID = v.customer_id
    and a.EVENT_START_LOCAL_DATETIME between min_session_datetime_hq and max_session_datetime_hq
    and membership_event_type = 'Cancellation'
group by 1,2,3,4,5,6,7;

CREATE OR REPLACE TEMPORARY TABLE _online_cancels as
select distinct
    v.customer_id
    ,STORE_BRAND
    ,TEST_KEY
    ,test_label
    ,CMS_MIN_TEST_START_DATETIME_HQ
    ,AB_TEST_SEGMENT
    ,v.min_session_id --not the actual session the customer cancelled. this is just to get distinct cancellations per customer
--     ,a.DATETIME_ADDED
    ,sum(case when a.CUSTOMER_ID is not null then 1 else 0 end) as online_cancellation
from (select distinct
        customer_id
        ,STORE_BRAND
        ,TEST_KEY
        ,test_label
        ,CMS_MIN_TEST_START_DATETIME_HQ
        ,AB_TEST_SEGMENT
        ,AB_TEST_START_LOCAL_DATETIME
        ,min(session_id) as min_session_id
        ,min(CONVERT_TIMEZONE('America/Los_Angeles',SESSION_LOCAL_DATETIME)::datetime) as min_session_datetime_hq
        ,max(CONVERT_TIMEZONE('America/Los_Angeles',SESSION_LOCAL_DATETIME)::datetime) + interval '3 hour' as max_session_datetime_hq
      from reporting_base_prod.shared.SESSION_AB_TEST_METADATA
      group by 1,2,3,4,5,6,7) as v
join lake_consolidated_view.ultra_merchant.CUSTOMER_LOG AS a on a.CUSTOMER_ID = v.customer_id
where
    a.datetime_added between min_session_datetime_hq and max_session_datetime_hq
    and comment = 'Membership has been set to Pay As You Go by customer using online cancel.'
group by 1,2,3,4,5,6,7;

-- delete from _online_cancels
--     where (STORE_BRAND in ('Fabletics','Yitty') and CMS_MIN_TEST_START_DATETIME_HQ >= '2023-02-22');

CREATE OR REPLACE TEMPORARY TABLE _online_cancels_session as
select
    customer_id
    ,STORE_BRAND
    ,TEST_KEY
    ,test_label
    ,CMS_MIN_TEST_START_DATETIME_HQ
    ,AB_TEST_SEGMENT
    ,m.session_id as min_session_id --not the actual session the customer cancelled. this is just to get distinct cancellations per customer
    ,1 as online_cancellation
from reporting_base_prod.shared.SESSION_AB_TEST_METADATA as m
join lake_consolidated_view.ultra_merchant.SESSION_DETAIL AS a on a.SESSION_ID = m.SESSION_ID
where
    name = 'has_cancelled';

CREATE OR REPLACE TEMPORARY TABLE _pause as
select distinct
    v.customer_id
    ,STORE_BRAND
    ,TEST_KEY
    ,test_label
    ,CMS_MIN_TEST_START_DATETIME_HQ
    ,AB_TEST_SEGMENT
    ,v.min_session_id --not the actual session the customer cancelled. this is just to get distinct cancellations per customer
    ,sum(case when a.MEMBERSHIP_ID is not null then 1 else 0 end) as membership_pause
from (select distinct
          customer_id
            ,MEMBERSHIP_ID
            ,STORE_BRAND
            ,TEST_KEY
            ,test_label
            ,CMS_MIN_TEST_START_DATETIME_HQ
            ,AB_TEST_SEGMENT
            ,AB_TEST_START_LOCAL_DATETIME
            ,min(session_id) as min_session_id
            ,min(CONVERT_TIMEZONE('America/Los_Angeles',SESSION_LOCAL_DATETIME)::datetime) as min_session_datetime_hq
            ,max(CONVERT_TIMEZONE('America/Los_Angeles',SESSION_LOCAL_DATETIME)::datetime) + interval '3 hour' as max_session_datetime_hq
      from reporting_base_prod.shared.SESSION_AB_TEST_METADATA
      group by 1,2,3,4,5,6,7,8) as v
join lake_consolidated_view.ultra_merchant.MEMBERSHIP_SNOOZE AS a on a.MEMBERSHIP_ID = v.MEMBERSHIP_ID
where
    a.DATETIME_ADDED between min_session_datetime_hq and max_session_datetime_hq
    and ADMINISTRATOR_ID is null --non gms snoozes, only online snoozes
group by 1,2,3,4,5,6,7;

-- delete from _pause
--     where (STORE_BRAND in ('Fabletics','Yitty') and CMS_MIN_TEST_START_DATETIME_HQ >= '2023-02-22');

CREATE OR REPLACE TEMPORARY TABLE _pauses_session as
select
    customer_id
    ,STORE_BRAND
    ,TEST_KEY
    ,test_label
    ,CMS_MIN_TEST_START_DATETIME_HQ
    ,AB_TEST_SEGMENT
    ,m.session_id as min_session_id --not the actual session the customer cancelled. this is just to get distinct cancellations per customer
    ,1 as membership_pause
from reporting_base_prod.shared.SESSION_AB_TEST_METADATA as m
join lake_consolidated_view.ultra_merchant.SESSION_DETAIL AS a on a.SESSION_ID = m.SESSION_ID
where
    name = 'has_snoozed';

CREATE OR REPLACE TEMPORARY TABLE _min_session_ids as
select distinct min_session_id,TEST_KEY,customer_id,test_label,AB_TEST_SEGMENT from _membership_cancels
union
select distinct min_session_id,TEST_KEY,customer_id,test_label,AB_TEST_SEGMENT from _online_cancels
union
select distinct min_session_id,TEST_KEY,customer_id,test_label,AB_TEST_SEGMENT from _online_cancels_session
union
select distinct min_session_id,TEST_KEY,customer_id,test_label,AB_TEST_SEGMENT from _pause
union
select distinct min_session_id,TEST_KEY,customer_id,test_label,AB_TEST_SEGMENT from _pauses_session;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.session_ab_test_membership_cancels as
-- CREATE OR REPLACE TEMPORARY TABLE _session_ab_test_membership_cancels as
select distinct -- where STORE_BRAND not in ('Fabletics','Yitty')
    c.min_session_id
    ,c.TEST_KEY
    ,c.test_label
    ,c.AB_TEST_SEGMENT
    ,c.customer_id
    ,sum(cancellation) as cancellations
--     ,coalesce(sum(oc.online_cancellation),sum(ocs.online_cancellation)) as online_cancellations
    ,sum(oc.online_cancellation) as online_cancellations
--     ,coalesce(sum(p.membership_pause),sum(ps.membership_pause)) as membership_pauses
    ,sum(p.membership_pause) as membership_pauses
from _min_session_ids as c
left join _membership_cancels as m on m.min_session_id = c.min_session_id
    and m.TEST_KEY = c.TEST_KEY
    and m.test_label = c.test_label
    and m.AB_TEST_SEGMENT = c.AB_TEST_SEGMENT
left join _online_cancels oc on c.min_session_id = oc.min_session_id --where m.customer_id = 782940163
    and c.TEST_KEY = oc.TEST_KEY
    and c.test_label = oc.test_label
    and c.AB_TEST_SEGMENT = oc.AB_TEST_SEGMENT
left join _online_cancels_session ocs on c.min_session_id = ocs.min_session_id --where m.customer_id = 782940163
    and c.TEST_KEY = ocs.TEST_KEY
    and c.test_label = ocs.test_label
    and c.AB_TEST_SEGMENT = ocs.AB_TEST_SEGMENT
left join _pause as p on c.min_session_id = p.min_session_id
    and c.TEST_KEY = p.TEST_KEY
    and c.test_label = p.test_label
    and c.AB_TEST_SEGMENT = p.AB_TEST_SEGMENT
left join _pauses_session as ps on c.min_session_id = ps.min_session_id
    and c.TEST_KEY = ps.TEST_KEY
    and c.test_label = ps.test_label
    and c.AB_TEST_SEGMENT = ps.AB_TEST_SEGMENT
group by 1,2,3,4,5;

ALTER TABLE reporting_base_prod.shared.session_ab_test_membership_cancels SET DATA_RETENTION_TIME_IN_DAYS = 0;

-- select
--     test_key
--     ,test_label
--     ,AB_TEST_SEGMENT
--     ,count(*),count(distinct min_session_id),sum(online_cancellations)
-- -- from _session_ab_test_membership_cancels
-- from reporting_base_prod.shared.session_ab_test_membership_cancels
-- where TEST_KEY = 'CANCELFIRSTORDER_v4'
-- group by 1,2,3

