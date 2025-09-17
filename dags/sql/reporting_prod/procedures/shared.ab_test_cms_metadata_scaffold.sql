
CREATE OR REPLACE TEMPORARY TABLE _membership_state_scaffold_unfiltered as
select
    m.*
    ,MEMBERSHIP_LEVEL_GROUP_ID
    ,CMS_MEMBERSHIP_STATE
    ,case when cast(MEMBERSHIP_LEVEL as varchar) is null then cast(MEMBERSHIP_LEVEL_GROUP_ID as varchar) else cast(MEMBERSHIP_LEVEL as varchar) end as membership_level_adj
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
full join REPORTING_PROD.SHARED.AB_TEST_MEMBERSHIP_MAPPING_REFERENCE as map;

delete from _membership_state_scaffold_unfiltered
where (
            MEMBERSHIP_LEVEL_GROUP_ID = 100
        and cms_membership_state = 'Lead'
        and MEMBERSHIP_LEVEL in ('500,1000','500,1000,200,300','500,400,1000','500,400,1000,200,300'))
   or (TEST_TYPE = 'membership' and cms_membership_state = 'Prospect')
   or (TEST_TYPE = 'membership' and
       (CMS_MEMBERSHIP_STATE <> 'Prospect' and test_type = 'session')
    and (lower(test_key) ilike '%quiz%'
        or lower(test_key) ilike '%reg%'
        or lower(test_start_location) ilike '%quiz%'
        or lower(test_start_location) ilike '%reg%'
        or lower(test_label) ilike '%speedy%'
        or lower(TEST_LABEL) ilike '%activating order consent%'
        or lower(TEST_LABEL) ilike '%prospect%'
        or lower(TEST_LABEL) ilike '%quiz%'
        or lower(TEST_FRAMEWORK_DESCRIPTION) ilike '%prospect%'
        or lower(test_framework_description) ilike '%speedy%'
        or lower(test_framework_description) ilike '%reg%'
        or lower(test_framework_description) ilike '%quiz%'
           )
    );

--session tests for prospects only
CREATE OR REPLACE TEMPORARY TABLE _session_prospect_scaffold_filtered as
select *
from _membership_state_scaffold_unfiltered
where
    (TEST_TYPE = 'session'
        and ((test_type = 'session' and CMS_MEMBERSHIP_STATE = 'Prospect' )
            and (lower(test_key) ilike '%quiz%'
                or lower(test_key) ilike '%reg%'
                or lower(test_start_location) ilike '%quiz%'
                or lower(test_start_location) ilike '%reg%'
                or lower(TEST_LABEL) ilike '%reg%'
                or lower(test_label) ilike '%speedy%'
                or lower(TEST_LABEL) ilike '%activating order consent%'
                or lower(TEST_LABEL) ilike '%prospect%'
                or lower(TEST_LABEL) ilike '%quiz%'
                or lower(TEST_FRAMEWORK_DESCRIPTION) ilike '%prospect%'
                or lower(test_framework_description) ilike '%speedy%'
                or lower(test_framework_description) ilike '%reg%'
                or lower(test_framework_description) ilike '%quiz%')));

--sessions tests that are for all membership states
CREATE OR REPLACE TEMPORARY TABLE _session_all_segments_scaffold_filtered as
select *
from _membership_state_scaffold_unfiltered
where
    TEST_TYPE = 'session'
    and TEST_KEY not in (select distinct test_key from _session_prospect_scaffold_filtered);


--membership tests
CREATE OR REPLACE TEMPORARY TABLE _membership_scaffold_filtered as
select *
from _membership_state_scaffold_unfiltered
where
    TEST_TYPE = 'membership'  and position(MEMBERSHIP_LEVEL_GROUP_ID in coalesce(membership_level,membership_level_adj));

CREATE OR REPLACE TEMPORARY TABLE _membership_state_scaffold_filtered as
select *
from _session_prospect_scaffold_filtered

union all

select *
from _session_all_segments_scaffold_filtered

union all

select *
from _membership_scaffold_filtered;

-------------------------------------------------------
--store scaffold
CREATE OR REPLACE TEMPORARY TABLE _stores as
select distinct
    STORE_BRAND,STORE_REGION,STORE_COUNTRY
from EDW_PROD.DATA_MODEL.DIM_STORE
where STORE_REGION is not null and STORE_REGION <> 'Unknown';

CREATE OR REPLACE TEMPORARY TABLE _store_scaffold as
select distinct
    TEST_KEY,TEST_LABEL,m.STORE_BRAND,LOCALE,STORE_REGION,STORE_COUNTRY
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
full join _stores as map
where --CMS_TEST_KEY = 'ENHANCEDNAVUI_v0' and
    LOCALE is not null
    and position(lower(map.STORE_COUNTRY) in lower(LOCALE)) <> 0 --and CMS_TEST_KEY = 'ABCostofShippingFKKI516_v1'
    and m.STORE_BRAND = map.STORE_BRAND

union

select distinct
    TEST_KEY,TEST_LABEL,STORE_BRAND,LOCALE,'NA' as STORE_REGION,'US' AS STORE_COUNTRY
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
where
    lower(LOCALE) ilike '%america%'

union

select distinct
    TEST_KEY,TEST_LABEL,m.STORE_BRAND,LOCALE,STORE_REGION,STORE_COUNTRY
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
FULL JOIN _stores as map
where
    lower(LOCALE) ilike '%europe%'
    and map.STORE_REGION = 'EU'
    and m.STORE_BRAND = map.STORE_BRAND

union

select distinct
    TEST_KEY,TEST_LABEL,STORE_BRAND,LOCALE,'EU' as STORE_REGION,'UK' AS STORE_COUNTRY
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
where
    lower(LOCALE) ilike '%gb%'

union

select distinct
    TEST_KEY,TEST_LABEL,STORE_BRAND,LOCALE,'NA' as STORE_REGION,'US' AS STORE_COUNTRY
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
where
    LOCALE is null
    and m.STORE_BRAND in ('ShoeDazzle','FabKids','Yitty')

union

select distinct
    TEST_KEY,TEST_LABEL,m.STORE_BRAND,LOCALE,STORE_REGION,STORE_COUNTRY
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
full join _stores as s
where
    LOCALE is null
  and m.STORE_BRAND not in ('ShoeDazzle','FabKids','Yitty')
    and m.STORE_BRAND = s.STORE_BRAND
order by 1,2,3,4;

-------------------------------------------------------
--FINAL OUTPUT

CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_SCAFFOLD as
select distinct
    CAMPAIGN_CODE
    ,TEST_FRAMEWORK_ID
    ,m.TEST_KEY
    ,CMS_IS_ACTIVE_VERSION
    ,statuscode
    ,status
    ,TEST_TYPE
    ,m.TEST_LABEL
    ,TEST_FRAMEWORK_DESCRIPTION
    ,IS_CMS_SPECIFIC_LEAD_DAY
    ,IS_CMS_SPECIFIC_GENDER
    ,CMS_MIN_TEST_START_DATETIME_HQ
    ,CMS_START_DATE
    ,CMS_END_DATE
    ,MEMBERSHIP_LEVEL
    ,CMS_MEMBERSHIP_STATE
    ,TEST_START_LOCATION
    ,m.LOCALE
    ,m.STORE_BRAND
    ,STORE_REGION as STORE_REGION
    ,STORE_COUNTRY AS STORE_COUNTRY
from _membership_state_scaffold_filtered as m
full join _store_scaffold st on st.TEST_KEY = m.TEST_KEY and st.TEST_LABEL = m.TEST_LABEL;

--manual deletes
delete from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_SCAFFOLD
where
    CAMPAIGN_CODE is null
    or (CAMPAIGN_CODE = 'HomepageBaby' and cms_membership_state <> 'Lead')
    or (CAMPAIGN_CODE = 'yittypostregallvipdefault_v2' and cms_membership_state <> 'Lead')
    or (CAMPAIGN_CODE = 'SHOPPABLESKIPV1' and cms_membership_state <> 'VIP')
    or (CAMPAIGN_CODE = 'VIPoffercopy' and cms_membership_state <> 'Lead')
    or (CAMPAIGN_CODE = 'SXF1278' and cms_membership_state <> 'Lead')
    or (CAMPAIGN_CODE = 'jfg1722' and cms_membership_state <> 'Prospect')
    or (CAMPAIGN_CODE = 'jfg1914' and cms_membership_state <> 'Prospect')
    or (CAMPAIGN_CODE = 'membercredit22' and cms_membership_state <> 'VIP')
    or (CAMPAIGN_CODE = 'FLMOfferUpsellv1' and cms_membership_state <> 'Prospect')
   or (CAMPAIGN_CODE = 'sdrr5239' and cms_membership_state <> 'Prospect')
    or (campaign_code = 'TariffFKKi2866' and cms_membership_state <> 'VIP')
    or (CAMPAIGN_CODE = 'FLWQuizSplash' and cms_membership_state <> 'Prospect')
   or (CAMPAIGN_CODE = 'jfg2397' and cms_membership_state <> 'Prospect')
    or (CAMPAIGN_CODE = 'USPDPDUALCTA' and cms_membership_state <> 'Prospect')
    or (TEST_FRAMEWORK_ID = 1249 and cms_membership_state <> 'Prospect')
;

-- -- distinct by
-- select
--     STORE_BRAND
--     ,TEST_KEY
--     ,TEST_LABEL
--     ,STATUSCODE
--     ,CMS_MEMBERSHIP_STATE
--      ,STORE_COUNTRY
--     ,count(*)
--     ,count(distinct CMS_MEMBERSHIP_STATE)
-- from REPORTING.SHARED.AB_TEST_CMS_METADATA_SCAFFOLD
-- where TEST_KEY = 'UrgencyMessagingUI_v0'
-- -- where TEST_KEY in ('SXF1361_v1','TariffFKKi2866_v0','echoUKDE_v0','SXF1361_v0','echoUKDE_v1','ABCAOnlineCancelFKKI3208_v0','ABCAOnlineCancelRedesignFKKI3328_v0','PostRegRecs_v1','ExposedCartProductsUpsellRetestV3FKKI2943_v0')
-- group by 1,2,3,4,5,6
