
set last_refreshed_datetime_hq = (select max(CONVERT_TIMEZONE('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::datetime) from reporting_base_prod.shared.SESSION_AB_TEST_METADATA);

CREATE OR REPLACE TEMPORARY TABLE _max_datetimes as --filters out preview only
select distinct
    m.test_framework_id
    ,test_type
    --m.STORE_BRAND
    ,m.CAMPAIGN_CODE
    ,m.test_key
    ,m.test_label
    ,cms_min_test_start_datetime_hq
    ,MAX(HQ_MAX_DATETIME) as last_refreshed_hq_datetime
    ,MAX(convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME))::datetime as max_ab_test_start_datetime
from reporting_base_prod.shared.SESSION_AB_TEST_METADATA as m
group by 1,2,3,4,5,6;

CREATE OR REPLACE TEMPORARY TABLE _last_3_months_campaign_code as
select distinct campaign_code,min(AB_TEST_START_LOCAL_DATETIME) as min_campaign_code_datetime,max(AB_TEST_START_LOCAL_DATETIME) as max_campaign_code_datetime
from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_METADATA
group by campaign_code
having max(AB_TEST_START_LOCAL_DATETIME) >= current_date - interval '3 month'
   and max(AB_TEST_START_LOCAL_DATETIME) <= current_date + interval '1 day'

union all

select distinct campaign_code,min(AB_TEST_START_LOCAL_DATETIME) as min_campaign_code_datetime,max(AB_TEST_START_LOCAL_DATETIME) as max_campaign_code_datetime --this is for validation, will remove eventually
from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_METADATA
where
        TEST_KEY in ('SXF1378_v2','JFG1558_v0','jfg576_v2','JFG2297_v1','TariffFKKi2866_v0','SHOPPABLESKIPV1_v3','membercredit22_v1','yittypostregallvipdefault_v2','UrgencyMessagingUI_v0'
        ,'SXFQuizv2_v3','jfg1722_v2','sdrr5154_v2','FLMOfferUpsellv1_v0','FLWAthleisureBottomsTest_v0')
group by campaign_code;

CREATE OR REPLACE TEMPORARY TABLE _max_datetime_campaign_code as
select distinct
    test_framework_id
    ,TEST_KEY
    ,test_label
    ,store_brand
    ,min_campaign_code_datetime
    ,max_campaign_code_datetime
from _last_3_months_campaign_code as c
join (select distinct campaign_code,test_framework_id,TEST_KEY,test_label,store_brand from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_METADATA) as m on m.campaign_code = c.campaign_code;

CREATE OR REPLACE TEMPORARY TABLE _avail_in_abt_dash as
select distinct test_key,test_label
from REPORTING_PROD.shared.ab_test_final;

CREATE OR REPLACE TEMPORARY TABLE _custom_flag as
select distinct
    TEST_KEY
    ,test_label
    ,FLAG_4_NAME
    ,FLAG_4_DESCRIPTION
from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_FLAGS where FLAG_4_NAME is not null;

CREATE OR REPLACE TEMPORARY TABLE _metadata as
select
    m.store_group
     ,m.STORE_BRAND
     ,m.TEST_LABEL
     ,m.CAMPAIGN_CODE
     ,m.TEST_KEY
     ,split_part(m.TEST_KEY,'_',2) AS test_key_version
     ,cast(m.TEST_FRAMEWORK_ID as smallint) as TEST_FRAMEWORK_ID
     ,TEST_FRAMEWORK_DESCRIPTION
     ,m.TEST_TYPE
     ,IS_CMS_SPECIFIC_LEAD_DAY
     ,IS_CMS_SPECIFIC_GENDER
     ,CMS_IS_ACTIVE_VERSION
     ,case when TEST_START_LOCATION = 'Any' and m.TEST_TYPE = 'membership' then 'Any LI'
           when TEST_START_LOCATION  in ('Any','Any Logged-Out Page') and m.TEST_TYPE = 'session' then 'Any LO'
           when TEST_START_LOCATION in ('Cart','Cart / Checkout') and m.TEST_TYPE = 'session' then 'Cart/Checkout LO'
           when TEST_START_LOCATION in ('Cart','Cart / Checkout') and m.TEST_TYPE = 'membership' then 'Cart/Checkout LI'

           when TEST_START_LOCATION in ('Shopping Grid') and m.TEST_TYPE = 'session' then 'Shopping Grid LO'
           when TEST_START_LOCATION in ('Shopping Grid') and m.TEST_TYPE = 'membership' then 'Shopping Grid LI'
           when prospect_traffic is null and lead_traffic = true and vip_traffic is null and guest_traffic is null and cancelled_traffic is null and m.test_type = 'membership' and TEST_START_LOCATION = 'Boutique' then 'Post-Reg LI'
--        when test_type = 'membership' and member_segments = 'Leads' and TEST_START_LOCATION = 'Boutique' then 'Post-Reg Logged-In'
           when m.test_type = 'membership' and test_start_location is not null then concat(TEST_START_LOCATION,' LI')
           when m.test_type = 'session' and test_start_location is not null then concat(TEST_START_LOCATION,' LO')
           when m.test_type = 'membership' and test_start_location is null then 'Unknown LI'
           when m.test_type = 'session' and test_start_location is null then 'Unknown LO'
    end as test_start_location
     ,DEVICE
     ,DESKTOP_GROUP_TYPE
     ,case when DESKTOP_GROUP is null then '-' else DESKTOP_GROUP end as DESKTOP_TRAFFIC
     ,MOBILE_GROUP_TYPE
     ,case when MOBILE_GROUP is null then '-' else MOBILE_GROUP end as MOBILE_TRAFFIC
     ,MOBILEAPP_GROUP_TYPE
     ,case when MOBILEAPP_GROUP is null then '-' else MOBILEAPP_GROUP end as APP_TRAFFIC
     ,m.CMS_MIN_TEST_START_DATETIME_HQ as activated_ab_test_start_datetime
     ,max_ab_test_start_datetime as max_ab_test_start_datetime
     ,min_campaign_code_datetime
     ,max_campaign_code_datetime
     ,datediff('day',m.CMS_MIN_TEST_START_DATETIME_HQ,max_ab_test_start_datetime) as days_running
     ,case when CMS_IS_ACTIVE_VERSION = TRUE then 'Active'
           when CMS_IS_ACTIVE_VERSION = FALSE and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 = 1 then 'Inactive < 1M'
           when CMS_IS_ACTIVE_VERSION = FALSE and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 between 2 and 3 then 'Inactive 2-3M'
           when CMS_IS_ACTIVE_VERSION = FALSE and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 between 4 and 6 then 'Inactive 4-6M'
           when CMS_IS_ACTIVE_VERSION = FALSE and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 between 7 and 12 then 'Inactive 7-12M'
           when CMS_IS_ACTIVE_VERSION = FALSE and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 >= 13 then 'Inactive 13M+'
    end as test_status
     ,last_refreshed_hq_datetime
     ,CMS_SESSION_FLAG_1
     ,CMS_SESSION_FLAG_1_DESCRIPTION
     ,CMS_SESSION_FLAG_2
     ,CMS_SESSION_FLAG_2_DESCRIPTION
     ,CMS_SESSION_FLAG_3
     ,CMS_SESSION_FLAG_3_DESCRIPTION
     ,FLAG_4_NAME as CUSTOM_FLAG
     ,FLAG_4_DESCRIPTION  as CUSTOM_FLAG_DESCRIPTION
     ,case when na_traffic = true and eu_traffic = true then 'NA+EU'
           when na_traffic = true and eu_traffic is null then 'NA'
           when na_traffic is null and eu_traffic = true then 'EU'
           else '-' end as test_region
     ,case when prospect_traffic = true and lead_traffic = true and vip_traffic = true and guest_traffic = true and cancelled_traffic = true and (lower(m.TEST_LABEL) not ilike '%quiz%' or lower(m.TEST_LABEL) not ilike '%prospect%') and m.TEST_TYPE = 'session' then 'ALL'
           when (lower(m.TEST_LABEL) ilike '%quiz%' or lower(m.TEST_LABEL) ilike '%prospect%') and m.TEST_TYPE = 'session' then 'Prospects'
           when (prospect_traffic = true and lead_traffic is null and vip_traffic is null and guest_traffic is null and cancelled_traffic is null) and m.TEST_TYPE = 'session' then 'Prospects'
           when prospect_traffic is null and lead_traffic = true and vip_traffic is null and guest_traffic is null and cancelled_traffic is null then 'Leads'
           when prospect_traffic is null and lead_traffic is null and vip_traffic is null and guest_traffic is null and cancelled_traffic = true then 'Cancelled'
           when prospect_traffic is null and lead_traffic is null and vip_traffic is null and guest_traffic = true and cancelled_traffic is null then 'Guest'
           when prospect_traffic is null and lead_traffic is null and vip_traffic = true and guest_traffic is null and cancelled_traffic is null then 'VIPS'
           when prospect_traffic = true and lead_traffic = true and vip_traffic is null and guest_traffic is null and cancelled_traffic is null and m.TEST_TYPE = 'session' then 'Prospects & Leads'
           when prospect_traffic = true and lead_traffic is null and vip_traffic is null and guest_traffic = true and cancelled_traffic is null and m.TEST_TYPE = 'session' then 'Prospects & Guest'
           when prospect_traffic = true and lead_traffic is null and vip_traffic is null and guest_traffic is null and cancelled_traffic = true and m.TEST_TYPE = 'session' then 'Prospects & Cancelled'
           when (prospect_traffic = true and lead_traffic = true and vip_traffic is null and guest_traffic = true and cancelled_traffic = true) and (lower(m.TEST_LABEL) not ilike '%quiz%' or lower(m.TEST_LABEL) not ilike '%prospect%') and m.TEST_TYPE = 'session' then 'Prospects, Leads, Cancelled, Guest'
           when (prospect_traffic = true and lead_traffic = true and vip_traffic = true and guest_traffic is null and cancelled_traffic = true) and (lower(m.TEST_LABEL) not ilike '%quiz%' or lower(m.TEST_LABEL) not ilike '%prospect%') and m.TEST_TYPE = 'session' then 'Prospects, Leads, VIP, Cancelled'
           when prospect_traffic is null and lead_traffic = true and vip_traffic = true and guest_traffic is null and cancelled_traffic is null then 'Leads & VIPS'
           when prospect_traffic is null and lead_traffic = true and vip_traffic is null and guest_traffic is null and cancelled_traffic = true then 'Leads & Cancelled'
           when prospect_traffic is null and lead_traffic = true and vip_traffic is null and guest_traffic is null and cancelled_traffic = true then 'Leads & Guest'
           when prospect_traffic is null and lead_traffic = true and vip_traffic is null and guest_traffic = true and cancelled_traffic = true then 'Leads, Cancelled, Guest'
           when prospect_traffic is null and lead_traffic = true and vip_traffic = true and guest_traffic = true and cancelled_traffic = true then 'Leads, VIPS, Cancelled, Guest'
           when prospect_traffic is null and lead_traffic = true and vip_traffic = true and guest_traffic = true and cancelled_traffic is null then 'Leads, VIPS, Guest'
           when prospect_traffic is null and lead_traffic = true and vip_traffic = true and guest_traffic is null and cancelled_traffic = true then 'Leads, VIPS, Cancelled'
           when prospect_traffic is null and lead_traffic is null and vip_traffic = true and guest_traffic is null and cancelled_traffic = true then 'VIPS & Cancelled'
           when prospect_traffic is null and lead_traffic is null and vip_traffic = true and guest_traffic = true and cancelled_traffic is null then 'VIPS & Guest'
           when prospect_traffic is null and lead_traffic is null and vip_traffic = true and guest_traffic = true and cancelled_traffic = true then 'VIPS, Cancelled, Guest' else '-' end as member_segments
     ,CASE when lower(TEST_FRAMEWORK_TICKET) ilike 'http%' then split_part(TEST_FRAMEWORK_TICKET,'/',5)
           else TEST_FRAMEWORK_TICKET end AS TEST_FRAMEWORK_TICKET
     ,CASE WHEN IS_CMS_SPECIFIC_GENDER = 'f' and m.STORE_BRAND = 'Fabletics' then 'Women'
           WHEN IS_CMS_SPECIFIC_GENDER = 'm' and m.STORE_BRAND = 'Fabletics' then 'Men'
           when m.STORE_BRAND <> 'Fabletics' then '-' else 'ALL' end as test_gender
     ,case when abt.test_key is not null and abt.test_label is not null then TRUE else FALSE end as is_currently_avail_in_dashboard
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
from REPORTING_PROD.shared.AB_TEST_CMS_METADATA as m
left join _max_datetime_campaign_code as max on max.test_framework_id = m.TEST_FRAMEWORK_ID
    and max.TEST_KEY = m.TEST_KEY
    --and max.store_brand = m.STORE_BRAND
    and max.test_label = m.TEST_LABEL
join _max_datetimes as t on t.TEST_KEY = m.TEST_KEY
    and t.test_framework_id = m.test_framework_id
    and t.test_label = m.test_label
    --and t.STORE_BRAND = m.STORE_BRAND
left join _avail_in_abt_dash as abt on abt.test_key = m.TEST_KEY
        and abt.test_label = m.TEST_LABEL
left join (select distinct TEST_KEY,TRUE as prospect_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where TEST_START_MEMBERSHIP_STATE = 'Prospect') as p on p.test_key = m.test_key
left join (select distinct TEST_KEY,TRUE as lead_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where TEST_START_MEMBERSHIP_STATE = 'Lead') as l on l.test_key = m.test_key
left join (select distinct TEST_KEY,TRUE as vip_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where TEST_START_MEMBERSHIP_STATE = 'VIP') as v on v.test_key = m.test_key
left join (select distinct TEST_KEY,TRUE as cancelled_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where TEST_START_MEMBERSHIP_STATE = 'Cancelled') as c on c.test_key = m.test_key
left join (select distinct TEST_KEY,TRUE as guest_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where TEST_START_MEMBERSHIP_STATE = 'Guest') as g on g.test_key = m.test_key
left join (select distinct TEST_KEY,TRUE as na_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where STORE_REGION = 'NA') as na on na.test_key = m.test_key
left join (select distinct TEST_KEY,TRUE as eu_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where STORE_REGION = 'EU') as eu on eu.test_key = m.test_key
left join _custom_flag as cf on
    cf.test_key = m.test_key
    and cf.test_label = m.test_label
where
    m.statuscode = 113

union all

select
     case when BRAND = 'Yitty' then 'Fabletics'
            else BRAND end as store_group
     ,brand as STORE_BRAND
     ,m.TEST_LABEL
     ,m.TEST_KEY as CAMPAIGN_CODE
     ,m.TEST_KEY
     ,m.TEST_KEY AS test_key_version
     ,'00' as TEST_FRAMEWORK_ID
     ,name as TEST_FRAMEWORK_DESCRIPTION
     ,m.TEST_TYPE
     ,null as IS_CMS_SPECIFIC_LEAD_DAY
     ,null as IS_CMS_SPECIFIC_GENDER
     ,case when m.status = 'Running / In Progress' then TRUE else FALSE end as CMS_IS_ACTIVE_VERSION
     ,case when m.test_type = 'Sorting Hat, Persistent'
    and test_start_location in ('Add To Cart','Any','Post-Reg','Post-Reg / Boutique','Cart/Checkout','My Account','PDP','Shopping Grid','Other','Skip The Month') then concat(test_start_location,' Logged-In')
           when m.test_type = 'Sorting Hat, Non Persistent' and test_start_location in ('Add To Cart','Any Page','Cart/Checkout','PDP','Shopping Grid','Other','Any','Quiz Start','Quiz Completion','Other') then concat(test_start_location,' Logged-Out')
           when m.test_type = 'Sorting Hat, Persistent' and test_start_location is not null then concat(TEST_START_LOCATION,' Logged-In')
           when m.test_type = 'Sorting Hat, Persistent' and test_start_location is null then 'Unknown Logged-In'
           when m.test_type = 'Sorting Hat, Non Persistent' and test_start_location is not null then concat(TEST_START_LOCATION,' Logged-Out')
           when m.test_type = 'Sorting Hat, Non Persistent' and test_start_location is null then 'Unknown Logged-Out'
    end as test_start_location
     ,test_platforms as DEVICE
     ,null as DESKTOP_GROUP_TYPE
     ,case when test_split is null then '-' else test_split end as DESKTOP_TRAFFIC
     ,null as MOBILE_GROUP_TYPE
     ,case when test_split is null then '-' else test_split end as MOBILE_TRAFFIC
     ,null as MOBILEAPP_GROUP_TYPE
     ,case when test_split is null then '-' else test_split end as APP_TRAFFIC
     ,md.CMS_MIN_TEST_START_DATETIME_HQ
     ,md.max_ab_test_start_datetime
     ,min_campaign_code_datetime
     ,max_campaign_code_datetime
     ,datediff('day',md.cms_min_test_start_datetime_hq,md.max_ab_test_start_datetime) as days_running
     ,case when  st.label = 'Active' then 'Active'
           when st.label <> 'Active' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 = 1 then 'Inactive < 1M'
           when st.label <> 'Active' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 between 2 and 3 then 'Inactive 2-3M'
           when st.label <> 'Active' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 between 4 and 6 then 'Inactive 4-6M'
           when st.label <> 'Active' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 between 7 and 12 then 'Inactive 7-12M'
           when st.label <> 'Active' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 >= 13 then 'Inactive 13M+'
    end as test_status
     ,md.last_refreshed_hq_datetime
     ,flag_1_name as CMS_SESSION_FLAG_1
     ,flag_1_description as CMS_SESSION_FLAG_1_DESCRIPTION
     ,flag_2_name as CMS_SESSION_FLAG_2
     ,flag_2_description as CMS_SESSION_FLAG_2_DESCRIPTION
     ,flag_3_name as CMS_SESSION_FLAG_3
     ,flag_3_description as CMS_SESSION_FLAG_3_DESCRIPTION
     ,FLAG_4_NAME as CUSTOM_FLAG
     ,FLAG_4_DESCRIPTION  as CUSTOM_FLAG_DESCRIPTION

     ,case when m.region = 'Global' then 'NA+EU'
           when m.region = 'NA' then 'NA'
           when m.region = 'EU' then 'EU'
           else 'N/A' end as test_region
     ,case when m.membership_state = 'ALL' then 'ALL'
           when m.membership_state = 'Prospects' then 'Prospects'
           when m.membership_state = 'Leads' then 'Leads'
           when m.membership_state = 'VIPS' then 'VIPS'
           when m.membership_state = 'Prospects & Leads' then 'Prospects & Leads'
           when m.membership_state = 'Leads & VIPS' then 'Leads & VIPS'
           else 'N/A' end as member_segments
     ,m.ticket as TEST_FRAMEWORK_TICKET
     ,CASE WHEN m.gender = 'Women Only' and brand in ('FL','Fabletics') then 'Women'
           WHEN m.gender = 'Men Only' and brand in ('FL','Fabletics') then 'Men'
           when brand not in ('FL','Fabletics') then '-' else 'ALL' end as test_gender
     ,case when abt.test_key is not null and abt.test_label is not null then TRUE else FALSE end as is_currently_avail_in_dashboard
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
-- select prospect_traffic,lead_traffic,vip_traffic,cancelled_traffic,guest_traffic
from lake_view.sharepoint.ab_test_metadata_import_oof as m
join _max_datetime_campaign_code as max on max.TEST_KEY = m.TEST_KEY::varchar
    and max.test_label = m.TEST_LABEL
join _max_datetimes as md on md.TEST_KEY = m.TEST_KEY::varchar
    and md.test_label = m.TEST_LABEL
left join _avail_in_abt_dash as abt on abt.test_key = m.TEST_KEY::varchar
    and abt.test_label = m.TEST_LABEL
join  lake_consolidated_view.ultra_merchant.test_metadata AS tm on m.test_key = edw_prod.stg.udf_unconcat_brand(tm.TEST_METADATA_ID)
JOIN lake_consolidated.ultra_merchant.statuscode AS st ON st.statuscode = tm.statuscode
LEFT JOIN lake_consolidated.ultra_merchant.test_winner AS tw ON tw.test_metadata_id = tm.test_metadata_id
-- join _max_datetimes as t on t.TEST_KEY = m.TEST_KEY
left join (select distinct TEST_KEY,TRUE as prospect_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Sorting Hat' and TEST_START_MEMBERSHIP_STATE = 'Prospect') as p on p.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as lead_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Sorting Hat' and TEST_START_MEMBERSHIP_STATE = 'Lead') as l on l.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as vip_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Sorting Hat' and TEST_START_MEMBERSHIP_STATE = 'VIP') as v on v.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as cancelled_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Sorting Hat' and TEST_START_MEMBERSHIP_STATE = 'Cancelled') as c on c.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as guest_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Sorting Hat' and TEST_START_MEMBERSHIP_STATE = 'Guest') as g on g.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as na_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Sorting Hat' and STORE_REGION = 'NA') as na on na.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as eu_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Sorting Hat' and STORE_REGION = 'EU') as eu on eu.test_key = m.test_key::varchar
left join _custom_flag as cf on cf.TEST_KEY = m.TEST_KEY::varchar and cf.test_label = m.test_label
where m.test_type ilike 'Sorting Hat%'

union all

select
     case when BRAND = 'Yitty' then 'Fabletics'
            else BRAND end as store_group
    ,brand as STORE_BRAND
     ,m.TEST_LABEL
     ,m.TEST_KEY as CAMPAIGN_CODE
     ,m.TEST_KEY
     ,m.TEST_KEY AS test_key_version
     ,'00' as TEST_FRAMEWORK_ID
     ,m.TEST_LABEL as TEST_FRAMEWORK_DESCRIPTION
     ,m.TEST_TYPE
     ,null as IS_CMS_SPECIFIC_LEAD_DAY
     ,null as IS_CMS_SPECIFIC_GENDER
     ,case when m.status = 'Running / In Progress' then TRUE else FALSE end as CMS_IS_ACTIVE_VERSION
     ,case when m.test_type in ('Customer Detail','Membership Detail')
    and test_start_location in ('Add To Cart','Any','Post-Reg','Post-Reg / Boutique','Cart/Checkout','My Account','PDP','Shopping Grid','Other','Skip The Month') then concat(test_start_location,' Logged-In')
           when m.test_type = 'Session Detail' and test_start_location in ('Add To Cart','Any Page','Cart/Checkout','PDP','Shopping Grid','Other','Any','Quiz Start','Quiz Completion','Other') then concat(test_start_location,' Logged-Out')
           when m.test_type in ('Customer Detail','Membership Detail') and test_start_location is not null then concat(TEST_START_LOCATION,' Logged-In')
           when m.test_type in ('Customer Detail','Membership Detail') and test_start_location is null then 'Unknown Logged-In'
           when m.test_type = 'Session Detail' and test_start_location is not null then concat(TEST_START_LOCATION,' Logged-Out')
           when m.test_type = 'Session Detail' and test_start_location is null then 'Unknown Logged-Out'
    end as test_start_location
     ,test_platforms as DEVICE
     ,null as DESKTOP_GROUP_TYPE
     ,case when test_split is null then '-' else test_split end as DESKTOP_TRAFFIC
     ,null as MOBILE_GROUP_TYPE
     ,case when test_split is null then '-' else test_split end as MOBILE_TRAFFIC
     ,null as MOBILEAPP_GROUP_TYPE
     ,case when test_split is null then '-' else test_split end as APP_TRAFFIC
     ,cms_min_test_start_datetime_hq as CMS_MIN_TEST_START_DATETIME_HQ
     ,max_ab_test_start_datetime
     ,min_campaign_code_datetime
     ,max_campaign_code_datetime
     ,datediff('day',cms_min_test_start_datetime_hq,max_ab_test_start_datetime) as days_running
     ,case when  m.status = 'Running / In Progress' then 'Active'
           when m.status <>'Running / In Progress' and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 = 1 then 'Inactive < 1M'
           when m.status <>'Running / In Progress' and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 between 2 and 3 then 'Inactive 2-3M'
           when m.status <>'Running / In Progress' and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 between 4 and 6 then 'Inactive 4-6M'
           when m.status <>'Running / In Progress' and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 between 7 and 12 then 'Inactive 7-12M'
           when m.status <>'Running / In Progress' and datediff('month',max_ab_test_start_datetime,last_refreshed_hq_datetime) + 1 >= 13 then 'Inactive 13M+'
    end as test_status
     ,last_refreshed_hq_datetime
     ,flag_1_name as CMS_SESSION_FLAG_1
     ,flag_1_description as CMS_SESSION_FLAG_1_DESCRIPTION
     ,flag_2_name as CMS_SESSION_FLAG_2
     ,flag_2_description as CMS_SESSION_FLAG_2_DESCRIPTION
     ,flag_3_name as CMS_SESSION_FLAG_3
     ,flag_3_description as CMS_SESSION_FLAG_3_DESCRIPTION
     ,FLAG_4_NAME as CUSTOM_FLAG
     ,FLAG_4_DESCRIPTION  as CUSTOM_FLAG_DESCRIPTION
     ,case when m.region = 'Global' then 'NA+EU'
           when m.region = 'NA' then 'NA'
           when m.region = 'EU' then 'EU'
           else 'N/A' end as test_region
     ,case when m.membership_state = 'ALL' then 'ALL'
           when m.membership_state = 'Prospects' then 'Prospects'
           when m.membership_state = 'Leads' then 'Leads'
           when m.membership_state = 'VIPS' then 'VIPS'
           when m.membership_state = 'Prospects & Leads' then 'Prospects & Leads'
           when m.membership_state = 'Leads & VIPS' then 'Leads & VIPS'
           else 'N/A' end as member_segments
     ,m.ticket as TEST_FRAMEWORK_TICKET
     ,CASE WHEN m.gender = 'Women Only' and brand in ('FL','Fabletics') then 'Women'
           WHEN m.gender = 'Men Only' and brand in ('FL','Fabletics') then 'Men'
           when brand not in ('FL','Fabletics') then '-' else 'ALL' end as test_gender
     ,case when abt.test_key is not null and abt.test_label is not null then TRUE else FALSE end as is_currently_avail_in_dashboard
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
from lake_view.sharepoint.ab_test_metadata_import_oof as m
 join _max_datetime_campaign_code as max on max.TEST_KEY = m.TEST_KEY::varchar
 join _max_datetimes as md on md.TEST_KEY = m.TEST_KEY::varchar
    and md.test_label = m.TEST_LABEL
 left join _avail_in_abt_dash as abt on abt.test_key = m.TEST_KEY::varchar
    and abt.test_label = m.TEST_LABEL
left join (select distinct TEST_KEY,test_label,TRUE as prospect_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type ilike '%Detail' and TEST_START_MEMBERSHIP_STATE = 'Prospect') as p on p.test_key = m.test_key::varchar and p.test_label = m.test_label
left join (select distinct TEST_KEY,test_label,TRUE as lead_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type ilike '%Detail' and TEST_START_MEMBERSHIP_STATE = 'Lead') as l on l.test_key = m.test_key::varchar and l.test_label = m.test_label
left join (select distinct TEST_KEY,test_label,TRUE as vip_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type ilike '%Detail' and TEST_START_MEMBERSHIP_STATE = 'VIP') as v on v.test_key = m.test_key::varchar and v.test_label = m.test_label
left join (select distinct TEST_KEY,test_label,TRUE as cancelled_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type ilike '%Detail' and TEST_START_MEMBERSHIP_STATE = 'Cancelled') as c on c.test_key = m.test_key::varchar and c.test_label = m.test_label
left join (select distinct TEST_KEY,test_label,TRUE as guest_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type ilike '%Detail' and TEST_START_MEMBERSHIP_STATE = 'Guest') as g on g.test_key = m.test_key::varchar and g.test_label = m.test_label
left join (select distinct TEST_KEY,test_label,TRUE as na_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type ilike '%Detail' and STORE_REGION = 'NA') as na on na.test_key = m.test_key::varchar and m.test_label = na.test_label
left join (select distinct TEST_KEY,test_label,TRUE as eu_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type ilike '%Detail' and STORE_REGION = 'EU') as eu on eu.test_key = m.test_key::varchar and eu.test_label = m.test_label
left join _custom_flag as cf on cf.TEST_KEY = m.TEST_KEY::varchar and cf.test_label = m.test_label
where m.test_type ilike '%Detail'

union all

select
     case when BRAND = 'Yitty' then 'Fabletics'
            else BRAND end as store_group
    ,brand as STORE_BRAND
     ,m.variant_a_start as TEST_LABEL
     ,m.TEST_KEY as CAMPAIGN_CODE
     ,m.TEST_KEY
     ,m.TEST_KEY AS test_key_version
     ,'00' as TEST_FRAMEWORK_ID
     ,m.TEST_LABEL as TEST_FRAMEWORK_DESCRIPTION
     ,m.TEST_TYPE
     ,null IS_CMS_SPECIFIC_LEAD_DAY
     ,case when m.gender = 'Women Only' then 'f'
                when m.gender = 'Men Only' then 'm' else null end as IS_CMS_SPECIFIC_GENDER
     ,case when m.status = 'Running / In Progress' then TRUE else FALSE end as CMS_IS_ACTIVE_VERSION
     ,case when m.test_type = 'Sorting Hat, Persistent'
    and test_start_location in ('Add To Cart','Any','Post-Reg','Post-Reg / Boutique','Cart/Checkout','My Account','PDP','Shopping Grid','Other','Skip The Month') then concat(test_start_location,' Logged-In')
           when m.test_type = 'Builder' and test_start_location in ('Add To Cart','Any Page','Cart/Checkout','PDP','Shopping Grid','Other','Any','Quiz Start','Quiz Completion','Other') then concat(test_start_location,' Logged-Out')
           when m.test_type = 'Builder' and test_start_location is not null then concat(TEST_START_LOCATION,' Logged-In')
           when m.test_type = 'Builder' and test_start_location is null then 'Unknown Logged-In'
           when m.test_type = 'Builder' and test_start_location is not null then concat(TEST_START_LOCATION,' Logged-Out')
           when m.test_type = 'Builder' and test_start_location is null then 'Unknown Logged-Out'
    end as test_start_location
     ,test_platforms as DEVICE
     ,null as DESKTOP_GROUP_TYPE
     ,case when test_split is null then '-' else test_split end as DESKTOP_TRAFFIC
     ,null as MOBILE_GROUP_TYPE
     ,case when test_split is null then '-' else test_split end as MOBILE_TRAFFIC
     ,null as MOBILEAPP_GROUP_TYPE
     ,case when test_split is null then '-' else test_split end as APP_TRAFFIC
     ,md.CMS_MIN_TEST_START_DATETIME_HQ
     ,md.max_ab_test_start_datetime
     ,min_campaign_code_datetime
     ,max_campaign_code_datetime
     ,datediff('day',md.cms_min_test_start_datetime_hq,md.max_ab_test_start_datetime) as days_running
     ,case when  m.status = 'Running / In Progress' then 'Active'
           when m.status <> 'Running / In Progress' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 = 1 then 'Inactive < 1M'
           when m.status <> 'Running / In Progress' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 between 2 and 3 then 'Inactive 2-3M'
           when m.status <> 'Running / In Progress' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 between 4 and 6 then 'Inactive 4-6M'
           when m.status <> 'Running / In Progress' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 between 7 and 12 then 'Inactive 7-12M'
           when m.status <> 'Running / In Progress' and datediff('month',md.max_ab_test_start_datetime,md.last_refreshed_hq_datetime) + 1 >= 13 then 'Inactive 13M+'
    end as test_status
     ,md.last_refreshed_hq_datetime
     ,flag_1_name as CMS_SESSION_FLAG_1
     ,flag_1_description as CMS_SESSION_FLAG_1_DESCRIPTION
     ,flag_2_name as CMS_SESSION_FLAG_2
     ,flag_2_description as CMS_SESSION_FLAG_2_DESCRIPTION
     ,flag_3_name as CMS_SESSION_FLAG_3
     ,flag_3_description as CMS_SESSION_FLAG_3_DESCRIPTION
     ,FLAG_4_NAME as CUSTOM_FLAG
     ,FLAG_4_DESCRIPTION  as CUSTOM_FLAG_DESCRIPTION

     ,case when m.region = 'Global' then 'NA+EU'
           when m.region = 'NA' then 'NA'
           when m.region = 'EU' then 'EU'
           else 'N/A' end as test_region
     ,case when m.membership_state = 'ALL' then 'ALL'
           when m.membership_state = 'Prospects' then 'Prospects'
           when m.membership_state = 'Leads' then 'Leads'
           when m.membership_state = 'VIPS' then 'VIPS'
           when m.membership_state = 'Prospects & Leads' then 'Prospects & Leads'
           when m.membership_state = 'Leads & VIPS' then 'Leads & VIPS'
           else 'N/A' end as member_segments
     ,m.ticket as TEST_FRAMEWORK_TICKET
     ,CASE WHEN m.gender = 'Women Only' and brand in ('FL','Fabletics') then 'Women'
           WHEN m.gender = 'Men Only' and brand in ('FL','Fabletics') then 'Men'
           when brand not in ('FL','Fabletics') then '-' else 'ALL' end as test_gender
     ,case when abt.test_key is not null and abt.test_label is not null then TRUE else FALSE end as is_currently_avail_in_dashboard
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
-- select prospect_traffic,lead_traffic,vip_traffic,cancelled_traffic,guest_traffic
from lake_view.sharepoint.ab_test_metadata_import_oof as m
join _max_datetime_campaign_code as max on max.TEST_KEY = m.TEST_KEY::varchar
    and max.test_label = m.TEST_LABEL
join _max_datetimes as md on md.TEST_KEY = m.TEST_KEY::varchar
    and md.test_label = m.TEST_LABEL
left join _avail_in_abt_dash as abt on abt.test_key = m.TEST_KEY::varchar
    and abt.test_label = m.TEST_LABEL
left join (select distinct TEST_KEY,TRUE as prospect_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Builder' and TEST_START_MEMBERSHIP_STATE = 'Prospect') as p on p.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as lead_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Builder' and TEST_START_MEMBERSHIP_STATE = 'Lead') as l on l.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as vip_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Builder' and TEST_START_MEMBERSHIP_STATE = 'VIP') as v on v.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as cancelled_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Builder' and TEST_START_MEMBERSHIP_STATE = 'Cancelled') as c on c.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as guest_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Builder' and TEST_START_MEMBERSHIP_STATE = 'Guest') as g on g.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as na_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Builder' and STORE_REGION = 'NA') as na on na.test_key = m.test_key::varchar
left join (select distinct TEST_KEY,TRUE as eu_traffic
            from reporting_base_prod.shared.SESSION_AB_TEST_METADATA where test_type = 'Builder' and STORE_REGION = 'EU') as eu on eu.test_key = m.test_key::varchar
left join _custom_flag as cf on cf.TEST_KEY = m.TEST_KEY::varchar and cf.test_label = m.test_label
where m.test_type = 'Builder';


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.shared.ab_test_metadata_final as
select distinct
    dd.FULL_DATE
    ,case when max_test_date_rnk = 1 then test_key_version
        else '' end as test_key_version_rnk_label
    ,t.*
from edw_prod.DATA_MODEL.DIM_DATE as dd
         full join
     (select
          m.*
           ,AB_TEST_START_LOCAL_DATE as test_date
           ,test_key_rnk
           ,case when is_test_key_rnk = true then rank() over (partition by m.TEST_KEY,TEST_LABEL,STORE_BRAND,TEST_FRAMEWORK_ID,is_test_key_rnk ORDER BY AB_TEST_START_LOCAL_DATE ASC) end as max_test_date_rnk
      from _metadata as m
      join (select distinct
                            TEST_KEY
                           ,AB_TEST_START_LOCAL_DATE
                           ,rank() over (partition by test_key,TEST_LABEL,STORE_BRAND,TEST_FRAMEWORK_ID,CMS_MIN_TEST_START_DATETIME_HQ ORDER BY AB_TEST_START_LOCAL_DATE desc) as test_key_rnk
                           ,case when rank() over (partition by test_key,TEST_LABEL,STORE_BRAND,TEST_FRAMEWORK_ID,CMS_MIN_TEST_START_DATETIME_HQ ORDER BY AB_TEST_START_LOCAL_DATE desc) between 1 and 2 then TRUE else FALSE end as is_test_key_rnk
                     from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
                     join (select distinct AB_TEST_KEY,convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date as AB_TEST_START_LOCAL_DATE from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_START) as s on m.TEST_KEY = s.AB_TEST_KEY
                     where
                            m.statuscode = 113
                           and CMS_MIN_TEST_START_DATETIME_HQ >= '2022-01-01'
                       -- CAMPAIGN_CODE in ('PasswordlessRegFKKI3242') --'jfg2387'
                     order by 1,2
      ) as s on s.TEST_KEY = m.TEST_KEY
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,is_test_key_rnk,test_key_rnk
--       order by test_key_version,test_date
     )
         as t on t.test_date::date = dd.full_date
where
    dd.MONTH_DATE between '2022-01-01' and date_trunc('month',current_date)::date
    --and CAMPAIGN_CODE in ('PasswordlessRegFKKI3242','jfg2387')

union all

select distinct
    dd.FULL_DATE
              ,case when max_test_date_rnk = 1 then test_key_version
                    else '' end as test_key_version_rnk_label
              ,t.*
from edw_prod.DATA_MODEL.DIM_DATE as dd
         full join
     (select
          m.*
           ,AB_TEST_START_LOCAL_DATE as test_date
           ,test_key_rnk
           ,case when is_test_key_rnk = true then rank() over (partition by m.TEST_KEY,TEST_LABEL,STORE_BRAND,TEST_FRAMEWORK_ID,is_test_key_rnk ORDER BY AB_TEST_START_LOCAL_DATE ASC) end as max_test_date_rnk
      from _metadata as m
               join (select distinct
                         TEST_KEY
                           ,AB_TEST_START_LOCAL_DATETIME::date as AB_TEST_START_LOCAL_DATE
                           ,rank() over (partition by test_key,TEST_LABEL,STORE_BRAND,CMS_MIN_TEST_START_DATETIME_HQ ORDER BY AB_TEST_START_LOCAL_DATE desc) as test_key_rnk
                           ,case when rank() over (partition by test_key,TEST_LABEL,STORE_BRAND,CMS_MIN_TEST_START_DATETIME_HQ ORDER BY AB_TEST_START_LOCAL_DATE desc) between 1 and 2 then TRUE else FALSE end as is_test_key_rnk
                     from REPORTING_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
                              --join (select distinct AB_TEST_KEY,convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date as AB_TEST_START_LOCAL_DATE from REPORTING_BASE.SHARED.SESSION_AB_TEST_START) as s on m.TEST_KEY = s.AB_TEST_KEY
                     where
                       CMS_MIN_TEST_START_DATETIME_HQ >= '2022-01-01'
                       -- CAMPAIGN_CODE in ('PasswordlessRegFKKI3242') --'jfg2387'
                     order by 1,2
      ) as s on s.TEST_KEY = m.TEST_KEY
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,is_test_key_rnk,test_key_rnk
--       order by test_key_version,test_date
     )
         as t on t.test_date::date = dd.full_date
where
    dd.MONTH_DATE between '2022-01-01' and date_trunc('month',current_date)::date
    and TEST_TYPE ilike '%Detail' or lower(TEST_TYPE) ilike 'Sorting%' or TEST_TYPE = 'Builder'
order by
    test_key_version
    ,FULL_DATE;
