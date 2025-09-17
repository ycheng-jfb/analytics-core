set last_refreshed_datetime_hq = (select max(HQ_MAX_DATETIME) from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_METADATA);

CREATE OR REPLACE TEMPORARY TABLE _custom_flag as
select distinct
    TEST_KEY
    ,test_label
    ,FLAG_4_NAME
    ,FLAG_4_DESCRIPTION
from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_FLAGS where FLAG_4_NAME is not null;

CREATE OR REPLACE TEMPORARY TABLE _test_framework_current as
select distinct
    TEST_FRAMEWORK_ID
      ,CAMPAIGN_CODE
      ,f.STATUSCODE as current_statuscode_campaign_code
      ,c.label as current_status_campaign_code
      ,DATETIME_ADDED as cms_datetime_added
      ,case when TEST_FRAMEWORK_GROUP_ID in (19,20) or TEST_FRAMEWORK_DESKTOPGROUP_OTHER in ('1/1/98','1/98/1','98/1/1','1/99','99/1')  then TRUE else FALSE end as is_currently_fake_inactive_campaign_code_desktop
      ,case when TEST_FRAMEWORK_GROUP_ID_MOBILE in (24,25) or TEST_FRAMEWORK_MOBILEGROUP_OTHER in ('1/1/98','1/98/1','98/1/1','1/99','99/1') then TRUE else FALSE  end as is_currently_fake_inactive_campaign_code_mobile
      ,case when TEST_FRAMEWORK_GROUP_ID_MOBILEAPP in (150,151) or TEST_FRAMEWORK_MOBILEAPPGROUP_OTHER in ('1/1/98','1/98/1','98/1/1','1/99','99/1')  then TRUE else FALSE end as is_currently_fake_inactive_campaign_code_app
--     ,case when developer_defined in ('preview_only','preview_excluded') then TRUE else FALSE end as is_preview_only
from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK as f
         join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.STATUSCODE as c on c.STATUSCODE = f.STATUSCODE
where
    DATETIME_ADDED BETWEEN CURRENT_DATE - INTERVAL '1 YEAR' AND CURRENT_DATE + INTERVAL '1 DAY'
  and CAMPAIGN_CODE is not null;

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

CREATE OR REPLACE TEMPORARY TABLE _metadata_drafts AS
select
    k.CAMPAIGN_CODE
     ,cms_datetime_added
     ,k.STORE_BRAND
     ,k.STORE_GROUP_ID
     ,t.STORE_GROUP
     ,test_framework_ticket
     ,k.TEST_FRAMEWORK_ID
     ,k.TEST_KEY
     ,k.TEST_LABEL
     ,k.TEST_FRAMEWORK_DESCRIPTION
     ,CMS_IS_ACTIVE_VERSION
--      ,is_preview_only
     ,is_currently_fake_inactive_campaign_code_desktop
     ,is_currently_fake_inactive_campaign_code_mobile
     ,is_currently_fake_inactive_campaign_code_app
     ,current_status_campaign_code
     ,current_statuscode_campaign_code
     ,status --delete
     ,status as status_test_key
     ,k.test_key as to_test_key
     ,version
     ,status as to_status
     ,k.STATUSCODE as to_statuscode
     ,LAG(k.test_key) OVER(PARTITION BY k.campaign_code ORDER BY k.DATETIME_MODIFIED asc) as from_test_key
     ,LAG(status) OVER(PARTITION BY k.campaign_code ORDER BY k.DATETIME_MODIFIED asc) as from_status
     ,LAG(k.STATUSCODE) OVER(PARTITION BY k.campaign_code ORDER BY k.DATETIME_MODIFIED asc) as from_statuscode
     ,TEST_TYPE
     ,IS_CMS_SPECIFIC_LEAD_DAY
     ,IS_CMS_SPECIFIC_GENDER
     ,datetime_modified
     ,CMS_START_DATE
     ,CMS_END_DATE
     ,test_location_cf
     ,test_location_other_cf
     ,test_start_location_desktop
     ,test_start_location_mobile
     ,test_start_location_app
     ,TEST_START_LOCATION
     ,k.locale
     ,DEVICE
     ,DESKTOP_GROUP_TYPE
     ,DESKTOP_GROUP
     ,DESKTOP_GROUP_ID
     ,IS_VALID_DESKTOP_GROUP
     ,MOBILE_GROUP_TYPE
     ,MOBILE_GROUP
     ,MOBILE_GROUP_ID
     ,IS_VALID_MOBILE_GROUP
     ,MOBILEAPP_GROUP_TYPE
     ,MOBILEAPP_GROUP
     ,MOBILEAPP_GROUP_ID
     ,IS_VALID_MOBILEAPP_GROUP
     ,k.MEMBERSHIP_LEVEL
     ,case when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100' then 'Logged-In Leads'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,200' then 'Logged-In Leads & Guests'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,200,300' then 'Logged-In Leads, Guests & Cancelled'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,300' then 'Logged-In Leads & Cancelled'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,500' then 'Logged-In Leads & VIPS'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,500,1000' then 'Logged-In Leads & VIPS'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,500,1000,200' then 'Logged-In Leads, VIPS & Guests'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,500,1000,200,300' then 'All Logged-In'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,500,200' then 'Logged-In Leads, VIPS & Guests'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,500,200,300' then 'All Logged-In'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,500,400,1000,200,300' then 'All Logged-In'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '200' then 'Logged-In Guests'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '300' then 'Logged-In Cancelled VIPS'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '500' then 'Logged-In VIPS'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '500,1000' then 'Logged-In VIPS'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '500,1000,200,300' then 'Logged-In VIPS, Guests & Cancelled'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '500,400,1000' then 'Logged-In VIPS'
           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '500,400,1000,200,300' then 'Logged-In VIPS, Guests & Cancelled'

           when TEST_TYPE = 'membership' and k.MEMBERSHIP_LEVEL = '100,500,400,1000,200,300' and TEST_TYPE = 'session' then 'All Logged-Out'
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL is null then 'Prospects'
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL = '100' then 'Logged-Out Leads'
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL = '100,200,300' then 'Logged-Out Leads, Guests & Cancelled'
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL = '100,500,1000,200,300' then 'All Logged-Out '
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL = '100,500' then 'Logged-Out Leads & VIPS'
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL = '100,500,1000' then 'Logged-Out Leads & VIPS'
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL = '100,500,400,1000,200,300' then 'All Logged-Out'
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL = '200' then 'Logged-Out Guests'
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL = '500' then 'Logged-Out VIPS'
           when TEST_TYPE = 'session' and k.MEMBERSHIP_LEVEL = '500,1000' then 'Logged-Out VIPS'
           else 'Unknown'
    end as member_segments
     ,CMS_SESSION_FLAG_1
     ,CMS_SESSION_FLAG_1_DESCRIPTION
     ,CMS_SESSION_FLAG_2
     ,CMS_SESSION_FLAG_2_DESCRIPTION
     ,CMS_SESSION_FLAG_3
     ,CMS_SESSION_FLAG_3_DESCRIPTION
     ,FLAG_4_NAME as CUSTOM_FLAG
     ,FLAG_4_DESCRIPTION  as CUSTOM_FLAG_DESCRIPTION
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
from  reporting_prod.shared.AB_TEST_CMS_METADATA as k
join _test_framework_current as c on c.TEST_FRAMEWORK_ID = k.TEST_FRAMEWORK_ID
left join _custom_flag as cf on cf.test_key = k.test_key
        and cf.test_label = k.test_label
join edw_prod.DATA_MODEL.dim_store as t on t.store_group_id = k.store_group_id
ORDER BY datetime_modified;

CREATE OR REPLACE TEMPORARY TABLE _scaffold as
select distinct
    TEST_FRAMEWORK_ID
    ,CAMPAIGN_CODE
    ,TEST_KEY
    ,statuscode --delete
    ,statuscode  as statuscode_test_key
    ,status --delete
    ,status  as status_test_key
    ,STORE_BRAND
    ,'' || LISTAGG(distinct STORE_REGION, '|') WITHIN GROUP (ORDER BY STORE_REGION ASC) || '' AS store_region
    ,'' || LISTAGG(distinct STORE_COUNTRY, '|') WITHIN GROUP (ORDER BY STORE_COUNTRY ASC) || '' AS store_country
    ,'' || LISTAGG(distinct CMS_MEMBERSHIP_STATE, '|') WITHIN GROUP (ORDER BY CMS_MEMBERSHIP_STATE ASC) || '' AS CMS_MEMBERSHIP_STATE
from reporting_prod.shared.AB_TEST_CMS_METADATA_SCAFFOLD
group by 1,2,3,4,5,6,7,8;

CREATE OR REPLACE TEMPORARY TABLE _metadata_status as
select distinct
    m.TEST_FRAMEWORK_ID
    ,cms_datetime_added
    ,case when m.STORE_BRAND = 'Yitty' then 'Fabletics'
            else m.STORE_BRAND end as store_group
    ,m.STORE_BRAND
    ,m.TEST_LABEL
    ,m.TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET AS ticket
    ,m.CMS_IS_ACTIVE_VERSION as is_current_active_version
    ,CMS_START_DATE
    ,CMS_END_DATE
    ,m.CAMPAIGN_CODE
    --,current_status --delete
    ,current_status_campaign_code
    --,current_statuscode --delete
    --       ,is_preview_only
    ,current_statuscode_campaign_code
    ,is_currently_fake_inactive_campaign_code_desktop
    ,is_currently_fake_inactive_campaign_code_mobile
    ,is_currently_fake_inactive_campaign_code_app
    ,m.to_status --delete
    ,m.to_status as current_status_test_key
    ,m.to_statuscode as current_statuscode_test_key
    ,m.to_test_key
    ,COALESCE(split_part(to_test_key,'_',2),'-') AS to_version
    ,from_status
    ,from_test_key
    ,COALESCE(split_part(from_test_key,'_',2),'-') AS from_version
    ,datetime_modified AS status_update_datetime_pst
    ,m.TEST_TYPE
    ,test_location_cf
    ,test_location_other_cf
    ,test_start_location_desktop
    ,test_start_location_mobile
    ,test_start_location_app
    ,test_start_location
    ,IS_CMS_SPECIFIC_GENDER
    ,IS_CMS_SPECIFIC_LEAD_DAY
    ,TRAFFIC_SPLIT_TYPE
    ,GROUP_NAME
    ,TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,MOBILE_GROUP
    ,DESKTOP_GROUP
    ,MOBILEAPP_GROUP
    ,CMS_SESSION_FLAG_1
    ,CMS_SESSION_FLAG_1_DESCRIPTION
    ,CMS_SESSION_FLAG_2
    ,CMS_SESSION_FLAG_2_DESCRIPTION
    ,CMS_SESSION_FLAG_3
    ,CMS_SESSION_FLAG_3_DESCRIPTION
    ,CUSTOM_FLAG
    ,CUSTOM_FLAG_DESCRIPTION
    ,'' || LISTAGG(distinct STORE_REGION, '|') WITHIN GROUP (ORDER BY STORE_REGION DESC) || '' AS store_region
    ,'' || LISTAGG(distinct STORE_COUNTRY, '|') WITHIN GROUP (ORDER BY STORE_COUNTRY DESC) || '' AS store_country
    ,'' || LISTAGG(distinct DEVICE_TYPE, '|') WITHIN GROUP (ORDER BY DEVICE_TYPE DESC) || '' AS DEVICE_TYPE
    ,'' || LISTAGG(distinct CMS_MEMBERSHIP_STATE, '|') WITHIN GROUP (ORDER BY CMS_MEMBERSHIP_STATE DESC) || '' AS MEMBERSHIP_STATE
    ,rank() over (partition by m.CAMPAIGN_CODE,m.STORE_BRAND,current_status_campaign_code ORDER BY m.datetime_modified desc) as rnk_by_campaign_code
    ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
from _metadata_drafts as m
left join REPORTING_PROD.SHARED.AB_TEST_CMS_GROUPS as g on g.TEST_KEY = m.TEST_KEY
    and g.STORE_BRAND = m.STORE_BRAND
    and g.statuscode = m.to_statuscode
left join _scaffold as s on s.TEST_KEY = m.TEST_KEY
    and s.STORE_BRAND = m.STORE_BRAND
    and s.statuscode_test_key = m.to_statuscode
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,last_refreshed_datetime_hq

union all
--NON CMS
select distinct
    9999 TEST_FRAMEWORK_ID
    ,cms_min_test_start_datetime_hq cms_datetime_added
    ,case when brand = 'Yitty' then 'Fabletics' else brand end as store_group
    ,brand as STORE_BRAND
    ,m.TEST_LABEL
    ,m.TEST_LABEL as TEST_FRAMEWORK_DESCRIPTION
    ,ticket
    ,case when status = 'Running / In Progress' then TRUE else FALSE end as is_current_active_version
    ,adjusted_activated_datetime as CMS_START_DATE
    ,adjusted_end_datetime as CMS_END_DATE
    ,m.test_key as CAMPAIGN_CODE
    ,case when status = 'Running / In Progress' then 'Active'
        when status in ('In Dev') then 'Draft'
        when status = 'Closed / Paused' then 'Deactivated' end current_status_campaign_code
    ,null as current_statuscode_campaign_code
    ,null as is_currently_fake_inactive_campaign_code_desktop
    ,null as is_currently_fake_inactive_campaign_code_mobile
    ,null as is_currently_fake_inactive_campaign_code_app
    ,current_status_campaign_code as to_status
    ,current_status_campaign_code as current_status_test_key
    ,null as current_statuscode_test_key
    ,m.test_key as to_test_key
    ,m.test_key as  to_version
    ,null as  from_status
    ,null as  from_test_key
    ,null as   from_version
    ,null as   status_update_datetime_pst
    ,m.TEST_TYPE
    ,null as  test_location_cf
    ,null as  test_location_other_cf
    ,null as  test_start_location_desktop
    ,null as  test_start_location_mobile
    ,null as  test_start_location_app
    ,test_start_location
    ,null as  IS_CMS_SPECIFIC_GENDER
    ,null as  IS_CMS_SPECIFIC_LEAD_DAY
    ,null as  TRAFFIC_SPLIT_TYPE
    ,test_split as GROUP_NAME
    ,null as  TEST_GROUP
    ,null as  TEST_GROUP_DESCRIPTION
    ,null as  MOBILE_GROUP
    ,null as  DESKTOP_GROUP
    ,null as  MOBILEAPP_GROUP
    ,FLAG_1_NAME as CMS_SESSION_FLAG_1
    ,FLAG_1_DESCRIPTION AS CMS_SESSION_FLAG_1_DESCRIPTION
    ,FLAG_2_NAME AS CMS_SESSION_FLAG_2
    ,FLAG_2_DESCRIPTION AS CMS_SESSION_FLAG_2_DESCRIPTION
    ,FLAG_3_NAME AS CMS_SESSION_FLAG_3
    ,FLAG_3_DESCRIPTION AS CMS_SESSION_FLAG_3_DESCRIPTION
    ,FLAG_4_NAME as custom_flag
    ,FLAG_4_DESCRIPTION as custom_flag_description
    ,REGION AS store_region
    ,REGION AS store_country
    ,TEST_PLATFORMS AS DEVICE_TYPE
    ,MEMBERSHIP_STATE AS MEMBERSHIP_STATE
    ,1 as rnk_by_campaign_code
    ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
from lake_view.sharepoint.ab_test_metadata_import_oof as m
left join _custom_flag cf on m.TEST_KEY = cf.TEST_KEY
    and m.test_label = cf.test_label
join _max_datetimes as max on max.TEST_KEY = m.TEST_KEY
    and max.test_label = m.TEST_LABEL;

CREATE OR REPLACE TEMPORARY TABLE _current_campaign_code_preview_only_status as
select TEST_FRAMEWORK_ID
from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK
where STATUSCODE = 113 and lower(DEVELOPER_DEFINED) = 'preview_only';


--
CREATE OR REPLACE TRANSIENT TABLE reporting_prod.shared.AB_TEST_CMS_METADATA_DRAFTS as
select distinct --current_status_campaign_code,
    m.*
    ,case
       when current_status_campaign_code = 'Active' and s.TEST_FRAMEWORK_ID is null
           and (is_currently_fake_inactive_campaign_code_desktop = false and is_currently_fake_inactive_campaign_code_mobile = false and is_currently_fake_inactive_campaign_code_app = false)
           or (current_status_campaign_code = 'Active' and test_type not in ('membership','session'))
           then 'Active'
       when current_status_campaign_code = 'Active'
           and (is_currently_fake_inactive_campaign_code_desktop = true or is_currently_fake_inactive_campaign_code_mobile = true or is_currently_fake_inactive_campaign_code_app = true)
           then 'Active (Pending Close)'

       when current_status_campaign_code in ('Approved','Pending Approval')
           or (current_status_campaign_code = 'Active' and s.TEST_FRAMEWORK_ID is not null)
               and (is_currently_fake_inactive_campaign_code_desktop = false and is_currently_fake_inactive_campaign_code_mobile = false and is_currently_fake_inactive_campaign_code_app = false)

           then 'Approved / Pending Approval / Preview'
       when current_status_campaign_code = 'Draft' then 'Draft'
       when current_status_campaign_code = 'Deleted' then 'Deleted'
       when current_status_campaign_code = 'Deactivated' then 'Deactivated'
       else 'unknown' end as last_recent_status --by version
    ,DATEDIFF('MONTH',status_update_datetime_pst,CURRENT_DATE) AS last_recent_status_update_month
from _metadata_status as m
         left join _current_campaign_code_preview_only_status as s on s.TEST_FRAMEWORK_ID = m.TEST_FRAMEWORK_ID
where current_status_campaign_code is not null;


-- select CAMPAIGN_CODE,current_status_campaign_code
-- from _metadata_status
-- where TEST_TYPE = 'Builder'
