create or replace temp table _test_framework as
select TEST_FRAMEWORK_ID TEST_FRAMEWORK_ID
      ,coalesce(CAMPAIGN_CODE,(lag(CAMPAIGN_CODE) ignore nulls over(partition by test_framework_id order by datetime_modified))) CAMPAIGN_CODE
      ,coalesce(code,(lag(code) ignore nulls over(partition by test_framework_id order by datetime_modified))) AS code
      ,coalesce(STATUSCODE,(lag(STATUSCODE) ignore nulls over(partition by test_framework_id order by datetime_modified))) STATUSCODE
      ,coalesce(type,(lag(type) ignore nulls over(partition by test_framework_id order by datetime_modified))) type
      ,coalesce(DATETIME_ADDED,(lag(DATETIME_ADDED) ignore nulls over(partition by test_framework_id order by datetime_modified))) DATETIME_ADDED
      ,coalesce(DATETIME_MODIFIED,(lag(DATETIME_MODIFIED) ignore nulls over(partition by test_framework_id order by datetime_modified))) DATETIME_MODIFIED
      ,coalesce(DEVELOPER_DEFINED,(lag(DEVELOPER_DEFINED) ignore nulls over(partition by test_framework_id order by datetime_modified)))  as DEVELOPER_DEFINED
      ,coalesce(iFF(label='',null,label),(lag(IFF(label='',null,label)) ignore nulls over(partition by test_framework_id order by datetime_modified)))  as label
      ,coalesce(TEST_FRAMEWORK_DESCRIPTION,(lag(TEST_FRAMEWORK_DESCRIPTION) ignore nulls over(partition by test_framework_id order by datetime_modified))) TEST_FRAMEWORK_DESCRIPTION
      ,coalesce(START_DATE,(lag(START_DATE) ignore nulls over(partition by test_framework_id order by datetime_modified)))  as START_DATE
      ,coalesce(END_DATE,(lag(END_DATE) ignore nulls over(partition by test_framework_id order by datetime_modified)))  as END_DATE
      ,coalesce(MEMBERSHIP_LEVEL,(lag(MEMBERSHIP_LEVEL) ignore nulls over(partition by test_framework_id order by datetime_modified))) MEMBERSHIP_LEVEL
      ,coalesce(TEST_FRAMEWORK_XFA_ID,(lag(TEST_FRAMEWORK_XFA_ID) ignore nulls over(partition by test_framework_id order by datetime_modified))) TEST_FRAMEWORK_XFA_ID
      ,coalesce(TEST_FRAMEWORK_XFA_OTHER,(lag(TEST_FRAMEWORK_XFA_OTHER) ignore nulls over(partition by test_framework_id order by datetime_modified))) TEST_FRAMEWORK_XFA_OTHER
      ,coalesce(iff(store_group_id=0,null,store_group_id),(lag(iff(store_group_id=0,null,store_group_id)) ignore nulls over(partition by test_framework_id order by datetime_modified))) store_group_id
      ,coalesce(LOCALE,(lag(LOCALE) ignore nulls over(partition by test_framework_id order by datetime_modified))) LOCALE
      ,coalesce(LEAD_DAY,(lag(LEAD_DAY) ignore nulls over(partition by test_framework_id order by datetime_modified)))  as LEAD_DAY
      ,coalesce(TEST_FRAMEWORK_GROUP_ID,(lag(TEST_FRAMEWORK_GROUP_ID) ignore nulls over(partition by test_framework_id order by datetime_modified)))  as TEST_FRAMEWORK_GROUP_ID
      ,coalesce(TEST_FRAMEWORK_GROUP_ID_MOBILE,(lag(TEST_FRAMEWORK_GROUP_ID_MOBILE) ignore nulls over(partition by test_framework_id order by datetime_modified)))  as TEST_FRAMEWORK_GROUP_ID_MOBILE
      ,coalesce(TEST_FRAMEWORK_GROUP_ID_MOBILEAPP,(lag(TEST_FRAMEWORK_GROUP_ID_MOBILEAPP) ignore nulls over(partition by test_framework_id order by datetime_modified)))  as TEST_FRAMEWORK_GROUP_ID_MOBILEAPP
      ,coalesce(TEST_FRAMEWORK_DESKTOPGROUP_OTHER,(lag(TEST_FRAMEWORK_DESKTOPGROUP_OTHER) ignore nulls over(partition by test_framework_id order by datetime_modified))) TEST_FRAMEWORK_DESKTOPGROUP_OTHER
      ,coalesce(TEST_FRAMEWORK_MOBILEGROUP_OTHER,(lag(TEST_FRAMEWORK_MOBILEGROUP_OTHER) ignore nulls over(partition by test_framework_id order by datetime_modified))) TEST_FRAMEWORK_MOBILEGROUP_OTHER
      ,coalesce(TEST_FRAMEWORK_MOBILEAPPGROUP_OTHER,(lag(TEST_FRAMEWORK_MOBILEAPPGROUP_OTHER) ignore nulls over(partition by test_framework_id order by datetime_modified))) TEST_FRAMEWORK_MOBILEAPPGROUP_OTHER
      ,coalesce(CODE_MOBILE,(lag(CODE_MOBILE) ignore nulls over(partition by test_framework_id order by datetime_modified))) CODE_MOBILE
      ,coalesce(CODE_MOBILEAPP,(lag(CODE_MOBILEAPP) ignore nulls over(partition by test_framework_id order by datetime_modified))) CODE_MOBILEAPP
      ,coalesce(TEST_FRAMEWORK_TICKET_URL,(lag(TEST_FRAMEWORK_TICKET_URL) ignore nulls over(partition by test_framework_id order by datetime_modified))) TEST_FRAMEWORK_TICKET_URL
from LAKE_consolidated.ULTRA_CMS_history.test_framework;

CREATE OR REPLACE TEMPORARY TABLE _filter_out as
select distinct code as test_key
from _test_framework
WHERE
    CAMPAIGN_CODE in ('USFndTest','dani1','dani2','test12','FNDSTKTEST001s','dani4','dani3','STKTESTFNDRR','Placeholder','FNDSTKTEST001','testmytest','maabtestpilot1','CopySETSTKSRR468GroupV23','SAVAGEXEMP'
    ,'FNDSTKTEST01','FNDSTKTEST001updated','FNDSTKTEST001w','FNDSTKTEST001','maabtestroundtwo','frameworktestPDP','nothing2','maabtestpilot','SVTESTABTEST001','FNDSTKTEST001w','SETGGKTESTSRR1252Prod1','maabtestpilot2', 'FNDSTK'
    ,'yittyQuizABTestDataTest','fableticsSWQuizTestDataTest_v0','pymtoptions1','FNDRR-3231','nRRTest20220701','SETGGKTESTFNDRR1879Test','SETGGKTESTFNDRR1879Testd','CopySETSTKSRR468GroupV2','CopySETSTKSRR468GroupV2_v1,'
    ,'sdfasdfadsf','Test1','Testframeworkaudit','Test2021','cooltestwow','VJTest001','oRRTest202207','jfg2385','unique','T2175a','p3s546d5rfytui','test6121')
           or CAMPAIGN_CODE ilike 'SETGGKTEST%' or lower(CAMPAIGN_CODE) ilike 'fnd%' or lower(CAMPAIGN_CODE) ilike '%%devtest%%' or lower(CAMPAIGN_CODE) ilike '%branchtest%' or lower(CAMPAIGN_CODE) ilike '%do not turn off%'
           or (coalesce(code,CODE_MOBILE,CODE_MOBILEAPP) not ilike '%_v%') or lower(CAMPAIGN_CODE) ilike '%test2023%'
           or code in ('PasswordlessRegFKKI3242_v0','PasswordlessRegFKKI3242_v1','PasswordlessRegFKKI3242_v2','PasswordlessRegFKKI3242_v4','PasswordlessRegFKKI3242_v8','PasswordlessRegFKKI3242_v9','jfg1281_v1','sdki1757_v5'
                ,'jfg2397_v0','jfg2387_v0','jfg1281_v1','jfg1281_v2','jfg1281_v3','jfg1281_v4','jfg1281_v5','jfg1677_v5','jfg842_v3','jfg842_v2','jfg842_v3','jfg842_v4','SDRR5520_v0','SDRR5520_v6','SDRR5520_v7','Test202301112_v0'
                ,'Test202301112_v0','Test202301111_v0','remix1019_v0','remix1019_v1','SAVEOFFERSEGMENTATIONLITE_v0','SAVEOFFERSEGMENTATIONLITE_v1','SAVEOFFERSEGMENTATIONLITE_v2','SAVEOFFERSEGMENTATIONLITE_v3','SAVEOFFERSEGMENTATIONLITE_v4'
                ,'SAVEOFFERSEGMENTATIONLITE_v5')
            or test_framework_id in (119330,126720,129010);

CREATE OR REPLACE TEMPORARY TABLE _keys_unfiltered AS
SELECT DISTINCT
    a.TEST_FRAMEWORK_ID
      ,STORE_BRAND
      ,a.CAMPAIGN_CODE
      ,a.CODE AS test_key
      ,a.STATUSCODE
      ,s.LABEL as status
      ,case when f.code is not null and f.statuscode = 113 then TRUE
            when f.code is not null and f.statuscode = 113 and (a.DEVELOPER_DEFINED <> 'preview_only' or f.DEVELOPER_DEFINED <> 'preview_only') then FALSE
            else FALSE end as cms_is_active_version
--       ,ss.STATUSCODE as current_statuscode
--       ,ss.LABEL as current_status
      ,a.type

      ,a.DATETIME_ADDED
      ,a.DATETIME_MODIFIED

      ,a.DEVELOPER_DEFINED as archive_dev_defined
      ,f.DEVELOPER_DEFINED as current_dev_defined

      ,a.label as test_label
      ,a.TEST_FRAMEWORK_DESCRIPTION
      ,CASE when lower(a.TEST_FRAMEWORK_TICKET_URL) ilike 'http%' then split_part(a.TEST_FRAMEWORK_TICKET_URL,'/',5)
            else a.TEST_FRAMEWORK_TICKET_URL end AS TEST_FRAMEWORK_TICKET
      ,a.START_DATE as cms_start_date
      ,a.END_DATE as cms_end_date
      ,a.MEMBERSHIP_LEVEL

      ,concat(x.label,' (CF)') as test_location_cf
      ,concat(a.TEST_FRAMEWORK_XFA_OTHER,' (CF)') as test_location_other_cf
      ,concat(test_start_location_desktop,' (DA)') as test_start_location_desktop
      ,concat(test_start_location_mobile,' (DA)') as test_start_location_mobile
      ,concat(test_start_location_app,' (DA)') as test_start_location_app

      ,a.STORE_GROUP_ID
      ,a.LOCALE
      ,a.LEAD_DAY as is_cms_specific_lead_day
      ,gender as is_cms_specific_gender

      ,case when a.TEST_FRAMEWORK_GROUP_ID not in (19,20) then TRUE
            when a.TEST_FRAMEWORK_GROUP_ID is null and a.TEST_FRAMEWORK_DESKTOPGROUP_OTHER is not null and a.TEST_FRAMEWORK_DESKTOPGROUP_OTHER not in ('0/0/100','0/100/0','100/0/0','1/98/1','0/100','1/99','100/0','98/1/1','1/1/98','1/1/1/97','1/97/1/1','1/1/97/1','97/1/1/1') then TRUE
            else FALSE end as is_valid_desktop_group

      ,case when a.TEST_FRAMEWORK_GROUP_ID_MOBILE not in (24,25) then TRUE
            when a.TEST_FRAMEWORK_GROUP_ID_MOBILE is null and a.TEST_FRAMEWORK_MOBILEGROUP_OTHER is not null and a.TEST_FRAMEWORK_MOBILEGROUP_OTHER not in ('0/0/100','0/100/0','100/0/0','1/98/1','0/100','1/99','100/0','98/1/1','1/1/98','1/1/1/97','1/97/1/1','1/1/97/1','97/1/1/1') then TRUE
            else FALSE end as is_valid_mobile_group

      ,case when a.TEST_FRAMEWORK_GROUP_ID_MOBILEAPP not in (150,151) then TRUE
            when a.TEST_FRAMEWORK_GROUP_ID_MOBILEAPP is null and a.TEST_FRAMEWORK_MOBILEAPPGROUP_OTHER is not null and a.TEST_FRAMEWORK_MOBILEAPPGROUP_OTHER not in ('0/0/100','0/100/0','100/0/0','1/98/1','0/100','1/99','100/0','98/1/1','1/1/98','1/1/1/97','1/97/1/1','1/1/97/1','97/1/1/1') then TRUE
            else FALSE end as is_valid_mobileapp_group

      ,a.TEST_FRAMEWORK_GROUP_ID as desktop_group_id
      ,a.TEST_FRAMEWORK_GROUP_ID_MOBILE as mobile_group_id
      ,a.TEST_FRAMEWORK_GROUP_ID_MOBILEAPP as mobileapp_group_id

      ,coalesce(a.TEST_FRAMEWORK_DESKTOPGROUP_OTHER,g1.group_name) as desktop_group
      ,coalesce(a.TEST_FRAMEWORK_MOBILEGROUP_OTHER,g2.group_name) as mobile_group
      ,coalesce(a.TEST_FRAMEWORK_MOBILEAPPGROUP_OTHER,g3.group_name)  as mobileapp_group

from _test_framework as a --historical
join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.STATUSCODE as s on s.STATUSCODE = a.STATUSCODE
left join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK as f on f.code = a.code --active
left join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.STATUSCODE as ss on ss.STATUSCODE = f.STATUSCODE
left join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_GROUP as g1 on g1.TEST_FRAMEWORK_GROUP_ID = a.TEST_FRAMEWORK_GROUP_ID --desktop
    and g1.DEVICE_TYPE = 'Desktop'
left join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_GROUP as g2 on g2.TEST_FRAMEWORK_GROUP_ID = a.TEST_FRAMEWORK_GROUP_ID_MOBILE --mobile
    and g2.DEVICE_TYPE = 'Mobile'
left join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_GROUP as g3 on g3.TEST_FRAMEWORK_GROUP_ID = a.TEST_FRAMEWORK_GROUP_ID_MOBILEAPP --mobile app
    and g3.DEVICE_TYPE = 'MobileApp'
left join (select distinct TEST_FRAMEWORK_ID,value as gender, max(DATETIME_ADDED) as max_value from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA -- to get gender-specific field in cms
            where object = 'test_framework_gender_flag' group by 1,2) as d on d.TEST_FRAMEWORK_ID = a.TEST_FRAMEWORK_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_XFA as x on edw_prod.stg.udf_unconcat_brand(x.TEST_FRAMEWORK_XFA_ID) = a.TEST_FRAMEWORK_XFA_ID

left join (select distinct TEST_FRAMEWORK_ID,value as test_start_location_desktop, max(DATETIME_ADDED) as max_value,rank() over (partition by TEST_FRAMEWORK_ID ORDER BY DATETIME_ADDED desc) as rnk from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA -- test start location DT
            where object = 'test_start_location_desktop' group by 1,2,DATETIME_ADDED) as l1 on l1.TEST_FRAMEWORK_ID = a.TEST_FRAMEWORK_ID and l1.rnk = 1
left join (select distinct TEST_FRAMEWORK_ID,value as test_start_location_mobile, max(DATETIME_ADDED) as max_value,rank() over (partition by TEST_FRAMEWORK_ID ORDER BY DATETIME_ADDED desc) as rnk from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA -- test start location Mobile
                    where object = 'test_start_location_mobile' group by 1,2,DATETIME_ADDED) as l2 on l2.TEST_FRAMEWORK_ID = a.TEST_FRAMEWORK_ID and l2.rnk = 1
left join (select distinct TEST_FRAMEWORK_ID,value as test_start_location_app, max(DATETIME_ADDED) as max_value,rank() over (partition by TEST_FRAMEWORK_ID ORDER BY DATETIME_ADDED desc) as rnk from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA -- test start location App
                    where object = 'test_start_location_mobile_app' group by 1,2,DATETIME_ADDED) as l3 on l3.TEST_FRAMEWORK_ID = a.TEST_FRAMEWORK_ID and l3.rnk = 1
join (select distinct STORE_GROUP_ID,STORE_BRAND from edw_prod.DATA_MODEL.DIM_STORE) as st on st.STORE_GROUP_ID = a.STORE_GROUP_ID
WHERE
    a.CAMPAIGN_CODE is not null
    and a.code not in (select distinct _filter_out.test_key from _filter_out);

delete from _keys_unfiltered
where
    DATETIME_MODIFIED < '2022-01-01' and test_key is null;

CREATE OR REPLACE TEMPORARY TABLE _keys_filtered as
select rank() over (partition by test_key,STATUSCODE ORDER BY DATETIME_MODIFIED desc) as rnk, *
from _keys_unfiltered;

delete from _keys_filtered
where
    rnk > 1
   or (STORE_BRAND = 'Fabletics' and lower(CAMPAIGN_CODE) ilike 'yitty%')
   or (STORE_BRAND = 'Yitty' and lower(CAMPAIGN_CODE) not ilike 'yitty%' and DATETIME_MODIFIED < '2022-05-01')
   or (STORE_BRAND = 'Yitty' and lower(CAMPAIGN_CODE) not ilike 'yitty%' and LOCALE in ('gb,de,fr','gb,de,ca,fr','ca','de','de,fr','de,fr,nl,dk,se','dk','es','europe','gb','gb,de','gb,de,fr','gb,de,fr,es','gb,de,fr,es,nl,dk,se','gb,es','gb,fr,es','gb,fr,es,nl,dk,se'));

CREATE OR REPLACE TEMPORARY TABLE _keys as
select distinct
    TEST_FRAMEWORK_ID
    ,STORE_BRAND
    ,CAMPAIGN_CODE
    ,test_key
    ,STATUSCODE
    ,STATUS
    --      ,current_statuscode
    --     ,current_status
    ,case
      when STATUSCODE = 113 and current_dev_defined = 'preview_only' or archive_dev_defined = 'preview_only' then FALSE
      else cms_is_active_version end as cms_is_active_version
    ,type
    ,DATETIME_MODIFIED
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CMS_START_DATE
    ,CMS_END_DATE
    ,MEMBERSHIP_LEVEL
    ,test_location_cf
    ,test_location_other_cf
    ,test_start_location_desktop
    ,test_start_location_mobile
    ,test_start_location_app
    ,CONCAT(coalesce(test_location_cf,test_location_other_cf), ' | ',coalesce(test_start_location_desktop,test_start_location_mobile,test_start_location_app))  as test_start_location
    ,STORE_GROUP_ID
    ,LOCALE
    ,is_cms_specific_lead_day
    ,is_cms_specific_gender
    ,case when desktop_group is not null and mobile_group is null and mobileapp_group is null then 'Desktop'
       when mobile_group is not null and desktop_group is null and mobileapp_group is null then 'Mobile'
       when mobileapp_group is not null and desktop_group is null and mobile_group is null then 'App'
       when desktop_group is not null and mobile_group is not null and mobileapp_group is null then 'Desktop & Mobile'
       when desktop_group is not null and mobileapp_group is not null and mobile_group is null then 'Desktop & App'
       when mobile_group is not null and mobileapp_group is not null and desktop_group is null then 'Mobile & App'
       when mobile_group is not null and mobileapp_group is not null and desktop_group is not null then 'Desktop, Mobile & App'
       else null end as cms_device_meta
    ,case when is_valid_desktop_group = TRUE and desktop_group_id is null and desktop_group = '20/80' then 'Standard'
       when desktop_group_id is not null  then 'Standard'
       when is_valid_desktop_group = TRUE and desktop_group_id is null and desktop_group is not null then 'Custom'
       else null end as desktop_group_type
    ,case when is_valid_desktop_group = TRUE and desktop_group = '20/80' then 12
       else desktop_group_id end as desktop_group_id
    ,desktop_group
    ,is_valid_desktop_group
    ,case when is_valid_mobile_group = TRUE and mobile_group_id is null and mobile_group = '20/80' then 'Standard'
       when mobile_group_id is not null then 'Standard'
       when is_valid_mobile_group = TRUE and mobile_group_id is null and mobile_group is not null then 'Custom'
       else null end as mobile_group_type
    ,case when is_valid_mobile_group = TRUE and mobile_group = '20/80' then 16
       else mobile_group_id end as mobile_group_id
    ,mobile_group
    ,is_valid_mobile_group
    ,case when is_valid_mobileapp_group = TRUE and mobileapp_group_id is null and mobileapp_group = '20/80' then 'Standard'
       when mobileapp_group_id is not null then 'Standard'
       when is_valid_mobileapp_group = TRUE and mobileapp_group_id is null and mobile_group is not null then 'Custom'
       else null end as mobileapp_group_type
    ,case when is_valid_mobileapp_group = TRUE and mobileapp_group = '20/80' then 147
       else mobileapp_group_id end as mobileapp_group_id
    ,mobileapp_group
    ,is_valid_mobileapp_group
    ,archive_dev_defined
    ,current_dev_defined
from _keys_filtered;

CREATE OR REPLACE TEMPORARY TABLE _session_flag_1 as
select distinct
    t.TEST_FRAMEWORK_ID
    ,t.test_key
    ,test_label
    ,OBJECT
    ,value as session_flag
    ,1 as session_flag_nm
    ,comments as session_flag_description
    ,rank() over (partition by test_key ORDER BY a.DATETIME_ADDED asc) as rnk
from _keys_filtered as t
         join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA as a on t.test_framework_id = a.TEST_FRAMEWORK_ID
where
        OBJECT = 'session_flag_1'
  and value <> 'session_flag_1';

CREATE OR REPLACE TEMPORARY TABLE _session_flag_2 as
select distinct
    t.TEST_FRAMEWORK_ID
    ,t.test_key
    ,test_label
    ,OBJECT
    ,value as session_flag
    ,1 as session_flag_nm
    ,comments as session_flag_description
    ,rank() over (partition by test_key ORDER BY a.DATETIME_ADDED asc) as rnk
from _keys_filtered as t
join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA as a on t.test_framework_id = a.TEST_FRAMEWORK_ID
where
    OBJECT = 'session_flag_2'
    and value <> 'session_flag_2';

CREATE OR REPLACE TEMPORARY TABLE _session_flag_3 as
select distinct
    t.TEST_FRAMEWORK_ID
    ,t.test_key
    ,test_label
    ,OBJECT
    ,value as session_flag
    ,1 as session_flag_nm
    ,comments as session_flag_description
    ,rank() over (partition by test_key ORDER BY a.DATETIME_ADDED asc) as rnk
from _keys_filtered as t
join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA as a on t.test_framework_id = a.TEST_FRAMEWORK_ID
where
    OBJECT = 'session_flag_3'
    and value <> 'session_flag_3';

CREATE OR REPLACE TEMPORARY TABLE _true_min_test_start_datetimes as --filters out preview only and getting correct test start datetimes when tests are in preview mode
select distinct TEST_KEY,test_label,min(convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME))::datetime as min_AB_TEST_START_HQ_DATETIME
from _keys as k
join REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_START as s on s.AB_TEST_KEY = k.test_key
group by 1,2;

CREATE OR REPLACE TEMPORARY TABLE _last_recent_status as
select TEST_FRAMEWORK_ID,CAMPAIGN_CODE,test_key,test_label,store_brand,STATUSCODE,STATUS,DATETIME_MODIFIED,rank() over (partition by CAMPAIGN_CODE,test_label ORDER BY DATETIME_MODIFIED desc) as rnk
from _keys order by 3,6 asc;

CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as
select
    k.TEST_FRAMEWORK_ID
     ,case when k.STORE_BRAND = 'Yitty' then 'Fabletics'
            else k.STORE_BRAND end as store_group
     ,k.STORE_BRAND
     ,k.test_label as TEST_LABEL
     ,k.CAMPAIGN_CODE
     ,CMS_IS_ACTIVE_VERSION
     ,l.status as current_status
     ,l.STATUSCODE as current_statuscode
     ,k.TEST_KEY
     ,split_part(k.test_key,'_',2) AS version
     ,k.STATUS
     ,k.STATUSCODE

     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,type as TEST_TYPE
     ,IS_CMS_SPECIFIC_LEAD_DAY
     ,IS_CMS_SPECIFIC_GENDER
--      ,case when k.STATUSCODE = 113 and archive_dev_defined is not null or current_dev_defined is not null then min_AB_TEST_START_HQ_DATETIME else k.DATETIME_MODIFIED end as CMS_MIN_TEST_START_DATETIME_HQ
     ,case when k.STATUSCODE = 113 and tt.TEST_KEY is not null then min_AB_TEST_START_HQ_DATETIME
            else k.DATETIME_MODIFIED end as CMS_MIN_TEST_START_DATETIME_HQ
     ,k.DATETIME_MODIFIED
     ,CMS_START_DATE
     ,CMS_END_DATE
     ,MEMBERSHIP_LEVEL

     ,test_location_cf
     ,test_location_other_cf
     ,test_start_location_desktop
     ,test_start_location_mobile
     ,test_start_location_app
     ,test_start_location

     ,STORE_GROUP_ID
     ,LOCALE
     ,CMS_DEVICE_META AS DEVICE
     ,DESKTOP_GROUP_TYPE
     ,DESKTOP_GROUP
     ,DESKTOP_GROUP_ID
     ,is_valid_desktop_group
     ,MOBILE_GROUP_TYPE
     ,MOBILE_GROUP
     ,MOBILE_GROUP_ID
     ,is_valid_mobile_group
     ,MOBILEAPP_GROUP_TYPE
     ,MOBILEAPP_GROUP
     ,MOBILEAPP_GROUP_ID
     ,is_valid_mobileapp_group
     ,f1.session_flag as CMS_SESSION_FLAG_1
     ,f1.session_flag_description as CMS_SESSION_FLAG_1_DESCRIPTION
     ,f2.session_flag as CMS_SESSION_FLAG_2
     ,f2.session_flag_description as CMS_SESSION_FLAG_2_DESCRIPTION
     ,f3.session_flag as CMS_SESSION_FLAG_3
     ,f3.session_flag_description as CMS_SESSION_FLAG_3_DESCRIPTION
    ,current_dev_defined
    ,archive_dev_defined
from _keys as k
left join _true_min_test_start_datetimes as tt on tt.TEST_KEY = k.test_key
    and tt.test_label = k.test_label
    and k.STATUSCODE = 113
left join _last_recent_status as l on l.TEST_FRAMEWORK_ID = k.TEST_FRAMEWORK_ID
    and l.test_label = k.test_label
    and l.store_brand = k.store_brand
    and l.CAMPAIGN_CODE = k.CAMPAIGN_CODE
    and rnk  = 1
left join (select distinct TEST_FRAMEWORK_ID,test_key,test_label,session_flag,session_flag_description from _session_flag_1 where rnk = 1) as f1 on k.test_key = f1.test_key and k.test_label = f1.test_label
left join (select distinct TEST_FRAMEWORK_ID,test_key,test_label,session_flag,session_flag_description from _session_flag_2 where rnk = 1) as f2 on k.test_key = f2.test_key and k.test_label = f2.test_label
left join (select distinct TEST_FRAMEWORK_ID,test_key,test_label,session_flag,session_flag_description from _session_flag_3 where rnk = 1) as f3 on k.test_key = f3.test_key and k.test_label = f3.test_label
where k.test_key is not null;

update REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA
    set CMS_SESSION_FLAG_1 = null
        ,CMS_SESSION_FLAG_1_DESCRIPTION = null
        ,CMS_SESSION_FLAG_2 = null
        ,CMS_SESSION_FLAG_2_DESCRIPTION = null
        ,CMS_SESSION_FLAG_3 = null
        ,CMS_SESSION_FLAG_3_DESCRIPTION = null
    where TEST_FRAMEWORK_ID = 1177;

delete from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA
where
    STORE_BRAND = 'Yitty'
    and CAMPAIGN_CODE in ('echoca','echofres','USPDPDUALCTA');

update REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA
set CMS_MIN_TEST_START_DATETIME_HQ = '2022-12-01 15:34:47.313'
where TEST_KEY = 'jfg842_v5';

update REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA
set CMS_MIN_TEST_START_DATETIME_HQ = '2022-11-16 10:58:07.220'
where TEST_KEY = 'ShorterDisclosureFKKI3443_v2';

update REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA
set IS_CMS_SPECIFIC_LEAD_DAY = null
where TEST_FRAMEWORK_ID in (128410,128510,128610);
