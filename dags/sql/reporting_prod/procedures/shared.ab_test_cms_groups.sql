
CREATE OR REPLACE TEMPORARY TABLE _standard_groups_scaffold as
select
    STORE_BRAND
     ,TEST_FRAMEWORK_ID
     ,CAMPAIGN_CODE
     ,TEST_KEY
     ,test_label
     ,cms_min_test_start_datetime_hq
     ,status
     ,statuscode
     ,DEVICE_TYPE
     ,'Standard' as TRAFFIC_SPLIT_TYPE
     ,TRAFFIC_SPLIT_CUSTOM as GROUP_NAME
     ,case when test_group_num = 1 then 'Control'
           when test_group_num = 2 and num_splits = 2 then 'Variant'
           when test_group_num = 2 and num_splits > 2 then 'Variant A'
           when test_group_num = 3 and num_splits in (3,4)  then 'Variant B'
           when test_group_num = 4 and num_splits = 4 then 'Variant C'
    end as test_group
FROM (
         SELECT distinct
             STORE_BRAND
               ,CAMPAIGN_CODE
               ,TEST_KEY
                ,test_label
               ,cms_min_test_start_datetime_hq
               ,status
               ,statuscode
               ,l.TEST_FRAMEWORK_ID
               ,DESKTOP_GROUP as TRAFFIC_SPLIT_CUSTOM
               ,split_list
               ,num_splits
               ,f.index + 1 AS test_group_num
               ,f.value::INT AS bin_size
               ,device_type
         FROM (
                  SELECT distinct
                      STORE_BRAND
                        ,t.TEST_FRAMEWORK_ID
                        ,CAMPAIGN_CODE
                        ,t.TEST_KEY
                        ,test_label
                        ,cms_min_test_start_datetime_hq
                        ,status
                        ,statuscode
                        ,'Desktop' as device_type
                        ,DESKTOP_GROUP
                        ,split(DESKTOP_GROUP, '/') AS split_list
                        ,array_size(split_list) AS num_splits
                  from reporting_prod.shared.AB_TEST_CMS_METADATA as t
                  where
                          DESKTOP_GROUP_TYPE = 'Standard'
              ) l
                  JOIN TABLE (flatten(INPUT => split_list)) f
     ) a

union all

select
    STORE_BRAND
     ,TEST_FRAMEWORK_ID
     ,CAMPAIGN_CODE
     ,TEST_KEY
     ,test_label
     ,cms_min_test_start_datetime_hq
     ,status
     ,statuscode
     ,DEVICE_TYPE
     ,'Standard' as TRAFFIC_SPLIT_TYPE
     ,TRAFFIC_SPLIT_CUSTOM as GROUP_NAME
     ,case when test_group_num = 1 then 'Control'
           when test_group_num = 2 and num_splits = 2 then 'Variant'
           when test_group_num = 2 and num_splits > 2 then 'Variant A'
           when test_group_num = 3 and num_splits in (3,4)  then 'Variant B'
           when test_group_num = 4 and num_splits = 4 then 'Variant C'
    end as test_group
FROM (
         SELECT distinct
             STORE_BRAND
               ,CAMPAIGN_CODE
               ,TEST_KEY
               ,test_label
               ,cms_min_test_start_datetime_hq
               ,status
               ,statuscode
               ,l.TEST_FRAMEWORK_ID
               ,MOBILE_GROUP as TRAFFIC_SPLIT_CUSTOM
               ,split_list
               ,num_splits
               ,f.index + 1 AS test_group_num
               ,f.value::INT AS bin_size
               ,device_type
         FROM (
                  SELECT distinct
                      STORE_BRAND
                        ,t.TEST_FRAMEWORK_ID
                        ,CAMPAIGN_CODE
                        ,t.TEST_KEY
                        ,test_label
                        ,cms_min_test_start_datetime_hq
                        ,status
                        ,statuscode
                        ,'Mobile' as device_type
                        ,MOBILE_GROUP
                        ,split(MOBILE_GROUP, '/') AS split_list
                        ,array_size(split_list) AS num_splits
                  from reporting_prod.shared.AB_TEST_CMS_METADATA as t
                  where
                          MOBILE_GROUP_TYPE = 'Standard'
              ) l
                  JOIN TABLE (flatten(INPUT => split_list)) f
     ) a

union all

select
    STORE_BRAND
     ,TEST_FRAMEWORK_ID
     ,CAMPAIGN_CODE
     ,TEST_KEY
     ,test_label
     ,cms_min_test_start_datetime_hq
     ,status
     ,statuscode
     ,DEVICE_TYPE
     ,'Standard' as TRAFFIC_SPLIT_TYPE
     ,TRAFFIC_SPLIT_CUSTOM as GROUP_NAME
     ,case when test_group_num = 1 then 'Control'
           when test_group_num = 2 and num_splits = 2 then 'Variant'
           when test_group_num = 2 and num_splits > 2 then 'Variant A'
           when test_group_num = 3 and num_splits in (3,4)  then 'Variant B'
           when test_group_num = 4 and num_splits = 4 then 'Variant C'
    end as test_group
FROM (
         SELECT distinct
             STORE_BRAND
               ,CAMPAIGN_CODE
               ,TEST_KEY
               ,test_label
               ,cms_min_test_start_datetime_hq
               ,status
               ,statuscode
               ,l.TEST_FRAMEWORK_ID
               ,MOBILEAPP_GROUP as TRAFFIC_SPLIT_CUSTOM
               ,split_list
               ,num_splits
               ,f.index + 1 AS test_group_num
               ,f.value::INT AS bin_size
               ,device_type
         FROM (
                  SELECT distinct
                      STORE_BRAND
                        ,t.TEST_FRAMEWORK_ID
                        ,CAMPAIGN_CODE
                        ,t.TEST_KEY
                        ,test_label
                        ,cms_min_test_start_datetime_hq
                        ,status
                        ,statuscode
                        ,'Mobile App' as device_type
                        ,MOBILEAPP_GROUP
                        ,split(MOBILEAPP_GROUP, '/') AS split_list
                        ,array_size(split_list) AS num_splits
                  from reporting_prod.shared.AB_TEST_CMS_METADATA as t
                  where
                          MOBILEAPP_GROUP_TYPE = 'Standard'
              ) l
                  JOIN TABLE (flatten(INPUT => split_list)) f
     ) a
;

CREATE OR REPLACE TEMPORARY TABLE _standard_groups as
select distinct --desktop groups
    s.STORE_BRAND
     ,s.TRAFFIC_SPLIT_TYPE as DESKTOP_GROUP_TYPE
     ,s.CMS_MIN_TEST_START_DATETIME_HQ
     ,s.TEST_FRAMEWORK_ID
     ,s.CAMPAIGN_CODE
     ,s.TEST_KEY
     ,s.test_label
     ,s.status
     ,s.statuscode
     ,'Desktop' as device_type
     ,s.TRAFFIC_SPLIT_TYPE
     ,s.GROUP_NAME
     ,s.TEST_GROUP
     ,coalesce(t.test_group_description,m.test_group_description) as test_group_description
     ,coalesce(t.TEST_VALUE_START,m.TEST_VALUE_START) as TEST_VALUE_START
     ,coalesce(t.TEST_VALUE_END,m.TEST_VALUE_END) as TEST_VALUE_END
     ,TRUE as is_correct_group_description
     ,1 as rnk
from _standard_groups_scaffold as s
         left join (select distinct
                        STORE_BRAND
                          ,k.DESKTOP_GROUP_TYPE
                          ,CMS_MIN_TEST_START_DATETIME_HQ
                          ,k.TEST_FRAMEWORK_ID
                          ,k.CAMPAIGN_CODE
                          ,TEST_KEY
                          ,test_label
                          ,status
                          ,statuscode
                          ,'Desktop' as device_type
                          ,TRAFFIC_SPLIT_TYPE
                          ,k.DESKTOP_GROUP as GROUP_NAME
                          ,map.TEST_GROUP
                          ,lower(comments) as test_group_description
                          ,TEST_VALUE_START
                          ,TEST_VALUE_END
                          ,case when CMS_MIN_TEST_START_DATETIME_HQ between min_datetime_added - interval '1 day' and max_datetime_added + interval '1 day' then TRUE else FALSE end as is_correct_group_description
                          ,rank() over (partition by test_key,TEST_GROUP,STATUSCODE ORDER BY min_datetime_added desc) as rnk
                    from reporting_prod.shared.AB_TEST_CMS_METADATA as k
                             left join (select distinct test_framework_id,value,comments,min(DATETIME_ADDED) as min_datetime_added,coalesce(max(DATETIME_ADDED),'9999-12-31') as max_datetime_added from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
                                        where object = 'test_framework_group' and value not ilike 'mobile%'
                                        group by 1,2,3) as f on f.TEST_FRAMEWORK_ID = k.TEST_FRAMEWORK_ID
                             left join reporting_prod.SHARED.AB_TEST_GROUP_MAPPING_REFERENCE as map on map.TRAFFIC_SPLIT = k.DESKTOP_GROUP
                        and map.PLATFORM = 'Desktop' and f.value = map.TEST_FRAMEWORK_GROUP -- and (f.value = map.TEST_FRAMEWORK_GROUP or f.value in ('control','variant_1','variant'))
                    where DESKTOP_GROUP_TYPE = 'Standard' or TEST_GROUP is not null
                        ) as t
                   on t.TEST_KEY = s.TEST_KEY
                        and t.TEST_LABEL = s.TEST_LABEL
                       and s.STORE_BRAND = t.STORE_BRAND
                       and s.status = t.status
                       and s.test_group = t.test_group
                       and rnk = 1  and t.TEST_GROUP is not null and is_correct_group_description = TRUE  --where s.test_framework_id = 1150 and s.test_key = 'PasswordlessRegFKKI3242_v2'
         left join ( select distinct
                         STORE_BRAND
                           ,k.DESKTOP_GROUP_TYPE
                           ,CMS_MIN_TEST_START_DATETIME_HQ
                           ,k.TEST_FRAMEWORK_ID
                           ,k.CAMPAIGN_CODE
                           ,TEST_KEY
                           ,test_label
                           ,status
                           ,statuscode
                           ,'Desktop' as device_type
                           ,TRAFFIC_SPLIT_TYPE
                           ,k.DESKTOP_GROUP as GROUP_NAME
                           ,map.TEST_GROUP
                           ,lower(comments) as test_group_description
                           ,TEST_VALUE_START
                           ,TEST_VALUE_END
                           ,case when CMS_MIN_TEST_START_DATETIME_HQ between min_datetime_added - interval '1 day' and max_datetime_added + interval '1 day' then TRUE else FALSE end as is_correct_group_description
                           ,rank() over (partition by test_key,TEST_GROUP,STATUSCODE ORDER BY min_datetime_added desc) as rnk
         from reporting_prod.shared.AB_TEST_CMS_METADATA as k
                              left join (select distinct test_framework_id,value,comments,min(DATETIME_ADDED) as min_datetime_added,coalesce(max(DATETIME_ADDED),'9999-12-31') as max_datetime_added from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
                                         where object = 'test_framework_group' and value not ilike 'mobile%'
                                         group by 1,2,3) as f on f.TEST_FRAMEWORK_ID = k.TEST_FRAMEWORK_ID
                              left join reporting_prod.SHARED.AB_TEST_GROUP_MAPPING_REFERENCE as map on map.TRAFFIC_SPLIT = k.DESKTOP_GROUP
                         and map.PLATFORM = 'Desktop' and f.value = map.TEST_FRAMEWORK_GROUP -- and (f.value = map.TEST_FRAMEWORK_GROUP or f.value in ('control','variant_1','variant'))
                     where
                             DESKTOP_GROUP_TYPE = 'Standard' --and k.test_framework_id = 1150 and k.test_key = 'PasswordlessRegFKKI3242_v2'
) as m
                   on m.TEST_KEY = s.TEST_KEY
                       and m.TEST_LABEL = s.TEST_LABEL
                       and s.STORE_BRAND = m.STORE_BRAND
                       and s.status = m.status
                       and s.test_group = m.test_group
                       and m.is_correct_group_description = false
                       and m.rnk = 1
where s.DEVICE_TYPE = 'Desktop' --and s.TEST_KEY = 'sd2191_v5' and s.status = 'Active'

union all

select distinct--mobile groups
    s.STORE_BRAND
     ,s.TRAFFIC_SPLIT_TYPE as MOBILE_GROUP_TYPE
     ,s.CMS_MIN_TEST_START_DATETIME_HQ
     ,s.TEST_FRAMEWORK_ID
     ,s.CAMPAIGN_CODE
     ,s.TEST_KEY
     ,s.test_label
     ,s.status
     ,s.statuscode
     ,'Mobile' as device_type
     ,s.TRAFFIC_SPLIT_TYPE
     ,s.GROUP_NAME
     ,s.TEST_GROUP
     ,coalesce(t.test_group_description,m.test_group_description) as test_group_description
     ,coalesce(t.TEST_VALUE_START,m.TEST_VALUE_START) as TEST_VALUE_START
     ,coalesce(t.TEST_VALUE_END,m.TEST_VALUE_END) as TEST_VALUE_END
     ,TRUE as is_correct_group_description
     ,1 as rnk
from _standard_groups_scaffold as s
         left join (select distinct
                        STORE_BRAND
                          ,k.MOBILE_GROUP_TYPE
                          ,CMS_MIN_TEST_START_DATETIME_HQ
                          ,k.TEST_FRAMEWORK_ID
                          ,k.CAMPAIGN_CODE
                          ,TEST_KEY
                          ,test_label
                          ,status
                          ,statuscode
                          ,'Mobile' as device_type
                          ,TRAFFIC_SPLIT_TYPE
                          ,k.MOBILE_GROUP as GROUP_NAME
                          ,map.TEST_GROUP
                          ,lower(comments) as test_group_description
                          ,TEST_VALUE_START
                          ,TEST_VALUE_END
                          ,case when CMS_MIN_TEST_START_DATETIME_HQ between min_datetime_added - interval '1 day' and max_datetime_added + interval '1 day' then TRUE else FALSE end as is_correct_group_description
                          ,rank() over (partition by test_key,TEST_GROUP,STATUSCODE ORDER BY min_datetime_added desc) as rnk
                    from reporting_prod.shared.AB_TEST_CMS_METADATA as k
                             left join (select distinct test_framework_id,value,comments,min(DATETIME_ADDED) as min_datetime_added,coalesce(max(DATETIME_ADDED),'9999-12-31') as max_datetime_added from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
                                        where object = 'test_framework_group' and value ilike 'mobile%' and not contains(value,'app')
                                        group by 1,2,3) as f on f.TEST_FRAMEWORK_ID = k.TEST_FRAMEWORK_ID
                             left join reporting_prod.SHARED.AB_TEST_GROUP_MAPPING_REFERENCE as map on map.TRAFFIC_SPLIT = k.MOBILE_GROUP
                        and map.PLATFORM = 'Mobile' and f.value = map.TEST_FRAMEWORK_GROUP
                    where MOBILE_GROUP_TYPE = 'Standard' or TEST_GROUP is not null) as t
                   on t.TEST_KEY = s.TEST_KEY
                       and t.TEST_LABEL = s.TEST_LABEL
                       and s.STORE_BRAND = t.STORE_BRAND
                       and s.status = t.status
                       and s.test_group = t.test_group
                       and rnk = 1  and is_correct_group_description = TRUE and t.TEST_GROUP is not null
         left join ( select distinct
                         STORE_BRAND
                           ,k.MOBILE_GROUP_TYPE
                           ,CMS_MIN_TEST_START_DATETIME_HQ
                           ,k.TEST_FRAMEWORK_ID
                           ,k.CAMPAIGN_CODE
                           ,TEST_KEY
                           ,test_label
                           ,status
                           ,statuscode
                           ,'Mobile' as device_type
                           ,TRAFFIC_SPLIT_TYPE
                           ,k.MOBILE_GROUP as GROUP_NAME
                           ,map.TEST_GROUP
                           ,lower(comments) as test_group_description
                           ,TEST_VALUE_START
                           ,TEST_VALUE_END
                           ,case when CMS_MIN_TEST_START_DATETIME_HQ between min_datetime_added - interval '1 day' and max_datetime_added + interval '1 day' then TRUE else FALSE end as is_correct_group_description
                           ,rank() over (partition by test_key,TEST_GROUP,STATUSCODE ORDER BY min_datetime_added desc) as rnk
                     from reporting_prod.shared.AB_TEST_CMS_METADATA as k
                              left join (select distinct test_framework_id,value,comments,min(DATETIME_ADDED) as min_datetime_added,coalesce(max(DATETIME_ADDED),'9999-12-31') as max_datetime_added from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
                                         where object = 'test_framework_group' and value ilike 'mobile%' and not contains(value,'app')
                                         group by 1,2,3) as f on f.TEST_FRAMEWORK_ID = k.TEST_FRAMEWORK_ID
                              left join reporting_prod.SHARED.AB_TEST_GROUP_MAPPING_REFERENCE as map on map.TRAFFIC_SPLIT = k.MOBILE_GROUP
                         and map.PLATFORM = 'Mobile' and f.value = map.TEST_FRAMEWORK_GROUP
                     where
                             MOBILE_GROUP_TYPE = 'Standard'
) as m
                   on m.TEST_KEY = s.TEST_KEY
                       and m.TEST_LABEL = s.TEST_LABEL
                       and s.STORE_BRAND = m.STORE_BRAND
                       and s.status = m.status
                       and s.test_group = m.test_group
                       and m.is_correct_group_description = false
                       and m.rnk = 1
where s.DEVICE_TYPE = 'Mobile'

union all

select distinct--mobile app groups
    s.STORE_BRAND
     ,s.TRAFFIC_SPLIT_TYPE as MOBILEAPP_GROUP_TYPE
     ,s.CMS_MIN_TEST_START_DATETIME_HQ
     ,s.TEST_FRAMEWORK_ID
     ,s.CAMPAIGN_CODE
     ,s.TEST_KEY
     ,s.test_label
     ,s.status
     ,s.statuscode
     ,'App' as device_type
     ,s.TRAFFIC_SPLIT_TYPE
     ,s.GROUP_NAME
     ,s.TEST_GROUP
     ,coalesce(t.test_group_description,m.test_group_description) as test_group_description
     ,coalesce(t.TEST_VALUE_START,m.TEST_VALUE_START) as TEST_VALUE_START
     ,coalesce(t.TEST_VALUE_END,m.TEST_VALUE_END) as TEST_VALUE_END
     ,TRUE as is_correct_group_description
     ,1 as rnk
from _standard_groups_scaffold as s
         left join (select distinct
                        STORE_BRAND
                          ,k.MOBILEAPP_GROUP_TYPE
                          ,CMS_MIN_TEST_START_DATETIME_HQ
                          ,k.TEST_FRAMEWORK_ID
                          ,k.CAMPAIGN_CODE
                          ,TEST_KEY
                          ,test_label
                          ,status
                          ,statuscode
                          ,'Mobile App' as device_type
                          ,TRAFFIC_SPLIT_TYPE
                          ,k.MOBILEAPP_GROUP as GROUP_NAME
                          ,map.TEST_GROUP
                          ,lower(comments) as test_group_description
                          ,TEST_VALUE_START
                          ,TEST_VALUE_END
                          ,case when CMS_MIN_TEST_START_DATETIME_HQ between min_datetime_added - interval '1 day' and max_datetime_added + interval '1 day' then TRUE else FALSE end as is_correct_group_description
                          ,rank() over (partition by test_key,TEST_GROUP,STATUSCODE ORDER BY min_datetime_added desc) as rnk
                    from reporting_prod.shared.AB_TEST_CMS_METADATA as k
                             left join (select distinct test_framework_id,value,comments,min(DATETIME_ADDED) as min_datetime_added,coalesce(max(DATETIME_ADDED),'9999-12-31') as max_datetime_added from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
                                        where object = 'test_framework_group' and value ilike 'mobile_app%'
                                        group by 1,2,3) as f on f.TEST_FRAMEWORK_ID = k.TEST_FRAMEWORK_ID
                             left join reporting_prod.SHARED.AB_TEST_GROUP_MAPPING_REFERENCE as map on map.TRAFFIC_SPLIT = k.MOBILEAPP_GROUP
                        and map.PLATFORM = 'Mobile App' and f.value = map.TEST_FRAMEWORK_GROUP
                    where MOBILEAPP_GROUP_TYPE = 'Standard' or TEST_GROUP is not null) as t
                   on t.TEST_KEY = s.TEST_KEY
                       and t.TEST_LABEL = s.TEST_LABEL
                       and s.STORE_BRAND = t.STORE_BRAND
                       and s.status = t.status
                       and s.test_group = t.test_group
                       and rnk = 1  and is_correct_group_description = TRUE and t.TEST_GROUP is not null
         left join ( select distinct
                         STORE_BRAND
                           ,k.MOBILEAPP_GROUP_TYPE
                           ,CMS_MIN_TEST_START_DATETIME_HQ
                           ,k.TEST_FRAMEWORK_ID
                           ,k.CAMPAIGN_CODE
                           ,TEST_KEY
                           ,test_label
                           ,status
                           ,statuscode
                           ,'Mobile App' as device_type
                           ,TRAFFIC_SPLIT_TYPE
                           ,k.MOBILEAPP_GROUP as GROUP_NAME
                           ,map.TEST_GROUP
                           ,lower(comments) as test_group_description
                           ,TEST_VALUE_START
                           ,TEST_VALUE_END
                           ,case when CMS_MIN_TEST_START_DATETIME_HQ between min_datetime_added - interval '1 day' and max_datetime_added + interval '1 day' then TRUE else FALSE end as is_correct_group_description
                           ,rank() over (partition by test_key,TEST_GROUP,STATUSCODE ORDER BY min_datetime_added desc) as rnk
                     from reporting_prod.shared.AB_TEST_CMS_METADATA as k
                              left join (select distinct test_framework_id,value,comments,min(DATETIME_ADDED) as min_datetime_added,coalesce(max(DATETIME_ADDED),'9999-12-31') as max_datetime_added from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
                                         where object = 'test_framework_group' and value ilike 'mobile_app%'
                                         group by 1,2,3) as f on f.TEST_FRAMEWORK_ID = k.TEST_FRAMEWORK_ID
                              left join reporting_prod.SHARED.AB_TEST_GROUP_MAPPING_REFERENCE as map on map.TRAFFIC_SPLIT = k.MOBILEAPP_GROUP
                         and map.PLATFORM = 'Mobile App' and f.value = map.TEST_FRAMEWORK_GROUP
                     where
                             MOBILEAPP_GROUP_TYPE = 'Standard'
) as m
                   on m.TEST_KEY = s.TEST_KEY
                       and m.TEST_LABEL = s.TEST_LABEL
                       and s.STORE_BRAND = m.STORE_BRAND
                       and s.status = m.status
                       and s.test_group = m.test_group
                       and m.is_correct_group_description = false
                       and m.rnk = 1
where s.DEVICE_TYPE = 'Mobile App'
;

CREATE OR REPLACE TEMPORARY TABLE _custom_control as
select distinct
    m.test_framework_id,TEST_KEY,test_label
              ,coalesce(c.value,c2.value) as value
              ,coalesce(c.test_group,c2.test_group) as test_group
              ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
         left join
     (select distinct test_framework_id,
                      value,
                      'Control' as test_group,
                      comments as test_group_description,
                      coalesce(DATETIME_ADDED,datetime_modified) - interval '1 hour' as record_start_time,
                      coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%control'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join -- for records that are missing datetime added or have that weird 6/18 datetime
    (select distinct test_framework_id,
                     value,
                     'Control' as test_group,
                     comments as test_group_description,
                     coalesce(DATETIME_ADDED,datetime_modified) - interval '1 hour' as record_start_time,
                     coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
     from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
     where
             OBJECT = 'test_framework_group'
       and value ilike '%control'
    ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
    and c2.test_group is not null;


CREATE OR REPLACE TEMPORARY TABLE _custom_variant_a as
select distinct
    m.test_framework_id
    ,TEST_KEY,test_label
      ,coalesce(c.value,c2.value) as value
      ,coalesce(c.test_group,c2.test_group) as test_group
      ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant A' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time,
          coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_1'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant A' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time,
          coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_1'
     ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
         and c2.test_group is not null;

CREATE OR REPLACE TEMPORARY TABLE _custom_variant_b as
select distinct
    m.test_framework_id,TEST_KEY,test_label
  ,coalesce(c.value,c2.value) as value
  ,coalesce(c.test_group,c2.test_group) as test_group
  ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
 left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant B' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time,
          coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_2'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant B' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time,
          coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_2'
     ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
         and c2.test_group is not null;


CREATE OR REPLACE TEMPORARY TABLE _custom_variant_c as
select distinct
    m.test_framework_id,TEST_KEY,test_label
              ,coalesce(c.value,c2.value) as value
              ,coalesce(c.test_group,c2.test_group) as test_group
              ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant C' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_3'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant C' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_3'
     ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
         and c2.test_group is not null
;

CREATE OR REPLACE TEMPORARY TABLE _custom_variant_d as
select distinct
    m.test_framework_id,TEST_KEY,test_label
              ,coalesce(c.value,c2.value) as value
              ,coalesce(c.test_group,c2.test_group) as test_group
              ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value
                    ,'Variant D' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_4'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value
                    ,'Variant D' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_4'
     ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
         and c2.test_group is not null;

CREATE OR REPLACE TEMPORARY TABLE _custom_variant_e as
select distinct
    m.test_framework_id,TEST_KEY,test_label
              ,coalesce(c.value,c2.value) as value
              ,coalesce(c.test_group,c2.test_group) as test_group
              ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant E' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_5'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant E' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_5'
     ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
         and c2.test_group is not null;

CREATE OR REPLACE TEMPORARY TABLE _custom_variant_f as
select distinct
    m.test_framework_id,TEST_KEY,test_label
              ,coalesce(c.value,c2.value) as value
              ,coalesce(c.test_group,c2.test_group) as test_group
              ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value
                    ,'Variant F' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_6'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value
                    ,'Variant F' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_6'
     ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
         and c2.test_group is not null;


CREATE OR REPLACE TEMPORARY TABLE _custom_variant_g as
select distinct
    m.test_framework_id,TEST_KEY,test_label
              ,coalesce(c.value,c2.value) as value
              ,coalesce(c.test_group,c2.test_group) as test_group
              ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant G' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_7'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value,'Variant G' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_7'
     ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
         and c2.test_group is not null;


CREATE OR REPLACE TEMPORARY TABLE _custom_variant_h as
select distinct
    m.test_framework_id,TEST_KEY,test_label
              ,coalesce(c.value,c2.value) as value
              ,coalesce(c.test_group,c2.test_group) as test_group
              ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value
                    ,'Variant H' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_8'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value
                    ,'Variant H' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_8'
     ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
         and c2.test_group is not null;

CREATE OR REPLACE TEMPORARY TABLE _custom_variant_i as
select distinct
    m.test_framework_id,TEST_KEY,test_label
              ,coalesce(c.value,c2.value) as value
              ,coalesce(c.test_group,c2.test_group) as test_group
              ,coalesce(c.test_group_description,c2.test_group_description) as test_group_description
from reporting_prod.SHARED.AB_TEST_CMS_METADATA as m
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value
                    ,'Variant I' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_9'
     ) as c on c.test_framework_id = m.TEST_FRAMEWORK_ID
         and m.CMS_MIN_TEST_START_DATETIME_HQ >= c.record_start_time
         and m.CMS_MIN_TEST_START_DATETIME_HQ < c.record_end_time
         and c.test_group is not null
         left join
     (select distinct TEST_FRAMEWORK_ID
                    ,value
                    ,'Variant I' as test_group
                    ,comments as test_group_description
                    ,DATETIME_ADDED as record_start_time
                    ,coalesce(lead(DATETIME_ADDED) over (partition by TEST_FRAMEWORK_ID,value,comments order by DATETIME_ADDED),'9999-12-31') as record_end_time
      from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK_DATA
      where
              OBJECT = 'test_framework_group'
        and value ilike '%variant_9'
     ) as c2 on c2.test_framework_id = m.TEST_FRAMEWORK_ID
         and c2.test_group is not null;

CREATE OR REPLACE TEMPORARY TABLE _custom_groups as
--desktop
select
    STORE_BRAND
     ,a.TEST_FRAMEWORK_ID
     ,CAMPAIGN_CODE
     ,a.TEST_KEY
     ,a.test_label
     ,status
     ,statuscode
     ,DEVICE_TYPE
     ,'Custom' as TRAFFIC_SPLIT_TYPE
     ,TRAFFIC_SPLIT_CUSTOM as GROUP_NAME
     ,case when test_group_num = 1 then 'Control'
           when test_group_num = 2 then 'Variant A'
           when test_group_num = 3 then 'Variant B'
           when test_group_num = 4 then 'Variant C'
           when test_group_num = 5 then 'Variant D'
           when test_group_num = 6 then 'Variant E'
           when test_group_num = 7 then 'Variant F'
           when test_group_num = 8 then 'Variant G'
           when test_group_num = 9 then 'Variant H'
           when test_group_num = 10 then 'Variant I'
    end as test_group
     ,case when test_group_num = 1 then c.test_group_description
           when test_group_num = 2 then v1.test_group_description
           when test_group_num = 3 then v2.test_group_description
           when test_group_num = 4 then v3.test_group_description
           when test_group_num = 5 then v4.test_group_description
           when test_group_num = 6 then v5.test_group_description
           when test_group_num = 7 then v6.test_group_description
           when test_group_num = 8 then v7.test_group_description
           when test_group_num = 9 then v8.test_group_description
           when test_group_num = 10 then v9.test_group_description
           else null end as test_group_description
     ,lag(upper_bound, 1, 0) OVER (PARTITION BY a.TEST_KEY,a.TEST_LABEL,statuscode ORDER BY test_group_num) + 1 as TEST_VALUE_START
     ,upper_bound as TEST_VALUE_END
--      ,test_group_num
FROM (
         SELECT distinct
             STORE_BRAND
               ,CAMPAIGN_CODE
               ,TEST_KEY
                ,test_label
               ,status
               ,statuscode
               ,l.TEST_FRAMEWORK_ID
               ,DESKTOP_GROUP as TRAFFIC_SPLIT_CUSTOM
               ,split_list
               ,num_splits
               ,f.index + 1 AS test_group_num
               ,f.value::INT AS bin_size
               ,device_type
               ,sum(bin_size)
                OVER (PARTITION BY TEST_KEY,TEST_LABEL,statuscode ORDER BY test_group_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS upper_bound
         FROM (
                  SELECT distinct
                      STORE_BRAND
                        ,t.TEST_FRAMEWORK_ID
                        ,CAMPAIGN_CODE
                        ,t.TEST_KEY
                        ,test_label
                        ,status
                        ,statuscode
                        ,'Desktop' as device_type
                        ,DESKTOP_GROUP
                        ,split(DESKTOP_GROUP, '/') AS split_list
                        ,array_size(split_list) AS num_splits
                  from reporting_prod.shared.AB_TEST_CMS_METADATA as t
                  where
                          DESKTOP_GROUP_TYPE = 'Custom' --and TEST_KEY = 'ExposedCartProductsUpsellRetestFKKI2819_v0'
              ) l
                  JOIN TABLE (flatten(INPUT => split_list)) f
     ) a
         left join _custom_control as c on c.test_framework_id = a.TEST_FRAMEWORK_ID and c.TEST_KEY = a.TEST_KEY and c.test_label = a.test_label
    and c.value not ilike 'mobile%' and a.test_group_num = 1
         left join _custom_variant_a as v1 on v1.test_framework_id = a.TEST_FRAMEWORK_ID and v1.TEST_KEY = a.TEST_KEY and v1.test_label = a.test_label
    and v1.value not ilike 'mobile%' and a.test_group_num = 2
         left join _custom_variant_b as v2 on v2.test_framework_id = a.TEST_FRAMEWORK_ID and v2.TEST_KEY = a.TEST_KEY and v2.test_label = a.test_label
    and v2.value not ilike 'mobile%' and a.test_group_num = 3
         left join _custom_variant_c as v3 on v3.test_framework_id = a.TEST_FRAMEWORK_ID and v3.TEST_KEY = a.TEST_KEY and v3.test_label = a.test_label
    and v3.value not ilike 'mobile%' and a.test_group_num = 4
         left join _custom_variant_d as v4 on v4.test_framework_id = a.TEST_FRAMEWORK_ID and v4.TEST_KEY = a.TEST_KEY and v4.test_label = a.test_label
    and v4.value not ilike 'mobile%' and a.test_group_num = 5
         left join _custom_variant_e as v5 on v5.test_framework_id = a.TEST_FRAMEWORK_ID and v5.TEST_KEY = a.TEST_KEY and v5.test_label = a.test_label
    and v5.value not ilike 'mobile%' and a.test_group_num = 6
         left join _custom_variant_f as v6 on v6.test_framework_id = a.TEST_FRAMEWORK_ID and v6.TEST_KEY = a.TEST_KEY and v6.test_label = a.test_label
    and v6.value not ilike 'mobile%' and a.test_group_num = 7
         left join _custom_variant_g as v7 on v7.test_framework_id = a.TEST_FRAMEWORK_ID and v7.TEST_KEY = a.TEST_KEY and v7.test_label = a.test_label
    and v7.value not ilike 'mobile%' and a.test_group_num = 8
         left join _custom_variant_g as v8 on v8.test_framework_id = a.TEST_FRAMEWORK_ID and v8.TEST_KEY = a.TEST_KEY and v8.test_label = a.test_label
    and v8.value not ilike 'mobile%' and a.test_group_num = 9
         left join _custom_variant_i as v9 on v9.test_framework_id = a.TEST_FRAMEWORK_ID and v9.TEST_KEY = a.TEST_KEY and v9.test_label = a.test_label
    and v9.value not ilike 'mobile%' and a.test_group_num = 10

union

--mobile
select
    STORE_BRAND
     ,a.TEST_FRAMEWORK_ID
     ,CAMPAIGN_CODE
     ,a.TEST_KEY
     ,a.test_label
     ,status
     ,statuscode
     ,DEVICE_TYPE
     ,'Custom' as TRAFFIC_SPLIT_TYPE
     ,TRAFFIC_SPLIT_CUSTOM as GROUP_NAME
     ,case when test_group_num = 1 then 'Control'
           when test_group_num = 2 then 'Variant A'
           when test_group_num = 3 then 'Variant B'
           when test_group_num = 4 then 'Variant C'
           when test_group_num = 5 then 'Variant D'
           when test_group_num = 6 then 'Variant E'
           when test_group_num = 7 then 'Variant F'
           when test_group_num = 8 then 'Variant G'
           when test_group_num = 9 then 'Variant H'
           when test_group_num = 10 then 'Variant I'
    end as test_group
     ,case when test_group_num = 1 then c.test_group_description
           when test_group_num = 2 then v1.test_group_description
           when test_group_num = 3 then v2.test_group_description
           when test_group_num = 4 then v3.test_group_description
           when test_group_num = 5 then v4.test_group_description
           when test_group_num = 6 then v5.test_group_description
           when test_group_num = 7 then v6.test_group_description
           when test_group_num = 8 then v7.test_group_description
           when test_group_num = 9 then v8.test_group_description
           when test_group_num = 10 then v9.test_group_description
           else null end as test_group_description
     ,lag(upper_bound, 1, 0) OVER (PARTITION BY a.TEST_KEY,a.TEST_LABEL,statuscode ORDER BY test_group_num) + 1 as TEST_VALUE_START
     ,upper_bound as TEST_VALUE_END
--      ,test_group_num
FROM (
         SELECT distinct
             STORE_BRAND
               ,CAMPAIGN_CODE
               ,TEST_KEY
               ,test_label
               ,status
               ,statuscode
               ,l.TEST_FRAMEWORK_ID
               ,MOBILE_GROUP as TRAFFIC_SPLIT_CUSTOM
               ,split_list
               ,num_splits
               ,f.index + 1 AS test_group_num
               ,f.value::INT AS bin_size
               ,device_type
               ,sum(bin_size)
                    OVER (PARTITION BY TEST_KEY,TEST_LABEL,statuscode ORDER BY test_group_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS upper_bound
         FROM (
                  SELECT distinct
                      STORE_BRAND
                                ,t.TEST_FRAMEWORK_ID
                                ,CAMPAIGN_CODE
                                ,t.TEST_KEY
                                ,test_label
                                ,status
                                ,statuscode
                                ,'Mobile' as device_type
                                ,MOBILE_GROUP
                                ,split(MOBILE_GROUP, '/') AS split_list
                                ,array_size(split_list) AS num_splits
                  from reporting_prod.shared.AB_TEST_CMS_METADATA as t
                  where
                          MOBILE_GROUP_TYPE = 'Custom' --and TEST_KEY = 'ExposedCartProductsUpsellRetestFKKI2819_v0'
              ) l
                  JOIN TABLE (flatten(INPUT => split_list)) f
     ) a
         left join _custom_control as c on c.test_framework_id = a.TEST_FRAMEWORK_ID and c.TEST_KEY = a.TEST_KEY and c.test_label = a.test_label
    and c.value not ilike 'mobile%' and a.test_group_num = 1
         left join _custom_variant_a as v1 on v1.test_framework_id = a.TEST_FRAMEWORK_ID and v1.TEST_KEY = a.TEST_KEY and v1.test_label = a.test_label
    and v1.value not ilike 'mobile%' and a.test_group_num = 2
         left join _custom_variant_b as v2 on v2.test_framework_id = a.TEST_FRAMEWORK_ID and v2.TEST_KEY = a.TEST_KEY and v2.test_label = a.test_label
    and v2.value not ilike 'mobile%' and a.test_group_num = 3
         left join _custom_variant_c as v3 on v3.test_framework_id = a.TEST_FRAMEWORK_ID and v3.TEST_KEY = a.TEST_KEY and v3.test_label = a.test_label
    and v3.value not ilike 'mobile%' and a.test_group_num = 4
         left join _custom_variant_d as v4 on v4.test_framework_id = a.TEST_FRAMEWORK_ID and v4.TEST_KEY = a.TEST_KEY and v4.test_label = a.test_label
    and v4.value not ilike 'mobile%' and a.test_group_num = 5
         left join _custom_variant_e as v5 on v5.test_framework_id = a.TEST_FRAMEWORK_ID and v5.TEST_KEY = a.TEST_KEY and v5.test_label = a.test_label
    and v5.value not ilike 'mobile%' and a.test_group_num = 6
         left join _custom_variant_f as v6 on v6.test_framework_id = a.TEST_FRAMEWORK_ID and v6.TEST_KEY = a.TEST_KEY and v6.test_label = a.test_label
    and v6.value not ilike 'mobile%' and a.test_group_num = 7
         left join _custom_variant_g as v7 on v7.test_framework_id = a.TEST_FRAMEWORK_ID and v7.TEST_KEY = a.TEST_KEY and v7.test_label = a.test_label
    and v7.value not ilike 'mobile%' and a.test_group_num = 8
         left join _custom_variant_g as v8 on v8.test_framework_id = a.TEST_FRAMEWORK_ID and v8.TEST_KEY = a.TEST_KEY and v8.test_label = a.test_label
    and v8.value not ilike 'mobile%' and a.test_group_num = 9
         left join _custom_variant_i as v9 on v9.test_framework_id = a.TEST_FRAMEWORK_ID and v9.TEST_KEY = a.TEST_KEY and v9.test_label = a.test_label
    and v9.value not ilike 'mobile%' and a.test_group_num = 10

union

--mobile app
select
    STORE_BRAND
     ,a.TEST_FRAMEWORK_ID
     ,CAMPAIGN_CODE
     ,a.TEST_KEY
     ,a.test_label
     ,status
     ,statuscode
     ,DEVICE_TYPE
     ,'Custom' as TRAFFIC_SPLIT_TYPE
     ,TRAFFIC_SPLIT_CUSTOM as GROUP_NAME
     ,case when test_group_num = 1 then 'Control'
           when test_group_num = 2 then 'Variant A'
           when test_group_num = 3 then 'Variant B'
           when test_group_num = 4 then 'Variant C'
           when test_group_num = 5 then 'Variant D'
           when test_group_num = 6 then 'Variant E'
           when test_group_num = 7 then 'Variant F'
           when test_group_num = 8 then 'Variant G'
           when test_group_num = 9 then 'Variant H'
           when test_group_num = 10 then 'Variant I'
    end as test_group
     ,case when test_group_num = 1 then c.test_group_description
           when test_group_num = 2 then v1.test_group_description
           when test_group_num = 3 then v2.test_group_description
           when test_group_num = 4 then v3.test_group_description
           when test_group_num = 5 then v4.test_group_description
           when test_group_num = 6 then v5.test_group_description
           when test_group_num = 7 then v6.test_group_description
           when test_group_num = 8 then v7.test_group_description
           when test_group_num = 9 then v8.test_group_description
           when test_group_num = 10 then v9.test_group_description
           else null end as test_group_description
     ,lag(upper_bound, 1, 0) OVER (PARTITION BY a.TEST_KEY,a.TEST_LABEL,statuscode ORDER BY test_group_num) + 1 as TEST_VALUE_START
     ,upper_bound as TEST_VALUE_END
--      ,test_group_num
FROM (
         SELECT distinct
             STORE_BRAND
               ,CAMPAIGN_CODE
               ,TEST_KEY
               ,test_label
               ,status
               ,statuscode
               ,l.TEST_FRAMEWORK_ID
               ,MOBILEAPP_GROUP as TRAFFIC_SPLIT_CUSTOM
               ,split_list
               ,num_splits
               ,f.index + 1 AS test_group_num
               ,f.value::INT AS bin_size
               ,device_type
               ,sum(bin_size)
                    OVER (PARTITION BY TEST_KEY,TEST_LABEL,statuscode ORDER BY test_group_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS upper_bound
         FROM (
                  SELECT distinct
                      STORE_BRAND
                        ,t.TEST_FRAMEWORK_ID
                        ,CAMPAIGN_CODE
                        ,t.TEST_KEY
                        ,test_label
                        ,status
                        ,statuscode
                        ,'App' as device_type
                        ,MOBILEAPP_GROUP
                        ,split(MOBILEAPP_GROUP, '/') AS split_list
                        ,array_size(split_list) AS num_splits
                  from reporting_prod.shared.AB_TEST_CMS_METADATA as t
                  where
                          MOBILEAPP_GROUP_TYPE = 'Custom'
                  --and CMS_TEST_KEY = 'SXF4144_v2'

              ) l
                  JOIN TABLE (flatten(INPUT => split_list)) f
     ) a
         left join _custom_control as c on c.test_framework_id = a.TEST_FRAMEWORK_ID and c.TEST_KEY = a.TEST_KEY and c.test_label = a.test_label
    and c.value not ilike 'mobile%' and a.test_group_num = 1
         left join _custom_variant_a as v1 on v1.test_framework_id = a.TEST_FRAMEWORK_ID and v1.TEST_KEY = a.TEST_KEY and v1.test_label = a.test_label
    and v1.value not ilike 'mobile%' and a.test_group_num = 2
         left join _custom_variant_b as v2 on v2.test_framework_id = a.TEST_FRAMEWORK_ID and v2.TEST_KEY = a.TEST_KEY and v2.test_label = a.test_label
    and v2.value not ilike 'mobile%' and a.test_group_num = 3
         left join _custom_variant_c as v3 on v3.test_framework_id = a.TEST_FRAMEWORK_ID and v3.TEST_KEY = a.TEST_KEY and v3.test_label = a.test_label
    and v3.value not ilike 'mobile%' and a.test_group_num = 4
         left join _custom_variant_d as v4 on v4.test_framework_id = a.TEST_FRAMEWORK_ID and v4.TEST_KEY = a.TEST_KEY and v4.test_label = a.test_label
    and v4.value not ilike 'mobile%' and a.test_group_num = 5
         left join _custom_variant_e as v5 on v5.test_framework_id = a.TEST_FRAMEWORK_ID and v5.TEST_KEY = a.TEST_KEY and v5.test_label = a.test_label
    and v5.value not ilike 'mobile%' and a.test_group_num = 6
         left join _custom_variant_f as v6 on v6.test_framework_id = a.TEST_FRAMEWORK_ID and v6.TEST_KEY = a.TEST_KEY and v6.test_label = a.test_label
    and v6.value not ilike 'mobile%' and a.test_group_num = 7
         left join _custom_variant_g as v7 on v7.test_framework_id = a.TEST_FRAMEWORK_ID and v7.TEST_KEY = a.TEST_KEY and v7.test_label = a.test_label
    and v7.value not ilike 'mobile%' and a.test_group_num = 8
         left join _custom_variant_g as v8 on v8.test_framework_id = a.TEST_FRAMEWORK_ID and v8.TEST_KEY = a.TEST_KEY and v8.test_label = a.test_label
    and v8.value not ilike 'mobile%' and a.test_group_num = 9
         left join _custom_variant_i as v9 on v9.test_framework_id = a.TEST_FRAMEWORK_ID and v9.TEST_KEY = a.TEST_KEY and v9.test_label = a.test_label
    and v9.value not ilike 'mobile%' and a.test_group_num = 10;

delete from _custom_groups where TEST_VALUE_START > TEST_VALUE_END;
-------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _groups as
select distinct
    STORE_BRAND
      ,TEST_FRAMEWORK_ID
      ,CAMPAIGN_CODE
      ,TEST_KEY
      ,test_label
      ,status
      ,statuscode
      ,DEVICE_TYPE
      ,TRAFFIC_SPLIT_TYPE as TRAFFIC_SPLIT_TYPE
      ,GROUP_NAME
      ,TEST_GROUP
      ,lower(TEST_GROUP_DESCRIPTION) as TEST_GROUP_DESCRIPTION
      ,TEST_VALUE_START
      ,TEST_VALUE_END
from _standard_groups

union

select distinct
    STORE_BRAND
      ,TEST_FRAMEWORK_ID
      ,CAMPAIGN_CODE
      ,TEST_KEY
      ,test_label
      ,status
      ,statuscode
      ,DEVICE_TYPE
      ,TRAFFIC_SPLIT_TYPE as TRAFFIC_SPLIT_TYPE
      ,GROUP_NAME as GROUP_NAME
      ,TEST_GROUP
      ,lower(TEST_GROUP_DESCRIPTION) as TEST_GROUP_DESCRIPTION
      ,TEST_VALUE_START
      ,TEST_VALUE_END
from _custom_groups;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.shared.ab_test_cms_groups as
select *
from _groups
where test_key is not null

union all

select distinct  --tests that are not setup on mobile app but on mobile will still get session on mobile app,
              -- so need to make sure that we are capturing those session
    STORE_BRAND
      ,TEST_FRAMEWORK_ID
      ,CAMPAIGN_CODE
      ,TEST_KEY
      ,test_label
      ,status
      ,statuscode
      ,'App' as DEVICE_TYPE
      ,TRAFFIC_SPLIT_TYPE
      ,GROUP_NAME
      ,TEST_GROUP
      ,TEST_GROUP_DESCRIPTION
      ,TEST_VALUE_START
      ,TEST_VALUE_END
from _groups
where
        DEVICE_TYPE = 'Mobile'
  and test_key is not null
  and test_key in (select test_key from reporting_prod.SHARED.AB_TEST_CMS_METADATA
                   where STORE_BRAND in ('Fabletics','JustFab','Yitty') and MOBILEAPP_GROUP is null and MOBILE_GROUP is not null);


-- These test framework ids are missing rows in test_framework_data table, so it will look null
-- 169,186,229,231,278,280,281,285,443,615,628,643,648,670,677,678,686,693,696,962,967,1075


-- insert into reporting.SHARED.AB_TEST_GROUP_MAPPING_REFERENCE
-- values
--     ('standard','Desktop','100/0',1,1,'Control','control')
--     ,('standard','Desktop','100/0',2,2,'Variant','variant_1')
--      ,('standard','Desktop','0/100',1,1,'Control','control')
--      ,('standard','Desktop','0/100',2,2,'Variant','variant_1');

-- select
--     TEST_KEY
--     ,test_label
--     ,STORE_BRAND
--     ,statuscode
--     ,device_type
--     ,TEST_GROUP
--     ,count(*) as count
--     ,count(distinct TEST_KEY) as count_distinct
-- from _standard_groups
-- where TEST_KEY = 'UrgencyMessagingUI_v0'
-- group by 1,2,3,4,5,6
