
CREATE OR REPLACE TEMPORARY TABLE _max_dates as
select distinct
    test_framework_id
    ,MAX(convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME))::datetime as max_ab_test_start_datetime
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
left join REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_START as t on t.AB_TEST_KEY = m.TEST_KEY
where
    m.statuscode = 113
--     and m.CMS_MIN_TEST_START_DATETIME_HQ >= current_date - interval '1 year'
group by 1
having max_ab_test_start_datetime >= current_date - interval '3 month'
;

----use this to reload data for only last 3 months --COMMENT
-- CREATE OR REPLACE TEMPORARY TABLE _last_3_months as
-- select distinct test_framework_id
-- from _max_dates
-- where max_ab_test_start_datetime >=  current_date - interval '3 month'
--             and max_ab_test_start_datetime <= current_date + interval '1 day';

CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED as
select distinct m.*,max_ab_test_start_datetime
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
join _max_dates as x on x.test_framework_id = m.test_framework_id
where statuscode = 113

union all

select distinct m.*,current_date max_ab_test_start_datetime
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA as m
where
    STATUSCODE = 113
    and CAMPAIGN_CODE = 'FLimagesort' and split_part(test_key,'_v',2) >= 6;
    --and CAMPAIGN_CODE in ('MYVIPSNOOZEOPTION','SAVEOFFERSEGMENTATIONLITE');

delete from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
where
    ((statuscode = 113 and (is_valid_desktop_group = FALSE and is_valid_mobile_group = FALSE and is_valid_mobileapp_group = FALSE))
    and max_ab_test_start_datetime is null)
    or campaign_code in
            (select
                distinct campaign_code
                from reporting_prod.shared.ab_test_cms_metadata
                where
                    (statuscode in (108,110) or current_statuscode in (108,110))
                    and cms_min_test_start_datetime_hq >= '2021-01-01'
                    group by 1 having count(distinct test_framework_id) > 1);
