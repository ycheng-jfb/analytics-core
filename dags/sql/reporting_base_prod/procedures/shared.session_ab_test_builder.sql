SET current_end_date_hq = (
    SELECT
        MIN(t.max_hq_datetime)
    FROM (
        SELECT
            MAX(CONVERT_TIMEZONE('America/Los_Angeles', SESSION_LOCAL_DATETIME))::datetime as max_hq_datetime
        FROM
            reporting_base_prod.shared.session
        WHERE
            session_local_datetime >= current_timestamp - interval '2 day'
    ) t
);

--legacy code from OOF sheet prior to Builder API
CREATE OR REPLACE TEMPORARY TABLE _builder_tests as
select *
from LAKE_VIEW.sharepoint.ab_test_metadata_import_oof
where
    test_type = 'Builder'
    and test_label != ' |  | '
    and test_label in
        ('PDP | FLW OTG HW Legging - PDPST AB Test (Feb 2024) - Control | FLW OTG HW Legging - PDPST AB Test (Feb 2024) - No ST Test'
                ,'Toggles | Postreg - Scrubs - W Toggle AB Test (March 2024) - Control | Postreg - Scrubs - W Toggle AB Test (March 2024) - Test'
                ,'FLWEU Timer | FLEU1023_W_LP_2x24_upsell_Oct_TOM_LL_Test | Variation 1'
                ,'FLM Shorts | Mens - Shorts - Autosize AB Test (June 2024) - Control | Mens - Shorts - Autosize AB Test (June 2024) - Test'
                ,'PDP | FLW Define PH HW Legging - PDPST AB Test (Feb 2024) - Control | FLW Define PH HW Legging - PDPST AB Test (Feb 2024) - Test');


CREATE OR REPLACE TEMPORARY TABLE _builder_test_variation_name as
select RTRIM(control_start) as test_variation_name from _builder_tests
union
select RTRIM(variant_a_start) as test_variation_name from _builder_tests
union
select RTRIM(variant_b_start) as test_variation_name from _builder_tests
union
select RTRIM(variant_c_start) as test_variation_name from _builder_tests
union
select RTRIM(variant_d_start) as test_variation_name from _builder_tests;

CREATE OR REPLACE TEMPORARY TABLE _builder_session_FABLETICS as
select
     properties_session_id as session_id
    ,CONTEXT_PAGE_PATH
    ,CASE WHEN properties_test_variation_name IS NULL then RTRIM(REPLACE(GET_PATH(PARSE_JSON(_rescued_data), 'properties.test_variation_name'), '"'))
        else RTRIM(properties_test_variation_name) end as test_variation_name
    ,timestamp::timestamp as abt_start_datetime_utc
    ,rank() over (partition by properties_session_id,test_variation_name ORDER BY timestamp asc) as rnk
from lake.SEGMENT_FL.JAVASCRIPT_FABLETICS_BUILDER_TEST_ENTERED
where
    test_variation_name in (select * from _builder_test_variation_name) ;

CREATE OR REPLACE TEMPORARY TABLE _builder_session_base as
select distinct
    session_id
    ,CONTEXT_PAGE_PATH
    ,'Control' as test_group
    ,test_variation_name
    ,min(abt_start_datetime_utc) as abt_start_datetime_utc
from _builder_session_FABLETICS
where test_variation_name in (select RTRIM(control_start) from _builder_tests)
    and rnk = 1
group by 1,2,3,4
union all
select distinct
    session_id
    ,CONTEXT_PAGE_PATH
    ,'Variant A' as test_group
    ,test_variation_name
    ,min(abt_start_datetime_utc) as abt_start_datetime_utc
from _builder_session_FABLETICS
where test_variation_name in (select RTRIM(variant_a_start) from _builder_tests)
    and rnk = 1
group by 1,2,3,4
union all
select distinct
    session_id
    ,CONTEXT_PAGE_PATH
    ,'Variant B' as test_group
    ,test_variation_name
    ,min(abt_start_datetime_utc) as abt_start_datetime_utc
from _builder_session_FABLETICS
where test_variation_name in (select RTRIM(variant_b_start) from _builder_tests)
    and rnk = 1
group by 1,2,3,4
union all
select distinct
    session_id
    ,CONTEXT_PAGE_PATH
    ,'Variant C' as test_group
    ,test_variation_name
    ,min(abt_start_datetime_utc) as abt_start_datetime_utc
from _builder_session_FABLETICS
where test_variation_name in (select RTRIM(variant_c_start) from _builder_tests)
    and rnk = 1
group by 1,2,3,4
union all
select distinct
    session_id
    ,CONTEXT_PAGE_PATH
    ,'Variant D' as test_group
    ,test_variation_name
    ,min(abt_start_datetime_utc) as abt_start_datetime_utc
from _builder_session_FABLETICS
where test_variation_name in (select RTRIM(variant_d_start) from _builder_tests)
    and rnk = 1
group by 1,2,3,4;

-- select test_variation_name,test_group,count(*) as sessions,count(distinct session_id) as sessions_distinct,sessions - sessions_distinct as diff
-- from _builder_session_base
-- group by 1,2 order by 1,2

CREATE OR REPLACE TEMPORARY TABLE _test_key_scaffold as
select distinct
    a.test_label
    ,b1.test_variation_name
    ,b1.test_group
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_activated_datetime
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_end_datetime
from _builder_tests as a
join _builder_session_base as b1 on RTRIM(a.control_start) = RTRIM(b1.test_variation_name)
    and b1.test_group = 'Control'

union

select distinct
    a.test_label
    ,b1.test_variation_name
    ,case when test_split = '50/50' then 'Variant' else 'Variant A' end as test_group
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_activated_datetime
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_end_datetime
from _builder_tests as a
join _builder_session_base as b1 on RTRIM(a.variant_a_start) = RTRIM(b1.test_variation_name)
    and b1.test_group = 'Variant A'

union

select distinct
    a.test_label
    ,b1.test_variation_name
    ,b1.test_group
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_activated_datetime
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_end_datetime
from _builder_tests as a
join _builder_session_base as b1 on RTRIM(variant_b_start) = RTRIM(b1.test_variation_name)
    and b1.test_group = 'Variant B'

union

select distinct
    a.test_label
    ,b1.test_variation_name
    ,b1.test_group
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_activated_datetime
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_end_datetime
from _builder_tests as a
join _builder_session_base as b1 on RTRIM(a.variant_c_start) = RTRIM(b1.test_variation_name)
    and b1.test_group = 'Variant C'

union

select distinct
    a.test_label
    ,b1.test_variation_name
    ,b1.test_group
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_activated_datetime
    ,case when adjusted_activated_datetime = '1900-01-01 00:00:00.000000000' then null end as adjusted_end_datetime
from _builder_tests as a
join _builder_session_base as b1 on RTRIM(a.variant_d_start) = RTRIM(b1.test_variation_name)
    and b1.test_group = 'Variant D'
order by 1,3,2;

CREATE OR REPLACE TEMPORARY TABLE _min_max_datetimes as --filtering for live tests and deactivated tests within last 3 months
select
    test_label
    ,status
    ,test_start_location
    ,min_abt_start_datetime_utc
    ,max_abt_start_datetime_utc
from
    (
        select
            b.test_label
            ,status
            ,test_start_location
            ,min(abt_start_datetime_utc) as min_abt_start_datetime_utc
            ,max(abt_start_datetime_utc) as max_abt_start_datetime_utc
        FROM _builder_tests as b
        join _test_key_scaffold as g on rtrim(g.test_label) = rtrim(b.test_label)
        join _builder_session_base as sd on rtrim(sd.test_variation_name) = rtrim(g.test_variation_name)
        group by 1,2,3
    )
where max_abt_start_datetime_utc >= current_date - interval '3 month';

CREATE OR REPLACE TEMPORARY TABLE _test_start_page_path as
select *
from
    (
        select
            oo.test_label
            ,test_start_location
            ,CONTEXT_PAGE_PATH
            ,count(*) as count
            ,ratio_to_report(count) over (partition by oo.test_label) * 100 as pct
            ,rank() over (partition by oo.test_label order by count desc) as rnk --first page path with most volume by test label
        FROM _min_max_datetimes as oo
        join _test_key_scaffold as g on rtrim(g.test_label) = rtrim(oo.test_label)
        join _builder_session_base as sd on rtrim(sd.test_variation_name) = rtrim(g.test_variation_name)
        where
            test_start_location in ('Shopping Grid','Post-Reg / Boutique','Post-Reg')
        group by 1,2,3 order by 1,pct desc
    )
where rnk = 1;

------------------------------------------------------------------------------------------------------------------------
--api migration code for only tests added >= Sept 13
CREATE OR REPLACE TEMPORARY TABLE _builder_tests_from_api as
select max(case when test_variation_name ilike '%constructor%' then true else false end) as constructor_test,
brand, content_type, published, builder_id, path, domain, site_country, adjusted_activated_datetime_utc, adjusted_activated_datetime_pst, test_name
from lake.builder.builder_api_metadata api
WHERE
    (brand <> 'SXF' and ADJUSTED_ACTIVATED_DATETIME_PST >= '2024-09-13')
    or (brand = 'SXF' and ADJUSTED_ACTIVATED_DATETIME_PST >= '2024-05-01')
    OR  builder_id in ('781b805eff344ef286b44088d77003cd', 'd5b2c056fe8f43cfbd01439f66e54cef','dcd09f55386e4daebe90a2818d71c1db')
GROUP BY brand, content_type, published, builder_id, path, domain, site_country, adjusted_activated_datetime_utc, adjusted_activated_datetime_pst, test_name
order by builder_id;

CREATE OR REPLACE TEMPORARY TABLE _eligible_tests as --filtering for live tests and deactivated tests within last 3 months
select
    b.*
    ,case  when ct.called_at_utc is null and published <> 'archived' then 'Running / In Progress'
        when ct.called_at_utc is not null OR published = 'archived' then 'Closed / Paused' end as status  -- previously oof spreadsheet; status of whether test is running or not
    ,ct.called_at_utc
    ,case when (called_at_utc is not null and called_at_utc < current_date - interval '3 month') then 'test called over 3 months ago'
        else 'keep for reporting'
            end as comment
FROM _builder_tests_from_api as b
left join lake.builder.called_tests ct on b.builder_id = ct.builder_id
where
    comment = 'keep for reporting';

CREATE OR REPLACE TEMPORARY TABLE _builder_test_targeting_from_api as
select
    builder_id
    ,max(case when property = 'membershipStatus' then value end) as membershipStatus
    ,coalesce(max(case when property = 'locale' then value end), max(case when property = 'tld' then value end)) as locale
    ,max(case when property = 'platform' then value end) as platform
    ,cast (max(case when property = 'loggedIn' then value end) as BOOLEAN) as loggedIn
    ,max(case when property = 'device' then value end) as device
    ,max(case when property = 'brand' then value end) as brand
    ,case when brand ilike '%yitty%' then false  else true end as yty_exclusion
    ,max(case when property = 'urlPath' then value end) as urlPath
    ,max(case when property = 'gender' then value end) as gender
    ,max(case when property = 'experienceId' then value end) as experienceId
from (select builder_id,  property, operator, value from (
    select builder_id||'_'||property as composite_key, builder_id, property, operator, value, meta_create_datetime,
    rank() over (partition by composite_key ORDER BY meta_create_datetime desc ) as rnk
    FROM lake.builder.builder_api_targeting
)
where rnk = 1
)
group by builder_id
;

CREATE OR REPLACE TEMPORARY TABLE _builder_session_base_from_api as
select
    properties_session_id as session_id
    ,brand
    ,m.builder_id
    ,m.test_name
    ,COALESCE(properties_test_variation_id,JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id')) as test_variation_id
    ,m.test_variation_name
    ,case when m.assignment = 'control' then 'Control'
        when m.assignment = 'Variant_1' then 'Variant A'
        when m.assignment = 'Variant_2' then 'Variant B'
        when m.assignment = 'Variant_3' then 'Variant C' end
            as test_group
    ,min(timestamp::timestamp) as abt_start_datetime_utc
from lake.SEGMENT_FL.JAVASCRIPT_FABLETICS_BUILDER_TEST_ENTERED as t
join lake.builder.builder_api_metadata as m on COALESCE(properties_test_variation_id,JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id')) = m.test_variation_id
where
    m.brand = 'Fabletics'
    and m.builder_id in (select builder_id from _eligible_tests)
group by 1,2,3,4,5,6,7

union all ---JF

select
    properties_session_id as session_id
    ,brand
    ,m.builder_id
    ,m.test_name
    ,JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id') as test_variation_id
    ,m.test_variation_name
    ,case when m.assignment = 'control' then 'Control'
        when m.assignment = 'Variant_1' then 'Variant A'
        when m.assignment = 'Variant_2' then 'Variant B'
        when m.assignment = 'Variant_3' then 'Variant C' end
            as test_group
    ,min(timestamp::timestamp) as abt_start_datetime_utc
from lake.segment_gfb.JAVASCRIPT_JUSTFAB_BUILDER_TEST_ENTERED
join lake.builder.builder_api_metadata as m on JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id') = m.test_variation_id
where
    m.brand = 'JustFab'
    and m.builder_id in (select builder_id from _eligible_tests)
group by 1,2,3,4,5,6,7

union all --- SD

select
    properties_session_id as session_id
    ,brand
    ,m.builder_id
    ,m.test_name
    ,JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id') as test_variation_id
    ,m.test_variation_name
    ,case when m.assignment = 'control' then 'Control'
        when m.assignment = 'Variant_1' then 'Variant A'
        when m.assignment = 'Variant_2' then 'Variant B'
        when m.assignment = 'Variant_3' then 'Variant C' end
            as test_group
    ,min(timestamp::timestamp) as abt_start_datetime_utc
from lake.segment_gfb.JAVASCRIPT_SHOEDAZZLE_BUILDER_TEST_ENTERED as t
join lake.builder.builder_api_metadata as m on JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id') = m.test_variation_id
where
    m.brand = 'ShoeDazzle'
    and m.builder_id in (select builder_id from _eligible_tests)
group by 1,2,3,4,5,6,7

union all --- FK

select
    properties_session_id as session_id
    ,brand
    ,m.builder_id
    ,m.test_name
    ,JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id') as test_variation_id
    ,m.test_variation_name
    ,case when m.assignment = 'control' then 'Control'
        when m.assignment = 'Variant_1' then 'Variant A'
        when m.assignment = 'Variant_2' then 'Variant B'
        when m.assignment = 'Variant_3' then 'Variant C' end
            as test_group
    ,min(timestamp::timestamp) as abt_start_datetime_utc
from lake.segment_gfb.javascript_fabkids_builder_test_entered as t
join lake.builder.builder_api_metadata as m on JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id') = m.test_variation_id
where
    m.brand = 'FabKids'
    and m.builder_id in (select builder_id from _eligible_tests)
group by 1,2,3,4,5,6,7

union all --- SXF

select
    properties_session_id as session_id
    ,brand
    ,m.builder_id
    ,m.test_name
    ,JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id') as test_variation_id
    ,m.test_variation_name
    ,case when m.assignment = 'control' then 'Control'
        when m.assignment = 'Variant_1' then 'Variant A'
        when m.assignment = 'Variant_2' then 'Variant B'
        when m.assignment = 'Variant_3' then 'Variant C' end
            as test_group
    ,min(timestamp::timestamp) as abt_start_datetime_utc
from lake.segment_sxf.JAVASCRIPT_SXF_BUILDER_TEST_ENTERED
join lake.builder.builder_api_metadata as m on JSON_EXTRACT_PATH_TEXT(_rescued_data, 'properties.test_variation_id') = m.test_variation_id
where
    m.brand = 'SXF'
    and m.builder_id in (select builder_id from _eligible_tests)
group by 1,2,3,4,5,6,7;

CREATE OR REPLACE TEMPORARY TABLE _min_max_datetimes_from_api as --filtering for live tests and deactivated tests within last 3 months
select
    b.brand
    ,b.builder_id
    ,b.test_name
    ,min(sd.abt_start_datetime_utc) as min_abt_start_datetime_utc -- from _builder_session_base first time a builder test event was seen for this test
    ,max(sd.abt_start_datetime_utc) as max_abt_start_datetime_utc -- from _builder_session_base latest time a builder test event was seen for this test
FROM _eligible_tests as b
left join _builder_session_base_from_api as sd on sd.builder_id = b.builder_id
    and sd.brand = b.brand
group by 1,2,3;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE _non_framework_builder AS
SELECT distinct
    test_type
    ,ticket as TEST_FRAMEWORK_TICKET
    ,test_key
    ,test_key AS test_name
    ,oo.test_label
    ,brand
    ,region
    ,test_platforms
    ,oo.membership_state
    ,gender
    ,oo.test_start_location
    ,test_split
    ,convert_timezone('UTC','America/Los_Angeles',min_abt_start_datetime_utc)::datetime AS test_activated_datetime_pst
    ,min_abt_start_datetime_utc as test_activated_datetime_utc
    ,oo.status
    ,sd.SESSION_ID
    ,g.test_variation_name AS ab_test_segment
    ,g.test_group
    ,g.test_variation_name AS test_group_description
    ,convert_timezone('UTC','America/Los_Angeles',abt_start_datetime_utc)::datetime as test_start_datetime_added_pst
    ,CASE WHEN f.MEMBERSHIP_STATE IS NULL THEN 'Prospect' else f.MEMBERSHIP_STATE end AS test_start_membership_state
    ,CASE WHEN f.MEMBERSHIP_STATE = 'VIP' then datediff('month',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime) + 1 else null end AS test_start_vip_month_tenure
    ,CASE WHEN f.MEMBERSHIP_STATE = 'Lead' then datediff('day',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime) + 1 else null end AS test_start_lead_daily_tenure
    ,case when (t.STORE_REGION = 'NA' and oo.region in ('NA','Global','N/A')) then TRUE
           when t.STORE_REGION = 'EU' and oo.region in ('EU','Global','N/A') then TRUE
               else FALSE end as is_valid_session
    ,IS_BOT
    ,case when yty_exclusion = true and t.store_brand = 'Yitty' then FALSE else TRUE end as is_valid_brand_session
    ,s.platform
    ,path.CONTEXT_PAGE_PATH as test_start_page_path
FROM _builder_tests as oo
JOIN _min_max_datetimes as max on max.test_label = oo.test_label
join _test_key_scaffold as g on g.test_label = oo.test_label
join _builder_session_base as sd on sd.test_variation_name = g.test_variation_name
join reporting_base_prod.SHARED.SESSION as s on s.meta_original_session_id = sd.SESSION_ID
join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
    and t.store_type <> 'Retail'
left join _test_start_page_path as path on path.test_label = g.test_label
LEFT JOIN edw_prod.reference.store_timezone AS st ON st.store_id = t.store_id
LEFT JOIN edw_prod.DATA_MODEL.FACT_MEMBERSHIP_EVENT f ON f.customer_id = s.customer_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_START_LOCAL_DATETIME)::datetime <= convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_END_LOCAL_DATETIME)::datetime > convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime
WHERE
    convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime between coalesce(g.adjusted_activated_datetime,max.min_abt_start_datetime_utc) and coalesce(g.adjusted_end_datetime,'9999-12-01')
    and CONVERT_TIMEZONE('America/Los_Angeles', s.SESSION_LOCAL_DATETIME)::datetime between coalesce(g.adjusted_activated_datetime,max.min_abt_start_datetime_utc) and coalesce(g.adjusted_end_datetime,'9999-12-01')
    and IS_TEST_CUSTOMER_ACCOUNT = FALSE;

delete from _non_framework_builder
where
    membership_state in ('Prospects','Prospect') and test_start_membership_state <> 'Prospect'
        or membership_state in ('Leads','Lead') and test_start_membership_state <> 'Lead'
        or membership_state in ('VIPS','VIP') and test_start_membership_state <> 'VIP'
        or membership_state in ('Prospects and Leads','Prospect and Lead') and test_start_membership_state not in ('Prospect','Lead')
        or membership_state in ('Leads and VIPS','Lead and VIP') and test_start_membership_state not in ('Lead','VIP')
        or (test_start_membership_state = 'Prospect' and IS_BOT = TRUE);

delete from _non_framework_builder
where
    (test_platforms = 'Desktop & Mobile' and platform = 'Mobile App')
    or (test_platforms = 'Desktop' and platform not in ('Desktop','Tablet'))
    or (test_platforms = 'Mobile' and platform not in ('Mobile','Unknown'))
    or (test_platforms = 'MobileApp' and platform <> 'Mobile App')
    or (test_platforms = 'Mobile & App' and platform not in ('Mobile','Mobile App'));

CREATE OR REPLACE TEMPORARY TABLE _non_framework_builder_from_api as
SELECT distinct
    'Builder' as test_type
    ,'N/A' as TEST_FRAMEWORK_TICKET -- from oof sheet, mostly N/A
    ,oo.builder_id as test_key
    ,oo.test_name
    ,oo.test_name as test_label -- from oof sheet; concatenated test variant names
    ,oo.builder_id
    ,oo.brand
    -- Targeting
    ,btt.locale as region
    ,btt.platform as test_platforms
    ,btt.membershipStatus as membership_state
    ,btt.gender as gender
    ,oo.path as test_start_location
    ,null test_split
    ,oo.adjusted_activated_datetime_pst as test_activated_datetime_pst
    ,oo.adjusted_activated_datetime_utc as test_activated_datetime_utc
    ,oo.status -- previosly from oof, now from _min_max_datetimes Q: Is this still needed?  If not, we can remove all of _min_max_datetimes
    ,sd.SESSION_ID
    ,sd.test_variation_name as ab_test_segment
    ,sd.test_variation_id
    ,sd.test_group
    ,sd.test_variation_name AS test_group_description
    ,t.STORE_REGION
    ,convert_timezone('UTC','America/Los_Angeles',abt_start_datetime_utc)::datetime as test_start_datetime_added_pst
    ,CASE WHEN f.MEMBERSHIP_STATE IS NULL THEN 'Prospect' else f.MEMBERSHIP_STATE end AS test_start_membership_state
    ,CASE WHEN f.MEMBERSHIP_STATE = 'VIP' then datediff('month',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime) + 1 else null end AS test_start_vip_month_tenure
    ,CASE WHEN f.MEMBERSHIP_STATE = 'Lead' then datediff('day',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime) + 1 else null end AS test_start_lead_daily_tenure
    ,CASE WHEN  t.STORE_REGION = 'NA' and btt.locale ilike ANY ('%US%', '%CA%', '%.com%') OR btt.locale is null then TRUE
           when t.STORE_REGION = 'EU' and btt.locale ilike ANY ('%NL%', '%DE%', '%ES%', '%SE%', '%UK%', '%DK%', '%FR%', '%eu%') OR btt.locale is null then TRUE
           else FALSE end as is_valid_session
    ,IS_BOT
    ,t.store_brand
    ,btt.yty_exclusion
    ,case when btt.yty_exclusion = true and t.store_brand = 'Yitty' then FALSE else TRUE end as is_valid_brand_session
    ,s.platform
    ,case when ((btt.membershipStatus ilike '%visitor%' OR btt.loggedIn = FALSE) and test_start_membership_state <> 'Prospect'
        or btt.membershipStatus ilike '%loggedOut%' and test_start_membership_state <> 'Prospect'
        or btt.membershipStatus ilike '%lead%' and test_start_membership_state NOT IN ('Lead')
        or btt.membershipStatus ilike '%vip%' and test_start_membership_state <> 'VIP'
        or (test_start_membership_state = 'Prospect' and IS_BOT = TRUE)) then FALSE
        else TRUE end as is_valid_membership_session
    , case when ((btt.platform = 'web' and s.platform = 'Mobile App')
    or (btt.platform = 'mobile_app' and s.platform <> 'Mobile App')) then FALSE
    else TRUE end as is_valid_platform_session
FROM _eligible_tests as oo
JOIN _min_max_datetimes_from_api as max on max.brand = oo.brand
    and max.builder_id = oo.builder_id
left join _builder_test_targeting_from_api btt on oo.builder_id = btt.builder_id
join _builder_session_base_from_api as sd on sd.brand = oo.brand
    and sd.builder_id= oo.builder_id
join reporting_base_prod.SHARED.SESSION as s on s.meta_original_session_id = sd.SESSION_ID::number
join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
    and t.store_type <> 'Retail'
LEFT JOIN edw_prod.reference.store_timezone AS st ON st.store_id = t.store_id
LEFT JOIN edw_prod.DATA_MODEL.FACT_MEMBERSHIP_EVENT f ON f.customer_id = s.customer_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_START_LOCAL_DATETIME)::datetime <= convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_END_LOCAL_DATETIME)::datetime > convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime
WHERE
    convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime between max.min_abt_start_datetime_utc and '9999-12-01' -- is this still needed?
    and CONVERT_TIMEZONE('America/Los_Angeles', s.SESSION_LOCAL_DATETIME)::datetime between max.min_abt_start_datetime_utc and '9999-12-01' -- is this still needed?
    and IS_TEST_CUSTOMER_ACCOUNT = FALSE

union all

SELECT distinct
    'Builder' as test_type
    ,'N/A' as TEST_FRAMEWORK_TICKET
    ,'AGG CONSTRUCTOR' as test_key
    ,'AGG CONSTRUCTOR' as test_name
    ,'AGG CONSTRUCTOR' as test_label
    ,'AGG CONSTRUCTOR' as builder_id
    ,oo.brand
    -- Targeting
    ,btt.locale as region
    ,btt.platform as test_platforms
    ,btt.membershipStatus as membership_state
    ,btt.gender as gender
    ,oo.path as test_start_location
    ,null test_split
    ,oo.adjusted_activated_datetime_pst as test_activated_datetime_pst
    ,oo.adjusted_activated_datetime_utc as test_activated_datetime_utc
    ,oo.status -- previosly from oof, now from _min_max_datetimes Q: Is this still needed?  If not, we can remove all of _min_max_datetimes
    ,sd.SESSION_ID
    ,sd.test_group as ab_test_segment
    ,null test_variation_id
    ,sd.test_group
    ,sd.test_group AS test_group_description
    ,t.STORE_REGION
    ,convert_timezone('UTC','America/Los_Angeles',abt_start_datetime_utc)::datetime as test_start_datetime_added_pst
    ,CASE WHEN f.MEMBERSHIP_STATE IS NULL THEN 'Prospect' else f.MEMBERSHIP_STATE end AS test_start_membership_state
    ,CASE WHEN f.MEMBERSHIP_STATE = 'VIP' then datediff('month',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime) + 1 else null end AS test_start_vip_month_tenure
    ,CASE WHEN f.MEMBERSHIP_STATE = 'Lead' then datediff('day',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime) + 1 else null end AS test_start_lead_daily_tenure
    ,CASE WHEN  t.STORE_REGION = 'NA' and btt.locale ilike ANY ('%US%', '%CA%', '%.com%') OR btt.locale is null then TRUE
           when t.STORE_REGION = 'EU' and btt.locale ilike ANY ('%NL%', '%DE%', '%ES%', '%SE%', '%UK%', '%DK%', '%FR%', '%eu%') OR btt.locale is null then TRUE
           else FALSE end as is_valid_session
    ,IS_BOT
    ,t.store_brand
    ,btt.yty_exclusion
    ,case when btt.yty_exclusion = true and t.store_brand = 'Yitty' then FALSE else TRUE end as is_valid_brand_session
    ,s.platform
    ,case when ((btt.membershipStatus ilike '%visitor%' OR btt.loggedIn = FALSE) and test_start_membership_state <> 'Prospect'
        or btt.membershipStatus ilike '%loggedOut%' and test_start_membership_state <> 'Prospect'
        or btt.membershipStatus ilike '%lead%' and test_start_membership_state NOT IN ('Lead')
        or btt.membershipStatus ilike '%vip%' and test_start_membership_state <> 'VIP'
        or (test_start_membership_state = 'Prospect' and IS_BOT = TRUE)) then FALSE
        else TRUE end as is_valid_membership_session
    , case when ((btt.platform = 'web' and s.platform = 'Mobile App')
    or (btt.platform = 'mobile_app' and s.platform <> 'Mobile App')) then FALSE
    else TRUE end as is_valid_platform_session
FROM _eligible_tests as oo
JOIN _min_max_datetimes_from_api as max on max.brand = oo.brand
    and max.builder_id = oo.builder_id
left join _builder_test_targeting_from_api btt on oo.builder_id = btt.builder_id
join _builder_session_base_from_api as sd on sd.brand = oo.brand
    and sd.builder_id= oo.builder_id
join reporting_base_prod.SHARED.SESSION as s on s.meta_original_session_id = sd.SESSION_ID::number
join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
    and t.store_type <> 'Retail'
LEFT JOIN edw_prod.reference.store_timezone AS st ON st.store_id = t.store_id
LEFT JOIN edw_prod.DATA_MODEL.FACT_MEMBERSHIP_EVENT f ON f.customer_id = s.customer_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_START_LOCAL_DATETIME)::datetime <= convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_END_LOCAL_DATETIME)::datetime > convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime
WHERE
    convert_timezone('UTC','America/Los_Angeles',sd.abt_start_datetime_utc)::datetime between max.min_abt_start_datetime_utc and '9999-12-01' -- is this still needed?
    and CONVERT_TIMEZONE('America/Los_Angeles', s.SESSION_LOCAL_DATETIME)::datetime between max.min_abt_start_datetime_utc and '9999-12-01' -- is this still needed?
    and IS_TEST_CUSTOMER_ACCOUNT = FALSE
    and constructor_test = true;

-- select test_key,test_group,region,test_start_location,test_activated_datetime_pst,test_activated_datetime_utc,count(*) as sessions,count(distinct SESSION_ID) as sessions_distinct,sessions - sessions_distinct as diff
-- from _non_framework_builder_from_api
-- group by all order by sessions

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.session_ab_test_builder as --comment
--CREATE OR REPLACE TEMPORARY TABLE _session_ab_test_builder as --comment
select
    9999 as TEST_FRAMEWORK_ID
     ,s.test_label
     ,s.test_label as TEST_FRAMEWORK_DESCRIPTION
     ,s.TEST_FRAMEWORK_TICKET
     ,s.TEST_KEY::varchar CAMPAIGN_CODE
     ,s.TEST_TYPE
     ,'OOF' builder_data_source
     ,test_start_location
     ,s.test_activated_datetime_pst as cms_min_test_start_datetime_hq
     ,test_activated_datetime_utc
     ,null as CMS_START_DATE
     ,null as CMS_END_DATE
     ,m.STORE_ID
     ,r.store_brand
     ,r.STORE_REGION
     ,r.STORE_COUNTRY
     ,STORE_NAME
     ,SPECIALTY_COUNTRY_CODE
     ,s.TEST_KEY::varchar as test_key
     ,null test_variation_id
     ,null test_variation_name
     ,case when test_group = 'Control' then 1
         when test_group in ('Variant','Variant A') then 2
         when test_group = 'Variant B' then 3
         when test_group = 'Variant C' then 4
          when test_group = 'Variant D' then 5
         end as AB_TEST_SEGMENT
     ,s.test_group
     ,TRUE is_valid_membership_session
     ,TRUE is_valid_platform_session
     ,is_valid_brand_session
     ,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz as AB_TEST_START_LOCAL_DATETIME
     ,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz::date as AB_TEST_START_LOCAL_DATE
     ,date_trunc('month',CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz::date) as AB_TEST_START_LOCAL_MONTH_DATE
     ,rank() over (partition by m.customer_id,s.TEST_KEY,test_start_datetime_added_pst ORDER BY s.session_id asc) as rnk_monthly

     ,m.SESSION_LOCAL_DATETIME
     ,date_trunc('month',m.SESSION_LOCAL_DATETIME)::date as SESSION_LOCAL_MONTH_DATE
     ,m.VISITOR_ID
     ,m.SESSION_ID
     ,m.META_ORIGINAL_SESSION_ID
     ,m.CUSTOMER_ID
     ,m.MEMBERSHIP_ID

     -- membership
     ,m.MEMBERSHIP_STATE as SESSION_START_MEMBERSHIP_STATE
     ,s.TEST_START_MEMBERSHIP_STATE

     ,s.TEST_START_LEAD_DAILY_TENURE

     ,case when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) between 1 and 3 THEN concat('H',datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1)
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) BETWEEN 4 and 24 THEN 'D1'
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) between 2 and 30 then concat('D',TEST_START_LEAD_DAILY_TENURE)
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) >= 31 then 'M2+'
           ELSE null end as TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE
     ,case when s.TEST_START_VIP_MONTH_TENURE between 13 and 24 then 'M13-24'
           when s.TEST_START_VIP_MONTH_TENURE >= 25 then 'M25+'
           else concat('M',s.TEST_START_VIP_MONTH_TENURE) end as TEST_START_VIP_MONTH_TENURE_GROUP

     ,price as MEMBERSHIP_PRICE
     ,t.LABEL as MEMBERSHIP_TYPE
     ,case when s.TEST_START_MEMBERSHIP_STATE = 'VIP' then CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE else null end as CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE
     ,case when s.TEST_START_MEMBERSHIP_STATE = 'VIP' then CUMULATIVE_CASH_GROSS_PROFIT_DECILE else null end as CUMULATIVE_CASH_GROSS_PROFIT_DECILE
     --tech
     ,case when lower(m.platform) in ('desktop','tablet') then 'Desktop'
           when lower(m.platform) in ('mobile','unknown') then 'Mobile'
           when m.platform = 'Mobile App' then 'App' else m.platform end as PLATFORM
     ,m.PLATFORM AS platform_raw
     ,case when os in ('Mac OS','Mac OSX','iOS') then 'Apple'
           when os = 'Unknown' or os is null then 'Unknown'
           else 'Android' end as OS
     ,OS AS OS_RAW
     ,BROWSER
     --quiz / reg funnel
     ,IS_QUIZ_START_ACTION
     ,IS_QUIZ_COMPLETE_ACTION
     ,IS_SKIP_QUIZ_ACTION
     ,IS_LEAD_REGISTRATION_ACTION
     ,IS_QUIZ_REGISTRATION_ACTION
     ,IS_SPEEDY_REGISTRATION_ACTION
     ,IS_SKIP_QUIZ_REGISTRATION_ACTION
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_reg_type

     ,REGISTRATION_LOCAL_DATETIME
     -- flags for FL
     ,case when lower(GATEWAY_NAME) ilike 'yt_%%' or lower(GATEWAY_NAME) ilike '%%ytyus%%' then TRUE else FALSE end IS_YITTY_GATEWAY
     ,case when lower(GATEWAY_NAME) ilike '%%flm%%' then TRUE else FALSE end IS_MALE_GATEWAY
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_SESSION_ACTION end IS_MALE_SESSION_ACTION
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_SESSION end IS_MALE_SESSION
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_CUSTOMER end IS_MALE_CUSTOMER
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A'
           when r.STORE_BRAND = 'Fabletics' and IS_MALE_SESSION = TRUE or IS_MALE_CUSTOMER = TRUE then TRUE else FALSE end AS IS_MALE_SESSION_CUSTOMER

     ,coalesce(IS_PDP_VISIT,false)::BOOLEAN as IS_PDP_VISIT
     ,IS_ATB_ACTION
     ,IS_SKIP_MONTH_ACTION
     ,IS_LOGIN_ACTION
    ,coalesce(IS_CART_CHECKOUT_VISIT,false)::BOOLEAN as IS_CART_CHECKOUT_VISIT
     --acquisition
     ,h.CHANNEL
     ,h.SUBCHANNEL
     ,g.GATEWAY_TYPE
     ,g.GATEWAY_SUB_TYPE
     ,m.DM_GATEWAY_ID
     ,g.GATEWAY_CODE
     ,g.GATEWAY_NAME
     ,m.DM_SITE_ID

     ,DEFAULT_STATE_PROVINCE as customer_state
     ,HOW_DID_YOU_HEAR as HDYH

     ,null as IS_CMS_SPECIFIC_GENDER
     ,case when s.gender = 'Women Only' and test_start_membership_state = 'Prospect' and IS_MALE_SESSION_CUSTOMER = FALSE then TRUE
           when s.gender = 'Women Only' and test_start_membership_state <> 'Prospect' and IS_MALE_CUSTOMER = FALSE then TRUE
           when s.gender = 'Men Only' and test_start_membership_state = 'Prospect' and IS_MALE_SESSION_CUSTOMER = TRUE then TRUE
           when s.gender = 'Men Only' and test_start_membership_state <> 'Prospect' and IS_MALE_CUSTOMER = TRUE then TRUE
           when s.gender = 'Men and Women' and test_start_membership_state = 'Prospect' then TRUE
           when s.gender = 'Men and Women' and test_start_membership_state <> 'Prospect' then TRUE
           else false end as is_valid_gender_sessions
     ,m.IS_BOT
     ,null as IS_CMS_SPECIFIC_LEAD_DAY
     ,null as flag_is_cms_filtered_lead
     ,m.is_migrated_session
     ,test_start_page_path
    ,$current_end_date_hq as hq_max_datetime
from _non_framework_builder as s
JOIN reporting_base_prod.SHARED.SESSION AS m ON s.SESSION_ID = m.meta_original_session_id
join edw_prod.DATA_MODEL.DIM_STORE as r on r.STORE_ID = m.STORE_ID
LEFT JOIN reporting_base_prod.SHARED.DIM_GATEWAY g ON g.dm_gateway_id = m.dm_gateway_id
    and EFFECTIVE_START_DATETIME <= m.SESSION_LOCAL_DATETIME
    and EFFECTIVE_END_DATETIME > m.SESSION_LOCAL_DATETIME
left join edw_prod.DATA_MODEL.dim_customer as dc on dc.CUSTOMER_ID = m.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.CUSTOMER_ID = dc.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE as t on t.MEMBERSHIP_TYPE_ID = p.MEMBERSHIP_TYPE_ID
left join reporting_base_prod.SHARED.MEDIA_SOURCE_CHANNEL_MAPPING as h on h.MEDIA_SOURCE_HASH = m.MEDIA_SOURCE_HASH
left join (select distinct ltd.customer_id,SOURCE_ACTIVATION_LOCAL_DATETIME,SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,ltd.VIP_COHORT_MONTH_DATE,CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,CUMULATIVE_CASH_GROSS_PROFIT_DECILE
            from edw_prod.analytics_base.customer_lifetime_value_LTD as ltd
            join edw_prod.DATA_MODEL.FACT_ACTIVATION as f on f.ACTIVATION_KEY = ltd.ACTIVATION_KEY and f.customer_id = ltd.customer_id) as ltd
                on ltd.customer_id = m.customer_id and SESSION_LOCAL_DATETIME between SOURCE_ACTIVATION_LOCAL_DATETIME and SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME
where is_valid_session = TRUE and IS_TEST_CUSTOMER_ACCOUNT = FALSE and is_valid_brand_session = TRUE

union all

select
    9999 as TEST_FRAMEWORK_ID
     ,builder_id as test_label
     ,s.test_label::varchar(512) as TEST_FRAMEWORK_DESCRIPTION
     ,s.TEST_FRAMEWORK_TICKET::varchar(256) as test_framework_ticket
     ,s.TEST_KEY::varchar as CAMPAIGN_CODE
     ,s.TEST_TYPE::VARCHAR(256) as test_type
     ,'API' builder_data_source
     ,test_start_location
     ,s.test_activated_datetime_pst::TIMESTAMP_NTZ(9) as cms_min_test_start_datetime_hq
     ,test_activated_datetime_utc::TIMESTAMP_NTZ(9) as test_activated_datetime_utc
     ,null as CMS_START_DATE
     ,null as CMS_END_DATE
     ,m.STORE_ID
     ,r.store_brand
     ,r.STORE_REGION
     ,r.STORE_COUNTRY
     ,STORE_NAME
     ,SPECIALTY_COUNTRY_CODE
     ,s.TEST_KEY::varchar as test_key
     ,s.test_variation_id
     ,test_group_description as test_variation_name
     ,case when test_group in ('Control', 'control') then 1
         when test_group in ('Variant','Variant A', 'Variant_1') then 2
         when test_group IN('Variant B', 'Variant_2') then 3
         when test_group in( 'Variant C', 'Variant_3') then 4
          when test_group in ('Variant D', 'Variant_4') then 5
         end as AB_TEST_SEGMENT
     ,s.test_group::VARCHAR(9) as test_group
     ,s.is_valid_membership_session
     ,s.is_valid_platform_session
     ,s.is_valid_brand_session
     ,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz as AB_TEST_START_LOCAL_DATETIME
     ,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz::date as AB_TEST_START_LOCAL_DATE
     ,date_trunc('month',CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz::date) as AB_TEST_START_LOCAL_MONTH_DATE
     ,rank() over (partition by m.customer_id,s.TEST_KEY,test_start_datetime_added_pst ORDER BY s.session_id asc) as rnk_monthly

     ,m.SESSION_LOCAL_DATETIME
     ,date_trunc('month',m.SESSION_LOCAL_DATETIME)::date as SESSION_LOCAL_MONTH_DATE
     ,m.VISITOR_ID
     ,m.SESSION_ID
     ,m.META_ORIGINAL_SESSION_ID
     ,m.CUSTOMER_ID
     ,m.MEMBERSHIP_ID

     -- membership
     ,m.MEMBERSHIP_STATE as SESSION_START_MEMBERSHIP_STATE
     ,s.TEST_START_MEMBERSHIP_STATE

     ,s.TEST_START_LEAD_DAILY_TENURE

     ,case when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) between 1 and 3 THEN concat('H',datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1)
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) BETWEEN 4 and 24 THEN 'D1'
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) between 2 and 30 then concat('D',TEST_START_LEAD_DAILY_TENURE)
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) >= 31 then 'M2+'
           ELSE null end as TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE
     ,case when s.TEST_START_VIP_MONTH_TENURE between 13 and 24 then 'M13-24'
           when s.TEST_START_VIP_MONTH_TENURE >= 25 then 'M25+'
           else concat('M',s.TEST_START_VIP_MONTH_TENURE) end as TEST_START_VIP_MONTH_TENURE_GROUP

     ,price as MEMBERSHIP_PRICE
     ,t.LABEL as MEMBERSHIP_TYPE
     ,case when s.TEST_START_MEMBERSHIP_STATE = 'VIP' then CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE else null end as CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE
     ,case when s.TEST_START_MEMBERSHIP_STATE = 'VIP' then CUMULATIVE_CASH_GROSS_PROFIT_DECILE else null end as CUMULATIVE_CASH_GROSS_PROFIT_DECILE
     --tech
     ,case when lower(m.platform) in ('desktop','tablet') then 'Desktop'
           when lower(m.platform) in ('mobile','unknown') then 'Mobile'
           when m.platform = 'Mobile App' then 'App' else m.platform end as PLATFORM
     ,m.PLATFORM AS platform_raw
     ,case when os in ('Mac OS','Mac OSX','iOS') then 'Apple'
           when os = 'Unknown' or os is null then 'Unknown'
           else 'Android' end as OS
     ,OS AS OS_RAW
     ,BROWSER
     --quiz / reg funnel
     ,IS_QUIZ_START_ACTION
     ,IS_QUIZ_COMPLETE_ACTION
     ,IS_SKIP_QUIZ_ACTION
     ,IS_LEAD_REGISTRATION_ACTION
     ,IS_QUIZ_REGISTRATION_ACTION
     ,IS_SPEEDY_REGISTRATION_ACTION
     ,IS_SKIP_QUIZ_REGISTRATION_ACTION
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_reg_type

     ,REGISTRATION_LOCAL_DATETIME
     -- flags for FL
     ,case when lower(GATEWAY_NAME) ilike 'yt_%%' or lower(GATEWAY_NAME) ilike '%%ytyus%%' then TRUE else FALSE end IS_YITTY_GATEWAY
     ,case when lower(GATEWAY_NAME) ilike '%%flm%%' then TRUE else FALSE end IS_MALE_GATEWAY
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_SESSION_ACTION end IS_MALE_SESSION_ACTION
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_SESSION end IS_MALE_SESSION
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_CUSTOMER end IS_MALE_CUSTOMER
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A'
           when r.STORE_BRAND = 'Fabletics' and IS_MALE_SESSION = TRUE or IS_MALE_CUSTOMER = TRUE then TRUE else FALSE end AS IS_MALE_SESSION_CUSTOMER

     ,coalesce(IS_PDP_VISIT,false)::BOOLEAN as IS_PDP_VISIT
     ,IS_ATB_ACTION
     ,IS_SKIP_MONTH_ACTION
     ,IS_LOGIN_ACTION
    ,coalesce(IS_CART_CHECKOUT_VISIT,false)::BOOLEAN as IS_CART_CHECKOUT_VISIT
     --acquisition
     ,h.CHANNEL
     ,h.SUBCHANNEL
     ,g.GATEWAY_TYPE
     ,g.GATEWAY_SUB_TYPE
     ,m.DM_GATEWAY_ID
     ,g.GATEWAY_CODE
     ,g.GATEWAY_NAME
     ,m.DM_SITE_ID

     ,DEFAULT_STATE_PROVINCE as customer_state
     ,HOW_DID_YOU_HEAR as HDYH

     ,null as IS_CMS_SPECIFIC_GENDER
     ,case when s.gender = 'Women Only' and test_start_membership_state = 'Prospect' and IS_MALE_SESSION_CUSTOMER = FALSE then TRUE
           when s.gender = 'Women Only' and test_start_membership_state <> 'Prospect' and IS_MALE_CUSTOMER = FALSE then TRUE
           when s.gender = 'Men Only' and test_start_membership_state = 'Prospect' and IS_MALE_SESSION_CUSTOMER = TRUE then TRUE
           when s.gender = 'Men Only' and test_start_membership_state <> 'Prospect' and IS_MALE_CUSTOMER = TRUE then TRUE
           when s.gender = 'Men and Women' and test_start_membership_state = 'Prospect' then TRUE
           when s.gender = 'Men and Women' and test_start_membership_state <> 'Prospect' then TRUE
           else false end as is_valid_gender_sessions
     ,m.IS_BOT
     ,null as IS_CMS_SPECIFIC_LEAD_DAY
     ,null as flag_is_cms_filtered_lead
     ,m.is_migrated_session
     ,null test_start_page_path
    ,$current_end_date_hq as hq_max_datetime
from _non_framework_builder_from_api as s
JOIN reporting_base_prod.SHARED.SESSION AS m ON s.SESSION_ID = m.meta_original_session_id
join edw_prod.DATA_MODEL.DIM_STORE as r on r.STORE_ID = m.STORE_ID
LEFT JOIN reporting_base_prod.SHARED.DIM_GATEWAY g ON g.dm_gateway_id = m.dm_gateway_id
    and EFFECTIVE_START_DATETIME <= m.SESSION_LOCAL_DATETIME
    and EFFECTIVE_END_DATETIME > m.SESSION_LOCAL_DATETIME
left join edw_prod.DATA_MODEL.dim_customer as dc on dc.CUSTOMER_ID = m.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.CUSTOMER_ID = dc.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE as t on t.MEMBERSHIP_TYPE_ID = p.MEMBERSHIP_TYPE_ID
left join reporting_base_prod.SHARED.MEDIA_SOURCE_CHANNEL_MAPPING as h on h.MEDIA_SOURCE_HASH = m.MEDIA_SOURCE_HASH
left join (select distinct ltd.customer_id,SOURCE_ACTIVATION_LOCAL_DATETIME,SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,ltd.VIP_COHORT_MONTH_DATE,CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,CUMULATIVE_CASH_GROSS_PROFIT_DECILE
            from edw_prod.analytics_base.customer_lifetime_value_LTD as ltd
            join edw_prod.DATA_MODEL.FACT_ACTIVATION as f on f.ACTIVATION_KEY = ltd.ACTIVATION_KEY and f.customer_id = ltd.customer_id) as ltd
                on ltd.customer_id = m.customer_id and SESSION_LOCAL_DATETIME between SOURCE_ACTIVATION_LOCAL_DATETIME and SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME
where
    is_valid_session = TRUE
    and IS_TEST_CUSTOMER_ACCOUNT = FALSE
    and is_valid_brand_session = TRUE
    and is_valid_membership_session = TRUE
    and is_valid_platform_session = TRUE;

ALTER TABLE reporting_base_prod.shared.session_ab_test_builder SET DATA_RETENTION_TIME_IN_DAYS = 0; --comment

-- select builder_data_source,test_key,test_label,test_group,AB_TEST_SEGMENT,platform,count(*) AS SESSIONS,count(distinct SESSION_ID) AS SESSIONS_DISTINCT, SESSIONS - SESSIONS_DISTINCT AS DIFF
-- from reporting_base_prod.shared.session_ab_test_builder
-- -- from _session_ab_test_builder
-- group by 1,2,3,4,5,6 order by 1 asc


