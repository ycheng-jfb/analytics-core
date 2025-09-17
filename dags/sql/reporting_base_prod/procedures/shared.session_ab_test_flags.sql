
CREATE OR REPLACE TEMPORARY TABLE _last_3_months as
select distinct TEST_KEY,test_label,test_framework_id,STORE_BRAND,CMS_MIN_TEST_START_DATETIME_HQ,max(AB_TEST_START_LOCAL_DATETIME) as max_AB_TEST_START_LOCAL_DATETIME
from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_METADATA --where TEST_KEY = 'CANCELFIRSTORDER_v4'
group by 1,2,3,4,5

union

select distinct TEST_KEY,test_label,test_framework_id,STORE_BRAND,CMS_MIN_TEST_START_DATETIME_HQ,current_date as max_AB_TEST_START_LOCAL_DATETIME
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
where CAMPAIGN_CODE in ('MYVIPSNOOZEOPTION','SAVEOFFERSEGMENTATIONLITE')
group by 1,2,3,4,5;

delete from  _last_3_months
    where max_AB_TEST_START_LOCAL_DATETIME <= CURRENT_DATE - INTERVAL '3 MONTH';

-- --sxf byo custom flag - identify vip visitors that have outstanding membership credits at abt start
-- CREATE OR REPLACE TEMPORARY TABLE _SXF1378_v2_byo_custom_flag as
-- select distinct
--     test_framework_id
--     ,TEST_KEY
--     ,test_label
--     ,CUSTOMER_ID
--     ,minimum_session_id
--     ,minimum_ab_test_start_local_datetime
--     ,null as record_type
--     ,sum(outstanding_mem_credit_balance) as outstanding_mem_credit_balance_ab_test_start
-- from (select
--           test_framework_id
--            ,test_key
--            ,test_label
--            ,cc.CUSTOMER_ID
--            ,minimum_ab_test_start_local_datetime
--            ,minimum_session_id
--            ,case when e.CREDIT_ACTIVITY_TYPE = 'Issued' then e.CREDIT_ACTIVITY_GROSS_VAT_LOCAL_AMOUNT else -e.CREDIT_ACTIVITY_GROSS_VAT_LOCAL_AMOUNT end as outstanding_mem_credit_balance
--            ,CREDIT_ID
--            ,IS_BOP_VIP
--            ,CREDIT_ACTIVITY_TYPE
--            ,CREDIT_ACTIVITY_LOCAL_DATETIME
--       from (select
--                 test_framework_id
--                  ,test_key
--                  ,test_label
--                  ,CUSTOMER_ID
--                  ,min(SESSION_ID) as minimum_session_id
--                  ,min(AB_TEST_START_LOCAL_DATETIME) as minimum_ab_test_start_local_datetime
--             from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA
--             where
--                     TEST_KEY = 'SXF1378_v2'
--               and TEST_START_MEMBERSHIP_STATE = 'VIP'
--             group by 1,2,3,4) as cc
--                join edw_prod.DATA_MODEL.DIM_CREDIT as cr on cc.CUSTOMER_ID = cr.CUSTOMER_ID
--                join reporting_base_prod.SHARED.FACT_CREDIT_EVENT as e on e.CREDIT_KEY = cr.CREDIT_KEY
--       where
--               CREDIT_TENDER = 'Cash' --and cc.CUSTOMER_ID = 576366046
--         and CREDIT_ACTIVITY_LOCAL_DATETIME <= cc.minimum_ab_test_start_local_datetime)
-- group by 1,2,3,4,5,6,7;
--
-- CREATE OR REPLACE TEMPORARY TABLE _jfg1722_flag_ssu_sign_in as
-- select distinct m.customer_id,TEST_KEY,test_label,'customer' as record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as d on d.CUSTOMER_ID = m.CUSTOMER_ID
-- where
--     name = 'signup_source'
--     and lower(value) ilike 'speedy_signup'
--     and m.TEST_START_MEMBERSHIP_STATE = 'Prospect'
--     and m.TEST_KEY ilike 'jfg1722%';
--
-- CREATE OR REPLACE TEMPORARY TABLE _jfg2397_passwordless_reg as
-- select distinct m.SESSION_ID,TEST_KEY,'customer' as record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as d on d.CUSTOMER_ID = m.CUSTOMER_ID
-- where
--     lower(name) ilike 'autogen%'
--     and IS_LEAD_REGISTRATION_ACTION = TRUE
--     and m.campaign_code = 'jfg2397';
--
-- CREATE OR REPLACE TEMPORARY TABLE _fl_snooze_1191 as
-- select distinct m.SESSION_ID,TEST_KEY,null as record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join lake.segment_fl.JAVASCRIPT_FABLETICS_PAGE as p on try_to_number(properties_session_id) = edw_prod.stg.udf_unconcat_brand(m.SESSION_ID)
-- where
--     edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) = 119120
--     and (PROPERTIES_PATH ilike '%my-vip%' and PROPERTIES_PAGE_NAME ilike '%my-vip%');
--
-- CREATE OR REPLACE TEMPORARY TABLE _fl_oc_1215_paused_pre_test_start as
-- select distinct m.SESSION_ID,TEST_KEY,AB_TEST_START_LOCAL_DATETIME,DATE_START,DATE_END,null as record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.membership as d on d.CUSTOMER_ID = m.CUSTOMER_ID
-- join LAKE_CONSOLIDATED_VIEW.ultra_merchant.membership_snooze as z on z.membership_id = d.membership_id
-- where
--     edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) = 1215
--     and m.AB_TEST_START_LOCAL_DATETIME between DATE_START and DATE_END;
--
-- CREATE OR REPLACE TEMPORARY TABLE _fl_oc_1215_used_pause_offer as
-- select distinct m.SESSION_ID,TEST_KEY,AB_TEST_START_LOCAL_DATETIME,m.CUSTOMER_ID,null as record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join
--     (select distinct
--          CUSTOMER_ID
--         ,ORDER_LOCAL_DATETIME
--     from reporting_base_prod.shared.ORDER_LINE_PSOURCE_PAYMENT
--     where PROMO_CODE = 'ONLINECANCELLATIONTEST') as c on c.CUSTOMER_ID = m.CUSTOMER_ID
--             and ORDER_LOCAL_DATETIME < AB_TEST_START_LOCAL_DATETIME
-- where
--     edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) = 1215;
--
-- CREATE OR REPLACE TEMPORARY TABLE _gfb_shipping_tests_1268_1270_1271 as
-- select test_key,CUSTOMER_ID,null record_type,min(min_session_id) as min_session_id
-- from
--     (select distinct test_key,m.CUSTOMER_ID,min(SESSION_ID) as min_session_id
--     from lake.SEGMENT_GFB.JAVASCRIPT_FABKIDS_PAGE as p
--     join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m on edw_prod.stg.udf_unconcat_brand(m.CUSTOMER_ID) = try_to_number(p.PROPERTIES_CUSTOMER_ID)
--     join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = m.STORE_ID
--     where
--         edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) in (1268,1270,1271)
--         and convert_timezone('UTC',STORE_TIME_ZONE,to_timestamp(timestamp))::datetime >= AB_TEST_START_LOCAL_DATETIME
--         and timestamp::timestamp >= '2023-05-01'
--         and (PROPERTIES_PATH ilike '/cart%'
--             or PROPERTIES_PATH ilike '/checkout%'
--             or PROPERTIES_PATH in ('/terms','/terms.htm','/policy/shipping','/how-it-works.htm','/how-it-works'))
--     group by 1,2
--
--     union
--
--     select distinct test_key,m.CUSTOMER_ID,min(SESSION_ID) as min_session_id
--     from LAKE.SEGMENT_GFB.JAVA_FABKIDS_PRODUCT_ADDED as p
--     join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m on edw_prod.stg.udf_unconcat_brand(m.CUSTOMER_ID) = try_to_number(p.PROPERTIES_CUSTOMER_ID)
--     join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = m.STORE_ID
--     where
--         edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) in (1268,1270,1271)
--         and convert_timezone('UTC',STORE_TIME_ZONE,to_timestamp(timestamp))::datetime >= AB_TEST_START_LOCAL_DATETIME
--         and timestamp::timestamp >= '2023-05-01'
--     group by 1,2)
-- group by 1,2
--
-- union all
--
-- select test_key,CUSTOMER_ID,null record_type,min(min_session_id) as min_session_id
-- from
--     (select distinct test_key,m.CUSTOMER_ID,min(SESSION_ID) as min_session_id
--     from lake.SEGMENT_GFB.JAVASCRIPT_JUSTFAB_PAGE as p
--     join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m on edw_prod.stg.udf_unconcat_brand(m.CUSTOMER_ID) = try_to_number(p.PROPERTIES_CUSTOMER_ID)
--     join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = m.STORE_ID
--     where
--         edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) in (1268,1270,1271)
--         and convert_timezone('UTC',STORE_TIME_ZONE,to_timestamp(timestamp))::datetime >= AB_TEST_START_LOCAL_DATETIME
--         and timestamp::timestamp >= '2023-05-01'
--         and (PROPERTIES_PATH ilike '/cart%'
--             or PROPERTIES_PATH ilike '/checkout%'
--             or PROPERTIES_PATH ilike '/remixcheckout%'
--             or PROPERTIES_PATH in ('/terms','/policy/shipping','/how-it-works'))
--     group by 1,2
--
--     union
--
--     select distinct test_key,m.CUSTOMER_ID,min(SESSION_ID) as min_session_id
--     from LAKE.SEGMENT_GFB.JAVA_JUSTFAB_PRODUCT_ADDED as p
--     join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m on edw_prod.stg.udf_unconcat_brand(m.CUSTOMER_ID) = try_to_number(p.PROPERTIES_CUSTOMER_ID)
--     join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = m.STORE_ID
--     where
--         edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) in (1268,1270,1271)
--         and convert_timezone('UTC',STORE_TIME_ZONE,to_timestamp(timestamp))::datetime >= AB_TEST_START_LOCAL_DATETIME
--         and timestamp::timestamp >= '2023-05-01'
--     group by 1,2)
-- group by 1,2
--
-- union all
--
-- select test_key,CUSTOMER_ID,null record_type,min(min_session_id) as min_session_id
-- from
--     (select distinct test_key,m.CUSTOMER_ID,min(SESSION_ID) as min_session_id
--     from lake.SEGMENT_GFB.JAVASCRIPT_JUSTFAB_PAGE as p
--     join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m on edw_prod.stg.udf_unconcat_brand(m.CUSTOMER_ID) = try_to_number(p.PROPERTIES_CUSTOMER_ID)
--     join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = m.STORE_ID
--     where
--         edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) in (1268,1270,1271)
--         and convert_timezone('UTC',STORE_TIME_ZONE,to_timestamp(timestamp))::datetime >= AB_TEST_START_LOCAL_DATETIME
--         and timestamp::timestamp >= '2023-05-01'
--         and (PROPERTIES_PATH ilike '/cart%'
--             or PROPERTIES_PATH ilike '/checkout%'
--             or PROPERTIES_PATH in ('/terms','/terms.htm','/policy/shipping','/how-it-works.htm','/how-it-works'))
--     group by 1,2
--
--     union
--
--     select distinct test_key,m.CUSTOMER_ID,min(SESSION_ID) as min_session_id
--     from LAKE.SEGMENT_GFB.JAVA_SHOEDAZZLE_PRODUCT_ADDED as p
--     join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m on edw_prod.stg.udf_unconcat_brand(m.CUSTOMER_ID) = try_to_number(p.PROPERTIES_CUSTOMER_ID)
--     join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = m.STORE_ID
--     where
--         edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) in (1268,1270,1271)
--         and convert_timezone('UTC',STORE_TIME_ZONE,to_timestamp(timestamp))::datetime >= AB_TEST_START_LOCAL_DATETIME
--         and timestamp::timestamp >= '2023-05-01'
--     group by 1,2)
-- group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _fl_passwordless_login  as
select distinct m.CUSTOMER_ID,TEST_KEY,'other' record_type,min(m.SESSION_ID) as min_session_id
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join
    (
        select PROPERTIES_SESSION_ID,min(convert_timezone('UTC','America/Los_Angeles',to_timestamp(timestamp)::datetime)) as login_datetime_pst
        from lake.SEGMENT_FL.JAVASCRIPT_FABLETICS_PAGE as p
        where
            PROPERTIES_PAGE_NAME ilike '/login%'
            and timestamp >= '2024-06-01'
    group by 1
    ) as d on d.PROPERTIES_SESSION_ID = edw_prod.stg.udf_unconcat_brand(session_id)
where
    m.TEST_FRAMEWORK_ID = 129220
    and login_datetime_pst >= AB_TEST_START_LOCAL_DATETIME
group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _fl_oc_cancel_first_order  as
select distinct m.CUSTOMER_ID,TEST_KEY,'customer' record_type,min(m.SESSION_ID) as min_session_id
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join lake_consolidated_view.ultra_merchant.CUSTOMER_DETAIL as d on d.CUSTOMER_ID = m.CUSTOMER_ID
where
    lower(name) ilike 'cancelorderpopup_no_clicked_count'
    and m.TEST_KEY = 'CANCELFIRSTORDER_v4'
    and d.DATETIME_ADDED >= AB_TEST_START_LOCAL_DATETIME
group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _fl_save_offer_segmentation_1216  as
select distinct m.CUSTOMER_ID,TEST_KEY,'membership' record_type,min(m.SESSION_ID) as min_session_id
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join lake_consolidated_view.ultra_merchant.membership_detail as d on d.membership_id = m.membership_id
where
    lower(name) ilike 'saveoffer_lowest'
    and m.campaign_code = 'SAVEOFFERSEGMENTATIONLITE'
group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _fl_builder_algo_id_dec_2023_adid_experience_f1 as
select distinct m.CUSTOMER_ID,TEST_KEY,'other' record_type,m.SESSION_ID,experience_flag
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join lake_consolidated_view.ultra_merchant.membership_detail as d on d.membership_id = m.membership_id
left join work.dbo.fl_ad_id_algo_post_reg_dec2023_adj as flag on flag.SESSION_ID = m.SESSION_ID
    and test_label =
        'FLW Postreg HP | Postreg - Womens standard_2legging_offer AB Test (Dec 2023) - Control | Postreg - Womens standard_2legging_offer AB Test (Dec 2023) - Test'
WHERE
    test_label =
        'FLW Postreg HP | Postreg - Womens standard_2legging_offer AB Test (Dec 2023) - Control | Postreg - Womens standard_2legging_offer AB Test (Dec 2023) - Test'

    and experience_flag ilike 'ad_id%'
;

CREATE OR REPLACE TEMPORARY TABLE _fl_builder_algo_id_dec_2023_geo_experience_f2 as
select distinct m.CUSTOMER_ID,TEST_KEY,'other' record_type,m.SESSION_ID,experience_flag
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join lake_consolidated_view.ultra_merchant.membership_detail as d on d.membership_id = m.membership_id
join work.dbo.fl_ad_id_algo_post_reg_dec2023_adj as flag on flag.SESSION_ID = m.SESSION_ID
WHERE
test_label =
        'FLW Postreg HP | Postreg - Womens standard_2legging_offer AB Test (Dec 2023) - Control | Postreg - Womens standard_2legging_offer AB Test (Dec 2023) - Test'
and experience_flag ilike 'geo%';

-------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE _personalization_feb2024_t1 AS
    SELECT DISTINCT session_id,
                    customer_id,
                    test_key,
                    test_label,
                    test_group,
                    store_id,
                    date_trunc('SECOND', convert_timezone('America/Los_Angeles', ab_test_start_local_datetime)) AS ab_test_start_datetime
    FROM reporting_base_prod.shared.session_ab_test_metadata
    WHERE test_label IN
          ('FLW Postreg HP | Postreg - Womens standard_2legging_offer AB Test (Dec 2023) - Control | Postreg - Womens standard_2legging_offer AB Test (Dec 2023) - Test',
           'YITTY Post Reg Ad ID | Homepage - Postreg - Yitty - 6/5 Postreg Extension - Postreg Segment Control | Postreg - Yitty - Postreg Extension - Personalization AB Test (June 2024) - ad_id Grid Test',
           'Postreg - Mens 80% Off + BAU Gateway - Personalization AB Test (June 2024) - Control  | Postreg - Mens 80% Off + BAU Gateway - Personalization AB Test (June 2024) - Control  | Postreg - Mens 80% Off + BAU Gateway - Personalization AB Test (June 2024) - Perso Grid Test ',
           'Postreg - Scrubs - Men''s Personalization AB Test (June 2024) - Control | Postreg - Scrubs - Men''s Personalization AB Test (June 2024) - Control | Postreg - Scrubs - Men''s Personalization AB Test (June 2024) - Perso Grid Test',
           'Postreg - Scrubs - Women''s Personalization AB Test (June 2024) - Control | Postreg - Scrubs - Women''s Personalization AB Test (June 2024) - Control | Postreg - Scrubs - Women''s Personalization AB Test (June 2024) - Perso Grid Test',
           'Postreg - Mens - US - Personalization AB Test (June 2024) - Control | Postreg - Mens - US - Personalization AB Test (June 2024) - Control | Postreg - Mens - US - Personalization AB Test (June 2024) - Perso Grid Test'
          );

CREATE OR REPLACE TEMPORARY TABLE _personalization_feb2024_called_algo_ids AS
(SELECT DISTINCT try_to_number(plv.properties_session_id)                                                                                                                       AS session_id,
                 iff(try_parse_json(properties_algo_id) IS NULL, FALSE, TRUE)                                                                                                   AS is_json,
                 CASE WHEN is_json THEN parse_json(properties_algo_id):id::string ELSE properties_algo_id END                                                                   AS called_algo_id,
                 CASE WHEN is_json THEN parse_json(properties_algo_id):type::string
                      ELSE iff(properties_algo_id ILIKE '%W' OR properties_algo_id ILIKE '%M', 'adId', 'geo') END                                                               AS called_type,

                 date_trunc('SECOND',
                            convert_timezone('UTC', 'America/Los_Angeles', to_timestamp(min(plv.timestamp))))                                                                   AS grid_view_time --UTC
 FROM lake.segment_fl.javascript_fabletics_product_list_viewed plv
 WHERE plv.properties_algo_id != ''
   AND plv.properties_session_id IN (SELECT DISTINCT edw_prod.stg.udf_unconcat_brand(session_id) FROM _personalization_feb2024_t1)
 GROUP BY session_id, plv.properties_algo_id);

CREATE OR REPLACE TEMP TABLE _personalization_feb2024_customer_detail AS
(SELECT customer_id,
        max(CASE WHEN name = 'gender' THEN value END)       AS cd_gender,
        max(CASE WHEN name = 'shipping_zip' THEN value END) AS cd_shipping_zip,
        max(CASE WHEN name = 'utm_term' THEN value END)     AS cd_utm_term
 FROM lake_consolidated.ultra_merchant.customer_detail
 WHERE customer_id IN (SELECT customer_id FROM _personalization_feb2024_t1)
   AND name IN ('gender', 'shipping_zip', 'utm_term')
 GROUP BY customer_id);

CREATE OR REPLACE TEMP TABLE _personalization_feb2024_session_detail AS
(SELECT session_id,
        max(CASE WHEN name = 'gender' THEN value END)         AS sd_gender,
        max(CASE WHEN name = 'algorithmTrace' THEN value END) AS sd_algorithm_trace,
        max(CASE WHEN name = 'utm_term' THEN value END)       AS sd_utm_term
 FROM lake_consolidated.ultra_merchant.session_detail
 WHERE session_id IN (SELECT session_id FROM _personalization_feb2024_t1)
   AND name IN ('gender', 'algorithmTrace', 'utm_term')
 GROUP BY session_id);

CREATE OR REPLACE TEMPORARY TABLE _personalization_feb2024_ad_id_and_geo AS
(SELECT *
 FROM (SELECT DISTINCT ad_id_gender AS algo_id, min(convert_timezone('UTC', 'America/Los_Angeles', last_modified)) AS date_algo_id_was_added_to_index
       FROM reporting_prod.data_science.tableau_ad_id_historical
       GROUP BY algo_id
       ORDER BY date_algo_id_was_added_to_index DESC)
 UNION ALL
 SELECT DISTINCT concat('YTW_', zipcode) AS algo_id, to_timestamp('2023-12-18') AS date_algo_id_was_added_to_index
 FROM reporting_prod.data_science.tableau_geo_snowflake
 UNION ALL
 SELECT DISTINCT concat('FLW_', zipcode) AS algo_id, to_timestamp('2023-12-18') AS date_algo_id_was_added_to_index
 FROM reporting_prod.data_science.tableau_geo_snowflake);

CREATE OR REPLACE TABLE _personalization_feb2024_t2 AS
(SELECT edw_prod.stg.udf_unconcat_brand(t1.session_id)                                                        AS session_id,
        t1.session_id                                                                                         AS concat_session_id,
        t1.customer_id,
        t1.store_id,
        t1.test_key,
        t1.test_label,
        t1.test_group,
        t1.ab_test_start_datetime,
        ds.store_brand,
        cd.cd_shipping_zip,
        sd.sd_gender,
        sd.sd_algorithm_trace,
        sd.sd_utm_term,
        CASE WHEN ds.store_brand = 'Fabletics' AND contains(test_label, 'Scrubs') AND sd.sd_gender = 'M' THEN 'SCM'
             WHEN ds.store_brand = 'Fabletics' AND contains(test_label, 'Scrubs') THEN 'SCW'
             WHEN ds.store_brand = 'Fabletics' AND sd.sd_gender = 'M' THEN 'FLM'
             WHEN ds.store_brand = 'Fabletics' THEN 'FLW'
             WHEN ds.store_brand = 'Yitty' AND sd.sd_gender = 'M' THEN 'YTM'
             WHEN ds.store_brand = 'Yitty' THEN 'YTW' END                                                     AS subbrandprefix,
        --KATE CHANGE this subbrandprefix needed to specify scrubs bc otherwise all the control who would have gotten the algo would be 123_FLW which wouldnt be in the index
        CASE WHEN sd.sd_utm_term ILIKE 'fb_ad_id_%' THEN split_part(sd.sd_utm_term, 'fb_ad_id_', 2) END       AS estimated_ad_id,
        to_varchar(CASE WHEN sd.sd_utm_term ILIKE 'fb_ad_id_%' THEN 'adId' ELSE 'geo' END)                    AS estimated_type,
        ifnull(concat(estimated_ad_id, '_', subbrandprefix), concat(subbrandprefix, '_', cd.cd_shipping_zip)) AS estimated_algo_id,
        c.called_algo_id,
        c.called_type,
        c.grid_view_time
 FROM _personalization_feb2024_t1 AS t1
      LEFT JOIN _personalization_feb2024_customer_detail AS cd ON cd.customer_id = t1.customer_id
      LEFT JOIN _personalization_feb2024_session_detail AS sd ON sd.session_id = t1.session_id
      LEFT JOIN edw_prod.data_model.dim_store AS ds ON t1.store_id = ds.store_id
      LEFT JOIN _personalization_feb2024_called_algo_ids c ON c.session_id = edw_prod.stg.udf_unconcat_brand(t1.session_id));

CREATE OR REPLACE TEMPORARY TABLE _personalization_feb2024_final AS
    SELECT *,
           iff(date_algo_id_was_added_to_index < t2.ab_test_start_datetime, concat(estimated_type, '_algo'), concat(estimated_type, '_fpl')) AS estimated_algorithm_trace,
           CASE WHEN test_group = 'Variant' THEN sd_algorithm_trace ELSE estimated_algorithm_trace END                                       AS final_grouping,
           iff(estimated_algo_id = called_algo_id, 1, 0)                                                                                     AS correct_algo_id_estimation,
           iff(estimated_algorithm_trace = sd_algorithm_trace, 1, 0)                                                                         AS correct_algorithm_trace_estimation
    FROM _personalization_feb2024_t2 AS t2
         LEFT JOIN _personalization_feb2024_ad_id_and_geo a ON a.algo_id = t2.estimated_algo_id;

CREATE OR REPLACE TEMPORARY TABLE _fl_builder_algo_id_feb_2024_adid_experience_f1 AS
    SELECT customer_id, test_key, test_label, 'other' record_type, session_id, final_grouping AS experience_flag
    FROM _personalization_feb2024_final
-- where final_grouping in ('adId_fpl','adId_algo');
    WHERE final_grouping IN ('adId_algo');
--KATE CHANGE ad_id fpl means nothing anymore, they arent actually getting the fpl they are just getting the control


CREATE OR REPLACE TEMPORARY TABLE _fl_builder_algo_id_feb_2024_geo_experience_f2 AS
    SELECT customer_id, test_key, 'other' record_type, session_id, final_grouping AS experience_flag
    FROM _personalization_feb2024_final
    WHERE final_grouping IN ('geo_fpl', 'geo_algo');

-- --fk vip offer drawer outstanding membership credits at abt start
CREATE OR REPLACE TEMPORARY TABLE _fk_remix3196FK_custom_flag as
select distinct
    test_framework_id
    ,TEST_KEY
    ,test_label
    ,CUSTOMER_ID
    ,minimum_session_id
    ,minimum_ab_test_start_local_datetime
    ,null as record_type
    ,sum(outstanding_mem_credit_balance) as outstanding_mem_credit_balance_ab_test_start
from (select
          test_framework_id
           ,test_key
           ,test_label
           ,cc.CUSTOMER_ID
           ,minimum_ab_test_start_local_datetime
           ,minimum_session_id
           ,case when e.CREDIT_ACTIVITY_TYPE = 'Issued' then e.CREDIT_ACTIVITY_GROSS_VAT_LOCAL_AMOUNT else -e.CREDIT_ACTIVITY_GROSS_VAT_LOCAL_AMOUNT end as outstanding_mem_credit_balance
           ,cr.CREDIT_ID
           ,IS_BOP_VIP
           ,CREDIT_ACTIVITY_TYPE
           ,CREDIT_ACTIVITY_LOCAL_DATETIME
      from (select
                test_framework_id
                 ,test_key
                 ,test_label
                 ,CUSTOMER_ID
                 ,min(SESSION_ID) as minimum_session_id
                 ,min(AB_TEST_START_LOCAL_DATETIME) as minimum_ab_test_start_local_datetime
            from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA
            where
                TEST_KEY = 'REMIX3196FK_v0'
            group by 1,2,3,4) as cc
               join edw_prod.DATA_MODEL.DIM_CREDIT as cr on cc.CUSTOMER_ID = cr.CUSTOMER_ID
               join reporting_base_prod.SHARED.FACT_CREDIT_EVENT as e on e.CREDIT_KEY = cr.CREDIT_KEY
      where
              CREDIT_TENDER = 'Cash' --and cc.CUSTOMER_ID = 576366046
        and CREDIT_ACTIVITY_LOCAL_DATETIME <= cc.minimum_ab_test_start_local_datetime)
group by 1,2,3,4,5,6,7
having outstanding_mem_credit_balance_ab_test_start > 0;

-------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _fl_optin_copy_test_129520 as
select distinct m.CUSTOMER_ID,TEST_KEY,'other' record_type,min(m.SESSION_ID) as min_session_id
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join edw_prod.DATA_MODEL.FACT_ORDER as fo on fo.SESSION_ID = m.SESSION_ID
join edw_prod.DATA_MODEL.DIM_ADDRESS as a2 on a2.ADDRESS_ID = fo.SHIPPING_ADDRESS_ID
where
    TEST_FRAMEWORK_ID = 129520
    and a2.STATE in ('CA','TX','MD','PA','DC')
group by 1,2,3;

-------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TEMPORARY TABLE _bounceback_min_pdp_view_session as
select
    test_framework_id
    ,test_key
    ,test_label
    ,CUSTOMER_ID
    ,min(convert_timezone(STORE_TIME_ZONE,'America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME::Datetime)::Datetime) as minimum_AB_TEST_START_PST_DATETIME
    ,min(AB_TEST_START_LOCAL_DATETIME) as minimum_AB_TEST_START_LOCAL_DATETIME
    ,min(SESSION_ID) as minimum_session_id
    ,min(SESSION_LOCAL_DATETIME) as minimum_session_local_datetime
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s
join EDW_PROD.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
join lake.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_VIEWED as v on edw_prod.stg.udf_unconcat_brand(SESSION_ID) = v.PROPERTIES_SESSION_ID
where
    CAMPAIGN_CODE = 'bounceBackECHO'
    and timestamp >= '2024-08-01'
    and PROPERTIES_IS_BUNDLE = false
group by all

union

select
    test_framework_id
    ,test_key
    ,test_label
    ,CUSTOMER_ID
    ,min(convert_timezone(STORE_TIME_ZONE,'America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME::Datetime)::Datetime) as minimum_AB_TEST_START_PST_DATETIME
    ,min(AB_TEST_START_LOCAL_DATETIME) as minimum_AB_TEST_START_LOCAL_DATETIME
    ,min(try_to_number(SESSION_ID)) as minimum_session_id
    ,min(SESSION_LOCAL_DATETIME) as minimum_session_local_datetime
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s
join EDW_PROD.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
join lake.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_VIEWED as v on edw_prod.stg.udf_unconcat_brand(SESSION_ID::varchar) = IFNULL(try_to_number(PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR
where
    CAMPAIGN_CODE = 'bounceBackECHO'
    and timestamp >= '2024-08-01'
    and PROPERTIES_SESSION_ID <> '0'
    and PROPERTIES_IS_BUNDLE = false
group by all;

CREATE OR REPLACE TEMPORARY TABLE _bounceback_outstanding_tokens as
select
    test_framework_id
    ,TEST_KEY
    ,test_label
    ,CUSTOMER_ID
    ,minimum_session_id
    ,minimum_ab_test_start_local_datetime
    ,null as record_type
from
    (SELECT DISTINCT
        m.CUSTOMER_ID
        ,minimum_session_id
        ,test_framework_id
        ,test_key
        ,test_label
        ,mt.MEMBERSHIP_TOKEN_ID
        ,minimum_AB_TEST_START_PST_DATETIME
        ,minimum_AB_TEST_START_LOCAL_DATETIME
        ,EFFECTIVE_START_DATETIME
        ,EFFECTIVE_END_DATETIME
        ,sc.label
    from LAKE_CONSOLIDATED.ULTRA_MERCHANT_HISTORY.MEMBERSHIP_TOKEN mt
    join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP m on m.MEMBERSHIP_ID = mt.MEMBERSHIP_ID
    join _bounceback_min_pdp_view_session as bb on bb.CUSTOMER_ID = m.CUSTOMER_ID
    join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TOKEN_REASON mtr on mtr.MEMBERSHIP_TOKEN_REASON_ID = mt.MEMBERSHIP_TOKEN_REASON_ID
    join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STATUSCODE sc on sc.STATUSCODE = mt.STATUSCODE
    where
        sc.LABEL = 'Active'
        and mtr.label in ('Token Billing', 'Converted Membership Credit')
        --and m.CUSTOMER_ID = 83730548520
        and EFFECTIVE_START_DATETIME <= minimum_AB_TEST_START_PST_DATETIME
        and EFFECTIVE_END_DATETIME >= minimum_AB_TEST_START_PST_DATETIME);

-- fl promo picker popup
CREATE OR REPLACE TEMPORARY TABLE _fl_promo_picker_popup as
select distinct
    test_framework_id
    ,TEST_KEY
    ,test_label
    ,CUSTOMER_ID
    ,SESSION_ID
    ,AB_TEST_START_LOCAL_DATETIME
    ,null as record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA
where
    CAMPAIGN_CODE = 'promopickerpopup'
    and AB_TEST_SEGMENT = 1 -- control
    and IS_QUIZ_START_ACTION = TRUE

union

select distinct
    test_framework_id
    ,TEST_KEY
    ,test_label
    ,CUSTOMER_ID
    ,s.SESSION_ID
    ,AB_TEST_START_LOCAL_DATETIME
    ,null as record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.SESSION_DETAIL as d on d.SESSION_ID = s.SESSION_ID
    and name = 'ppp_viewed'
where
    CAMPAIGN_CODE = 'promopickerpopup'
    and AB_TEST_SEGMENT in (2,3); --variant

-- select distinct
--     test_framework_id
--     ,TEST_KEY
--     ,test_label
--     ,CUSTOMER_ID
--     ,minimum_session_id
--     ,minimum_ab_test_start_local_datetime
--     ,null as record_type
--     ,sum(outstanding_mem_credit_balance) as outstanding_mem_credit_balance_ab_test_start
-- from (select
--           test_framework_id
--            ,test_key
--            ,test_label
--            ,cc.CUSTOMER_ID
--            ,minimum_ab_test_start_local_datetime
--            ,minimum_session_id
--            ,case when e.CREDIT_ACTIVITY_TYPE = 'Issued' then e.CREDIT_ACTIVITY_GROSS_VAT_LOCAL_AMOUNT else -e.CREDIT_ACTIVITY_GROSS_VAT_LOCAL_AMOUNT end as outstanding_mem_credit_balance
--            ,cr.CREDIT_ID
--            ,IS_BOP_VIP
--            ,CREDIT_ACTIVITY_TYPE
--            ,CREDIT_ACTIVITY_LOCAL_DATETIME
--       from _bounceback_min_pdp_view_session as cc
-- join edw_prod.DATA_MODEL.DIM_CREDIT as cr on cc.CUSTOMER_ID = cr.CUSTOMER_ID
-- join EDW_PROD.DATA_MODEL.FACT_CREDIT_EVENT as e on e.CREDIT_KEY = cr.CREDIT_KEY
-- where
--     CREDIT_TENDER = 'Cash' --and cc.CUSTOMER_ID = 576366046
--     and CREDIT_ACTIVITY_LOCAL_DATETIME <= cc.minimum_ab_test_start_local_datetime
--     AND ((original_credit_type = 'Fixed Credit' AND original_credit_reason = 'Membership Credit')
--             OR (original_credit_type = 'Token' AND original_credit_reason = 'Token Billing')
--                 OR (original_credit_type = 'Token' AND original_credit_reason = 'Refund'))
-- )
-- group by 1,2,3,4,5,6,7
-- having outstanding_mem_credit_balance_ab_test_start > 0;


-------------------------------------------------------------------------------------------------------

-- CREATE OR REPLACE TEMPORARY TABLE _constructor_search_eligible_sessions as
-- select TEST_KEY,CUSTOMER_ID,record_type,min(SESSION_ID) as minimum_session_id
-- from
--     (select
--         TEST_FRAMEWORK_ID
--         ,CAMPAIGN_CODE
--         ,TEST_KEY
--         ,CUSTOMER_ID
--         ,SESSION_ID
--         ,null as record_type
--         ,m.AB_TEST_START_LOCAL_DATETIME::DATETIME as AB_TEST_START_LOCAL_DATETIME
--         ,convert_timezone(STORE_TIME_ZONE,'UTC',AB_TEST_START_LOCAL_DATETIME::datetime)::datetime as AB_TEST_START_UTC_DATETIME
--         ,min(coalesce(pp.TIMESTAMP::datetime,p.TIMESTAMP::datetime)) as product_search_utc_min_datetime
--     from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
--     join EDW_PROD.DATA_MODEL.DIM_STORE as t on t.STORE_ID = m.STORE_ID
--     left join lake.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_SEARCHED as p on p.PROPERTIES_SESSION_ID = edw_prod.stg.udf_unconcat_brand(m.SESSION_ID)
--         and p.timestamp::datetime >= convert_timezone(STORE_TIME_ZONE,'UTC',AB_TEST_START_LOCAL_DATETIME::datetime)::datetime
--     left join lake.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_SEARCHED as pp on IFNULL(try_to_number(pp.PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR = edw_prod.stg.udf_unconcat_brand(m.SESSION_ID)
--         and pp.PROPERTIES_SESSION_ID <> '0'
--         and pp.timestamp::datetime >= convert_timezone(STORE_TIME_ZONE,'UTC',AB_TEST_START_LOCAL_DATETIME::datetime)::datetime
--     where
--         CAMPAIGN_CODE = 'FLUSSSVCSTR' and TEST_KEY <> 'FLUSSSVCSTR_v3 (User level)' --removing user level reporting since it's already being filtered in upstream table
--         and (p.PROPERTIES_SESSION_ID is not null or pp.PROPERTIES_SESSION_ID is not null)
--         --and m.SESSION_ID = 1551910971020
--         group by 1,2,3,4,5,6,7,8)
-- group by 1,2,3;

-------------------------------------------------------------------------------------------------------

-- CREATE OR REPLACE TEMPORARY TABLE _fl_vip_app_pdp as
-- select
--     customer_id
--     ,test_key
--     ,'membership' record_type
--     ,min(min_session_id) as min_session_id
-- from
--     (
--         select
--             USERID as customer_id
--             ,s.test_key
--             ,'membership' record_type
--             ,min(try_to_number(PROPERTIES_SESSION_ID)) as min_session_id
--         from LAKE.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_VIEWED as p
--         join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s on try_to_number(p.PROPERTIES_SESSION_ID) = edw_prod.stg.udf_unconcat_brand(s.SESSION_ID)
--         join EDW_PROD.DATA_MODEL.DIM_STORE as ds on ds.STORE_ID = s.STORE_ID
--         where
--             campaign_code = 'universalpdp2023'
--         group by 1,2,3
--
--         union
--
--         select
--             USERID as customer_id
--             ,s.test_key
--             ,'membership' record_type
--             ,min(try_to_number(PROPERTIES_SESSION_ID)) as min_session_id
--         from LAKE.SEGMENT_FL.REACT_NATIVE_FABLETICS_SCREEN as p
--         join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s on try_to_number(p.PROPERTIES_SESSION_ID) = edw_prod.stg.udf_unconcat_brand(s.SESSION_ID)
--         join EDW_PROD.DATA_MODEL.DIM_STORE as ds on ds.STORE_ID = s.STORE_ID
--         where
--             s.campaign_code = 'universalpdp2023'
--             and name ilike '%pdp%'
--         group by 1,2,3
--
--         union
--
--         select
--             p.customer_id
--             ,s.test_key
--             ,'membership' record_type
--             ,min(SESSION_ID) as min_session_id
--         from LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as p
--         join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s on try_to_number(p.CUSTOMER_ID) = s.CUSTOMER_ID
--         join EDW_PROD.DATA_MODEL.DIM_STORE as ds on ds.STORE_ID = s.STORE_ID
--         where
--             s.campaign_code = 'universalpdp2023'
--             and name = 'universal_pdp_viewed'
--         group by 1,2,3
--     )
-- group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _fl_minicart_click_1280 as
select
    customer_id
    ,test_key
    ,record_type
    ,min(SESSION_ID) as min_session_id
from
    (
    select
        s.SESSION_ID
        ,s.customer_id
        ,s.test_key
        ,'session' record_type
        ,AB_TEST_START_LOCAL_DATETIME
        ,p.DATETIME_ADDED
        ,convert_timezone('America/Los_Angeles',STORE_TIME_ZONE,p.DATETIME_ADDED)::datetime as flag_datetime_local
    from LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.SESSION_DETAIL as p
    join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s on p.SESSION_ID = s.SESSION_ID
    join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
    where
        s.campaign_code = 'RemovedCheckoutInterstitial'
        and name = 'clicked_minicart_CTA'
    )
where flag_datetime_local >= AB_TEST_START_LOCAL_DATETIME
group by 1,2,3
;

-- CREATE OR REPLACE TEMPORARY TABLE _template as
-- select distinct m.SESSION_ID,m.customer_id,TEST_KEY,'' record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join lake_consolidated_view.ultra_merchant.CUSTOMER_DETAIL as d on d.CUSTOMER_ID = m.CUSTOMER_ID
-- where
--     lower(name) ilike ''
--     and IS_LEAD_REGISTRATION_ACTION = TRUE
--     and m.campaign_code = '';

-- flag 4 (custom)
CREATE OR REPLACE TEMPORARY TABLE _custom_flag_sessions as
-- select distinct
--     m.SESSION_ID
--     ,m.test_framework_id
--     ,m.test_key
--     ,m.test_label
--     ,m.AB_TEST_SEGMENT
--     ,m.CUSTOMER_ID
--     ,'4' as flag
--     ,'SXF1378_v2, VIPS w Credits' flag_4_name
--     ,'vips with outstanding mem credits at abt start' flag_4_description
--     ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--     ,1 as rnk
--     ,record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
--          join _SXF1378_v2_byo_custom_flag as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.CUSTOMER_ID = cf.CUSTOMER_ID
--
-- union all
--
-- select distinct
--     m.SESSION_ID
--       ,m.test_framework_id
--       ,m.test_key
--       ,m.test_label
--       ,m.AB_TEST_SEGMENT
--       ,m.CUSTOMER_ID
--       ,'4' as flag
--       ,'jfg1722, speedy signup' flag_4_name
--       ,'registrations from speedy signup' flag_4_description
--       ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--       ,1 as rnk
--     ,record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
--  join _jfg1722_flag_ssu_sign_in as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.CUSTOMER_ID = cf.CUSTOMER_ID
--
-- union all
--
-- select distinct
--     m.SESSION_ID
--       ,m.test_framework_id
--       ,m.test_key
--       ,m.test_label
--       ,m.AB_TEST_SEGMENT
--       ,m.CUSTOMER_ID
--       ,'4' as flag
--       ,'smartyaddress actions' flag_4_name
--       ,'click, add, or edit billing/shipping' flag_4_description
--       ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--       ,1 as rnk
--     ,null record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
--  join (select distinct m.SESSION_ID,TEST_KEY,test_label
--        from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
--                 join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.session_detail as d on d.SESSION_ID = m.SESSION_ID
--        where
--                lower(name) in ('smarty_avs_shown','smarty_add_shipping','smarty_edit_shipping','smarty_add_billing','smarty_edit_billing')
--          and lower(m.TEST_KEY) ilike 'smartyaddress%') as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.SESSION_ID = cf.SESSION_ID
--
-- union all
--
-- select distinct
--     m.SESSION_ID
--     ,m.test_framework_id
--     ,m.test_key
--     ,m.test_label
--     ,m.AB_TEST_SEGMENT
--     ,m.CUSTOMER_ID
--     ,'4' as flag
--     ,'jfg2397, passwordless reg' flag_4_name
--     ,'autogen password reg' flag_4_description
--     ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--     ,1 as rnk
--     ,record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join _jfg2397_passwordless_reg as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.session_id = cf.session_id
--
-- union all
--
-- select distinct
--     m.SESSION_ID
--     ,m.test_framework_id
--     ,m.test_key
--     ,m.test_label
--     ,m.AB_TEST_SEGMENT
--     ,m.CUSTOMER_ID
--     ,'4' as flag
--     ,'MYVIPSNOOZEOPTION, Snooze in My VIP' flag_4_name
--     ,'Account My VIP Pageview' flag_4_description
--     ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--     ,1 as rnk
--     ,record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join _fl_snooze_1191 as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.session_id = cf.session_id
--
-- union all
--
-- select distinct
--     m.SESSION_ID
--     ,m.test_framework_id
--     ,m.test_key
--     ,m.test_label
--     ,m.AB_TEST_SEGMENT
--     ,m.CUSTOMER_ID
--     ,'4' as flag
--     ,'ONLINECANCELREDESIGN, Paused Pre Test Start' flag_4_name
--     ,'Paused Pre Test Start' flag_4_description
--     ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--     ,1 as rnk
--     ,record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join _fl_oc_1215_paused_pre_test_start as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.session_id = cf.session_id
--
-- union all
--
-- select distinct
--     m.SESSION_ID
--     ,m.test_framework_id
--     ,m.test_key
--     ,m.test_label
--     ,m.AB_TEST_SEGMENT
--     ,m.CUSTOMER_ID
--     ,'4' as flag
--     ,'GFB ABC Shipping, Qualified Traffic' flag_4_name
--     ,'Reached Page with New Shipping Price' flag_4_description
--     ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--     ,1 as rnk
--     ,record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join _gfb_shipping_tests_1268_1270_1271 as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.CUSTOMER_ID = cf.CUSTOMER_ID
-- where m.SESSION_ID >= cf.min_session_id
--

select distinct
    m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'4' as flag
    ,'invalid customers' flag_4_name
    ,'customers shipping from CA, TX, MD, PA, DC' flag_4_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fl_optin_copy_test_129520 as cf on cf.TEST_KEY = m.TEST_KEY
    and m.CUSTOMER_ID = cf.CUSTOMER_ID
where m.SESSION_ID >= cf.min_session_id

union all

select distinct
    m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'4' as flag
    ,'login page' flag_4_name
    ,'reached login page' flag_4_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fl_passwordless_login as cf on cf.TEST_KEY = m.TEST_KEY
    and m.CUSTOMER_ID = cf.CUSTOMER_ID
where m.SESSION_ID >= cf.min_session_id

union all  --_fk_remix3196FK_custom_flag

select distinct
    m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'4' as flag
    ,'saveoffer_lowest' flag_4_name
    ,'lowest decile bucket from segment via COPS API' flag_4_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fl_save_offer_segmentation_1216 as cf on cf.TEST_KEY = m.TEST_KEY
    and m.CUSTOMER_ID = cf.CUSTOMER_ID
where m.SESSION_ID >= cf.min_session_id

union all  --_fk_remix3196FK_custom_flag

select distinct
    m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'4' as flag
    ,'remix3196FK credits' flag_4_name
    ,'credits at test start' flag_4_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fk_remix3196FK_custom_flag as cf on cf.TEST_KEY = m.TEST_KEY
    and m.CUSTOMER_ID = cf.CUSTOMER_ID
where m.SESSION_ID >= cf.minimum_session_id

union all

select distinct
    m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'4' as flag
    ,'cancelorderpopup_no_clicked_count' flag_4_name
    ,'clicked NO to cancel' flag_4_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fl_oc_cancel_first_order as cf on cf.TEST_KEY = m.TEST_KEY
    and m.CUSTOMER_ID = cf.CUSTOMER_ID
where m.SESSION_ID >= cf.min_session_id

union all

select distinct
    m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'4' as flag
    ,'pdp view + outstanding tokens' flag_4_name
    ,'visitor has pdp view and outstanding tokens' flag_4_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _bounceback_outstanding_tokens as cf on cf.TEST_KEY = m.TEST_KEY
    and m.CUSTOMER_ID = cf.CUSTOMER_ID
where m.SESSION_ID >= cf.minimum_session_id

union all

select distinct
    m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'4' as flag
    ,'quiz or pppp flag' flag_4_name
    ,'control saw quiz; variant saw ppp' flag_4_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fl_promo_picker_popup as cf on cf.TEST_KEY = m.TEST_KEY
    and m.SESSION_ID = cf.SESSION_ID

-- union all
--
-- select distinct
--     m.SESSION_ID
--     ,m.test_framework_id
--     ,m.test_key
--     ,m.test_label
--     ,m.AB_TEST_SEGMENT
--     ,m.CUSTOMER_ID
--     ,'4' as flag
--     ,'product search event' flag_4_name
--     ,'visitor searched for product' flag_4_description
--     ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--     ,1 as rnk
--     ,record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join _constructor_search_eligible_sessions as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.CUSTOMER_ID = cf.CUSTOMER_ID
-- where m.SESSION_ID >= cf.minimum_session_id

-- union all
--
-- select distinct
--     m.SESSION_ID
--     ,m.test_framework_id
--     ,m.test_key
--     ,m.test_label
--     ,m.AB_TEST_SEGMENT
--     ,m.CUSTOMER_ID
--     ,'4' as flag
--     ,'universal pdp view' flag_4_name
--     ,'visited pdp on app' flag_4_description
--     ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--     ,1 as rnk
--     ,record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join _fl_vip_app_pdp as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.CUSTOMER_ID = cf.CUSTOMER_ID
-- where m.SESSION_ID >= cf.min_session_id

-- union all
--
-- select distinct
--     m.SESSION_ID
--     ,m.test_framework_id
--     ,m.test_key
--     ,m.test_label
--     ,m.AB_TEST_SEGMENT
--     ,m.CUSTOMER_ID
--     ,'4' as flag
--     ,'universal pdp view' flag_4_name
--     ,'visited pdp on app' flag_4_description
--     ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
--     ,1 as rnk
--     ,record_type
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
-- join _fl_vip_app_pdp as cf on cf.TEST_KEY = m.TEST_KEY
--     and m.CUSTOMER_ID = cf.CUSTOMER_ID
-- where m.SESSION_ID >= cf.min_session_id

union all

select distinct
    m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'4' as flag
    ,'clicked_minicart_CTA' flag_4_name
    ,'clicked mini cart' flag_4_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fl_minicart_click_1280 as cf on cf.TEST_KEY = m.TEST_KEY
    and m.CUSTOMER_ID = cf.CUSTOMER_ID
where m.SESSION_ID >= cf.min_session_id

union all

select distinct
    m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'4' as flag
    ,experience_flag flag_4_name
    ,experience_flag flag_4_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fl_builder_algo_id_dec_2023_adid_experience_f1 as cf on cf.TEST_KEY = m.TEST_KEY
    and m.SESSION_ID = cf.SESSION_ID

union all

SELECT DISTINCT
    m.session_id
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.ab_test_segment
    ,m.customer_id
    ,'4' AS flag
    ,experience_flag flag_4_name
    ,experience_flag flag_4_description
    ,'9999-12-01' AS flag_custom_local_datetime
    ,1 AS rnk
    ,record_type
FROM reporting_base_prod.shared.session_ab_test_metadata AS m
JOIN _fl_builder_algo_id_feb_2024_adid_experience_f1 AS cf ON cf.test_key = m.test_key AND edw_prod.stg.udf_unconcat_brand(m.session_id) = cf.session_id

;

-------------------------------------------------------------------------------------------------------
-- flag 5 (custom 2)
CREATE OR REPLACE TEMPORARY TABLE _custom_flag_2_sessions as
select
     m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'5' as flag
    ,experience_flag flag_5_name
    ,experience_flag flag_5_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fl_builder_algo_id_dec_2023_geo_experience_f2 as cf on cf.TEST_KEY = m.TEST_KEY
    and m.SESSION_ID = cf.SESSION_ID

union all

select
     m.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,m.AB_TEST_SEGMENT
    ,m.CUSTOMER_ID
    ,'5' as flag
    ,experience_flag flag_5_name
    ,experience_flag flag_5_description
    ,'9999-12-01' as FLAG_CUSTOM_LOCAL_DATETIME
    ,1 as rnk
    ,record_type
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _fl_builder_algo_id_feb_2024_geo_experience_f2 as cf on cf.TEST_KEY = m.TEST_KEY
    and edw_prod.stg.udf_unconcat_brand(m.SESSION_ID) = cf.SESSION_ID
;

-------------------------------------------------------

create or replace TEMPORARY TABLE _combined_ab_test_metadata as
 select distinct
    test_key
   ,test_label
    ,test_framework_id
    ,CMS_SESSION_FLAG_1 as flag_1_name
    ,CMS_SESSION_FLAG_1_DESCRIPTION as flag_1_description
    ,CMS_SESSION_FLAG_2 as flag_2_name
    ,CMS_SESSION_FLAG_2_DESCRIPTION as flag_2_description
    ,CMS_SESSION_FLAG_3 as flag_3_name
    ,CMS_SESSION_FLAG_3_DESCRIPTION as flag_3_description
 from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
 where CMS_SESSION_FLAG_1 is not null and CMS_SESSION_FLAG_2 is not null and CMS_SESSION_FLAG_3 is not null
 union
 select distinct
    test_key
   ,test_label
    ,null as test_framework_id
    ,flag_1_name
    ,flag_1_description
    ,flag_2_name
    ,flag_2_description
    ,flag_3_name
    ,flag_3_description
 from lake_view.sharepoint.ab_test_metadata_import_oof
 where flag_1_name is not null and flag_2_name is not null and flag_3_name is not null;


create or replace TABLE _session_flag_1_2_3_consolidated_details as
select distinct
    'session' record_type
    ,d.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID
    ,CASE WHEN lower(flag_1_name) = lower(d.name) THEN flag_1_name ELSE NULL END as flag_1_name
    ,flag_1_description
    ,CASE WHEN lower(flag_2_name) = lower(d.name) THEN flag_2_name ELSE NULL END as flag_2_name
    ,flag_2_description
    ,CASE WHEN lower(flag_3_name) = lower(d.name) THEN flag_3_name ELSE NULL END as flag_3_name
    ,flag_3_description
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_1_LOCAL_DATETIME
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_2_LOCAL_DATETIME
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_3_LOCAL_DATETIME
    ,rank() over (partition by m.test_key,d.SESSION_ID,lower(d.name) ORDER BY CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime asc) as rnk
from _combined_ab_test_metadata as m
join reporting_base_prod.shared.SESSION_AB_TEST_METADATA as s on s.test_key = m.TEST_KEY and s.test_label = m.test_label
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.SESSION_DETAIL as d
    on (lower(flag_1_name) = lower(d.name)
        or lower(flag_2_name) = lower(d.name)
        or lower(flag_3_name) = lower(d.name))
    and s.session_id = d.SESSION_ID
join lake_consolidated_view.ultra_merchant.session as n on n.session_id = d.session_id
left join edw_prod.data_model.dim_store as ds ON n.store_id = ds.store_id
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY --comment
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
where d.DATETIME_ADDED >= CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),s.AB_TEST_START_LOCAL_DATETIME)::datetime
qualify rnk = 1

union

select distinct
    'session' record_type
    ,d.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID
    ,CASE WHEN lower(m.flag_1_name) = lower(d.DATA_KEY) THEN m.flag_1_name ELSE NULL END as flag_1_name
    ,m.flag_1_description
    ,CASE WHEN lower(m.flag_2_name) = lower(d.DATA_KEY) THEN m.flag_2_name ELSE NULL END as flag_2_name
    ,m.flag_2_description
    ,CASE WHEN lower(m.flag_3_name) = lower(d.DATA_KEY) THEN m.flag_3_name ELSE NULL END as flag_3_name
    ,m.flag_3_description
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_1_LOCAL_DATETIME
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_2_LOCAL_DATETIME
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_3_LOCAL_DATETIME
    ,rank() over (partition by m.test_key,d.SESSION_ID,lower(d.DATA_KEY) ORDER BY CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime asc) as rnk
from _combined_ab_test_metadata as m
join reporting_base_prod.shared.SESSION_AB_TEST_METADATA as s on s.test_key = m.TEST_KEY and s.test_label = m.test_label
join LAKE_VIEW.KAFKA.SESSION_DETAIL as d
    on (lower(flag_1_name) = lower(d.DATA_KEY)
        or lower(flag_2_name) = lower(d.DATA_KEY)
        or lower(flag_3_name) = lower(d.DATA_KEY))
    and edw_prod.stg.udf_unconcat_brand(s.session_id) = d.SESSION_ID
join lake_consolidated_view.ultra_merchant.session as n on edw_prod.stg.udf_unconcat_brand(n.session_id) = d.session_id
left join edw_prod.data_model.dim_store as ds ON n.store_id = ds.store_id
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY --comment
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
where d.DATETIME_ADDED >= CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),s.AB_TEST_START_LOCAL_DATETIME)::datetime
qualify rnk = 1;

-------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _session_flag_1_sessions as
select distinct
    'customer' record_type
    ,s.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID
    ,'1' as flag
    ,m.flag_1_name
    ,m.flag_1_description
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_1_LOCAL_DATETIME
    ,rank() over (partition by m.test_key,s.SESSION_ID ORDER BY CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime asc) as rnk
from
    (select distinct
        test_key
        ,test_label
        ,test_framework_id
        ,CMS_SESSION_FLAG_1 as flag_1_name
        ,CMS_SESSION_FLAG_1_DESCRIPTION as flag_1_description
     from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
     where CMS_SESSION_FLAG_1 is not null and TEST_KEY <> 'CANCELFIRSTORDER_v4'
     union
     select distinct
        test_key
       ,test_label
        ,null as test_framework_id
        ,flag_1_name
        ,flag_1_description
     from lake_view.sharepoint.ab_test_metadata_import_oof
     where flag_1_name is not null
    ) as m
join reporting_base_prod.shared.SESSION_AB_TEST_METADATA as s on s.test_key = m.TEST_KEY and s.test_label = m.test_label
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as d on lower(m.flag_1_name) = lower(d.name)
and s.CUSTOMER_ID = d.CUSTOMER_ID
and d.DATETIME_ADDED >= AB_TEST_START_LOCAL_DATETIME
join lake_consolidated_view.ultra_merchant.session as n on n.session_id = s.session_id
left join edw_prod.data_model.dim_store as ds ON n.store_id = ds.store_id
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY --comment
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
where d.DATETIME_ADDED >= CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),s.AB_TEST_START_LOCAL_DATETIME)::datetime

union

--fl CANCELFIRSTORDER_v4 update
select distinct
    'customer' record_type
    ,s.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID
    ,'1' as flag
    ,m.flag_1_name
    ,m.flag_1_description
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_1_LOCAL_DATETIME
    ,rank() over (partition by m.test_key,s.SESSION_ID ORDER BY CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime asc) as rnk
from
    (select distinct
        test_key
        ,test_label
        ,test_framework_id
        ,'first_order_eligible_for_popup' as flag_1_name
        ,'clicked OC CTA; met criteria' as flag_1_description
     from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
     where CMS_SESSION_FLAG_1 is not null and TEST_KEY = 'CANCELFIRSTORDER_v4'
    ) as m
join reporting_base_prod.shared.SESSION_AB_TEST_METADATA as s on s.test_key = m.TEST_KEY and s.test_label = m.test_label
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as d on
    s.CUSTOMER_ID = d.CUSTOMER_ID
    and d.DATETIME_ADDED >= AB_TEST_START_LOCAL_DATETIME
    and d.name = 'first_order_eligible_for_popup'
join lake_consolidated_view.ultra_merchant.session as n on n.session_id = s.session_id
left join edw_prod.data_model.dim_store as ds ON n.store_id = ds.store_id
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY --comment
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
where d.DATETIME_ADDED >= CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),s.AB_TEST_START_LOCAL_DATETIME)::datetime

union

select distinct
    'membership' record_type
    ,s.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID
    ,'1' as flag
    ,m.flag_1_name
    ,m.flag_1_description
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_1_LOCAL_DATETIME
    ,rank() over (partition by m.test_key,s.SESSION_ID ORDER BY CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime asc) as rnk
from
    (select distinct
        test_key
       ,test_label
        ,test_framework_id
        ,CMS_SESSION_FLAG_1 as flag_1_name
        ,CMS_SESSION_FLAG_1_DESCRIPTION as flag_1_description
     from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
     where CMS_SESSION_FLAG_1 is not null
     union
     select distinct
        test_key
       ,test_label
        ,null as test_framework_id
        ,flag_1_name
        ,flag_1_description
     from lake_view.sharepoint.ab_test_metadata_import_oof
     where flag_1_name is not null
    ) as m
join reporting_base_prod.shared.SESSION_AB_TEST_METADATA as s on s.test_key = m.TEST_KEY and s.test_label = m.test_label
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_DETAIL as d on lower(m.flag_1_name) = lower(d.name)
    and s.MEMBERSHIP_ID = d.MEMBERSHIP_ID
    and d.DATETIME_ADDED >= AB_TEST_START_LOCAL_DATETIME
join LAKE_CONSOLIDATED_VIEW.ultra_merchant.session as n on n.session_id = s.session_id
left join edw_prod.data_model.dim_store as ds ON n.store_id = ds.store_id
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY --comment
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
where d.DATETIME_ADDED >= CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),s.AB_TEST_START_LOCAL_DATETIME)::datetime;

delete from _session_flag_1_sessions where rnk > 1;

-------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _session_flag_2_sessions as
select distinct
    'customer' record_type
    ,s.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID
    ,'2' as flag
    ,m.flag_2_name
    ,m.flag_2_description
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_2_LOCAL_DATETIME
    ,rank() over (partition by m.test_key,s.SESSION_ID ORDER BY CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime asc) as rnk
from
    (select distinct
        test_key
       ,test_label
        ,test_framework_id
        ,CMS_SESSION_FLAG_2 as flag_2_name
        ,CMS_SESSION_FLAG_2_DESCRIPTION as flag_2_description
     from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
     where CMS_SESSION_FLAG_2 is not null
     union
     select distinct
        test_key
       ,test_label
        ,null as test_framework_id
        ,flag_2_name
        ,flag_2_description
     from lake_view.sharepoint.ab_test_metadata_import_oof
     where flag_2_name is not null
    ) as m
join reporting_base_prod.shared.SESSION_AB_TEST_METADATA as s on s.test_key = m.TEST_KEY and s.test_label = m.test_label
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as d on lower(m.flag_2_name) = lower(d.name)
    and s.CUSTOMER_ID = d.CUSTOMER_ID
    and d.DATETIME_ADDED >= AB_TEST_START_LOCAL_DATETIME
join lake_consolidated_view.ultra_merchant.session as n on n.session_id = s.session_id
left join edw_prod.data_model.dim_store as ds ON n.store_id = ds.store_id
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY --comment
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
where d.DATETIME_ADDED >= CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),s.AB_TEST_START_LOCAL_DATETIME)::datetime

union

select distinct
    'membership' record_type
    ,s.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID
    ,'2' as flag
    ,m.flag_2_name
    ,m.flag_2_description
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_2_LOCAL_DATETIME
    ,rank() over (partition by m.test_key,s.SESSION_ID ORDER BY CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime asc) as rnk
from
    (select distinct
        test_key
        ,test_label
        ,test_framework_id
        ,CMS_SESSION_FLAG_2 as flag_2_name
        ,CMS_SESSION_FLAG_2_DESCRIPTION as flag_2_description
     from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
     where CMS_SESSION_FLAG_2 is not null
     union
     select distinct
        test_key
       ,test_label
        ,null as test_framework_id
        ,flag_2_name
        ,flag_2_description
     from lake_view.sharepoint.ab_test_metadata_import_oof
     where flag_2_name is not null
    ) as m
join reporting_base_prod.shared.SESSION_AB_TEST_METADATA as s on s.test_key = m.TEST_KEY and s.test_label = m.test_label
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_DETAIL as d on lower(m.flag_2_name) = lower(d.name)
    and s.MEMBERSHIP_ID = d.MEMBERSHIP_ID
    and d.DATETIME_ADDED >= AB_TEST_START_LOCAL_DATETIME
join LAKE_CONSOLIDATED_VIEW.ultra_merchant.session as n on n.session_id = s.session_id
left join edw_prod.data_model.dim_store as ds ON n.store_id = ds.store_id
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY --comment
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
where d.DATETIME_ADDED >= CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),s.AB_TEST_START_LOCAL_DATETIME)::datetime;

delete from _session_flag_2_sessions where rnk > 1;

-------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE _session_flag_3_sessions as
select distinct
    'customer' record_type
    ,s.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID
    ,'3' as flag
    ,m.flag_3_name
    ,m.flag_3_description
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_3_LOCAL_DATETIME
    ,rank() over (partition by m.test_key,s.SESSION_ID ORDER BY CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime asc) as rnk
from
    (select distinct
            test_key
           ,test_label
            ,test_framework_id
            ,CMS_SESSION_FLAG_3 as flag_3_name
            ,CMS_SESSION_FLAG_3_DESCRIPTION as flag_3_description
     from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
     where CMS_SESSION_FLAG_3 is not null
     union
     select distinct
            test_key
           ,test_label
            ,null as test_framework_id
            ,flag_3_name
            ,flag_3_description
     from lake_view.sharepoint.ab_test_metadata_import_oof
     where flag_3_name is not null
    ) as m
join reporting_base_prod.shared.SESSION_AB_TEST_METADATA as s on s.test_key = m.TEST_KEY and s.test_label = m.test_label
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as d on lower(m.flag_3_name) = lower(d.name)
    and s.CUSTOMER_ID = d.CUSTOMER_ID
    and d.DATETIME_ADDED >= AB_TEST_START_LOCAL_DATETIME
join LAKE_CONSOLIDATED_VIEW.ultra_merchant.session as n on n.session_id = s.session_id
left join edw_prod.data_model.dim_store as ds ON n.store_id = ds.store_id
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY --comment
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
where d.DATETIME_ADDED >= CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),s.AB_TEST_START_LOCAL_DATETIME)::datetime

union

select distinct
    'membership' record_type
    ,s.SESSION_ID
    ,m.test_framework_id
    ,m.test_key
    ,m.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID
    ,'3' as flag
    ,m.flag_3_name
    ,m.flag_3_description
    ,CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime as FLAG_3_LOCAL_DATETIME
    ,rank() over (partition by m.test_key,s.SESSION_ID ORDER BY CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),d.datetime_added)::datetime asc) as rnk
from
    (select distinct
        test_key
        ,test_label
        ,test_framework_id
        ,CMS_SESSION_FLAG_3 as flag_3_name
        ,CMS_SESSION_FLAG_3_DESCRIPTION as flag_3_description
     from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
     where CMS_SESSION_FLAG_3 is not null
     union
     select distinct
        test_key
       ,test_label
        ,null as test_framework_id
        ,flag_3_name
        ,flag_3_description
     from lake_view.sharepoint.ab_test_metadata_import_oof
     where flag_3_name is not null
    ) as m
join reporting_base_prod.shared.SESSION_AB_TEST_METADATA as s on s.test_key = m.TEST_KEY and s.test_label = m.test_label
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_DETAIL as d on lower(m.flag_3_name) = lower(d.name)
    and s.MEMBERSHIP_ID = d.MEMBERSHIP_ID
    and d.DATETIME_ADDED >= AB_TEST_START_LOCAL_DATETIME
join LAKE_CONSOLIDATED_VIEW.ultra_merchant.session as n on n.session_id = s.session_id
left join edw_prod.data_model.dim_store as ds ON n.store_id = ds.store_id
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY --comment
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
where d.DATETIME_ADDED >= CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),s.AB_TEST_START_LOCAL_DATETIME)::datetime;

delete from _session_flag_3_sessions where rnk > 1;

-------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _sessions as
select distinct SESSION_ID,test_key,test_label,AB_TEST_SEGMENT,CUSTOMER_ID,test_framework_id,record_type
from _session_flag_1_2_3_consolidated_details

union

select distinct SESSION_ID,test_key,test_label,AB_TEST_SEGMENT,CUSTOMER_ID,test_framework_id,record_type
from _session_flag_1_sessions

union

select distinct SESSION_ID,test_key,test_label,AB_TEST_SEGMENT,CUSTOMER_ID,test_framework_id,record_type
from _session_flag_2_sessions

union

select distinct SESSION_ID,test_key,test_label,AB_TEST_SEGMENT,CUSTOMER_ID,test_framework_id,record_type
from _session_flag_3_sessions

union

select distinct SESSION_ID,test_key,test_label,AB_TEST_SEGMENT,CUSTOMER_ID,test_framework_id,record_type
from _custom_flag_sessions

union

select distinct SESSION_ID,test_key,test_label,AB_TEST_SEGMENT,CUSTOMER_ID,test_framework_id,record_type
from _custom_flag_2_sessions;

CREATE OR REPLACE TEMPORARY TABLE _session_flag_1_sessions_final as
select SESSION_ID
    ,test_framework_id
    ,test_key
    ,test_label
    ,AB_TEST_SEGMENT
    ,CUSTOMER_ID
    ,record_type
    ,FLAG_1_LOCAL_DATETIME
    ,flag_1_name
    ,FLAG_1_DESCRIPTION
from _session_flag_1_sessions
union
select SESSION_ID
    ,test_framework_id
    ,test_key
    ,test_label
    ,AB_TEST_SEGMENT
    ,CUSTOMER_ID
    ,record_type
    ,FLAG_1_LOCAL_DATETIME
    ,flag_1_name
    ,FLAG_1_DESCRIPTION
from _session_flag_1_2_3_consolidated_details as sf
where sf.flag_1_name is not null;


CREATE OR REPLACE TEMPORARY TABLE _session_flag_2_sessions_final as
select SESSION_ID
    ,test_framework_id
    ,test_key
    ,test_label
    ,AB_TEST_SEGMENT
    ,CUSTOMER_ID
    ,record_type
    ,FLAG_2_LOCAL_DATETIME
    ,flag_2_name
    ,FLAG_2_DESCRIPTION
from _session_flag_2_sessions
union
select SESSION_ID
    ,test_framework_id
    ,test_key
    ,test_label
    ,AB_TEST_SEGMENT
    ,CUSTOMER_ID
    ,record_type
    ,FLAG_2_LOCAL_DATETIME
    ,flag_2_name
    ,FLAG_2_DESCRIPTION
from _session_flag_1_2_3_consolidated_details as sf
where sf.flag_2_name is not null;


CREATE OR REPLACE TEMPORARY TABLE _session_flag_3_sessions_final as
select SESSION_ID
    ,test_framework_id
    ,test_key
    ,test_label
    ,AB_TEST_SEGMENT
    ,CUSTOMER_ID
    ,record_type
    ,FLAG_3_LOCAL_DATETIME
    ,flag_3_name
    ,FLAG_3_DESCRIPTION
from _session_flag_3_sessions
union all
select SESSION_ID
    ,test_framework_id
    ,test_key
    ,test_label
    ,AB_TEST_SEGMENT
    ,CUSTOMER_ID
    ,record_type
    ,FLAG_3_LOCAL_DATETIME
    ,flag_3_name
    ,FLAG_3_DESCRIPTION
from _session_flag_1_2_3_consolidated_details as sf
where sf.flag_3_name is not null;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.session_ab_test_flags as
-- CREATE OR REPLACE TEMPORARY TABLE _session_ab_test_flags as
select distinct
    s.SESSION_ID
    ,s.test_framework_id
    ,s.test_key
    ,s.test_label
    ,s.AB_TEST_SEGMENT
    ,f1.record_type as record_type_1
    ,f1.FLAG_1_LOCAL_DATETIME
    ,f1.flag_1_name
    ,f1.FLAG_1_DESCRIPTION
    ,f2.record_type as record_type_2
    ,f2.FLAG_2_LOCAL_DATETIME
    ,f2.flag_2_name
    ,f2.FLAG_2_DESCRIPTION
    ,f3.record_type as record_type_3
    ,f3.FLAG_3_LOCAL_DATETIME
    ,f3.flag_3_name
    ,f3.FLAG_3_DESCRIPTION
    ,f4.record_type  as record_type_4
    ,f4.FLAG_4_NAME
    ,f4.FLAG_4_DESCRIPTION
    ,f5.flag_5_name
    ,f5.FLAG_5_DESCRIPTION
    ,f5.record_type as record_type_5
from _sessions as s
left join _session_flag_1_sessions_final as f1 on s.SESSION_ID = f1.SESSION_ID
    and s.test_key = f1.test_key
left join _session_flag_2_sessions_final as f2 on s.SESSION_ID = f2.SESSION_ID
    and s.test_key = f2.test_key
left join _session_flag_3_sessions_final as f3 on s.SESSION_ID = f3.SESSION_ID
    and s.test_key = f3.test_key
left join _custom_flag_sessions as f4 on s.SESSION_ID = f4.SESSION_ID
    and s.test_key = f4.test_key
left join _custom_flag_2_sessions as f5 on s.SESSION_ID = f5.SESSION_ID
    and s.test_key = f5.test_key;

delete from reporting_base_prod.shared.session_ab_test_flags where edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) = 1249;

delete from reporting_base_prod.shared.session_ab_test_flags
       where
           (edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) = 1216 or TEST_KEY = 'CANCELFIRSTORDER_v4')
            and (record_type_1 = 'session' or record_type_2 = 'session' or record_type_3 = 'session' or record_type_4 = 'session');

delete from reporting_base_prod.shared.session_ab_test_flags
where
    (FLAG_4_NAME ilike 'ad id%' and FLAG_5_NAME = 'geo')
                    or (FLAG_4_NAME = 'adId_algo' and FLAG_5_NAME = 'geo_algo')
                    or (FLAG_4_NAME = 'adId_fpl' and FLAG_5_NAME = 'geo_algo');

ALTER TABLE reporting_base_prod.shared.session_ab_test_flags SET DATA_RETENTION_TIME_IN_DAYS = 0;



-- select
--     test_key
--     ,test_framework_id
--     ,test_label
--      ,flag_1_name,record_type_1
--      ,flag_2_name,record_type_2
--      ,flag_3_name,record_type_3
--      ,flag_4_name,record_type_4
--     ,count(*),count(distinct session_id)
-- -- from _session_ab_test_flags
-- from reporting_base_prod.shared.session_ab_test_flags
-- -- where test_key = 'CANCELFIRSTORDER_v4'
-- where
--     TEST_LABEL in
--         ('YITTY Post Reg Ad ID | Homepage - Postreg - Yitty - 6/5 Postreg Extension - Postreg Segment Control | Postreg - Yitty - Postreg Extension - Personalization AB Test (June 2024) - ad_id Grid Test',
--            'Postreg - Mens 80% Off + BAU Gateway - Personalization AB Test (June 2024) - Control  | Postreg - Mens 80% Off + BAU Gateway - Personalization AB Test (June 2024) - Control  | Postreg - Mens 80% Off + BAU Gateway - Personalization AB Test (June 2024) - Perso Grid Test ',
--            'Postreg - Scrubs - Men''s Personalization AB Test (June 2024) - Control | Postreg - Scrubs - Men''s Personalization AB Test (June 2024) - Control | Postreg - Scrubs - Men''s Personalization AB Test (June 2024) - Perso Grid Test',
--            'Postreg - Scrubs - Women''s Personalization AB Test (June 2024) - Control | Postreg - Scrubs - Women''s Personalization AB Test (June 2024) - Control | Postreg - Scrubs - Women''s Personalization AB Test (June 2024) - Perso Grid Test',
--            'Postreg - Mens - US - Personalization AB Test (June 2024) - Control | Postreg - Mens - US - Personalization AB Test (June 2024) - Control | Postreg - Mens - US - Personalization AB Test (June 2024) - Perso Grid Test'
--           )
-- group by 1,2,3,4,5,6,7,8,9,10,11
