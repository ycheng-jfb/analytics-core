CREATE OR REPLACE TEMPORARY TABLE _grid_clicks AS
SELECT  meta_original_session_id ,
        u.session_id ,
        u.test_key ,
        u.test_label ,
        u.ab_test_segment ,
        test_group ,
        p.test_activated_datetime_utc ,
        min(TIMESTAMP::DATETIME)         AS grid_click_datetime_utc ,
        COUNT(*)                         AS COUNT
FROM  reporting_base_prod.shared.session_ab_test_builder  AS u
JOIN  reporting_prod.shared.ab_test_builder_grid_psources AS p
ON    u.test_label = p.test_label
JOIN  lake.segment_fl.javascript_fabletics_product_list_viewed AS v
ON    v.properties_session_id = u.meta_original_session_id
AND   v.properties_list_id = p.test_start_psource
AND   v.TIMESTAMP >= p.test_activated_datetime_utc
WHERE u.test_label = 'Bottoms Toggle | Postreg - Womens standard_2legging_offer - Sort Bottoms AB Test (Feb 2024) - 16 Control | Postreg - Womens standard_2legging_offer - Sort Bottoms AB Test (Feb 2024) - 15 Test'
GROUP BY 1,2,3,4,5,6,7;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.abt_session_ab_test_builder_grid_clicks AS
SELECT session_id ,
       meta_original_session_id ,
       test_key ,
       test_label ,
       ab_test_segment ,
       test_group ,
       test_activated_datetime_utc ,
       grid_click_datetime_utc
FROM _grid_clicks ;
