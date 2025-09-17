
CREATE OR REPLACE TRANSIENT TABLE reporting_prod.shared.ab_test_builder_grid_psources AS
SELECT
    s.test_key
   ,s.test_label
   ,test_activated_datetime_utc
   ,s.test_start_page_path
   ,p.properties_list_id AS test_start_psource
   ,COUNT(*) AS COUNT
FROM reporting_base_prod.shared.session_ab_test_builder AS s
JOIN lake.segment_fl.javascript_fabletics_product_list_viewed AS p ON p.properties_session_id = s.meta_original_session_id
    AND s.test_start_page_path = p.context_page_path
WHERE
    test_start_location IN ('Shopping Grid','Post-Reg / Boutique','Post-Reg')
    AND test_start_page_path IS NOT NULL
    AND p.properties_list_id IS NOT NULL
GROUP BY 1,2,3,4,5;
