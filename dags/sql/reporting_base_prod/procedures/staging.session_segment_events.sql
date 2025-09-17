SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'reporting_base_prod.staging.session_segment_events';
SET is_full_refresh = (SELECT IFF(public.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));


SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_xlarge', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (SELECT $target_table                               AS table_name,
                  NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
                  high_watermark_datetime                     AS new_high_watermark_datetime
           FROM (SELECT NULL                  AS dependent_table_name,
                        $execution_start_time AS high_watermark_datetime
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_fabkids_product_added' AS dependent_table_name,
                        MAX(meta_create_datetime)                     AS high_watermark_datetime
                 FROM lake.segment_gfb.java_fabkids_product_added
                 UNION ALL
                 SELECT 'lake.segment_fl.java_fabletics_product_added' AS dependent_table_name,
                        MAX(meta_create_datetime)                      AS high_watermark_datetime
                 FROM lake.segment_fl.java_fabletics_product_added
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_justfab_product_added' AS dependent_table_name,
                        MAX(meta_create_datetime)                     AS high_watermark_datetime
                 FROM lake.segment_gfb.java_justfab_product_added
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_shoedazzle_product_added' AS dependent_table_name,
                        MAX(meta_create_datetime)                        AS high_watermark_datetime
                 FROM lake.segment_gfb.java_shoedazzle_product_added
                 UNION ALL
                 SELECT 'lake.segment_sxf.java_sxf_product_added' AS dependent_table_name,
                        MAX(meta_create_datetime)                 AS high_watermark_datetime
                 FROM lake.segment_sxf.java_sxf_product_added
                 UNION ALL
                 SELECT 'lake.segment_gfb.javascript_fabkids_page' AS dependent_table_name,
                        MAX(meta_create_datetime)                  AS high_watermark_datetime
                 FROM lake.segment_gfb.javascript_fabkids_page
                 UNION ALL
                 SELECT 'lake.segment_fl.javascript_fabletics_page' AS dependent_table_name,
                        MAX(meta_create_datetime)                   AS high_watermark_datetime
                 FROM lake.segment_fl.javascript_fabletics_page
                 UNION ALL
                 SELECT 'lake.segment_gfb.javascript_justfab_page' AS dependent_table_name,
                        MAX(meta_create_datetime)                  AS high_watermark_datetime
                 FROM lake.segment_gfb.javascript_justfab_page
                 UNION ALL
                 SELECT 'lake.segment_gfb.javascript_shoedazzle_page' AS dependent_table_name,
                        MAX(meta_create_datetime)                     AS high_watermark_datetime
                 FROM lake.segment_gfb.javascript_shoedazzle_page
                 UNION ALL
                 SELECT 'lake.segment_sxf.javascript_sxf_page' AS dependent_table_name,
                        MAX(meta_create_datetime)              AS high_watermark_datetime
                 FROM lake.segment_sxf.javascript_sxf_page
                 UNION ALL
                 SELECT 'lake.segment_gfb.javascript_fabkids_product_viewed' AS dependent_table_name,
                        MAX(meta_create_datetime)                            AS high_watermark_datetime
                 FROM lake.segment_gfb.javascript_fabkids_product_viewed
                 UNION ALL
                 SELECT 'lake.segment_fl.javascript_fabletics_product_viewed' AS dependent_table_name,
                        MAX(meta_create_datetime)                             AS high_watermark_datetime
                 FROM lake.segment_fl.javascript_fabletics_product_viewed
                 UNION ALL
                 SELECT 'lake.segment_gfb.javascript_justfab_product_viewed' AS dependent_table_name,
                        MAX(meta_create_datetime)                            AS high_watermark_datetime
                 FROM lake.segment_gfb.javascript_justfab_product_viewed
                 UNION ALL
                 SELECT 'lake.segment_gfb.javascript_shoedazzle_product_viewed' AS dependent_table_name,
                        MAX(meta_create_datetime)                               AS high_watermark_datetime
                 FROM lake.segment_gfb.javascript_shoedazzle_product_viewed
                 UNION ALL
                 SELECT 'lake.segment_sxf.javascript_sxf_product_viewed' AS dependent_table_name,
                        MAX(meta_create_datetime)                        AS high_watermark_datetime
                 FROM lake.segment_sxf.javascript_sxf_product_viewed
                 UNION ALL
                 SELECT 'lake.segment_fl.react_native_fabletics_screen' AS dependent_table_name,
                        MAX(meta_create_datetime)                       AS high_watermark_datetime
                 FROM lake.segment_fl.react_native_fabletics_screen
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.react_native_justfab_screen' AS dependent_table_name,
                        MAX(meta_create_datetime)                      AS high_watermark_datetime
                 FROM lake.segment_gfb.react_native_justfab_screen
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_fl.java_fabletics_ecom_mobile_app_signed_in' AS dependent_table_name,
                        MAX(meta_create_datetime)                                  AS high_watermark_datetime
                 FROM lake.segment_fl.java_fabletics_ecom_mobile_app_signed_in
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_fl.java_fabletics_ecom_mobile_app_product_added' AS dependent_table_name,
                        MAX(meta_create_datetime)                                      AS high_watermark_datetime
                 FROM lake.segment_fl.java_fabletics_ecom_mobile_app_product_added
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed' AS dependent_table_name,
                        MAX(meta_create_datetime)                                        AS high_watermark_datetime
                 FROM lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_jf_ecom_app_signed_in' AS dependent_table_name,
                        MAX(meta_create_datetime)                     AS high_watermark_datetime
                 FROM lake.segment_gfb.java_jf_ecom_app_signed_in
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_jf_ecom_app_product_added' AS dependent_table_name,
                        MAX(meta_create_datetime)                         AS high_watermark_datetime
                 FROM lake.segment_gfb.java_jf_ecom_app_product_added
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_jf_ecom_app_order_completed' AS dependent_table_name,
                        MAX(meta_create_datetime)                           AS high_watermark_datetime
                 FROM lake.segment_gfb.java_jf_ecom_app_order_completed
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_justfab_complete_registration' AS dependent_table_name,
                        MAX(meta_create_datetime)                             AS high_watermark_datetime
                 FROM lake.segment_gfb.java_justfab_complete_registration
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_fabkids_complete_registration' AS dependent_table_name,
                        MAX(meta_create_datetime)                             AS high_watermark_datetime
                 FROM lake.segment_gfb.java_fabkids_complete_registration
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_shoedazzle_complete_registration' AS dependent_table_name,
                        MAX(meta_create_datetime)                                AS high_watermark_datetime
                 FROM lake.segment_gfb.java_shoedazzle_complete_registration
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_fl.java_fabletics_complete_registration' AS dependent_table_name,
                        MAX(meta_create_datetime)                              AS high_watermark_datetime
                 FROM lake.segment_fl.java_fabletics_complete_registration
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_sxf.java_sxf_complete_registration' AS dependent_table_name,
                        MAX(meta_create_datetime)                         AS high_watermark_datetime
                 FROM lake.segment_sxf.java_sxf_complete_registration
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_justfab_order_completed' AS dependent_table_name,
                        MAX(meta_create_datetime)                       AS high_watermark_datetime
                 FROM lake.segment_gfb.java_justfab_order_completed
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_fabkids_order_completed' AS dependent_table_name,
                        MAX(meta_create_datetime)                       AS high_watermark_datetime
                 FROM lake.segment_gfb.java_fabkids_order_completed
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_shoedazzle_order_completed' AS dependent_table_name,
                        MAX(meta_create_datetime)                          AS high_watermark_datetime
                 FROM lake.segment_gfb.java_shoedazzle_order_completed
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_fl.java_fabletics_order_completed' AS dependent_table_name,
                        MAX(meta_create_datetime)                        AS high_watermark_datetime
                 FROM lake.segment_fl.java_fabletics_order_completed
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_sxf.java_sxf_order_completed' AS dependent_table_name,
                        MAX(meta_create_datetime)                   AS high_watermark_datetime
                 FROM lake.segment_sxf.java_sxf_order_completed
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_justfab_signed_in' AS dependent_table_name,
                        MAX(meta_create_datetime)                 AS high_watermark_datetime
                 FROM lake.segment_gfb.java_justfab_signed_in
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_fabkids_signed_in' AS dependent_table_name,
                        MAX(meta_create_datetime)                 AS high_watermark_datetime
                 FROM lake.segment_gfb.java_fabkids_signed_in
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_gfb.java_shoedazzle_signed_in' AS dependent_table_name,
                        MAX(meta_create_datetime)                    AS high_watermark_datetime
                 FROM lake.segment_gfb.java_shoedazzle_signed_in
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_fl.java_fabletics_signed_in' AS dependent_table_name,
                        MAX(meta_create_datetime)                  AS high_watermark_datetime
                 FROM lake.segment_fl.java_fabletics_signed_in
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'lake.segment_sxf.java_sxf_signed_in' AS dependent_table_name,
                        MAX(meta_create_datetime)             AS high_watermark_datetime
                 FROM lake.segment_sxf.java_sxf_signed_in
                 WHERE TRY_TO_NUMBER(properties_session_id) IS NOT NULL
                 UNION ALL
                 SELECT 'reporting_base_prod.staging.segment_branch_open_events' AS dependent_table_name,
                        MAX(meta_update_datetime) AS high_watermark_datetime
                 FROM staging.segment_branch_open_events
                 ) AS h) AS s
    ON t.table_name = s.table_name
        AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
        t.new_high_watermark_datetime = s.new_high_watermark_datetime,
        t.meta_update_datetime = $execution_start_time::timestamp_ltz(3)
    WHEN NOT MATCHED
        THEN INSERT (
                     table_name,
                     dependent_table_name,
                     high_watermark_datetime,
                     new_high_watermark_datetime
        )
        VALUES (s.table_name,
                s.dependent_table_name,
                '1900-01-01'::timestamp_ltz,
                s.new_high_watermark_datetime);


SET wm_lake_segment_gfb_java_fabkids_product_added = public.udf_get_watermark($target_table,'lake.segment_gfb.java_fabkids_product_added');
SET wm_lake_segment_fl_java_fabletics_product_added = public.udf_get_watermark($target_table,'lake.segment_fl.java_fabletics_product_added');
SET wm_lake_segment_gfb_java_justfab_product_added = public.udf_get_watermark($target_table,'lake.segment_gfb.java_justfab_product_added');
SET wm_lake_segment_gfb_java_shoedazzle_product_added = public.udf_get_watermark($target_table,'lake.segment_gfb.java_shoedazzle_product_added');
SET wm_lake_segment_sxf_java_sxf_product_added = public.udf_get_watermark($target_table,'lake.segment_sxf.java_sxf_product_added');
SET wm_lake_segment_gfb_javascript_fabkids_page = public.udf_get_watermark($target_table,'lake.segment_gfb.javascript_fabkids_page');
SET wm_lake_segment_fl_javascript_fabletics_page = public.udf_get_watermark($target_table,'lake.segment_fl.javascript_fabletics_page');
SET wm_lake_segment_gfb_javascript_justfab_page = public.udf_get_watermark($target_table,'lake.segment_gfb.javascript_justfab_page');
SET wm_lake_segment_gfb_javascript_shoedazzle_page = public.udf_get_watermark($target_table,'lake.segment_gfb.javascript_shoedazzle_page');
SET wm_lake_segment_sxf_javascript_sxf_page = public.udf_get_watermark($target_table,'lake.segment_sxf.javascript_sxf_page');
SET wm_lake_segment_gfb_javascript_fabkids_product_viewed = public.udf_get_watermark($target_table,'lake.segment_gfb.javascript_fabkids_product_viewed');
SET wm_lake_segment_fl_javascript_fabletics_product_viewed = public.udf_get_watermark($target_table,'lake.segment_fl.javascript_fabletics_product_viewed');
SET wm_lake_segment_gfb_javascript_justfab_product_viewed = public.udf_get_watermark($target_table,'lake.segment_gfb.javascript_justfab_product_viewed');
SET wm_lake_segment_gfb_javascript_shoedazzle_product_viewed = public.udf_get_watermark($target_table,'lake.segment_gfb.javascript_shoedazzle_product_viewed');
SET wm_lake_segment_sxf_javascript_sxf_product_viewed = public.udf_get_watermark($target_table,'lake.segment_sxf.javascript_sxf_product_viewed');
SET wm_lake_segment_fl_react_native_fabletics_screen = public.udf_get_watermark($target_table,'lake.segment_fl.react_native_fabletics_screen');
SET wm_lake_segment_gfb_react_native_justfab_screen = public.udf_get_watermark($target_table,'lake.segment_gfb.react_native_justfab_screen');
SET wm_lake_segment_fl_java_fabletics_ecom_mobile_app_signed_in = public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_ecom_mobile_app_signed_in');
SET wm_lake_segment_fl_java_fabletics_ecom_mobile_app_product_added = public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_ecom_mobile_app_product_added');
SET wm_lake_segment_fl_java_fabletics_ecom_mobile_app_order_completed = public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed');
SET wm_lake_segment_gfb_java_jf_ecom_app_signed_in = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_jf_ecom_app_signed_in');
SET wm_lake_segment_gfb_java_jf_ecom_app_product_added = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_jf_ecom_app_product_added');
SET wm_lake_segment_gfb_java_jf_ecom_app_order_completed = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_jf_ecom_app_order_completed');
SET wm_lake_segment_fl_java_fabletics_complete_registration  = public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_complete_registration');
SET wm_lake_segment_gfb_java_justfab_complete_registration = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_justfab_complete_registration');
SET wm_lake_segment_gfb_java_fabkids_complete_registration = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_fabkids_complete_registration');
SET wm_lake_segment_gfb_java_shoedazzle_complete_registration = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_shoedazzle_complete_registration');
SET wm_lake_segment_sxf_java_sxf_complete_registration = public.udf_get_watermark($target_table, 'lake.segment_sxf.java_sxf_complete_registration');
SET wm_lake_segment_fl_java_fabletics_order_completed  = public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_order_completed');
SET wm_lake_segment_gfb_java_justfab_order_completed = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_justfab_order_completed');
SET wm_lake_segment_gfb_java_fabkids_order_completed = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_fabkids_order_completed');
SET wm_lake_segment_gfb_java_shoedazzle_order_completed = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_shoedazzle_order_completed');
SET wm_lake_segment_sxf_java_sxf_order_completed = public.udf_get_watermark($target_table, 'lake.segment_sxf.java_sxf_order_completed');
SET wm_lake_segment_fl_java_fabletics_signed_in  = public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_signed_in');
SET wm_lake_segment_gfb_java_justfab_signed_in = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_justfab_signed_in');
SET wm_lake_segment_gfb_java_fabkids_signed_in = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_fabkids_signed_in');
SET wm_lake_segment_gfb_java_shoedazzle_signed_in = public.udf_get_watermark($target_table, 'lake.segment_gfb.java_shoedazzle_signed_in');
SET wm_lake_segment_sxf_java_sxf_signed_in = public.udf_get_watermark($target_table, 'lake.segment_sxf.java_sxf_signed_in');
SET wm_reporting_staging_segment_branch_open_events = public.udf_get_watermark($target_table, 'reporting_base_prod.staging.segment_branch_open_events');

CREATE OR REPLACE TEMP TABLE _session_data
(
    meta_original_session_id NUMBER(38,0),
    session_id               NUMBER(38,0)
);

INSERT INTO _session_data
SELECT meta_original_session_id,
       session_id
FROM lake_consolidated.ultra_merchant.session s
         LEFT JOIN edw_prod.data_model.dim_store ds ON s.store_id = ds.store_id
WHERE store_type <> 'Retail'
  AND date_added >= '2022-01-01'
  AND $is_full_refresh = TRUE;

INSERT INTO _session_data
SELECT DISTINCT IFF(s.source in  ('excp', 'reporting_base_prod.staging.segment_branch_open_events'),
                    edw_prod.stg.udf_unconcat_brand(s.properties_session_id),
                    properties_session_id) AS meta_original_session_id,
                CASE
                    WHEN s.source ILIKE 'lake.segment_fl%' THEN CONCAT(meta_original_session_id, 20)
                    WHEN s.source ILIKE 'lake.segment_gfb%' THEN CONCAT(meta_original_session_id, 10)
                    WHEN s.source ILIKE 'lake.segment_sxf%' THEN CONCAT(meta_original_session_id, 30)
                    WHEN s.source ILIKE 'excp' THEN s.properties_session_id
                    WHEN s.source ILIKE 'reporting_base_prod.staging.segment_branch_open_events' THEN s.properties_session_id
                    ELSE meta_original_session_id
                    END                  AS session_id
FROM (SELECT TRY_TO_NUMBER(properties_session_id)          AS properties_session_id,
             'lake.segment_gfb.java_fabkids_product_added' AS source
      FROM lake.segment_gfb.java_fabkids_product_added
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_fabkids_product_added
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)           AS properties_session_id,
             'lake.segment_fl.java_fabletics_product_added' AS source
      FROM lake.segment_fl.java_fabletics_product_added
      WHERE meta_create_datetime > $wm_lake_segment_fl_java_fabletics_product_added
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)          AS properties_session_id,
             'lake.segment_gfb.java_justfab_product_added' AS source
      FROM lake.segment_gfb.java_justfab_product_added
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_justfab_product_added
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)             AS properties_session_id,
             'lake.segment_gfb.java_shoedazzle_product_added' AS source
      FROM lake.segment_gfb.java_shoedazzle_product_added
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_shoedazzle_product_added
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)      AS properties_session_id,
             'lake.segment_sxf.java_sxf_product_added' AS source
      FROM lake.segment_sxf.java_sxf_product_added
      WHERE meta_create_datetime > $wm_lake_segment_sxf_java_sxf_product_added
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)       AS properties_session_id,
             'lake.segment_gfb.javascript_fabkids_page' AS source
      FROM lake.segment_gfb.javascript_fabkids_page
      WHERE meta_create_datetime > $wm_lake_segment_gfb_javascript_fabkids_page
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)        AS properties_session_id,
             'lake.segment_fl.javascript_fabletics_page' AS source
      FROM lake.segment_fl.javascript_fabletics_page
      WHERE meta_create_datetime > $wm_lake_segment_fl_javascript_fabletics_page
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)       AS properties_session_id,
             'lake.segment_gfb.javascript_justfab_page' AS source
      FROM lake.segment_gfb.javascript_justfab_page
      WHERE meta_create_datetime > $wm_lake_segment_gfb_javascript_justfab_page
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)          AS properties_session_id,
             'lake.segment_gfb.javascript_shoedazzle_page' AS source
      FROM lake.segment_gfb.javascript_shoedazzle_page
      WHERE meta_create_datetime > $wm_lake_segment_gfb_javascript_shoedazzle_page
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)   AS properties_session_id,
             'lake.segment_sxf.javascript_sxf_page' AS source
      FROM lake.segment_sxf.javascript_sxf_page
      WHERE meta_create_datetime > $wm_lake_segment_sxf_javascript_sxf_page
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                 AS properties_session_id,
             'lake.segment_gfb.javascript_fabkids_product_viewed' AS source
      FROM lake.segment_gfb.javascript_fabkids_product_viewed
      WHERE meta_create_datetime > $wm_lake_segment_gfb_javascript_fabkids_product_viewed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                  AS properties_session_id,
             'lake.segment_fl.javascript_fabletics_product_viewed' AS source
      FROM lake.segment_fl.javascript_fabletics_product_viewed
      WHERE meta_create_datetime > $wm_lake_segment_fl_javascript_fabletics_product_viewed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                 AS properties_session_id,
             'lake.segment_gfb.javascript_justfab_product_viewed' AS source
      FROM lake.segment_gfb.javascript_justfab_product_viewed
      WHERE meta_create_datetime > $wm_lake_segment_gfb_javascript_justfab_product_viewed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                    AS properties_session_id,
             'lake.segment_gfb.javascript_shoedazzle_product_viewed' AS source
      FROM lake.segment_gfb.javascript_shoedazzle_product_viewed
      WHERE meta_create_datetime > $wm_lake_segment_gfb_javascript_shoedazzle_product_viewed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)             AS properties_session_id,
             'lake.segment_sxf.javascript_sxf_product_viewed' AS source
      FROM lake.segment_sxf.javascript_sxf_product_viewed
      WHERE meta_create_datetime > $wm_lake_segment_sxf_javascript_sxf_product_viewed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)            AS properties_session_id,
             'lake.segment_fl.react_native_fabletics_screen' AS source
      FROM lake.segment_fl.react_native_fabletics_screen
      WHERE meta_create_datetime > $wm_lake_segment_fl_react_native_fabletics_screen
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)           AS properties_session_id,
             'lake.segment_gfb.react_native_justfab_screen' AS source
      FROM lake.segment_gfb.react_native_justfab_screen
      WHERE meta_create_datetime > $wm_lake_segment_gfb_react_native_justfab_screen
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                       AS properties_session_id,
             'lake.segment_fl.java_fabletics_ecom_mobile_app_signed_in' AS source
      FROM lake.segment_fl.java_fabletics_ecom_mobile_app_signed_in
      WHERE meta_create_datetime > $wm_lake_segment_fl_java_fabletics_ecom_mobile_app_signed_in
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                           AS properties_session_id,
             'lake.segment_fl.java_fabletics_ecom_mobile_app_product_added' AS source
      FROM lake.segment_fl.java_fabletics_ecom_mobile_app_product_added
      WHERE meta_create_datetime > $wm_lake_segment_fl_java_fabletics_ecom_mobile_app_product_added
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                             AS properties_session_id,
             'lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed' AS source
      FROM lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed
      WHERE meta_create_datetime > $wm_lake_segment_fl_java_fabletics_ecom_mobile_app_order_completed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)          AS properties_session_id,
             'lake.segment_gfb.java_jf_ecom_app_signed_in' AS source
      FROM lake.segment_gfb.java_jf_ecom_app_signed_in
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_jf_ecom_app_signed_in
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)              AS properties_session_id,
             'lake.segment_gfb.java_jf_ecom_app_product_added' AS source
      FROM lake.segment_gfb.java_jf_ecom_app_product_added
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_jf_ecom_app_product_added
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                AS properties_session_id,
             'lake.segment_gfb.java_jf_ecom_app_order_completed' AS source
      FROM lake.segment_gfb.java_jf_ecom_app_order_completed
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_jf_ecom_app_order_completed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                   AS properties_session_id,
             'lake.segment_fl.java_fabletics_complete_registration' AS source
      FROM lake.segment_fl.java_fabletics_complete_registration
      WHERE meta_create_datetime > $wm_lake_segment_fl_java_fabletics_complete_registration
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                  AS properties_session_id,
             'lake.segment_gfb.java_justfab_complete_registration' AS source
      FROM lake.segment_gfb.java_justfab_complete_registration
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_justfab_complete_registration
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                  AS properties_session_id,
             'lake.segment_gfb.java_fabkids_complete_registration' AS source
      FROM lake.segment_gfb.java_fabkids_complete_registration
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_fabkids_complete_registration
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)                     AS properties_session_id,
             'lake.segment_gfb.java_shoedazzle_complete_registration' AS source
      FROM lake.segment_gfb.java_shoedazzle_complete_registration
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_shoedazzle_complete_registration
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)              AS properties_session_id,
             'lake.segment_sxf.java_sxf_complete_registration' AS source
      FROM lake.segment_sxf.java_sxf_complete_registration
      WHERE meta_create_datetime > $wm_lake_segment_sxf_java_sxf_complete_registration
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)             AS properties_session_id,
             'lake.segment_fl.java_fabletics_order_completed' AS source
      FROM lake.segment_fl.java_fabletics_order_completed
      WHERE meta_create_datetime > $wm_lake_segment_fl_java_fabletics_order_completed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)            AS properties_session_id,
             'lake.segment_gfb.java_justfab_order_completed' AS source
      FROM lake.segment_gfb.java_justfab_order_completed
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_justfab_order_completed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)            AS properties_session_id,
             'lake.segment_gfb.java_fabkids_order_completed' AS source
      FROM lake.segment_gfb.java_fabkids_order_completed
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_fabkids_order_completed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)               AS properties_session_id,
             'lake.segment_gfb.java_shoedazzle_order_completed' AS source
      FROM lake.segment_gfb.java_shoedazzle_order_completed
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_shoedazzle_order_completed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)        AS properties_session_id,
             'lake.segment_sxf.java_sxf_order_completed' AS source
      FROM lake.segment_sxf.java_sxf_order_completed
      WHERE meta_create_datetime > $wm_lake_segment_sxf_java_sxf_order_completed
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)       AS properties_session_id,
             'lake.segment_fl.java_fabletics_signed_in' AS source
      FROM lake.segment_fl.java_fabletics_signed_in
      WHERE meta_create_datetime > $wm_lake_segment_fl_java_fabletics_signed_in
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)      AS properties_session_id,
             'lake.segment_gfb.java_justfab_signed_in' AS source
      FROM lake.segment_gfb.java_justfab_signed_in
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_justfab_signed_in
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)      AS properties_session_id,
             'lake.segment_gfb.java_fabkids_signed_in' AS source
      FROM lake.segment_gfb.java_fabkids_signed_in
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_fabkids_signed_in
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)         AS properties_session_id,
             'lake.segment_gfb.java_shoedazzle_signed_in' AS source
      FROM lake.segment_gfb.java_shoedazzle_signed_in
      WHERE meta_create_datetime > $wm_lake_segment_gfb_java_shoedazzle_signed_in
      UNION ALL
      SELECT TRY_TO_NUMBER(properties_session_id)  AS properties_session_id,
             'lake.segment_sxf.java_sxf_signed_in' AS source
      FROM lake.segment_sxf.java_sxf_signed_in
      WHERE meta_create_datetime > $wm_lake_segment_sxf_java_sxf_signed_in
      UNION ALL
      SELECT sbo.session_id AS properties_session_id,
             'reporting_base_prod.staging.segment_branch_open_events' AS source
      FROM staging.segment_branch_open_events sbo
      WHERE meta_update_datetime > $wm_reporting_staging_segment_branch_open_events
      UNION ALL
      SELECT ex.session_id,
             'excp' AS source
      FROM staging.session_segment_events_excp ex) AS s
WHERE s.properties_session_id > 0
  AND $is_full_refresh = FALSE;

CREATE OR REPLACE TEMPORARY TABLE _session_branch_open_stg AS
SELECT DISTINCT base.session_id,
                ma_utm_source,
                ma_utm_medium,
                ma_utm_campaign,
                ma_utm_term,
                ma_utm_content
FROM _session_data base
    JOIN staging.segment_branch_open_events sbo
        ON base.session_id = sbo.session_id;

CREATE OR REPLACE TEMPORARY TABLE _flag_atb AS
SELECT session_id,
       SUM(atb_count) AS atb_count
FROM (SELECT sb.session_id,
             COUNT(*) AS atb_count
      FROM _session_data sb
               JOIN lake.segment_gfb.java_fabkids_product_added pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      GROUP BY sb.session_id
      UNION ALL
      SELECT sb.session_id,
             COUNT(*) AS atb_count
      FROM _session_data sb
               JOIN lake.segment_fl.java_fabletics_product_added pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      GROUP BY sb.session_id
      UNION ALL
      SELECT sb.session_id,
             COUNT(*) AS atb_count
      FROM _session_data sb
               JOIN lake.segment_gfb.java_justfab_product_added pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      GROUP BY sb.session_id
      UNION ALL
      SELECT sb.session_id,
             COUNT(*) AS atb_count
      FROM _session_data sb
               JOIN lake.segment_gfb.java_shoedazzle_product_added pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      GROUP BY sb.session_id
      UNION ALL
      SELECT sb.session_id,
             COUNT(*) AS atb_count
      FROM _session_data sb
               JOIN lake.segment_sxf.java_sxf_product_added pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      GROUP BY sb.session_id
      UNION ALL
      SELECT sb.session_id,
             COUNT(*) AS atb_count
      FROM _session_data sb
               JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_product_added pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      GROUP BY sb.session_id
      UNION ALL
      SELECT sb.session_id,
             COUNT(*) AS atb_count
      FROM _session_data sb
               JOIN lake.segment_gfb.java_jf_ecom_app_product_added pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      GROUP BY sb.session_id) AS a
GROUP BY session_id
ORDER BY session_id;

CREATE OR REPLACE TEMPORARY TABLE _segment_page_flag AS
SELECT DISTINCT session_id,
                context_page_path,
                properties_url,
                context_useragent                   AS user_agent,
                utm_source,
                utm_medium,
                utm_campaign                        AS utm_campaign_name,
                context_campaign_term               AS utm_campaign_term,
                context_campaign_content            AS utm_campaign_content,
                IFF(cart_checkout_visit >= 1, 1, 0) AS is_cart_checkout_session,
                IFF(pdp_view >= 1, 1, 0)            AS pdp_page_view,
                NVL(is_mobile_pageview, FALSE)      AS page_mobile_app
FROM (SELECT sb.session_id,
             context_page_path,
             properties_url,
             context_useragent,
             COALESCE(properties_utm_source, context_campaign_source) AS utm_source,
             COALESCE(properties_utm_medium, context_campaign_medium) AS utm_medium,
             COALESCE(properties_utm_campaign, context_campaign_name) AS utm_campaign,
             context_campaign_term,
             context_campaign_content,
             timestamp,
             originaltimestamp,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE ANY ('%/cart%', '%/checkout%') THEN 1 END)
                   OVER (PARTITION BY properties_session_id )         AS cart_checkout_visit,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE '%/product%' THEN 1 END)
                   OVER (PARTITION BY properties_session_id)          AS pdp_view,
             NVL(properties_frommobileapp, FALSE)                     AS is_mobile_pageview
      FROM _session_data sb
               JOIN lake.segment_fl.javascript_fabletics_page jfp
                    ON TRY_TO_NUMBER(jfp.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id,
             name AS context_page_path,
             NULL AS properties_url,
             context_useragent,
             NULL AS utm_source,
             NULL AS utm_medium,
             NULL AS utm_campaign,
             NULL AS utm_term,
             NULL AS utm_campaign_content,
             timestamp,
             originaltimestamp,
             0    AS cart_checkout_visit,
             0    AS pdp_view,
             TRUE AS is_mobile_pageview
      FROM _session_data sb
               JOIN lake.segment_fl.react_native_fabletics_screen frnfs
                    ON TRY_TO_NUMBER(frnfs.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id,
             NULL AS context_page_path,
             NULL AS properties_url,
             context_useragent,
             NULL AS utm_source,
             NULL AS utm_medium,
             NULL AS utm_campaign,
             NULL AS utm_term,
             NULL AS utm_campaign_content,
             timestamp,
             originaltimestamp,
             0    AS cart_checkout_visit,
             0    AS pdp_view,
             TRUE AS is_mobile_pageview
      FROM _session_data sb
               JOIN lake.segment_gfb.react_native_justfab_screen grnfs
                    ON TRY_TO_NUMBER(grnfs.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id,
             context_page_path,
             properties_url,
             context_useragent,
             context_campaign_souce                           AS utm_source,
             context_campaign_medium                          AS utm_medium,
             context_campaign_name                            AS utm_campaign,
             context_campaign_term                            AS utm_term,
             context_campaign_content                         AS utm_campaign_content,
             timestamp,
             originaltimestamp,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE ANY ('%/cart%', '%/checkout%') THEN 1 END)
                   OVER (PARTITION BY properties_session_id ) AS cart_checkout_visit,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE '%/product%' THEN 1 END)
                   OVER (PARTITION BY properties_session_id)  AS pdp_view,
             FALSE                                            AS is_mobile_pageview
      FROM _session_data sb
               JOIN lake.segment_gfb.javascript_justfab_page jjp
                    ON TRY_TO_NUMBER(jjp.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id,
             context_page_path,
             properties_url,
             context_useragent,
             context_campaign_source                          AS utm_source,
             context_campaign_medium                          AS utm_medium,
             context_campaign_name                            AS utm_campaign,
             context_campaign_term                            AS utm_term,
             context_campaign_content                         AS utm_campaign_content,
             timestamp,
             originaltimestamp,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE ANY ('%/cart%', '%/checkout%') THEN 1 END)
                   OVER (PARTITION BY properties_session_id ) AS cart_checkout_visit,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE '%/product%' THEN 1 END)
                   OVER (PARTITION BY properties_session_id)  AS pdp_view,
             FALSE                                            AS is_mobile_pageview
      FROM _session_data sb
               JOIN lake.segment_gfb.javascript_fabkids_page gjfp
                    ON TRY_TO_NUMBER(gjfp.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id,
             context_page_path,
             properties_url,
             context_useragent,
             context_campaign_source                          AS utm_source,
             context_campaign_medium                          AS utm_medium,
             context_campaign_name                            AS utm_campaign,
             context_campaign_term                            AS utm_term,
             context_campaign_content                         AS utm_campaign_content,
             timestamp,
             originaltimestamp,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE ANY ('%/cart%', '%/checkout%') THEN 1 END)
                   OVER (PARTITION BY properties_session_id ) AS cart_checkout_visit,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE '%/product%' THEN 1 END)
                   OVER (PARTITION BY properties_session_id)  AS pdp_view,
             FALSE                                            AS is_mobile_pageview
      FROM _session_data sb
               JOIN lake.segment_gfb.javascript_shoedazzle_page jsp
                    ON TRY_TO_NUMBER(jsp.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id,
             context_page_path,
             properties_url,
             context_useragent,
             context_campaign_source                                       AS utm_source,
             COALESCE(context_campaign_medium, context_campaign_tm_medium) AS utm_medium,
             context_campaign_name                                         AS utm_campaign,
             context_campaign_term                                         AS utm_term,
             context_campaign_content                                      AS utm_campaign_content,
             timestamp,
             originaltimestamp,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE ANY ('%/cart%', '%/checkout%') THEN 1 END)
                   OVER (PARTITION BY properties_session_id )              AS cart_checkout_visit,
             COUNT(CASE WHEN LOWER(properties_path) ILIKE '%/product%' THEN 1 END)
                   OVER (PARTITION BY properties_session_id)               AS pdp_view,
             FALSE                                                         AS is_mobile_pageview
      FROM _session_data sb
               JOIN lake.segment_sxf.javascript_sxf_page sjsp
                    ON TRY_TO_NUMBER(sjsp.properties_session_id) = sb.meta_original_session_id)
QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY timestamp, originaltimestamp) = 1
ORDER BY session_id;

CREATE OR REPLACE TEMPORARY TABLE _signed_in AS
SELECT DISTINCT session_id
FROM (SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_fl.java_fabletics_signed_in ma
                    ON TRY_TO_NUMBER(ma.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.java_justfab_signed_in si
                    ON TRY_TO_NUMBER(si.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.java_fabkids_signed_in si
                    ON TRY_TO_NUMBER(si.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.java_shoedazzle_signed_in si
                    ON TRY_TO_NUMBER(si.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_sxf.java_sxf_signed_in si
                    ON TRY_TO_NUMBER(si.properties_session_id) = sb.meta_original_session_id);

CREATE OR REPLACE TEMPORARY TABLE _order_completed AS
SELECT DISTINCT session_id, properties_is_activating
FROM (SELECT sb.session_id, properties_is_activating
      FROM _session_data sb
               JOIN lake.segment_fl.java_fabletics_order_completed oc
                    ON TRY_TO_NUMBER(oc.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id, properties_is_activating
      FROM _session_data sb
               JOIN lake.segment_gfb.java_justfab_order_completed oc
                    ON TRY_TO_NUMBER(oc.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id, properties_is_activating
      FROM _session_data sb
               JOIN lake.segment_gfb.java_fabkids_order_completed oc
                    ON TRY_TO_NUMBER(oc.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id, properties_is_activating
      FROM _session_data sb
               JOIN lake.segment_gfb.java_shoedazzle_order_completed oc
                    ON TRY_TO_NUMBER(oc.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id, properties_is_activating
      FROM _session_data sb
               JOIN lake.segment_sxf.java_sxf_order_completed oc
                    ON TRY_TO_NUMBER(oc.properties_session_id) = sb.meta_original_session_id)
QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY properties_is_activating DESC) = 1;

CREATE OR REPLACE TEMP TABLE _complete_registration AS
SELECT DISTINCT session_id
FROM (SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_fl.java_fabletics_complete_registration cr
                    ON TRY_TO_NUMBER(cr.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.java_justfab_complete_registration cr
                    ON TRY_TO_NUMBER(cr.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.java_fabkids_complete_registration cr
                    ON TRY_TO_NUMBER(cr.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.java_shoedazzle_complete_registration cr
                    ON TRY_TO_NUMBER(cr.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_sxf.java_sxf_complete_registration cr
                    ON TRY_TO_NUMBER(cr.properties_session_id) = sb.meta_original_session_id);

CREATE OR REPLACE TEMPORARY TABLE _pdp_visit AS
SELECT DISTINCT session_id
FROM (SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.javascript_fabkids_product_viewed pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_fl.javascript_fabletics_product_viewed pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.javascript_justfab_product_viewed pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.javascript_shoedazzle_product_viewed pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_sxf.javascript_sxf_product_viewed pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id);

CREATE OR REPLACE TEMP TABLE _mobile_app AS
SELECT DISTINCT session_id
FROM (SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.java_jf_ecom_app_order_completed oc
                    ON TRY_TO_NUMBER(oc.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.java_jf_ecom_app_product_added pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.java_jf_ecom_app_signed_in si
                    ON TRY_TO_NUMBER(si.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_gfb.react_native_justfab_screen sc
                    ON TRY_TO_NUMBER(sc.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed oc
                    ON TRY_TO_NUMBER(oc.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_signed_in si
                    ON TRY_TO_NUMBER(si.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_product_added pa
                    ON TRY_TO_NUMBER(pa.properties_session_id) = sb.meta_original_session_id
      UNION ALL
      SELECT sb.session_id
      FROM _session_data sb
               JOIN lake.segment_fl.react_native_fabletics_screen sc
                    ON TRY_TO_NUMBER(sc.properties_session_id) = sb.meta_original_session_id);

CREATE OR REPLACE TEMPORARY TABLE _segment_session_base AS
SELECT sd.session_id, meta_original_session_id
FROM _session_data sd
         JOIN (SELECT session_id
               FROM _flag_atb
               UNION
               SELECT session_id
               FROM _segment_page_flag
               UNION
               SELECT session_id
               FROM _pdp_visit
               UNION
               SELECT session_id
               FROM _complete_registration
               UNION
               SELECT session_id
               FROM _signed_in
               UNION
               SELECT session_id
               FROM _order_completed
               UNION
               SELECT session_id
               FROM _session_branch_open_stg
               UNION
               SELECT session_id
               FROM _mobile_app) a ON sd.session_id = a.session_id;

CREATE OR REPLACE TEMPORARY TABLE _segment_events_stg AS
SELECT sd.meta_original_session_id,
       sd.session_id,
       IFF(ma.session_id IS NOT NULL
               OR spf.page_mobile_app
               OR sbos.session_id IS NOT NULL, 1, 0)                             AS is_mobile_app,
       spf.context_page_path,
       spf.properties_url,
       COALESCE(utm_source, ma_utm_source)                                       AS utm_source,
       COALESCE(utm_campaign_name, ma_utm_campaign)                              AS utm_campaign,
       COALESCE(utm_medium, ma_utm_medium)                                       AS utm_medium,
       COALESCE(utm_campaign_term, ma_utm_term)                                  AS utm_term,
       COALESCE(utm_campaign_content, ma_utm_content)                            AS utm_content,
       user_agent,
       NVL(spf.is_cart_checkout_session, 0)                                      AS is_cart_checkout_session,
       IFF(spf.pdp_page_view OR pv.session_id IS NOT NULL, 1,
           0)                                                                    AS is_pdp_page_view,
       IFF(fa.session_id IS NOT NULL, 1, 0)                                      AS is_atb_session,
       COALESCE(fa.atb_count, 0)                                                 AS atb_count,
       IFF(pv.session_id IS NOT NULL, 1, 0)                                      AS is_pdp_view,
       IFF(cr.session_id IS NOT NULL, 1, 0)                                      AS is_registration_action,
       IFF(si.session_id IS NOT NULL, 1, 0)                                      AS is_signed_in_action,
       IFF(oc.session_id IS NOT NULL, 1, 0)                                      AS is_order_action,
       COALESCE(oc.properties_is_activating, FALSE)                              AS is_activating,
       IFF(sbos.session_id IS NOT NULL, 1, 0)                                    AS is_branch_open,
       HASH(sd.session_id, is_mobile_app, context_page_path, properties_url, utm_source, utm_medium, utm_campaign,
            utm_term, utm_content,
            user_agent, is_cart_checkout_session, is_pdp_page_view, is_atb_session, atb_count, is_pdp_view,
            is_registration_action,
            is_signed_in_action, is_order_action, is_activating, is_branch_open) AS meta_row_hash
FROM _segment_session_base sd
         LEFT JOIN _flag_atb fa ON sd.session_id = fa.session_id
         LEFT JOIN _segment_page_flag spf ON sd.session_id = spf.session_id
         LEFT JOIN _pdp_visit pv ON sd.session_id = pv.session_id
         LEFT JOIN _complete_registration cr ON sd.session_id = cr.session_id
         LEFT JOIN _signed_in si ON sd.session_id = si.session_id
         LEFT JOIN _order_completed oc ON sd.session_id = oc.session_id
         LEFT JOIN _session_branch_open_stg sbos ON sd.session_id = sbos.session_id
         LEFT JOIN _mobile_app ma ON sd.session_id = ma.session_id;
MERGE INTO staging.session_segment_events t
    USING _segment_events_stg AS src
    ON src.session_id = t.session_id
    WHEN NOT MATCHED THEN
        INSERT (meta_original_session_id, session_id, is_mobile_app, context_page_path, properties_url,
                utm_source, utm_campaign, utm_medium, utm_term, utm_content, user_agent, is_cart_checkout_session,
                is_pdp_page_view, is_atb_session, atb_count, is_pdp_view, is_registration_action, is_signed_in_action,
                is_order_action, is_activating, is_branch_open, meta_row_hash, meta_create_datetime,
                meta_update_datetime)
            VALUES (src.meta_original_session_id,
                    src.session_id,
                    src.is_mobile_app,
                    src.context_page_path,
                    src.properties_url,
                    src.utm_source,
                    src.utm_campaign,
                    src.utm_medium,
                    src.utm_term,
                    src.utm_content,
                    src.user_agent,
                    src.is_cart_checkout_session,
                    src.is_pdp_page_view,
                    src.is_atb_session,
                    src.atb_count,
                    src.is_pdp_view,
                    src.is_registration_action,
                    src.is_signed_in_action,
                    src.is_order_action,
                    src.is_activating,
                    src.is_branch_open,
                    src.meta_row_hash,
                    $execution_start_time,
                    $execution_start_time)
    WHEN MATCHED AND src.meta_row_hash != t.meta_row_hash
        THEN UPDATE SET
        t.meta_original_session_id = src.meta_original_session_id,
        t.session_id = src.session_id,
        t.is_mobile_app = src.is_mobile_app,
        t.context_page_path = src.context_page_path,
        t.properties_url = src.properties_url,
        t.utm_source = src.utm_source,
        t.utm_campaign = src.utm_campaign,
        t.utm_medium = src.utm_medium,
        t.utm_term = src.utm_term,
        t.utm_content = src.utm_content,
        t.user_agent = src.user_agent,
        t.is_cart_checkout_session = src.is_cart_checkout_session,
        t.is_pdp_page_view = src.is_pdp_page_view,
        t.is_atb_session = src.is_atb_session,
        t.atb_count = src.atb_count,
        t.is_pdp_view = src.is_pdp_view,
        t.is_registration_action = src.is_registration_action,
        t.is_signed_in_action = src.is_signed_in_action,
        t.is_order_action = src.is_order_action,
        t.is_activating = src.is_activating,
        t.is_branch_open = src.is_branch_open,
        t.meta_row_hash = src.meta_row_hash,
        t.meta_update_datetime = $execution_start_time;

UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = $execution_start_time
WHERE table_name = $target_table;

TRUNCATE TABLE staging.session_segment_events_excp;
