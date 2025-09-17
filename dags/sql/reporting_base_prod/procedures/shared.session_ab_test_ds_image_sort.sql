SET current_end_date_hq = (SELECT MIN(t.max_hq_datetime)
                           FROM (SELECT MAX(CONVERT_TIMEZONE('America/Los_Angeles', session_local_datetime))::datetime AS max_hq_datetime
                                 FROM reporting_base_prod.shared.session) t);

SET last_load_datetime = ( --comment
    SELECT MAX(a.max_record_pst)::date - 7 AS last_load_datetime
    FROM reporting_base_prod.shared.session_ab_test_ds_image_sort a
             JOIN reporting_base_prod.shared.session b
                 ON a.session_id = b.session_id);

CREATE OR REPLACE TEMPORARY TABLE _abt_sessions AS
SELECT DISTINCT s.session_id,
                edw_prod.stg.udf_unconcat_brand(s.session_id)                                                         AS meta_original_session_id,
                s.customer_id,
                edw_prod.stg.udf_unconcat_brand(s.customer_id)                                                        AS meta_original_customer_id,
                ab_test_start_local_datetime,
                CONVERT_TIMEZONE(store_time_zone, 'UTC',
                                 ab_test_start_local_datetime::datetime)::datetime                                    AS ab_test_start_utc_datetime,
                CONVERT_TIMEZONE(store_time_zone, 'America/Los_Angeles',
                                 ab_test_start_local_datetime::datetime)::datetime                                    AS ab_test_start_pst_datetime,
                s.session_local_datetime,
                test_key,
                test_label,
                s.test_framework_id,
                IFF(test_key = 'FLimagesort_v3', '2024-01-10',
                    effective_start_datetime::datetime)                                                               AS test_effective_start_datetime_pst,
                effective_end_datetime::datetime                                                                      AS test_effective_end_datetime_pst,
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                 test_effective_start_datetime_pst::datetime)::datetime                               AS test_effective_start_datetime_utc,
                COALESCE(CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', effective_end_datetime::datetime)::datetime,
                         '9999-12-31')                                                                                AS test_effective_end_datetime_utc,
                CONVERT_TIMEZONE('America/Los_Angeles', store_time_zone,
                                 test_effective_start_datetime_pst::datetime)::datetime                               AS test_effective_start_datetime_local,
                COALESCE(CONVERT_TIMEZONE('America/Los_Angeles', store_time_zone,
                                          effective_end_datetime::datetime)::datetime,
                         '9999-12-31')                                                                                AS test_effective_end_datetime_local,
                test_framework_ticket,
                test_type,
                s.campaign_code,
                s.test_framework_description,
                s.platform,
                t.store_id,
                ab_test_segment,
                CASE WHEN ab_test_segment = 1 THEN 'Control' ELSE 'Variant' END                                       AS test_group,
                CASE
                    WHEN ab_test_segment = 1 THEN 'merch driven image sort'
                    ELSE 'data science algo image sort' END                                                           AS test_group_description,
                s.store_brand,
                s.store_region,
                s.store_country,
                test_start_membership_state,
                CASE WHEN s.is_male_customer = TRUE THEN 'Male' ELSE 'Female' END                                     AS is_male_customer
FROM reporting_base_prod.shared.session_ab_test_cms_framework AS s --non filtered abt table
         JOIN reporting_base_prod.shared.session AS n
             ON n.session_id = s.session_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', s.ab_test_start_local_datetime::datetime)::datetime
                                                             BETWEEN s.cms_min_test_start_datetime_hq AND $current_end_date_hq
    AND s.session_local_datetime > $last_load_datetime --comment
         JOIN edw_prod.data_model.dim_store AS t
             ON t.store_id = n.store_id
         JOIN (SELECT test_framework_id,
                      code,
                      MIN(effective_start_datetime) AS effective_start_datetime,
                      MAX(effective_end_datetime)      effective_end_datetime
               FROM lake_consolidated_view.ultra_cms_history.test_framework
               WHERE campaign_code = 'FLimagesort'
                 AND statuscode = 113
               GROUP BY 1, 2) AS fr
             ON fr.test_framework_id = s.test_framework_id
    AND s.test_key = fr.code
WHERE s.campaign_code = 'FLimagesort'
  AND SPLIT_PART(s.test_key, '_v', 2) >= 6
  AND n.session_local_datetime >= test_effective_start_datetime_local;

CREATE OR REPLACE TEMPORARY TABLE _min_max AS
SELECT test_key,
       test_effective_start_datetime_pst,
       test_effective_end_datetime_pst,
       test_effective_start_datetime_utc,
       test_effective_end_datetime_utc,
       MIN(meta_original_session_id)             AS min_session_id,
       MAX(ab_test_start_pst_datetime::datetime) AS max_datetime_pst,
       MAX(ab_test_start_utc_datetime::datetime) AS max_datetime_utc
FROM _abt_sessions
GROUP BY ALL;

CREATE OR REPLACE TEMPORARY TABLE _product_test_times AS
SELECT effective_start_date AS product_effective_start_date_pst,
       effective_end_date   AS product_effective_end_date_pst,
       evaluation_batch_id,
       is_current_record,
       master_product_id    AS meta_original_master_product_id,
       master_test_number
FROM lake_fl.ultra_rollup.product_image_test_config_history
WHERE image_test_flag = TRUE
  AND hvr_is_deleted = FALSE
  AND evaluation_message = 'START';

CREATE OR REPLACE TEMPORARY TABLE _product_test_keys AS
SELECT GREATEST(tt.product_effective_start_date_pst, tf.effective_start_datetime) AS test_effective_start_datetime,
       LEAST(tt.product_effective_end_date_pst, tf.effective_end_datetime)        AS test_effective_end_datetime,
       tt.evaluation_batch_id,
       tt.is_current_record,
       meta_original_master_product_id,
       tt.master_test_number,
       tf.code                                                                    AS test_key
FROM _product_test_times tt
         LEFT JOIN
     (SELECT MIN(effective_start_datetime) AS effective_start_datetime,
             MAX(effective_end_datetime)   AS effective_end_datetime,
             code,
             'FLimagesort'                 AS campaign_code
      FROM lake_consolidated_view.ultra_cms_history.test_framework
      WHERE campaign_code = 'FLimagesort'
        AND statuscode = 113
      GROUP BY code) AS tf
     ON GREATEST(tt.product_effective_start_date_pst, tf.effective_start_datetime) <
        LEAST(tt.product_effective_end_date_pst, tf.effective_end_datetime)
WHERE campaign_code = 'FLimagesort'
--AND statuscode = 113
ORDER BY tf.campaign_code
    , master_test_number
    , product_effective_start_date_pst;

CREATE OR REPLACE TEMPORARY TABLE _products AS
SELECT master_test_number,
       meta_original_master_product_id,
       MIN(test_effective_start_datetime)                                                                   AS test_effective_start_datetime,
       MAX(test_effective_end_datetime)                                                                     AS test_effective_end_datetime,
       MIN(test_effective_start_datetime)                                                                   AS product_effective_start_date_pst,
       MAX(test_effective_end_datetime)                                                                     AS product_effective_end_date_pst,
       test_key,
       CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                        product_effective_start_date_pst::datetime)::datetime                               AS product_effective_start_datetime_utc,
       CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                        product_effective_end_date_pst::datetime)::datetime                                 AS product_effective_end_datetime_utc
FROM _product_test_keys
GROUP BY master_test_number,
    meta_original_master_product_id,
    test_key
ORDER BY master_test_number,
    meta_original_master_product_id,
    test_key;

CREATE OR REPLACE TEMPORARY TABLE _products_meta AS
SELECT DISTINCT test_key,
                master_test_number,
--     TEST_EFFECTIVE_START_DATETIME_PST,
--     TEST_EFFECTIVE_END_DATETIME_PST,
                store_id                                                                                                                       AS product_store_id,
                dp.group_code,
                sam.product_effective_start_date_pst,
                sam.product_effective_end_date_pst,
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                 sam.product_effective_start_date_pst::datetime)::datetime                                                     AS product_effective_start_datetime_utc,
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                 sam.product_effective_end_date_pst::datetime)::datetime                                                       AS product_effective_end_datetime_utc,
                CASE
                    WHEN dp.master_product_id = '-1' THEN dp.product_id
                    ELSE dp.master_product_id END                                                                                              AS master_product_id,
                CASE
                    WHEN dp.master_product_id = '-1' THEN edw_prod.stg.udf_unconcat_brand(dp.product_id)
                    ELSE edw_prod.stg.udf_unconcat_brand(dp.master_product_id) END                                                             AS meta_original_master_product_id,
                RANK() OVER (PARTITION BY test_key,meta_original_master_product_id,product_effective_start_datetime_utc ORDER BY base_sku ASC) AS rnk,
                NULL                                                                                                                              rnk_mpid_status_changed,
                product_alias,
                current_name                                                                                                                   AS product_name, --UBT NAME
                base_sku,
                dp.product_sku,
                ubt.color,
                ubt.category,
                ubt.class,
                ubt.subclass,
                CASE WHEN ubt.item_status ILIKE '%CORE' THEN 'CORE' ELSE 'FASHION' END                                                         AS lifestyle_type,                                                                                                                                                           --,ubt.INITIAL_LAUNCH as ubt_special_collection
                ubt.sub_brand                                                                                                                  AS sub_brand,
                ubt.gender                                                                                                                     AS gender,
                CASE
                    WHEN sub_brand = 'Scrubs' AND ubt.gender ILIKE 'MEN%' THEN 'SC-M-PROD'
                    WHEN sub_brand = 'Scrubs' THEN 'SC-W-PROD'
                    WHEN sub_brand = 'Fabletics' AND ubt.gender ILIKE 'MEN%' THEN 'FL-M-PROD'
                    WHEN sub_brand = 'Fabletics' THEN 'FL-W-PROD'
                    WHEN sub_brand = 'Yitty'
                        THEN 'YT-PROD' END                                                                                                     AS segment,
                CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.sku || '/' || ubt.sku ||
                       '-1_271x407.jpg')                                                                                                       AS image_url,    --ubt table
                COALESCE(i1.inactive_test_product_outcome,
                         i2.inactive_test_product_outcome)                                                                                     AS inactive_test_product_outcome
FROM _products AS sam
         JOIN edw_prod.data_model.dim_product AS dp
              ON sam.meta_original_master_product_id = edw_prod.stg.udf_unconcat_brand(dp.master_product_id)
         JOIN
     (SELECT DISTINCT ubt.*
      FROM lake.excel.fl_items_ubt ubt
               JOIN
           (SELECT sku, MAX(current_showroom) AS max_current_showroom
            FROM lake.excel.fl_items_ubt
            GROUP BY 1) AS ms
               ON ubt.sku = ms.sku
                      AND ubt.current_showroom = ms.max_current_showroom) ubt
     ON dp.product_sku = ubt.sku
         LEFT JOIN lake_fl.ultra_rollup.products_in_stock_product_feed AS fe
             ON ubt.sku = fe.product_sku
    AND store_group_id = 16
    AND is_outfit = FALSE
    AND is_bundle_product = FALSE
    AND product_type_id NOT IN (11, 4, 16)
    AND image_count > 0
         LEFT JOIN lake_view.sharepoint.ab_test_product_import_mpid_list AS i1
                   ON i1.master_product_id = edw_prod.stg.udf_unconcat_brand(dp.master_product_id)
                       AND i1.inactive_test_product_outcome IS NOT NULL
         LEFT JOIN lake_view.sharepoint.ab_test_product_import_mpid_list AS i2
             ON i2.product_sku = dp.product_sku
                AND i2.inactive_test_product_outcome IS NOT NULL

UNION ALL

SELECT DISTINCT test_key,
                master_test_number,
--     TEST_EFFECTIVE_START_DATETIME_PST,
--     TEST_EFFECTIVE_END_DATETIME_PST,
                dp.store_id                                                                                                                    AS product_store_id,
                dp.group_code,
                product_effective_start_date_pst,
                product_effective_end_date_pst,
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                 product_effective_start_date_pst::datetime)::datetime                                                         AS product_effective_start_datetime_utc,
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                 product_effective_end_date_pst::datetime)::datetime                                                           AS product_effective_end_datetime_utc,
                CASE
                    WHEN dp.master_product_id = '-1' THEN dp.product_id
                    ELSE dp.master_product_id END                                                                                              AS master_product_id,
                CASE
                    WHEN dp.master_product_id = '-1' THEN edw_prod.stg.udf_unconcat_brand(dp.product_id)
                    ELSE edw_prod.stg.udf_unconcat_brand(dp.master_product_id) END                                                             AS meta_original_master_product_id,
                RANK() OVER (PARTITION BY test_key,meta_original_master_product_id,product_effective_start_datetime_utc ORDER BY base_sku ASC) AS rnk,
                NULL                                                                                                                              rnk_mpid_status_changed,
                product_alias,
                product_name,
                base_sku,
                product_alias                                                                                                                  AS product_sku,
                color,
                NULL                                                                                                                              category,
                NULL                                                                                                                              category,
                NULL                                                                                                                              class,
                NULL                                                                                                                              subclass,
                NULL                                                                                                                              lifestyle_type,
                ubt.sub_brand                                                                                                                  AS sub_brand,
                ubt.gender                                                                                                                     AS gender,
                CASE
                    WHEN sub_brand = 'Scrubs' AND ubt.gender ILIKE 'MEN%' THEN 'SC-M-PROD'
                    WHEN sub_brand = 'Scrubs' THEN 'SC-W-PROD'
                    WHEN sub_brand = 'Fabletics' AND ubt.gender ILIKE 'MEN%' THEN 'FL-M-PROD'
                    WHEN sub_brand = 'Fabletics' THEN 'FL-W-PROD'
                    WHEN sub_brand = 'Yitty'
                        THEN 'YT-PROD' END                                                                                                     AS segment,
                CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.outfit_number || '/' ||
                       ubt.outfit_number ||
                       '-1_271x407.jpg')                                                                                                       AS image_url --ubt table
FROM _products AS sam
         JOIN edw_prod.data_model.dim_product AS dp
              ON sam.meta_original_master_product_id = edw_prod.stg.udf_unconcat_brand(dp.product_id) --bundles
         JOIN lake.excel.fl_outfits_ubt AS ubt
             ON ubt.outfit_number = dp.product_alias
WHERE dp.master_product_id = -1
  AND product_type = 'Bundle'
ORDER BY 1;
--select * from _products
--select * from _products_meta


CREATE OR REPLACE TEMPORARY TABLE _grid_views AS
SELECT --pieced
       event,
       a.test_key,
       d.master_test_number,
       test_type,
       platform,
       a.meta_original_session_id,
       a.meta_original_customer_id,
       meta_original_master_product_id,
       ab_test_start_utc_datetime,
       properties_loggedin_status,
       COUNT(DISTINCT CASE
                          WHEN properties_products_is_bundle THEN p.properties_products_product_id
                          ELSE properties_products_product_id END) AS grid_view_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.javascript_fabletics_product_list_viewed AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_products_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.test_key = d.test_key
  AND d.rnk = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT --pieced
       event,
       a.test_key,
       d.master_test_number,
       test_type,
       platform,
       a.meta_original_session_id,
       a.meta_original_customer_id,
       meta_original_master_product_id,
       ab_test_start_utc_datetime,
       properties_loggedin_status,
       COUNT(DISTINCT CASE
                          WHEN properties_products_is_bundle THEN p.properties_products_product_id
                          ELSE properties_products_product_id END) AS grid_view_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.react_native_fabletics_product_list_viewed AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_products_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.test_key = d.test_key
  AND d.rnk = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT --outfit
       event,
       a.test_key,
       d.master_test_number,
       test_type,
       platform,
       a.meta_original_session_id,
       a.meta_original_customer_id,
       properties_products_bundle_product_id                 AS meta_original_master_product_id,
       ab_test_start_utc_datetime,
       properties_loggedin_status,
       COUNT(DISTINCT properties_products_bundle_product_id) AS grid_view_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.javascript_fabletics_product_list_viewed AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id =
                                     IFF(p.properties_products_bundle_product_id = '', NULL,
                                         p.properties_products_bundle_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.test_key = d.test_key
  AND d.rnk = 1
  AND d.group_code = 'outfit'
  AND p.properties_products_is_bundle = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT --outfit
       event,
       a.test_key,
       d.master_test_number,
       test_type,
       platform,
       a.meta_original_session_id,
       a.meta_original_customer_id,
       properties_products_bundle_product_id                 AS meta_original_master_product_id,
       ab_test_start_utc_datetime,
       properties_loggedin_status,
       COUNT(DISTINCT properties_products_bundle_product_id) AS grid_view_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.react_native_fabletics_product_list_viewed AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         LEFT JOIN _products_meta AS d
             ON d.meta_original_master_product_id = p.properties_products_bundle_product_id--iff(p.PROPERTIES_PRODUCTS_BUNDLE_PRODUCT_ID = '',null,p.PROPERTIES_PRODUCTS_BUNDLE_PRODUCT_ID)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.test_key = d.test_key
  AND d.rnk = 1
  AND d.group_code = 'outfit'
  AND p.properties_products_is_bundle = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

-- delete from _grid_views where TEST_TYPE = 'membership' and PROPERTIES_LOGGEDIN_STATUS = false;

--select * from _grid_views limit 2
// VALIDATION
-- select test_key,meta_original_MASTER_PRODUCT_ID,count(*) as sessions, count(distinct meta_original_session_id) as sessions_distinct,sessions - sessions_distinct as diff
-- from _grid_views
-- group by 1,2
-- order by 5 desc

-- select test_key,product_type,count(distinct meta_original_MASTER_PRODUCT_ID) as mpid
-- from _grid_views group by 1,2 order by 1,2

CREATE OR REPLACE TEMPORARY TABLE _eligible_grid_view_sessions AS
SELECT meta_original_session_id, COUNT(*) AS count
FROM _grid_views
GROUP BY 1;

CREATE OR REPLACE TEMPORARY TABLE _pdp_views AS
SELECT DISTINCT event, --items
                a.test_key,
                d.master_test_number,
                test_type,
                platform,
                a.meta_original_session_id,
                a.meta_original_customer_id,
                meta_original_master_product_id,
                a.ab_test_start_utc_datetime,
                properties_loggedin_status,
                COUNT(DISTINCT meta_original_master_product_id) AS pdp_view_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.javascript_fabletics_product_viewed AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT DISTINCT event, --items
                a.test_key,
                d.master_test_number,
                test_type,
                platform,
                a.meta_original_session_id,
                a.meta_original_customer_id,
                meta_original_master_product_id,
                a.ab_test_start_utc_datetime,
                properties_loggedin_status,
                COUNT(DISTINCT meta_original_master_product_id) AS pdp_view_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.react_native_fabletics_product_viewed AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT DISTINCT event, --outfit
                a.test_key,
                d.master_test_number,
                test_type,
                platform,
                a.meta_original_session_id,
                a.meta_original_customer_id,
                p.properties_bundle_product_id               AS meta_original_master_product_id,
                a.ab_test_start_utc_datetime,
                properties_loggedin_status,
                COUNT(DISTINCT properties_bundle_product_id) AS pdp_view_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.javascript_fabletics_product_viewed AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
              ON d.meta_original_master_product_id = p.properties_bundle_product_id --iff(p.PROPERTIES_BUNDLE_PRODUCT_ID = '',null,p.PROPERTIES_BUNDLE_PRODUCT_ID)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
  AND d.group_code = 'outfit'
  AND p.properties_is_bundle = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT DISTINCT event, --outfit
                a.test_key,
                d.master_test_number,
                test_type,
                platform,
                a.meta_original_session_id,
                a.meta_original_customer_id,
                p.properties_bundle_product_id               AS meta_original_master_product_id,
                a.ab_test_start_utc_datetime,
                properties_loggedin_status,
                COUNT(DISTINCT properties_bundle_product_id) AS pdp_view_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.react_native_fabletics_product_viewed AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id = p.properties_bundle_product_id--iff(p.PROPERTIES_BUNDLE_PRODUCT_ID = '',null,p.PROPERTIES_BUNDLE_PRODUCT_ID)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
  AND d.group_code = 'outfit'
  AND p.properties_is_bundle = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

-- delete from _pdp_views
--     where TEST_TYPE = 'membership' and PROPERTIES_LOGGEDIN_STATUS = false;

// VALIDATION
-- select test_key,meta_original_MASTER_PRODUCT_ID,count(*) as sessions, count(distinct meta_original_session_id) as sessions_distinct,sessions - sessions_distinct as diff
-- from _pdp_views
-- group by 1,2
-- order by 5 desc

-- select test_key,product_type,count(distinct meta_original_MASTER_PRODUCT_ID) as mpid
-- from _pdp_views group by 1,2 order by 1,2

-- no atb react table for app. per jon, use java version since ATB is supposed to be server side event
CREATE OR REPLACE TEMPORARY TABLE _atb AS
SELECT DISTINCT event, --item
                a.test_key,
                d.master_test_number,
                test_type,
                platform,
                a.meta_original_session_id,
                a.meta_original_customer_id,
                meta_original_master_product_id,
                a.ab_test_start_utc_datetime,
                properties_loggedin_status,
                COUNT(*) AS atb_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.java_fabletics_product_added AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT DISTINCT event, --item
                a.test_key,
                d.master_test_number,
                test_type,
                platform,
                a.meta_original_session_id,
                a.meta_original_customer_id,
                meta_original_master_product_id,
                a.ab_test_start_utc_datetime,
                properties_loggedin_status,
                COUNT(*) AS atb_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_product_added AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT DISTINCT event, --outfit
                a.test_key,
                d.master_test_number,
                test_type,
                platform,
                a.meta_original_session_id,
                a.meta_original_customer_id,
                p.properties_bundle_product_id AS meta_original_master_product_id,
                a.ab_test_start_utc_datetime,
                properties_loggedin_status,
                COUNT(*)                       AS atb_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.java_fabletics_product_added AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id =
                                     IFF(p.properties_bundle_product_id = '', NULL, p.properties_bundle_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
  AND d.group_code = 'outfit'
  AND p.properties_is_bundle = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT DISTINCT event, --outfit
                a.test_key,
                d.master_test_number,
                test_type,
                platform,
                a.meta_original_session_id,
                a.meta_original_customer_id,
                p.properties_bundle_product_id AS meta_original_master_product_id,
                a.ab_test_start_utc_datetime,
                properties_loggedin_status,
                COUNT(*)                       AS atb_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_product_added AS p
              ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id =
                                     IFF(p.properties_bundle_product_id = '', NULL, p.properties_bundle_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(p.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
  AND d.group_code = 'outfit'
  AND p.properties_is_bundle = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

-- delete from _atb
--     where TEST_TYPE = 'membership' and PROPERTIES_LOGGEDIN_STATUS = false;

--select * from _atb limit 2
--select count(*) from _atb

// VALIDATION
-- select test_key,meta_original_MASTER_PRODUCT_ID,count(*) as sessions, count(distinct meta_original_session_id) as sessions_distinct,sessions - sessions_distinct as diff
-- from _atb
-- group by 1,2
-- order by 5 desc
--
-- select test_key,product_type,count(distinct meta_original_MASTER_PRODUCT_ID) as mpid
-- from _atb group by 1,2 order by 1,2

CREATE OR REPLACE TEMPORARY TABLE _orders AS
SELECT event,
       a.test_key,
       d.master_test_number,
       test_type,
       platform,
       a.meta_original_session_id,
       a.meta_original_customer_id,
       meta_original_master_product_id,
       a.ab_test_start_utc_datetime,
       properties_loggedin_status,
       SUM(CASE
               WHEN properties_is_activating = TRUE THEN o.properties_products_quantity
               ELSE 0 END)                                                                            AS units_activating_count,
       SUM(CASE
               WHEN properties_is_activating = FALSE THEN o.properties_products_quantity
               ELSE 0 END)                                                                            AS units_nonactivating_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.java_fabletics_order_completed AS o
              ON a.meta_original_session_id = TRY_TO_NUMBER(o.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id = TRY_TO_NUMBER(o.properties_products_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(o.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT event,
       a.test_key,
       d.master_test_number,
       test_type,
       platform,
       a.meta_original_session_id,
       a.meta_original_customer_id,
       meta_original_master_product_id,
       a.ab_test_start_utc_datetime,
       properties_loggedin_status,
       SUM(CASE
               WHEN properties_is_activating = TRUE THEN o.properties_products_quantity
               ELSE 0 END)                                                                            AS units_activating_count,
       SUM(CASE
               WHEN properties_is_activating = FALSE THEN o.properties_products_quantity
               ELSE 0 END)                                                                            AS units_nonactivating_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed AS o
              ON a.meta_original_session_id = TRY_TO_NUMBER(o.properties_session_id)
         JOIN _products_meta AS d
             ON d.meta_original_master_product_id = TRY_TO_NUMBER(o.properties_products_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(o.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT --outfits
       event,
       a.test_key,
       d.master_test_number,
       test_type,
       platform,
       a.meta_original_session_id,
       a.meta_original_customer_id,
       meta_original_master_product_id,
       a.ab_test_start_utc_datetime,
       properties_loggedin_status,
       SUM(CASE
               WHEN properties_is_activating = TRUE THEN o.properties_products_bundle_quantity
               ELSE 0 END)                                                                                   AS units_activating_count,
       SUM(CASE
               WHEN properties_is_activating = FALSE THEN o.properties_products_bundle_quantity
               ELSE 0 END)                                                                                   AS units_nonactivating_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.java_fabletics_order_completed AS o
              ON a.meta_original_session_id = TRY_TO_NUMBER(o.properties_session_id)
         JOIN _products_meta AS d
              ON d.meta_original_master_product_id = TRY_TO_NUMBER(o.properties_products_bundle_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(o.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
  AND d.group_code = 'outfit'
  AND properties_products_is_bundle = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION

SELECT event, --outfits
       a.test_key,
       d.master_test_number,
       test_type,
       platform,
       a.meta_original_session_id,
       a.meta_original_customer_id,
       meta_original_master_product_id,
       a.ab_test_start_utc_datetime,
       properties_loggedin_status,
       SUM(CASE
               WHEN properties_is_activating = TRUE THEN o.properties_products_bundle_quantity
               ELSE 0 END)                                                                                   AS units_activating_count,
       SUM(CASE
               WHEN properties_is_activating = FALSE THEN o.properties_products_bundle_quantity
               ELSE 0 END)                                                                                   AS units_nonactivating_count
FROM _abt_sessions AS a
         JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed AS o
              ON a.meta_original_session_id = TRY_TO_NUMBER(o.properties_session_id)
         JOIN _products_meta AS d
              ON d.meta_original_master_product_id = TRY_TO_NUMBER(o.properties_products_bundle_product_id)
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
WHERE TRY_TO_NUMBER(o.properties_session_id) >= mm.min_session_id
  AND a.test_key = d.test_key
  AND timestamp >= product_effective_start_datetime_utc
  AND timestamp <= product_effective_end_datetime_utc
  AND a.meta_original_session_id IN (SELECT meta_original_session_id FROM _eligible_grid_view_sessions)
  AND d.rnk = 1
  AND d.group_code = 'outfit'
  AND properties_products_is_bundle = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

-- delete from _orders
--     where TEST_TYPE = 'membership' and PROPERTIES_LOGGEDIN_STATUS = false;

-- select * from _orders limit 2
-- select count(*) from _orders
-- select * from _abt_sessions limit 2

// VALIDATION
-- select test_key,meta_original_MASTER_PRODUCT_ID,count(*) as sessions, count(distinct meta_original_session_id) as sessions_distinct,sessions - sessions_distinct as diff
-- from _orders
-- group by 1,2
-- order by 5 desc

DELETE
FROM reporting_base_prod.shared.session_ab_test_ds_image_sort a --comment
    USING _abt_sessions b
WHERE a.session_id = b.session_id;

INSERT INTO reporting_base_prod.shared.session_ab_test_ds_image_sort (session_id,
                                                                      customer_id,
                                                                      ab_test_start_local_date,
                                                                      ab_test_start_local_datetime,
                                                                      ab_test_start_pst_datetime,
                                                                      test_key,
                                                                      master_test_number,
                                                                      test_framework_id,
                                                                      test_label,
                                                                      test_framework_description,
                                                                      test_framework_ticket,
                                                                      campaign_code,
                                                                      test_type,
                                                                      test_activated_datetime,
                                                                      test_effective_end_datetime_pst,
                                                                      test_group,
                                                                      test_group_description,
                                                                      store_brand,
                                                                      store_region,
                                                                      store_country,
                                                                      test_start_membership_state,
                                                                      is_male_customer,
                                                                      platform,
                                                                      master_product_id,
                                                                      rnk_mpid_status_changed,
                                                                      product_effective_start_date_pst,
                                                                      product_effective_end_date_pst,
                                                                      product_name,
                                                                      base_sku,
                                                                      product_sku,
                                                                      color,
                                                                      sub_brand,
                                                                      gender,
                                                                      segment,
                                                                      image_url,
                                                                      grid_views,
                                                                      pdp_views,
                                                                      atb,
                                                                      units_activating,
                                                                      units_nonactivating,
                                                                      max_record_pst)
--CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.session_ab_test_ds_image_sort as
SELECT a.session_id,
       a.customer_id,
       a.ab_test_start_local_datetime::date   AS ab_test_start_local_date,
       a.ab_test_start_local_datetime,
       a.ab_test_start_pst_datetime,
       a.test_key,
       m.master_test_number,
       test_framework_id,
       test_label,
       test_framework_description,
       test_framework_ticket,
       campaign_code,
       a.test_type,
       a.test_effective_start_datetime_pst    AS test_activated_datetime,
       a.test_effective_end_datetime_pst,
       test_group,
       test_group_description,
       store_brand,
       store_region,
       store_country,
       test_start_membership_state,
       is_male_customer,
       a.platform,
       p.meta_original_master_product_id      AS master_product_id,
       rnk_mpid_status_changed,
       p.product_effective_start_date_pst,
       p.product_effective_end_date_pst,
       product_name,
       base_sku,
       m.product_sku,
       color,
       sub_brand,
       gender,
       segment,
       image_url,
       SUM(NVL(grid_view_count, 0))           AS grid_views,
       SUM(NVL(pdp_view_count, 0))            AS pdp_views,
       SUM(NVL(atb_count, 0))                 AS atb,
       SUM(NVL(units_activating_count, 0))    AS units_activating,
       SUM(NVL(units_nonactivating_count, 0)) AS units_nonactivating,
       max_datetime_pst                       AS max_record_pst
FROM _abt_sessions AS a
         JOIN _grid_views AS g
             ON a.meta_original_session_id = g.meta_original_session_id
                AND a.test_key = g.test_key
         JOIN _products AS p
             ON p.meta_original_master_product_id = g.meta_original_master_product_id
                AND a.test_key = p.test_key
                AND p.master_test_number = g.master_test_number
         JOIN _products_meta AS m
             ON p.meta_original_master_product_id = m.meta_original_master_product_id
                AND rnk = 1
                AND a.test_key = m.test_key
                AND m.master_test_number = g.master_test_number
         LEFT JOIN _pdp_views AS pv
             ON g.meta_original_session_id = pv.meta_original_session_id
                AND g.meta_original_master_product_id = pv.meta_original_master_product_id
                AND a.test_key = pv.test_key
                AND pv.master_test_number = g.master_test_number
         LEFT JOIN _atb AS atb
             ON g.meta_original_session_id = atb.meta_original_session_id
                AND g.meta_original_master_product_id = atb.meta_original_master_product_id
                AND a.test_key = atb.test_key
                AND atb.master_test_number = g.master_test_number
         LEFT JOIN _orders AS ord
             ON g.meta_original_session_id = ord.meta_original_session_id
                AND g.meta_original_master_product_id = ord.meta_original_master_product_id
                AND a.test_key = ord.test_key
                AND ord.master_test_number = g.master_test_number
         JOIN _min_max AS mm
             ON mm.test_key = a.test_key
GROUP BY ALL;

-- select * from reporting_base_prod.shared.session_ab_test_ds_image_sort_dev limit 10
-- select count(*) from reporting_base_prod.shared.session_ab_test_ds_image_sort_dev

-- select test_key,test_group,MASTER_PRODUCT_ID,count(*) as sessions,count(distinct SESSION_ID) as sessions_distinct,sessions - sessions_distinct as diff
-- -- from work.dbo.session_ab_test_ds_image_sort_dev
-- from reporting_base_prod.shared.session_ab_test_ds_image_sort_dev
-- group by 1,2,3
-- order by 4 desc
