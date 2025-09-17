ALTER SESSION
    SET TIMEZONE = 'America/Los_Angeles';
SET execution_start = CURRENT_TIMESTAMP;

SET wm_test_date_pst = DATE_TRUNC('month', DATEADD('month', -6, $execution_start));

CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__test_framework AS
SELECT meta_company_id,
       test_framework_id,
       store_group_id,
       label,
       start_date,
       end_date,
       start_time,
       end_time,
       type,
       campaign_code,
       code,
       statuscode,
       locale,
       membership_level,
       cohort_split,
       datetime_modified,
       test_framework_ticket_url,
       test_framework_description,
       membership_type_id_list,
       meta_original_test_framework_id,
       effective_start_datetime,
       effective_end_datetime,
       meta_update_datetime
FROM lake_consolidated_view.ultra_cms_history.test_framework AS m
WHERE statuscode = 113
  AND campaign_code = 'FLimagesort'
  AND SPLIT_PART(code, '_v', 2) >= 6
  AND m.meta_update_datetime::DATE >= $wm_test_date_pst
QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY datetime_modified DESC) = 1;

CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__product_test_times AS
SELECT effective_start_date                                         AS product_effective_start_date_pst,
       effective_end_date                                           AS product_effective_end_date_pst,
       evaluation_batch_id,
       is_current_record,
       master_product_id                                            AS meta_original_master_product_id,
       master_test_number,
       image_test_config,
       SPLIT_PART(REGEXP_SUBSTR(SPLIT_PART(PARSE_JSON(PARSE_JSON(image_test_config):"0"):"1"::STRING, ',', 1), '[^,]+'),
                  ':', 2)                                           AS variant_image,
       REPLACE(SPLIT_PART(REGEXP_SUBSTR(SPLIT_PART(PARSE_JSON(PARSE_JSON(image_test_config):"0"):"1"::STRING, ',', 2),
                                        '[^,]+'), ':', 2), '}', '') AS control_image
FROM lake_fl.ultra_rollup.product_image_test_config_history
WHERE image_test_flag = TRUE
  AND hvr_is_deleted = FALSE
  AND evaluation_message = 'START'
  AND datetime_modified::DATE >= $wm_test_date_pst;

SET wm_test_date = DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', (SELECT MIN(product_effective_start_date_pst)
                                                                FROM _ds_image_test__product_test_times)));

CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__product_evaluation AS
SELECT master_test_number,
       master_product_id,
       evaluation_message,
       evaluation_batch_id
FROM lake_fl.ultra_rollup.product_image_test_config_history
WHERE hvr_is_deleted = FALSE
  AND evaluation_message != 'START'
  AND master_test_number IN (SELECT DISTINCT master_test_number FROM _ds_image_test__product_test_times)
QUALIFY ROW_NUMBER() OVER (PARTITION BY master_test_number, master_product_id ORDER BY datetime_modified DESC) = 1;

CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__product_test_keys AS
SELECT GREATEST(tt.product_effective_start_date_pst, tf.effective_start_datetime) AS test_effective_start_datetime,
       LEAST(tt.product_effective_end_date_pst, tf.effective_end_datetime)        AS test_effective_end_datetime,
       tt.evaluation_batch_id,
       tt.is_current_record,
       meta_original_master_product_id,
       tt.master_test_number,
       tf.test_key,
       tf.test_type,
       tt.variant_image,
       tt.control_image
FROM _ds_image_test__product_test_times tt
         LEFT JOIN
     (SELECT MIN(effective_start_datetime) AS effective_start_datetime,
             MAX(effective_end_datetime)   AS effective_end_datetime,
             code                          AS test_key,
             'FLimagesort'                 AS campaign_code,
             type                          AS test_type
      FROM _ds_image_test__test_framework
      WHERE campaign_code = 'FLimagesort'
        AND statuscode = 113
      GROUP BY test_key, type) AS tf
     ON GREATEST(tt.product_effective_start_date_pst, tf.effective_start_datetime) <
        LEAST(tt.product_effective_end_date_pst, tf.effective_end_datetime)
WHERE campaign_code = 'FLimagesort'
ORDER BY tf.campaign_code,
         master_test_number,
         product_effective_start_date_pst;

CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__products AS
SELECT master_test_number,
       meta_original_master_product_id,
       MIN(test_effective_start_datetime)                                     AS test_effective_start_datetime,
       MAX(test_effective_end_datetime)                                       AS test_effective_end_datetime,
       MIN(test_effective_start_datetime)                                     AS product_effective_start_date_pst,
       MAX(test_effective_end_datetime)                                       AS product_effective_end_date_pst,
       test_key,
       test_type,
       CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                        product_effective_start_date_pst::datetime)::datetime AS product_effective_start_datetime_utc,
       CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                        product_effective_end_date_pst::datetime)::datetime   AS product_effective_end_datetime_utc,
    concat(control_image,':',variant_image) as image_pairs
FROM _ds_image_test__product_test_keys
GROUP BY master_test_number,
         meta_original_master_product_id,
         test_key,
         test_type,
         control_image,
         variant_image
ORDER BY master_test_number,
         meta_original_master_product_id,
         test_key;

CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__products_meta AS
SELECT DISTINCT test_key,
                sam.master_test_number,
                test_type,
                store_id                                                                               AS product_store_id,
                dp.group_code,
                sam.product_effective_start_date_pst,
                sam.product_effective_end_date_pst,
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                 sam.product_effective_start_date_pst::datetime)::datetime                                                         AS product_effective_start_datetime_utc,
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                 sam.product_effective_end_date_pst::datetime)::datetime                                                           AS product_effective_end_datetime_utc,
                IFF(dp.master_product_id = -1, dp.product_id, dp.master_product_id)                                                                AS master_product_id,
                IFF(dp.master_product_id = -1, dp.meta_original_product_id,
                    edw_prod.stg.udf_unconcat_brand(dp.master_product_id))                                                                         AS meta_original_master_product_id,
                RANK() OVER (PARTITION BY test_key,sam.meta_original_master_product_id,product_effective_start_datetime_utc ORDER BY base_sku ASC) AS rnk,
                NULL                                                                                                                                  rnk_mpid_status_changed,
                product_alias,
                current_name                                                                                                                       AS product_name,   --UBT NAME
                base_sku,
                dp.product_sku,
                ubt.color,
                ubt.category,
                ubt.class,
                ubt.subclass,
                IFF(ubt.item_status ILIKE '%CORE', 'CORE', 'FASHION')                                                                              AS lifestyle_type, --,ubt.INITIAL_LAUNCH as ubt_special_collection
                ubt.sub_brand                                                                                                                      AS sub_brand,
                ubt.gender                                                                                                                         AS gender,
                CASE
                    WHEN sub_brand = 'Scrubs' AND ubt.gender ILIKE 'MEN%' THEN 'SC-M-PROD'
                    WHEN sub_brand = 'Scrubs' THEN 'SC-W-PROD'
                    WHEN sub_brand = 'Fabletics' AND ubt.gender ILIKE 'MEN%' THEN 'FL-M-PROD'
                    WHEN sub_brand = 'Fabletics' THEN 'FL-W-PROD'
                    WHEN sub_brand = 'Yitty'
                        THEN 'YT-PROD' END                                                                                                         AS segment,
                CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.sku || '/' || ubt.sku ||
                       '-1_271x407.jpg')                                                                                                           AS image_url,      --ubt table
                COALESCE(i1.inactive_test_product_outcome,
                         i2.inactive_test_product_outcome)                                                                                         AS inactive_test_product_outcome,
                pe.evaluation_message,
                pe.evaluation_batch_id
FROM _ds_image_test__products AS sam
         JOIN edw_prod.stg.dim_product AS dp
              ON sam.meta_original_master_product_id = edw_prod.stg.udf_unconcat_brand(dp.master_product_id)
    AND split_part(dp.product_alias, ' ',1) = dp.product_sku
         JOIN
     (SELECT DISTINCT ubt.*
      FROM lake.excel.fl_merch_items_ubt_hierarchy ubt
      QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY current_showroom DESC) = 1) ubt
     ON dp.product_sku = ubt.sku
         LEFT JOIN lake_fl.ultra_rollup.products_in_stock_product_feed AS fe
                   ON ubt.sku = fe.product_sku
                       AND store_group_id = 16
                       AND is_outfit = FALSE
                       AND is_bundle_product = FALSE
                       AND product_type_id NOT IN (11, 4, 16)
                       AND image_count > 0
         LEFT JOIN lake_view.sharepoint.ab_test_product_import_mpid_list AS i1
                   ON i1.master_product_id = dp.meta_original_master_product_id
                       AND i1.inactive_test_product_outcome IS NOT NULL
         LEFT JOIN lake_view.sharepoint.ab_test_product_import_mpid_list AS i2
                   ON i2.product_sku = dp.product_sku
                       AND i2.inactive_test_product_outcome IS NOT NULL
         LEFT JOIN _ds_image_test__product_evaluation pe ON sam.master_test_number = pe.master_test_number
    AND sam.meta_original_master_product_id = pe.master_product_id

UNION ALL

SELECT DISTINCT test_key,
                sam.master_test_number,
                test_type,
                dp.store_id                                                                                                                        AS product_store_id,
                dp.group_code,
                product_effective_start_date_pst,
                product_effective_end_date_pst,
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                 product_effective_start_date_pst::datetime)::datetime                                                             AS product_effective_start_datetime_utc,
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                 product_effective_end_date_pst::datetime)::datetime                                                               AS product_effective_end_datetime_utc,
                IFF(dp.master_product_id = -1, dp.product_id, dp.master_product_id)                                                                AS master_product_id,
                IFF(dp.master_product_id = -1, dp.meta_original_product_id,
                    dp.meta_original_master_product_id)                                                                                            AS meta_original_master_product_id,
                RANK() OVER (PARTITION BY test_key,sam.meta_original_master_product_id,product_effective_start_datetime_utc ORDER BY base_sku ASC) AS rnk,
                NULL                                                                                                                                  rnk_mpid_status_changed,
                product_alias,
                product_name,
                base_sku,
                product_alias                                                                                                                      AS product_sku,
                color,
                NULL                                                                                                                                  category,
                NULL                                                                                                                                  class,
                NULL                                                                                                                                  subclass,
                NULL                                                                                                                                  lifestyle_type,
                ubt.sub_brand                                                                                                                      AS sub_brand,
                ubt.gender                                                                                                                         AS gender,
                CASE
                    WHEN sub_brand = 'Scrubs' AND ubt.gender ILIKE 'MEN%' THEN 'SC-M-PROD'
                    WHEN sub_brand = 'Scrubs' THEN 'SC-W-PROD'
                    WHEN sub_brand = 'Fabletics' AND ubt.gender ILIKE 'MEN%' THEN 'FL-M-PROD'
                    WHEN sub_brand = 'Fabletics' THEN 'FL-W-PROD'
                    WHEN sub_brand = 'Yitty'
                        THEN 'YT-PROD' END                                                                                                         AS segment,
                CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.outfit_number || '/' ||
                       ubt.outfit_number ||
                       '-1_271x407.jpg')                                                                                                           AS image_url, --ubt table
                NULL                                                                                                                               AS inactive_test_outcome,
                pe.evaluation_message,
                pe.evaluation_batch_id
FROM _ds_image_test__products AS sam
         JOIN edw_prod.stg.dim_product AS dp
              ON sam.meta_original_master_product_id = dp.meta_original_product_id--bundles
         JOIN lake.excel.fl_merch_outfits_ubt_hierarchy AS ubt
              ON ubt.outfit_number = dp.product_alias
         LEFT JOIN _ds_image_test__product_evaluation pe ON sam.master_test_number = pe.master_test_number
    AND sam.meta_original_master_product_id = pe.master_product_id
WHERE dp.master_product_id = -1
  AND product_type = 'Bundle'
ORDER BY 1;


CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__customers AS
SELECT DISTINCT m.customer_id,
                md.membership_id,
                md.name,
                value,
                md.datetime_modified,
                me.membership_state
FROM lake_consolidated.ultra_merchant.membership_detail md
         LEFT JOIN lake_consolidated.ultra_merchant.membership m ON md.membership_id = m.membership_id
         LEFT JOIN edw_prod.data_model.fact_membership_event me
                   ON m.customer_id = me.customer_id
                       AND
                      md.datetime_modified BETWEEN CONVERT_TIMEZONE('America/Los_Angeles', event_start_local_datetime)
                          AND CONVERT_TIMEZONE('America/Los_Angeles', event_end_local_datetime)
WHERE md.name IN (SELECT DISTINCT code FROM _ds_image_test__test_framework);

CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__session AS
SELECT meta_original_session_id,
       edw_prod.stg.udf_unconcat_brand(s.customer_id) AS meta_original_customer_id,
       platform,
       c.membership_id,
       name,
       value,
       session_local_datetime,
       store_id,
       is_male_customer,
       c.membership_state
FROM reporting_base_prod.shared.session s
         JOIN _ds_image_test__customers c ON s.customer_id = c.customer_id
         JOIN _ds_image_test__test_framework t
              ON CONVERT_TIMEZONE('America/Los_Angeles', session_local_datetime) BETWEEN effective_start_datetime AND effective_end_datetime AND
                 name = code
WHERE NOT is_bot
  AND NOT is_test_customer_account
  AND is_in_segment;


CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__grid_views AS
SELECT DISTINCT event,
                test_key,
                master_test_number,
                test_type,
                platform,
                meta_original_master_product_id,
                meta_original_session_id,
                meta_original_customer_id,
                properties_loggedin_status
FROM (SELECT --pieced
             event,
             d.test_key,
             d.master_test_number,
             d.test_type,
             a.platform,
             a.meta_original_session_id,
             a.meta_original_customer_id,
             meta_original_master_product_id,
             properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.javascript_fabletics_product_list_viewed AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_products_product_id)
                        AND
                       p.timestamp BETWEEN d.product_effective_start_datetime_utc AND d.product_effective_end_datetime_utc
                        AND d.rnk = 1
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(grid_views, 0) > 0
      WHERE TO_TIMESTAMP(timestamp)::datetime >= product_effective_start_datetime_utc
        AND TO_TIMESTAMP(timestamp)::datetime <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT --pieced
             event,
             d.test_key,
             d.master_test_number,
             d.test_type,
             a.platform,
             a.meta_original_session_id,
             a.meta_original_customer_id,
             meta_original_master_product_id,
             properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.react_native_fabletics_product_list_viewed AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_products_product_id)
                        AND d.rnk = 1
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(grid_views, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT --outfit
             event,
             d.test_key,
             d.master_test_number,
             d.test_type,
             a.platform,
             a.meta_original_session_id,
             a.meta_original_customer_id,
             properties_products_bundle_product_id AS meta_original_master_product_id,
             properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.javascript_fabletics_product_list_viewed AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND p.properties_products_is_bundle = TRUE
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id =
                       IFF(p.properties_products_bundle_product_id = '', NULL,
                           p.properties_products_bundle_product_id)
                        AND d.rnk = 1
                        AND d.group_code = 'outfit'
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(grid_views, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT --outfit
             event,
             d.test_key,
             d.master_test_number,
             d.test_type,
             a.platform,
             a.meta_original_session_id,
             a.meta_original_customer_id,
             properties_products_bundle_product_id AS meta_original_master_product_id,
             properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.react_native_fabletics_product_list_viewed AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND p.properties_products_is_bundle = TRUE
                        AND timestamp::DATE >= $wm_test_date
               LEFT JOIN _ds_image_test__products_meta AS d
                         ON d.meta_original_master_product_id = p.properties_products_bundle_product_id--iff(p.PROPERTIES_PRODUCTS_BUNDLE_PRODUCT_ID = '',null,p.PROPERTIES_PRODUCTS_BUNDLE_PRODUCT_ID)
                             AND d.rnk = 1
                             AND d.group_code = 'outfit'
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(grid_views, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL);


CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__pdp_views AS
SELECT DISTINCT event,
                test_key,
                master_test_number,
                test_type,
                platform,
                meta_original_master_product_id,
                meta_original_session_id,
                meta_original_customer_id,
                properties_loggedin_status
FROM (SELECT DISTINCT event, --items
                      d.test_key,
                      d.master_test_number,
                      d.test_type,
                      a.platform,
                      a.meta_original_session_id,
                      a.meta_original_customer_id,
                      meta_original_master_product_id,
                      properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.javascript_fabletics_product_viewed AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_product_id)
                        AND d.rnk = 1
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(pdp_views, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL


      UNION ALL

      SELECT DISTINCT event, --items
                      d.test_key,
                      d.master_test_number,
                      d.test_type,
                      a.platform,
                      a.meta_original_session_id,
                      a.meta_original_customer_id,
                      meta_original_master_product_id,
                      properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.react_native_fabletics_product_viewed AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_product_id)
                        AND d.rnk = 1
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(pdp_views, 0) > 0
      WHERE TO_TIMESTAMP(timestamp) >= product_effective_start_datetime_utc
        AND TO_TIMESTAMP(timestamp) <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT DISTINCT event, --outfit
                      d.test_key,
                      d.master_test_number,
                      d.test_type,
                      a.platform,
                      a.meta_original_session_id,
                      a.meta_original_customer_id,
                      p.properties_bundle_product_id AS meta_original_master_product_id,
                      properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.javascript_fabletics_product_viewed AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
                        AND p.properties_is_bundle = TRUE
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id =
                       p.properties_bundle_product_id --iff(p.PROPERTIES_BUNDLE_PRODUCT_ID = '',null,p.PROPERTIES_BUNDLE_PRODUCT_ID)
                        AND d.rnk = 1
                        AND d.group_code = 'outfit'
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(pdp_views, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT DISTINCT event, --outfit
                      d.test_key,
                      d.master_test_number,
                      d.test_type,
                      a.platform,
                      a.meta_original_session_id,
                      a.meta_original_customer_id,
                      p.properties_bundle_product_id AS meta_original_master_product_id,
                      properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.react_native_fabletics_product_viewed AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
                        AND p.properties_is_bundle = TRUE
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = p.properties_bundle_product_id--iff(p.PROPERTIES_BUNDLE_PRODUCT_ID = '',null,p.PROPERTIES_BUNDLE_PRODUCT_ID)
                        AND d.rnk = 1
                        AND d.group_code = 'outfit'
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(pdp_views, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL)
;

-- no atb react table for app. per jon, use java version since ATB is supposed to be server side event
CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__atb AS
SELECT DISTINCT event,
                timestamp,
                test_key,
                master_test_number,
                test_type,
                platform,
                meta_original_master_product_id,
                meta_original_session_id,
                meta_original_customer_id,
                properties_loggedin_status
FROM (SELECT DISTINCT event, --item
                      timestamp,
                      d.test_key,
                      d.master_test_number,
                      d.test_type,
                      a.platform,
                      a.meta_original_session_id,
                      a.meta_original_customer_id,
                      meta_original_master_product_id,
                      properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.java_fabletics_product_added AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_product_id)
                        AND d.rnk = 1
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(atb, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT DISTINCT event, --item
                      timestamp,
                      d.test_key,
                      d.master_test_number,
                      d.test_type,
                      a.platform,
                      a.meta_original_session_id,
                      a.meta_original_customer_id,
                      meta_original_master_product_id,
                      properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_product_added AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(p.properties_product_id)
                        AND d.rnk = 1
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(atb, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT DISTINCT event, --outfit
                      timestamp,
                      d.test_key,
                      d.master_test_number,
                      d.test_type,
                      a.platform,
                      a.meta_original_session_id,
                      a.meta_original_customer_id,
                      p.properties_bundle_product_id AS meta_original_master_product_id,
                      properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.java_fabletics_product_added AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND p.properties_is_bundle = TRUE
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id =
                       IFF(p.properties_bundle_product_id = '', NULL, p.properties_bundle_product_id)
                        AND d.rnk = 1
                        AND d.group_code = 'outfit'
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(atb, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT DISTINCT event, --outfit
                      timestamp,
                      d.test_key,
                      d.master_test_number,
                      d.test_type,
                      a.platform,
                      a.meta_original_session_id,
                      a.meta_original_customer_id,
                      p.properties_bundle_product_id AS meta_original_master_product_id,
                      properties_loggedin_status
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_product_added AS p
                    ON a.meta_original_session_id = TRY_TO_NUMBER(p.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
                        AND p.properties_is_bundle = TRUE
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id =
                       IFF(p.properties_bundle_product_id = '', NULL, p.properties_bundle_product_id)
                        AND d.rnk = 1
                        AND d.group_code = 'outfit'
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(atb, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL);


CREATE OR REPLACE TEMPORARY TABLE _ds_image_test__orders AS
SELECT DISTINCT event,
                timestamp,
                test_key,
                master_test_number,
                test_type,
                platform,
                meta_original_master_product_id,
                meta_original_session_id,
                meta_original_customer_id,
                properties_loggedin_status,
                properties_is_activating,
                properties_products_price
FROM (SELECT event,
             timestamp,
             d.test_key,
             d.master_test_number,
             d.test_type,
             a.platform,
             a.meta_original_session_id,
             a.meta_original_customer_id,
             meta_original_master_product_id,
             properties_loggedin_status,
             properties_is_activating,
             properties_products_price
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.java_fabletics_order_completed AS o
                    ON a.meta_original_session_id = TRY_TO_NUMBER(o.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(o.properties_products_product_id)
                        AND d.rnk = 1
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(units_nonactivating + units_activating, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT event,
             timestamp,
             d.test_key,
             d.master_test_number,
             d.test_type,
             a.platform,
             a.meta_original_session_id,
             a.meta_original_customer_id,
             meta_original_master_product_id,
             properties_loggedin_status,
             properties_is_activating,
             properties_products_price
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed AS o
                    ON a.meta_original_session_id = TRY_TO_NUMBER(o.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(o.properties_products_product_id)
                        AND d.rnk = 1
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(units_activating + units_nonactivating, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT --outfits
             event,
             timestamp,
             d.test_key,
             d.master_test_number,
             d.test_type,
             a.platform,
             a.meta_original_session_id,
             a.meta_original_customer_id,
             meta_original_master_product_id,
             properties_loggedin_status,
             properties_is_activating,
             properties_products_price
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.java_fabletics_order_completed AS o
                    ON a.meta_original_session_id = TRY_TO_NUMBER(o.properties_session_id)
                        AND timestamp::DATE >= $wm_test_date
                        AND properties_products_is_bundle = TRUE
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(o.properties_products_bundle_product_id)
                        AND d.rnk = 1
                        AND d.group_code = 'outfit'
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(units_nonactivating + units_activating, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL

      UNION ALL

      SELECT event, --outfits
             timestamp,
             d.test_key,
             d.master_test_number,
             d.test_type,
             a.platform,
             a.meta_original_session_id,
             a.meta_original_customer_id,
             meta_original_master_product_id,
             properties_loggedin_status,
             properties_is_activating,
             properties_products_price
      FROM _ds_image_test__session AS a
               JOIN lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed AS o
                    ON a.meta_original_session_id = TRY_TO_NUMBER(o.properties_session_id)
                        AND properties_products_is_bundle = TRUE
                        AND timestamp::DATE >= $wm_test_date
               JOIN _ds_image_test__products_meta AS d
                    ON d.meta_original_master_product_id = TRY_TO_NUMBER(o.properties_products_bundle_product_id)
                        AND d.rnk = 1
                        AND d.group_code = 'outfit'
               LEFT JOIN shared.session_ab_test_ds_image_test AS t
                         ON a.meta_original_session_id = t.session_id
                             AND d.master_product_id = t.master_product_id
                             AND t.product_effective_start_date_pst = d.product_effective_start_date_pst
                             AND t.product_effective_end_date_pst = d.product_effective_end_date_pst
                             AND NVL(units_nonactivating + units_activating, 0) > 0
      WHERE timestamp >= product_effective_start_datetime_utc
        AND timestamp <= product_effective_end_datetime_utc
        AND t.session_id IS NULL
        AND t.master_product_id IS NULL);


CREATE OR REPLACE TEMPORARY TABLE _stg_ds_image_testing AS
SELECT a.meta_original_session_id                                                                                   AS session_id,
       a.meta_original_customer_id                                                                                  AS customer_id,
       a.session_local_datetime::DATE                                                                               AS ab_test_start_local_date,
       a.session_local_datetime                                                                                     AS ab_test_start_local_datetime,
       CONVERT_TIMEZONE('America/Los_Angeles', a.session_local_datetime)                                            AS ab_test_start_pst_datetime,
       tf.code                                                                                                      AS test_key,
       m.master_test_number,
       tf.test_framework_id,
       tf.label                                                                                                     AS test_label,
       tf.test_framework_description,
       test_framework_ticket_url                                                                                    AS test_framework_ticket,
       campaign_code,
       tf.type                                                                                                      AS test_type,
       tf.effective_start_datetime                                                                                  AS test_activated_datetime,
       tf.effective_end_datetime                                                                                    AS test_effective_end_datetime_pst,
       IFF(a.value = 2, 'Variant', 'Control')                                                                       AS test_group,
       IFF(a.value = 2, 'VARIANT', 'CONTROL')                                                                       AS test_group_description,
       ds.store_brand,
       ds.store_region,
       ds.store_country,
       a.membership_state                                                                                           AS test_start_membership_state,
       is_male_customer,
       a.platform,
       p.meta_original_master_product_id                                                                            AS master_product_id,
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
       evaluation_message,
       evaluation_batch_id,
       p.image_pairs,
       NVL(COUNT(DISTINCT g.meta_original_master_product_id), 0)                                                    AS grid_views,
       NVL(COUNT(DISTINCT pv.meta_original_master_product_id), 0)                                                   AS pdp_views,
       NVL(COUNT(DISTINCT atb.meta_original_master_product_id), 0)                                                  AS atb,
       NVL(COUNT(DISTINCT IFF(ord.properties_is_activating = TRUE, ord.meta_original_master_product_id, NULL)),
           0)                                                                                                       AS units_activating,
       NVL(COUNT(DISTINCT IFF(ord.properties_is_activating = FALSE, ord.meta_original_master_product_id, NULL)),
           0)                                                                                                       AS units_nonactivating,
       (SELECT MAX(CONVERT_TIMEZONE('America/Los_Angeles', session_local_datetime))
        FROM _ds_image_test__session)                                                                                 AS max_record_pst,
       SUM(NVL(ord.properties_products_price, 0))                                                                   AS revenue
FROM _ds_image_test__session AS a
         LEFT JOIN _ds_image_test__products p
                   ON CONVERT_TIMEZONE('America/Los_Angeles', a.session_local_datetime) BETWEEN p.product_effective_start_date_pst AND p.product_effective_end_date_pst
                       AND a.name = p.test_key
         LEFT JOIN _ds_image_test__grid_views AS g
                   ON a.meta_original_session_id = g.meta_original_session_id
                       AND a.name = g.test_key
                       AND p.meta_original_master_product_id = g.meta_original_master_product_id
                       AND p.master_test_number = g.master_test_number
         LEFT JOIN _ds_image_test__products_meta AS m
                   ON p.meta_original_master_product_id = m.meta_original_master_product_id
                       AND rnk = 1
                       AND p.master_test_number = g.master_test_number
        LEFT JOIN edw_prod.stg.dim_store ds
            ON m.product_store_id = ds.store_id
         LEFT JOIN _ds_image_test__test_framework tf
                   ON m.test_key = tf.code
         LEFT JOIN _ds_image_test__pdp_views AS pv
                   ON a.meta_original_session_id = pv.meta_original_session_id
                       AND p.meta_original_master_product_id = pv.meta_original_master_product_id
                       AND pv.master_test_number = p.master_test_number
         LEFT JOIN _ds_image_test__atb AS atb
                   ON g.meta_original_session_id = atb.meta_original_session_id
                       AND g.meta_original_master_product_id = atb.meta_original_master_product_id
                       AND atb.master_test_number = p.master_test_number
         LEFT JOIN _ds_image_test__orders AS ord
                   ON g.meta_original_session_id = ord.meta_original_session_id
                       AND g.meta_original_master_product_id = ord.meta_original_master_product_id
                       AND ord.master_test_number = p.master_test_number
WHERE CONVERT_TIMEZONE('UTC', session_local_datetime) BETWEEN p.product_effective_start_datetime_utc AND p.product_effective_end_datetime_utc
  AND CONVERT_TIMEZONE('UTC', session_local_datetime) BETWEEN m.product_effective_start_datetime_utc AND m.product_effective_end_datetime_utc
GROUP BY ALL;

ALTER TABLE _stg_ds_image_testing
    ADD meta_row_hash number(38);

UPDATE _stg_ds_image_testing AS s
SET s.meta_row_hash = HASH(
        session_id, customer_id, ab_test_start_local_date, ab_test_start_local_datetime, ab_test_start_pst_datetime,
        test_key, master_test_number, test_framework_id, test_label, test_framework_description, test_framework_ticket,
        campaign_code, test_type, test_activated_datetime, test_group, test_group_description, store_brand,
        store_region, store_country, test_start_membership_state, is_male_customer, platform, master_product_id,
        rnk_mpid_status_changed, product_effective_start_date_pst, product_name, base_sku, color, sub_brand, gender,
        segment, image_url, image_pairs
                      );

MERGE INTO shared.session_ab_test_ds_image_test AS t
    USING _stg_ds_image_testing AS src
    ON EQUAL_NULL(t.session_id, src.session_id)
    AND EQUAL_NULL(t.master_product_id, src.master_product_id)
    WHEN NOT MATCHED THEN
        INSERT (
                session_id,
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
                evaluation_message,
                evaluation_batch_id,
                image_pairs,
                grid_views,
                pdp_views,
                atb,
                units_activating,
                units_nonactivating,
                revenue,
                max_record_pst,
                meta_create_datetime,
                meta_update_datetime,
                meta_row_hash)
            VALUES (src.session_id,
                    src.customer_id,
                    src.ab_test_start_local_date,
                    src.ab_test_start_local_datetime,
                    src.ab_test_start_pst_datetime,
                    src.test_key,
                    src.master_test_number,
                    src.test_framework_id,
                    src.test_label,
                    src.test_framework_description,
                    src.test_framework_ticket,
                    src.campaign_code,
                    src.test_type,
                    src.test_activated_datetime,
                    src.test_effective_end_datetime_pst,
                    src.test_group,
                    src.test_group_description,
                    src.store_brand,
                    src.store_region,
                    src.store_country,
                    src.test_start_membership_state,
                    src.is_male_customer,
                    src.platform,
                    src.master_product_id,
                    src.rnk_mpid_status_changed,
                    src.product_effective_start_date_pst,
                    src.product_effective_end_date_pst,
                    src.product_name,
                    src.base_sku,
                    src.product_sku,
                    src.color,
                    src.sub_brand,
                    src.gender,
                    src.segment,
                    src.image_url,
                    src.evaluation_message,
                    src.evaluation_batch_id,
                    src.image_pairs,
                    src.grid_views,
                    src.pdp_views,
                    src.atb,
                    src.units_activating,
                    src.units_nonactivating,
                    src.revenue,
                    src.max_record_pst,
                    $execution_start,
                    $execution_start,
                    src.meta_row_hash)
    WHEN MATCHED AND NOT (
        EQUAL_NULL(t.customer_id, src.customer_id)
            OR EQUAL_NULL(t.ab_test_start_local_date, src.ab_test_start_local_date)
            OR EQUAL_NULL(t.ab_test_start_local_datetime, src.ab_test_start_local_datetime)
            OR EQUAL_NULL(t.ab_test_start_pst_datetime, t.ab_test_start_pst_datetime)
            OR EQUAL_NULL(t.test_key, src.test_key)
            OR EQUAL_NULL(t.master_test_number, src.master_test_number)
            OR EQUAL_NULL(t.test_framework_id, src.test_framework_id)
            OR EQUAL_NULL(t.test_label, src.test_label)
            OR EQUAL_NULL(t.test_framework_description, src.test_framework_description)
            OR EQUAL_NULL(t.test_framework_ticket, src.test_framework_ticket)
            OR EQUAL_NULL(t.campaign_code, src.campaign_code)
            OR EQUAL_NULL(t.test_type, src.test_type)
            OR EQUAL_NULL(t.test_activated_datetime, src.test_activated_datetime)
            OR EQUAL_NULL(t.test_group, src.test_group)
            OR EQUAL_NULL(t.test_group_description, src.test_group_description)
            OR EQUAL_NULL(t.store_brand, src.store_brand)
            OR EQUAL_NULL(t.store_region, src.store_region)
            OR EQUAL_NULL(t.store_country, src.store_country)
            OR EQUAL_NULL(t.test_start_membership_state, src.test_start_membership_state)
            OR EQUAL_NULL(t.is_male_customer, src.is_male_customer)
            OR EQUAL_NULL(t.platform, src.platform)
            OR EQUAL_NULL(t.rnk_mpid_status_changed, src.rnk_mpid_status_changed)
            OR EQUAL_NULL(t.product_effective_start_date_pst, src.product_effective_start_date_pst)
            OR EQUAL_NULL(t.product_name, src.product_name)
            OR EQUAL_NULL(t.base_sku, src.base_sku)
            OR EQUAL_NULL(t.color, src.color)
            OR EQUAL_NULL(t.sub_brand, src.sub_brand)
            OR EQUAL_NULL(t.gender, src.gender)
            OR EQUAL_NULL(t.segment, src.segment)
            OR EQUAL_NULL(t.image_url, src.image_url)
            OR EQUAL_NULL(t.image_pairs, src.image_pairs)
            OR EQUAL_NULL(t.test_effective_end_datetime_pst, src.test_effective_end_datetime_pst)
            OR EQUAL_NULL(t.product_effective_end_date_pst, src.product_effective_end_date_pst)
            OR EQUAL_NULL(t.evaluation_message, src.evaluation_message)
            OR EQUAL_NULL(t.evaluation_batch_id, src.evaluation_batch_id)
            OR EQUAL_NULL(t.grid_views, src.grid_views)
            OR EQUAL_NULL(t.pdp_views, src.pdp_views)
            OR EQUAL_NULL(t.atb, src.atb)
            OR EQUAL_NULL(t.units_activating, src.units_activating)
            OR EQUAL_NULL(t.units_nonactivating, src.units_nonactivating)
            OR EQUAL_NULL(t.revenue, src.revenue)
            OR EQUAL_NULL(t.product_sku, src.product_sku)
        ) THEN
        UPDATE SET
            t.customer_id = src.customer_id,
            t.ab_test_start_local_date = src.ab_test_start_local_date,
            t.ab_test_start_local_datetime = src.ab_test_start_local_datetime,
            t.ab_test_start_pst_datetime = t.ab_test_start_pst_datetime,
            t.test_key = src.test_key,
            t.master_test_number = src.master_test_number,
            t.test_framework_id = src.test_framework_id,
            t.test_label = src.test_label,
            t.test_framework_description = src.test_framework_description,
            t.test_framework_ticket = src.test_framework_ticket,
            t.campaign_code = src.campaign_code,
            t.test_type = src.test_type,
            t.test_activated_datetime = src.test_activated_datetime,
            t.test_group = src.test_group,
            t.test_group_description = src.test_group_description,
            t.store_brand = src.store_brand,
            t.store_region = src.store_region,
            t.store_country = src.store_country,
            t.test_start_membership_state = src.test_start_membership_state,
            t.is_male_customer = src.is_male_customer,
            t.platform = src.platform,
            t.rnk_mpid_status_changed = src.rnk_mpid_status_changed,
            t.product_effective_start_date_pst = src.product_effective_start_date_pst,
            t.product_name = src.product_name,
            t.base_sku = src.base_sku,
            t.color = src.color,
            t.sub_brand = src.sub_brand,
            t.gender = src.gender,
            t.segment = src.segment,
            t.image_url = src.image_url,
            t.image_pairs = src.image_pairs,
            t.test_effective_end_datetime_pst = src.test_effective_end_datetime_pst,
            t.product_effective_end_date_pst = src.product_effective_end_date_pst,
            t.evaluation_message = src.evaluation_message,
            t.evaluation_batch_id = src.evaluation_batch_id,
            t.grid_views = src.grid_views,
            t.pdp_views = src.pdp_views,
            t.atb = src.atb,
            t.units_activating = src.units_activating,
            t.units_nonactivating = src.units_nonactivating,
            t.revenue = src.revenue,
            t.product_sku = src.product_sku,
            t.meta_update_datetime = $execution_start;
