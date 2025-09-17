SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'staging.segment_branch_open_events';
SET is_full_refresh = (SELECT IFF(public.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE,
                                  FALSE));


SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (SELECT $target_table                               AS table_name,
                  NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
                  high_watermark_datetime                     AS new_high_watermark_datetime
           FROM (SELECT NULL                  AS dependent_table_name,
                        $execution_start_time AS high_watermark_datetime
                 UNION ALL
                 SELECT 'lake.segment_fl.react_native_fabletics_branch_open' AS dependent_table_name,
                        MAX(meta_create_datetime)                            AS high_watermark_datetime
                 FROM lake.segment_fl.react_native_fabletics_branch_open
                 UNION ALL
                 SELECT 'lake.segment_fl.react_native_fabletics_identify' AS dependent_table_name,
                        MAX(meta_create_datetime)                         AS high_watermark_datetime
                 FROM lake.segment_fl.react_native_fabletics_identify
                 WHERE TRY_TO_NUMBER(userid) IS NOT NULL
                 UNION ALL
                 SELECT 'lake_consolidated.ultra_merchant.session' AS dependent_table_name,
                        MAX(meta_update_datetime)                  AS high_watermark_datetime
                 FROM lake_consolidated.ultra_merchant.session
                 WHERE session_id IS NOT NULL
                    AND customer_id IS NOT NULL
                 UNION ALL
                    SELECT 'lake_consolidated.ultra_merchant.visitor_session' AS dependent_table_name,
                        MAX(meta_update_datetime)                  AS high_watermark_datetime
                 FROM lake_consolidated.ultra_merchant.session
                 WHERE session_id IS NOT NULL
                 UNION ALL
                 SELECT 'reporting_base_prod.staging.customer_session' AS dependent_table_name,
                        MAX(meta_update_datetime)                      AS high_watermark_datetime
                 FROM staging.customer_session) AS h) AS s
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


SET wm_lake_segment_fl_react_native_fabletics_branch_open = public.udf_get_watermark($target_table,
                                                                                     'lake.segment_fl.react_native_fabletics_branch_open');
SET wm_lake_ultra_merchant_session = public.udf_get_watermark($target_table,
                                                              'lake_consolidated.ultra_merchant.session');
SET wm_lake_segment_fl_react_native_fabletics_identify = public.udf_get_watermark($target_table,
                                                                                  'lake.segment_fl.react_native_fabletics_identify');
SET wm_reporting_base_prod_staging_customer_session = public.udf_get_watermark($target_table,
                                                                               'reporting_base_prod.staging.customer_session');
SET wm_lake_ultra_merchant_visitor_session = public.udf_get_watermark($target_table,
                                                                      'lake_consolidated.ultra_merchant.visitor_session');


CREATE OR REPLACE TEMP TABLE _branch_open_base AS
SELECT customer_id,
       timestamp_hq,
       messageid,
       ma_utm_source,
       ma_utm_medium,
       ma_utm_campaign,
       ma_utm_term,
       ma_utm_content
FROM ( /* This section grabs the alkey from the URL where available. We are looking for only numeric alkeys as these are customer_ids.
          Once we have the customer_id we join to the customer_session table to find the session_id closest to the branch open event.*/
SELECT CONCAT(COALESCE(bo.properties_alkey, PARSE_JSON(bo._rescued_data):properties.alkey), 20)::VARCHAR AS customer_id,
             CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', bo.timestamp)          AS timestamp_hq,
             bo.messageid,
             LOWER(NVL(bo.properties_utm_source, bo.context_campaign_source))         AS ma_utm_source,
             LOWER(NVL(bo.properties_utm_medium, bo.context_campaign_medium))         AS ma_utm_medium,
             LOWER(NVL(bo."properties_$marketing_title", bo.context_campaign_name))   AS ma_utm_campaign,
             LOWER(bo.properties_utm_term)                                         AS ma_utm_term,
             LOWER(NVL(bo.properties_utm_content, bo.context_campaign_content))       AS ma_utm_content
      FROM lake.segment_fl.react_native_fabletics_branch_open bo
               LEFT JOIN staging.customer_session cs
                    ON CONCAT(COALESCE(bo.properties_alkey, PARSE_JSON(bo._rescued_data):properties.alkey), 20)::VARCHAR = cs.customer_id::VARCHAR
                        AND ABS(DATEDIFF(SECOND, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', bo.timestamp),
                                         cs.datetime_added)) <= 60
                        AND cs.datetime_added >= '2022-09-17 00:00:00.000'
                        AND cs.store_brand_abbr = 'FL'
               LEFT JOIN lake_consolidated.ultra_merchant.session s
                    ON cs.session_id = s.session_id
                        AND s.datetime_added >= '2022-09-17 00:00:00.000'
      WHERE REGEXP_LIKE(COALESCE(bo.properties_alkey, PARSE_JSON(bo._rescued_data):properties.alkey), '^[0-9]+$') --This regex statement looks for only numeric values
        AND (COALESCE(bo.meta_create_datetime, timestamp_hq) > $wm_lake_segment_fl_react_native_fabletics_branch_open
          OR cs.meta_update_datetime > $wm_reporting_base_prod_staging_customer_session
          OR s.meta_update_datetime > $wm_lake_ultra_merchant_session)
        AND COALESCE(bo.meta_create_datetime, timestamp_hq) >= '2022-09-18 00:00:00.000' --The first date we had the branch open event available
      UNION
      /* This section is for the remainder of the records that did not have a numeric alkey. We will look for any that have an @ or are NULL.
         We will use the context_idfv in this scenario. This will map to the context_device_id in the identify data.
         We will get the user_id from the identify data by joining on context_device_id. Once we have the user/customer_id we are able to join
         this to the customer_session table as well to find the session_id that is closest to the branch open event*/
      SELECT CONCAT(i.userid, 20)::VARCHAR                                    AS customer_id,
             CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', bo.timestamp)     AS timestamp_hq,
             bo.messageid,
             LOWER(NVL(bo.properties_utm_source, bo.context_campaign_source))       AS ma_utm_source,
             LOWER(NVL(bo.properties_utm_medium, bo.context_campaign_medium))       AS ma_utm_medium,
             LOWER(NVL(bo."properties_$marketing_title", bo.context_campaign_name)) AS ma_utm_campaign,
             LOWER(bo.properties_utm_term)                                       AS ma_utm_term,
             LOWER(NVL(bo.properties_utm_content, bo.context_campaign_content))     AS ma_utm_content
      FROM lake.segment_fl.react_native_fabletics_branch_open bo
               LEFT JOIN (SELECT DISTINCT userid, context_device_id, MAX(meta_create_datetime) AS meta_create_datetime
                     FROM lake.segment_fl.react_native_fabletics_identify
                     GROUP BY userid, context_device_id) i
                    ON bo.context_idfv::VARCHAR = i.context_device_id::VARCHAR
               LEFT JOIN staging.customer_session cs
                    ON CONCAT(i.userid, 20)::VARCHAR = cs.customer_id::VARCHAR
                        AND ABS(DATEDIFF(SECOND, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', bo.timestamp),
                                         cs.datetime_added)) <= 60
                        AND cs.datetime_added >= '2022-09-17 00:00:00.000'
                        AND cs.store_brand_abbr = 'FL'
               LEFT JOIN lake_consolidated.ultra_merchant.session AS s
                         ON cs.session_id = s.session_id
                             AND s.datetime_added >= '2022-09-17 00:00:00.000'
      WHERE (NOT REGEXP_LIKE(COALESCE(bo.properties_alkey, PARSE_JSON(bo._rescued_data):properties.alkey), '^[0-9]+$')
          OR COALESCE(bo.properties_alkey, PARSE_JSON(bo._rescued_data):properties.alkey) IS NULL)
        AND (COALESCE(bo.meta_create_datetime, timestamp_hq) > $wm_lake_segment_fl_react_native_fabletics_branch_open
          OR cs.meta_update_datetime > $wm_reporting_base_prod_staging_customer_session
          OR s.meta_update_datetime > $wm_lake_ultra_merchant_session
          OR i.meta_create_datetime > $wm_lake_segment_fl_react_native_fabletics_identify)
        AND COALESCE(bo.meta_create_datetime, timestamp_hq) >= '2022-09-18 00:00:00.000')
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, timestamp_hq ORDER BY messageid, ma_utm_source, ma_utm_medium) = 1
;

/* anonymousid/visitor_id was the first way we were able to join branch open events to sessions.
This is used for the first 6 months and occasionally there are still small volumes of records that we can use this logic for. */
CREATE OR REPLACE TEMP TABLE _branch_open_anonymous_base AS
SELECT s.session_id,
       s.meta_original_session_id,
       CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', bo.timestamp)           AS timestamp_hq,
       s.datetime_added,
       bo.messageid,
       LOWER(NVL(bo.properties_utm_source, bo.context_campaign_source))       AS ma_utm_source,
       LOWER(NVL(bo.properties_utm_medium, bo.context_campaign_medium))       AS ma_utm_medium,
       LOWER(NVL(bo."properties_$marketing_title", bo.context_campaign_name)) AS ma_utm_campaign,
       LOWER(bo.properties_utm_term)                                          AS ma_utm_term,
       LOWER(NVL(bo.properties_utm_content, bo.context_campaign_content))     AS ma_utm_content
FROM lake.segment_fl.react_native_fabletics_branch_open bo
         JOIN lake_consolidated.ultra_merchant.visitor_session vs
              ON vs.visitor_id = CONCAT(TRY_CAST(bo.anonymousid AS int), 20)
         LEFT JOIN lake_consolidated.ultra_merchant.session s
                   ON vs.session_id = s.session_id
WHERE (COALESCE(bo.meta_create_datetime,
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', bo.timestamp)) > $wm_lake_segment_fl_react_native_fabletics_branch_open
    OR COALESCE(bo.meta_create_datetime, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', bo.timestamp)) >
       $wm_lake_ultra_merchant_visitor_session)
  AND COALESCE(bo.meta_create_datetime, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', bo.timestamp)) >= '2022-09-18'
  AND bo.anonymousid != 'anonymous'
  AND ABS(DATEDIFF(SECOND, timestamp_hq, vs.datetime_added)) <= 180
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY s.session_id, vs.visitor_id ORDER BY ABS(DATEDIFF(SECOND, timestamp_hq, vs.datetime_added))) =
    1;


CREATE OR REPLACE TEMP TABLE _branch_open_stg AS
SELECT session_id,
       meta_original_session_id,
       messageid,
       timestamp_hq,
       ma_utm_source,
       ma_utm_medium,
       ma_utm_campaign,
       ma_utm_term,
       ma_utm_content,
       meta_row_hash
    FROM
(SELECT s.session_id,
       s.meta_original_session_id,
       base.messageid,
       base.timestamp_hq,
       s.datetime_added,
       base.ma_utm_source,
       base.ma_utm_medium,
       base.ma_utm_campaign,
       base.ma_utm_term,
       base.ma_utm_content,
       HASH(base.ma_utm_source, base.ma_utm_medium, base.ma_utm_campaign, base.ma_utm_term, base.ma_utm_content) AS meta_row_hash
FROM staging.customer_session AS s
         JOIN _branch_open_base base
              ON s.customer_id::VARCHAR = base.customer_id::VARCHAR
                  AND ABS(DATEDIFF(SECOND, base.timestamp_hq, s.datetime_added)) <= 60
                  AND base.timestamp_hq > NVL(s.previous_customer_session_datetime, '1900-01-01 00:00:00.000')
                  AND base.timestamp_hq < NVL(s.next_customer_session_datetime, '9999-12-31 00:00:00.000')
WHERE s.customer_id IS NOT NULL

                  AND base.timestamp_hq >= '2022-09-18 00:00:00.000'
AND s.datetime_added >= '2022-09-17 00:00:00.000'
AND s.store_brand_abbr = 'FL'
UNION
SELECT
    session_id,
    meta_original_session_id,
    messageid,
    timestamp_hq,
    datetime_added,
    ma_utm_source,
    ma_utm_medium,
    ma_utm_campaign,
    ma_utm_term,
    ma_utm_content,
    HASH(ma_utm_source, ma_utm_medium, ma_utm_campaign, ma_utm_term, ma_utm_content) as meta_row_hash
FROM _branch_open_anonymous_base)
QUALIFY ROW_NUMBER() OVER (PARTITION BY messageid ORDER BY ABS(DATEDIFF(SECOND, timestamp_hq, datetime_added))) = 1
AND ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY ABS(DATEDIFF(SECOND, timestamp_hq, datetime_added))) = 1;

MERGE INTO staging.segment_branch_open_events t
    USING _branch_open_stg AS src
    ON src.session_id = t.session_id
    WHEN NOT MATCHED THEN
        INSERT (session_id, meta_original_session_id, messageid, timestamp_hq, ma_utm_source, ma_utm_medium, ma_utm_campaign, ma_utm_term,
                ma_utm_content, meta_row_hash, meta_create_datetime,
                meta_update_datetime)
            VALUES (src.session_id,
                    src.meta_original_session_id,
                    src.messageid,
                    src.timestamp_hq,
                    src.ma_utm_source,
                    src.ma_utm_medium,
                    src.ma_utm_campaign,
                    src.ma_utm_term,
                    src.ma_utm_content,
                    src.meta_row_hash,
                    $execution_start_time,
                    $execution_start_time)
    WHEN MATCHED AND src.meta_row_hash != t.meta_row_hash
        THEN UPDATE SET
        t.session_id = src.session_id,
        t.meta_original_session_id = src.meta_original_session_id,
        t.messageid = src.messageid,
        t.timestamp_hq = src.timestamp_hq,
        t.ma_utm_source = src.ma_utm_source,
        t.ma_utm_medium = src.ma_utm_medium,
        t.ma_utm_campaign = src.ma_utm_campaign,
        t.ma_utm_term = src.ma_utm_term,
        t.ma_utm_content = src.ma_utm_content,
        t.meta_row_hash = src.meta_row_hash,
        t.meta_update_datetime = $execution_start_time;

UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = $execution_start_time
WHERE table_name = $target_table;

