SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'staging.customer_session';
SET is_full_refresh = (SELECT IFF(public.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (SELECT $target_table                               AS table_name,
                  NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
                  high_watermark_datetime                     AS new_high_watermark_datetime
           FROM (
                 SELECT 'lake_consolidated_view.ultra_merchant.session' AS dependent_table_name,
                        MAX(meta_update_datetime)                     AS high_watermark_datetime
                 FROM lake_consolidated.ultra_merchant.session
                 UNION ALL
                 SELECT NULL AS dependent_table_name,
                        NVL(MAX(meta_update_datetime), $execution_start_time)  AS high_watermark_datetime
                 FROM staging.customer_session
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

SET wm_lake_ultra_merchant_session = public.udf_get_watermark($target_table, 'lake_consolidated_view.ultra_merchant.session');

CREATE OR REPLACE TEMP TABLE _customer_session_base
(
    session_id  NUMERIC(38, 0),
    customer_id NUMERIC(38, 0)
);

INSERT INTO _customer_session_base
SELECT DISTINCT session_id, customer_id
FROM lake_consolidated.ultra_merchant.session s
WHERE customer_id IS NOT NULL
  AND $is_full_refresh
ORDER BY session_id;

CREATE OR REPLACE TEMP TABLE _session_base AS
SELECT DISTINCT session_id,
                customer_id
FROM lake_consolidated.ultra_merchant.session
WHERE meta_update_datetime > $wm_lake_ultra_merchant_session
  AND customer_id IS NOT NULL
  AND NOT $is_full_refresh
ORDER BY session_id;

CREATE OR REPLACE TEMP TABLE _customer_hist AS
SELECT DISTINCT s.session_id,
                s.customer_id
FROM staging.customer_session AS s
WHERE s.customer_id IN (SELECT DISTINCT customer_id FROM _session_base)
  AND NOT $is_full_refresh;

INSERT INTO _customer_session_base
SELECT DISTINCT session_id,
                customer_id
FROM (SELECT sb.session_id,
             sb.customer_id
      FROM _session_base sb
      UNION ALL
      SELECT ch.session_id,
             ch.customer_id
      FROM _customer_hist ch) AS a
WHERE NOT $is_full_refresh;

CREATE OR REPLACE TEMP TABLE _customer_session AS
SELECT base.session_id,
       s.datetime_added,
       s.customer_id,
       ds.store_brand_abbr,
       s.meta_original_session_id
FROM _customer_session_base base
         JOIN lake_consolidated.ultra_merchant.session AS s
              ON s.session_id = base.session_id
         JOIN edw_prod.stg.dim_store AS ds
             ON s.store_id = ds.store_id
WHERE s.customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.session_id ORDER BY s.datetime_modified DESC, base.customer_id DESC) = 1;

CREATE OR REPLACE TEMP TABLE _customer_session_stg AS
SELECT session_id,
       datetime_added,
       customer_id,
       store_brand_abbr,
           LAG(session_id, 1) OVER (PARTITION BY customer_id, store_brand_abbr
               ORDER BY datetime_added, session_id)         AS previous_customer_session_id,
           LAG(datetime_added, 1) OVER (PARTITION BY customer_id, store_brand_abbr
               ORDER BY datetime_added, session_id)         AS previous_customer_session_datetime,
           LEAD(session_id, 1) OVER (PARTITION BY customer_id, store_brand_abbr
               ORDER BY datetime_added, session_id)         AS next_customer_session_id,
           LEAD(datetime_added, 1) OVER (PARTITION BY customer_id, store_brand_abbr
               ORDER BY datetime_added, session_id) AS next_customer_session_datetime,
       meta_original_session_id,
       HASH(base.session_id, customer_id, store_brand_abbr, previous_customer_session_id, previous_customer_session_datetime,
            next_customer_session_id, next_customer_session_datetime)  AS meta_row_hash
FROM _customer_session base
;

MERGE INTO staging.customer_session tgt
    USING _customer_session_stg AS src
    ON src.session_id = tgt.session_id
    WHEN NOT MATCHED THEN
        INSERT (
                session_id, datetime_added, customer_id, store_brand_abbr,
                previous_customer_session_id, previous_customer_session_datetime,
                next_customer_session_id, next_customer_session_datetime,
                meta_original_session_id,
                meta_row_hash,
                meta_create_datetime,
                meta_update_datetime
            ) VALUES (src.session_id, src.datetime_added, src.customer_id, src.store_brand_abbr,
                      src.previous_customer_session_id, src.previous_customer_session_datetime,
                      src.next_customer_session_id, src.next_customer_session_datetime,
                      src.meta_original_session_id,
                      src.meta_row_hash,
                      $execution_start_time,
                      $execution_start_time)
    WHEN MATCHED AND src.meta_row_hash != tgt.meta_row_hash
        THEN UPDATE SET
        tgt.datetime_added = src.datetime_added,
        tgt.customer_id = src.customer_id,
        tgt.store_brand_abbr = src.store_brand_abbr,
        tgt.previous_customer_session_id = src.previous_customer_session_id,
        tgt.previous_customer_session_datetime = src.previous_customer_session_datetime,
        tgt.next_customer_session_id = src.next_customer_session_id ,
        tgt.next_customer_session_datetime = src.next_customer_session_datetime,
        tgt.meta_original_session_id = src.meta_original_session_id,
        tgt.meta_row_hash = src.meta_row_hash,
        tgt.meta_update_datetime = $execution_start_time;

UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = $execution_start_time
WHERE table_name = $target_table;
