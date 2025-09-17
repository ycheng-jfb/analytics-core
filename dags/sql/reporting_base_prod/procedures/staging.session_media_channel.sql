SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'staging.session_media_channel';
SET is_full_refresh = (SELECT IFF(public.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (SELECT $target_table                               AS table_name,
                  NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
                  high_watermark_datetime                     AS new_high_watermark_datetime
           FROM (
                 SELECT 'shared.session' AS dependent_table_name,
                        MAX(meta_update_datetime)                     AS high_watermark_datetime
                 FROM shared.session
                 UNION ALL
                 SELECT NULL AS dependent_table_name,
                        NVL(MAX(meta_update_datetime), $execution_start_time)  AS high_watermark_datetime
                 FROM staging.session_media_channel
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

SET wm_shared_session = public.udf_get_watermark($target_table, 'shared.session');

CREATE OR REPLACE TEMP TABLE _session_base_filter
(
    customer_id  NUMERIC(38, 0),
    visitor_id NUMERIC(38, 0)
);

INSERT INTO _session_base_filter
SELECT DISTINCT
    customer_id,
    visitor_id
FROM shared.session s
WHERE $is_full_refresh;

INSERT INTO _session_base_filter
SELECT DISTINCT
    customer_id,
    visitor_id
FROM shared.session
WHERE meta_update_datetime > $wm_shared_session
  AND NOT $is_full_refresh;

CREATE OR REPLACE TEMP TABLE _session_base AS
SELECT session_id,
       store_id,
       customer_id,
       visitor_id,
       session_local_datetime,
       media_source_hash,
       utm_source,
       utm_medium,
       utm_campaign,
       gateway_type,
       gateway_sub_type,
       seo_vendor,
       ip,
       meta_original_session_id
FROM shared.session
WHERE customer_id IN (SELECT customer_id FROM _session_base_filter WHERE customer_id IS NOT NULL)
   OR visitor_id IN (SELECT visitor_id FROM _session_base_filter WHERE visitor_id IS NOT NULL)
ORDER BY session_id ASC;

CREATE OR REPLACE TEMP TABLE _media_source_channel_mapping AS
SELECT DISTINCT
    c.media_source_hash,
    c.channel,
    c.subchannel,
    IFF(c.channel != 'direct traffic', c.media_source_hash, NULL) AS last_non_direct_media_source_hash,
    IFF(c.channel != 'direct traffic', c.channel, NULL)           AS last_non_direct_media_channel,
    IFF(c.channel != 'direct traffic', c.subchannel, NULL)        AS last_non_direct_media_subchannel,
    cdn.channel_type,
    cdn.channel_display_name,
    sdn.subchannel_display_name
FROM shared.media_source_channel_mapping c
    LEFT JOIN reporting_media_base_prod.lkp.channel_display_name cdn
        ON c.channel = cdn.channel_key
    LEFT JOIN reporting_media_base_prod.lkp.subchannel_display_name sdn
        ON c.subchannel = sdn.subchannel_key
WHERE c.event_source = 'session';

CREATE OR REPLACE TEMPORARY TABLE _session_media_channel_stg AS
SELECT s.session_id,
       s.store_id,
       s.customer_id,
       s.session_local_datetime,
       s.media_source_hash,
       s.utm_source,
       s.utm_medium,
       s.utm_campaign,
       s.gateway_type,
       s.gateway_sub_type,
       s.seo_vendor,
       s.ip,
       c.channel,
       c.subchannel,
       c.channel_type            AS cleansed_channel_type,
       c.channel_display_name    AS cleansed_channel,
       c.subchannel_display_name AS cleansed_subchannel,
       NVL(LEAD(c.last_non_direct_media_source_hash, 1) IGNORE NULLS
           OVER ( PARTITION BY s.customer_id ORDER BY s.session_local_datetime DESC, s.session_id DESC),
           LEAD(c.last_non_direct_media_source_hash, 1) IGNORE NULLS
               OVER (PARTITION BY s.visitor_id ORDER BY s.session_local_datetime DESC, s.session_id DESC)
       )                         AS last_non_direct_media_source_hash,
       COALESCE(c.last_non_direct_media_channel, LAG(c.last_non_direct_media_channel) IGNORE NULLS
           OVER ( PARTITION BY s.customer_id ORDER BY s.session_local_datetime DESC, s.session_id DESC),
                LAG(c.last_non_direct_media_channel) IGNORE NULLS
                    OVER (PARTITION BY s.visitor_id ORDER BY s.session_local_datetime DESC, s.session_id DESC)
       )                         AS last_non_direct_media_channel,
       COALESCE(c.last_non_direct_media_subchannel, LAG(c.last_non_direct_media_subchannel) IGNORE NULLS
           OVER ( PARTITION BY s.customer_id ORDER BY s.session_local_datetime DESC, s.session_id DESC),
                LAG(c.last_non_direct_media_subchannel) IGNORE NULLS
                    OVER (PARTITION BY s.visitor_id ORDER BY s.session_local_datetime DESC, s.session_id DESC)
       )                         AS last_non_direct_media_subchannel,
       s.meta_original_session_id
FROM _session_base s
    LEFT JOIN _media_source_channel_mapping AS c
        ON c.media_source_hash = s.media_source_hash;

UPDATE _session_media_channel_stg
SET last_non_direct_media_channel    = channel,
    last_non_direct_media_subchannel = subchannel
WHERE last_non_direct_media_channel IS NULL
  AND last_non_direct_media_subchannel IS NULL;

CREATE OR REPLACE TEMP TABLE _session_media_channel_final_stg AS
SELECT session_id,
       store_id,
       customer_id,
       session_local_datetime,
       media_source_hash,
       utm_source,
       utm_medium,
       utm_campaign,
       gateway_type,
       gateway_sub_type,
       seo_vendor,
       ip,
       channel,
       subchannel,
       cleansed_channel_type,
       cleansed_channel,
       cleansed_subchannel,
       last_non_direct_media_source_hash,
       HASH(
           session_id,
           store_id,
           customer_id,
           session_local_datetime,
           media_source_hash,
           utm_source,
           utm_medium,
           utm_campaign,
           gateway_type,
           gateway_sub_type,
           seo_vendor,
           ip,
           channel,
           subchannel,
           cleansed_channel_type,
           cleansed_channel,
           cleansed_subchannel,
           last_non_direct_media_source_hash,
           last_non_direct_media_channel,
           last_non_direct_media_subchannel
       ) AS meta_row_hash,
       last_non_direct_media_channel,
       last_non_direct_media_subchannel,
       meta_original_session_id
FROM _session_media_channel_stg;

UPDATE staging.session_media_channel t
SET t.store_id = src.store_id,
    t.customer_id = src.customer_id,
    t.session_local_datetime = src.session_local_datetime,
    t.media_source_hash = src.media_source_hash,
    t.utm_source = src.utm_source,
    t.utm_medium = src.utm_medium,
    t.utm_campaign = src.utm_campaign,
    t.gateway_type = src.gateway_type,
    t.gateway_sub_type = src.gateway_sub_type,
    t.seo_vendor = src.seo_vendor,
    t.ip = src.ip,
    t.channel = src.channel,
    t.subchannel = src.subchannel,
    t.cleansed_channel_type = src.cleansed_channel_type,
    t.cleansed_channel = src.cleansed_channel,
    t.cleansed_subchannel = src.cleansed_subchannel,
    t.last_non_direct_media_source_hash = src.last_non_direct_media_source_hash,
    t.meta_row_hash = src.meta_row_hash,
    t.meta_update_datetime = $execution_start_time,
    t.last_non_direct_media_channel = src.last_non_direct_media_channel,
    t.last_non_direct_media_subchannel = src.last_non_direct_media_subchannel,
    t.meta_original_session_id = src.meta_original_session_id
FROM _session_media_channel_final_stg src
WHERE t.session_id = src.session_id
  AND t.meta_row_hash != src.meta_row_hash;

INSERT INTO staging.session_media_channel (
	session_id,
	store_id,
	customer_id,
	session_local_datetime,
	media_source_hash,
	utm_source,
	utm_medium,
	utm_campaign,
	gateway_type,
	gateway_sub_type,
	seo_vendor,
	ip,
	channel,
	subchannel,
	cleansed_channel_type,
	cleansed_channel,
	cleansed_subchannel,
	last_non_direct_media_source_hash,
	meta_row_hash,
	last_non_direct_media_channel,
	last_non_direct_media_subchannel,
	meta_original_session_id,
	meta_create_datetime,
	meta_update_datetime
)
SELECT src.session_id,
       src.store_id,
       src.customer_id,
       src.session_local_datetime,
       src.media_source_hash,
       src.utm_source,
       src.utm_medium,
       src.utm_campaign,
       src.gateway_type,
       src.gateway_sub_type,
       src.seo_vendor,
       src.ip,
       src.channel,
       src.subchannel,
       src.cleansed_channel_type,
       src.cleansed_channel,
       src.cleansed_subchannel,
       src.last_non_direct_media_source_hash,
       src.meta_row_hash,
       src.last_non_direct_media_channel,
       src.last_non_direct_media_subchannel,
       src.meta_original_session_id,
       $execution_start_time,
       $execution_start_time
FROM _session_media_channel_final_stg src
    LEFT JOIN staging.session_media_channel tgt
        ON src.session_id = tgt.session_id
WHERE tgt.session_id IS NULL;

UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = $execution_start_time
WHERE table_name = $target_table;
