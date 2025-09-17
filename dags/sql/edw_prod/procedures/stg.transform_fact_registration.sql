SET target_table = 'stg.fact_registration';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

-- Switch warehouse if performing a full refresh
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_edw_stg_fact_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_membership_event'));
SET wm_lake_ultra_merchant_session = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.session'));
SET wm_lake_ultra_merchant_session_uri = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.session_uri'));
SET wm_lake_ultra_merchant_session_media_data = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.session_media_data'));
SET wm_lake_ultra_merchant_media_code = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.media_code'));
SET wm_reporting_base_shared_session = (SELECT stg.udf_get_watermark($target_table, 'reporting_base_prod.shared.session'));
SET wm_edw_stg_dim_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer'));
SET wm_lake_ultra_merchant_customer_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer_detail'));
SET wm_lake_ultra_merchant_customer_link = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer_link'));
SET edw_reference_fake_retail_leads_with_activations = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.fake_retail_leads_with_activations'));
SET wm_reporting_base_shared_media_source_channel_mapping = (SELECT stg.UDF_GET_WATERMARK($target_table, 'reporting_base_prod.shared.media_source_channel_mapping'));

/*
SELECT
    $wm_edw_stg_fact_membership_event,
    $wm_lake_ultra_merchant_session,
    $wm_lake_ultra_merchant_session_uri,
    $wm_lake_ultra_merchant_session_media_data,
    $wm_lake_ultra_merchant_media_code,
    $wm_reporting_base_shared_session,
    $wm_edw_stg_dim_customer,
    $wm_lake_ultra_merchant_customer_detail,
    $wm_lake_ultra_merchant_customer_link,
    $edw_reference_fake_retail_leads_with_activations,
    $wm_reporting_base_shared_media_source_channel_mapping;
*/

CREATE OR REPLACE TEMP TABLE _fact_registration__customer_base (customer_id INT);


-- Full Refresh
INSERT INTO _fact_registration__customer_base (customer_id)
SELECT DISTINCT fme.customer_id
FROM stg.fact_membership_event AS fme
WHERE $is_full_refresh = TRUE
ORDER BY fme.customer_id;

-- Incremental Refresh
INSERT INTO _fact_registration__customer_base (customer_id)
SELECT DISTINCT incr.customer_id
FROM (
    SELECT fme.customer_id
    FROM stg.fact_membership_event AS fme
    WHERE fme.meta_update_datetime > $wm_edw_stg_fact_membership_event
    UNION ALL
    SELECT fme.customer_id
    FROM stg.fact_membership_event AS fme
        JOIN (
            SELECT session_id
            FROM lake_consolidated.ultra_merchant.session
            WHERE meta_update_datetime > $wm_lake_ultra_merchant_session
            UNION ALL
            SELECT session_id
            FROM lake_consolidated.ultra_merchant.session_uri
            WHERE meta_update_datetime > $wm_lake_ultra_merchant_session_uri
            UNION ALL
            SELECT session_id
            FROM lake_consolidated.ultra_merchant.session_media_data
            WHERE meta_update_datetime > $wm_lake_ultra_merchant_session_media_data
            UNION ALL
            SELECT smd.session_id
            FROM lake_consolidated.ultra_merchant.session_media_data AS smd
                JOIN lake_consolidated.ultra_merchant.media_code AS mc
                    ON mc.media_code_id = smd.placement_media_code_id
            WHERE mc.meta_update_datetime > $wm_lake_ultra_merchant_media_code
            UNION ALL
            SELECT smd.session_id
            FROM lake_consolidated.ultra_merchant.session_media_data AS smd
                JOIN lake_consolidated.ultra_merchant.media_code AS mc
                    ON mc.media_code_id = smd.creative_media_code_id
            WHERE mc.meta_update_datetime > $wm_lake_ultra_merchant_media_code
            UNION ALL
            SELECT smd.session_id
            FROM lake_consolidated.ultra_merchant.session_media_data AS smd
                JOIN lake_consolidated.ultra_merchant.media_code AS mc
                    ON mc.media_code_id = smd.sub_media_code_id
            WHERE mc.meta_update_datetime > $wm_lake_ultra_merchant_media_code
            UNION ALL
            SELECT s.session_id
            FROM reporting_base_prod.shared.session s
                JOIN reporting_base_prod.shared.media_source_channel_mapping m
                    ON m.media_source_hash = s.media_source_hash
            WHERE m.meta_update_datetime > $wm_reporting_base_shared_media_source_channel_mapping
                OR s.meta_update_datetime > $wm_reporting_base_shared_session
            ) AS media_code
            ON media_code.session_id = fme.session_id
    UNION ALL
    SELECT customer_id
    FROM stg.dim_customer
    WHERE meta_update_datetime > $wm_edw_stg_dim_customer
    UNION ALL
    SELECT customer_id
    FROM lake_consolidated.ultra_merchant.customer_detail
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_customer_detail
    UNION ALL
    SELECT current_customer_id AS customer_id
    FROM lake_consolidated.ultra_merchant.customer_link
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_customer_link
    UNION ALL
    SELECT customer_id
    FROM   reference.fake_retail_leads_with_activations
    WHERE meta_update_datetime > $edw_reference_fake_retail_leads_with_activations
    UNION ALL
    SELECT customer_id
    FROM excp.fact_registration AS e
    WHERE e.meta_is_current_excp
        AND e.meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.customer_id;
-- SELECT * FROM _fact_registration__customer_base;

-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', CURRENT_WAREHOUSE()) FROM _fact_registration__customer_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

CREATE OR REPLACE TEMP TABLE _fact_registration__registrations_base AS
SELECT
    base.customer_id,
    fme.meta_original_customer_id,
    fme.membership_event_key,
    fme.store_id,
    fme.session_id,
    fme.event_start_local_datetime,
    FALSE AS is_secondary_registration,
    FALSE AS is_secondary_registration_after_vip
FROM stg.fact_membership_event AS fme
    JOIN _fact_registration__customer_base AS base
        ON base.customer_id = fme.customer_id
    JOIN stg.dim_membership_event_type AS dmet
        ON dmet.membership_event_type_key = fme.membership_event_type_key
WHERE dmet.membership_event_type = 'Registration'
    AND NOT NVL(fme.is_deleted, FALSE);
-- SELECT * FROM _fact_registration__registrations_base;

-- Create records in base for the sceondary registrations
INSERT INTO _fact_registration__registrations_base
(
    customer_id,
    meta_original_customer_id,
    membership_event_key,
    store_id,
    session_id,
    event_start_local_datetime,
    is_secondary_registration,
    is_secondary_registration_after_vip
)

SELECT
    base.customer_id,
    dc.meta_original_customer_id,
    -1 AS membership_event_key,
    dc.secondary_registration_store_id AS store_id,
    -1 AS session_id, /* we currently don't have a way to tie secondary registrations to session_id in source */
    dc.secondary_registration_local_datetime AS event_start_local_datetime,
    TRUE AS is_secondary_registration,
    COALESCE(is_secondary_registration_after_vip, FALSE) AS is_secondary_registration_after_vip
FROM _fact_registration__customer_base AS base
    JOIN stg.dim_customer AS dc
        ON dc.customer_id = base.customer_id
WHERE dc.secondary_registration_store_id IS NOT NULL
AND NOT EXISTS(
        SELECT 1
        FROM _fact_registration__registrations_base AS rb
        WHERE rb.customer_id = base.customer_id
          AND rb.store_id = dc.secondary_registration_store_id
    );

-- Check for deletes in fact_membership_event and perform soft deletes on fact_registration
UPDATE stg.fact_registration AS fr
SET fr.is_deleted           = TRUE,
    fr.meta_update_datetime = $execution_start_time
FROM stg.fact_membership_event fme
WHERE fr.membership_event_key = fme.membership_event_key
  AND fme.meta_update_datetime > $wm_edw_stg_fact_membership_event
  AND fme.is_deleted
  AND NOT fr.is_deleted;

CREATE OR REPLACE TEMP TABLE _fact_registration__sessions AS
WITH s AS (
    SELECT
        s.session_id,
        IFNULL(s.dm_gateway_id,0) AS dm_gateway_id,
        IFNULL(s.dm_site_id,0) AS dm_site_id
    FROM lake_consolidated.ultra_merchant.session AS s
        JOIN _fact_registration__registrations_base AS rb
            ON rb.session_id = s.session_id
    WHERE rb.session_id IS NOT NULL
    ),
    su AS (
    SELECT
        su.session_id,
        su.uri,
        su.user_agent,
        su.device_type_id,
        su.datetime_modified
    FROM lake_consolidated.ultra_merchant.session_uri AS su
        JOIN _fact_registration__registrations_base AS rb
            ON rb.session_id = su.session_id
    WHERE rb.session_id IS NOT NULL
        AND LOWER(su.user_agent) <> 'coldfusion'
    )
SELECT
    s.session_id,
    s.dm_gateway_id,
    s.dm_site_id,
    su.uri,
    su.user_agent,
    su.device_type_id,
    ROW_NUMBER() OVER (PARTITION BY s.session_id ORDER BY COALESCE(su.datetime_modified, '1900-01-01') DESC) AS row_num
FROM s
    LEFT JOIN su
        ON su.session_id = s.session_id
QUALIFY row_num = 1;
-- SELECT * FROM _fact_registration__sessions;

CREATE OR REPLACE TEMP TABLE _fact_registration__uri AS
SELECT
    session_id,
    LOWER('https://' || public.udf_decode_url(public.udf_decode_url(uri))) AS uri
FROM _fact_registration__sessions
WHERE uri IS NOT NULL;


CREATE OR REPLACE TEMP TABLE _fact_registration__channel AS
SELECT
    rb.session_id,
    m.channel_type as registration_channel_type,
    m.channel as registration_channel,
    m.subchannel as registration_subchannel,
    s.ga_device,
    s.ga_browser,
    s.utm_medium,
    s.utm_source,
    s.utm_campaign,
    s.utm_content,
    s.utm_term,
    ROW_NUMBER() OVER (PARTITION BY s.session_id ORDER BY COALESCE(s.session_local_datetime, '1900-01-01') DESC) AS row_num
FROM _fact_registration__registrations_base rb
JOIN reporting_base_prod.shared.session s
     ON rb.session_id= s.session_id
JOIN reporting_base_prod.shared.media_source_channel_mapping m
    ON m.media_source_hash = s.media_source_hash
WHERE rb.session_id <> - 1 -- exclude secondary registrations as they have no session id
QUALIFY row_num = 1;

UPDATE _fact_registration__uri
SET uri = REPLACE(uri, 'utm_medium', '&utm_medium')
WHERE uri ILIKE '%%utm_medium%%';

CREATE OR REPLACE TEMP TABLE _fact_registration__uri_parameters AS
SELECT
    u.session_id,
    u.uri,
    f.key,
    f.value::VARCHAR AS value
FROM _fact_registration__uri AS u,
    LATERAL flatten(INPUT => parse_url(REPLACE(u.uri,'#','?'))['parameters']) AS f
WHERE f.key IN ('ccode', 'pcode', 'scode', 'utm_medium', 'utm_source', 'utm_campaign', 'utm_term', 'utm_content', 'acode', 'master_product_id');

CREATE OR REPLACE TEMP TABLE _fact_registration__uri_pivoted AS
SELECT
    session_id,
	uri,
    ccode,
    pcode,
    scode,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_term,
    utm_content,
    acode,
    master_product_id
FROM _fact_registration__uri_parameters
PIVOT(MAX(value) FOR key IN ('ccode', 'pcode', 'scode', 'utm_medium', 'utm_source', 'utm_campaign', 'utm_term', 'utm_content', 'acode', 'master_product_id')) AS p
    (session_id, uri, ccode, pcode, scode, utm_medium, utm_source, utm_campaign, utm_term, utm_content, acode, master_product_id);

CREATE OR REPLACE TEMP TABLE _fact_registration__session_attributes AS
SELECT
    s.session_id,
    CASE
        --App
        WHEN s.user_agent ILIKE 'JustFab %%' THEN 'App'
        WHEN s.user_agent ILIKE '%%JustFabMobile%%' THEN 'App'
        --Firefox
        WHEN s.user_agent ILIKE '%%Firefox%%' THEN 'Firefox'
        --Chrome
        WHEN s.user_agent ILIKE '%%Chromium%%' THEN 'Chrome'
        WHEN s.user_agent ILIKE '%%Gecko) Ch%%' THEN 'Chrome'
        WHEN s.user_agent ILIKE '%%Gecko) Version/4.0%%' THEN 'Chrome'
        WHEN s.user_agent ILIKE '%%Chrome%%' THEN 'Chrome'
        WHEN s.user_agent ILIKE '%% CriOS%%' THEN 'Chrome'
        --IE
        WHEN s.user_agent ILIKE '%%Win%%rv:11%%' THEN 'Internet Explorer'
        WHEN s.user_agent ILIKE '%%MSIE%%' THEN 'Internet Explorer'
        WHEN s.user_agent ILIKE '%%IEMobile%%' THEN 'Internet Explorer Mobile'
        --Opera
        WHEN s.user_agent ILIKE '%%Opera Mini%%' THEN 'Opera Mini'
        WHEN s.user_agent ILIKE '%%Opera%%' THEN 'Opera'
        --Safari
        WHEN s.user_agent ILIKE '%%Safari%%' THEN 'Safari'
        WHEN s.user_agent ILIKE '%%iPhone%%' THEN 'Safari'
        WHEN s.user_agent ILIKE '%%iPad%%' THEN 'Safari'
        --Other
        WHEN s.user_agent ILIKE '%%Dalvik%%' THEN 'Other'
        WHEN s.user_agent ILIKE '%%Silk%%' THEN 'Other'
        WHEN s.user_agent ILIKE '%%Nintendo%%' THEN 'Other'
        WHEN s.user_agent ILIKE '%%Playstation%%' THEN 'Other'
        WHEN s.user_agent ILIKE '%%DreamPassport%%' THEN 'Other'
        WHEN s.user_agent ILIKE '%%Mozilla%%' THEN 'Other'
        ELSE 'Unknown'
    END AS browser,
    CASE
        --Android
        WHEN s.user_agent ILIKE '%% KF%%' THEN 'Android'
        WHEN s.user_agent ILIKE '%%Android%%' THEN 'Android'
        --Chrome OS
        WHEN s.user_agent ILIKE '%% CrOS %%' THEN 'Chrome OS'
        --iOS
        WHEN s.user_agent ILIKE '%%iPad%%' THEN 'iOS'
        WHEN s.user_agent ILIKE '%%iPhone%%' THEN 'iOS'
        WHEN s.user_agent ILIKE '%%iPod%%' THEN 'iOS'
        --Mac
        WHEN s.user_agent ILIKE '%%OS X%%' THEN 'Mac OSX'
        WHEN s.user_agent ILIKE '%%Darwin%%' THEN 'Mac OS'
        WHEN s.user_agent ILIKE '%%Macintosh%%' THEN 'Mac OS'
        --Windows
        WHEN s.user_agent ILIKE '%%Windows Phone%%' THEN 'Windows Mobile'
        WHEN s.user_agent ILIKE '%%Windows%%' THEN 'Windows'
        WHEN s.user_agent ILIKE '%%MSIE%%' THEN 'Windows'
        --Linux
        WHEN s.user_agent ILIKE '%%Linux x86_64%%' THEN 'Linux'
        WHEN s.user_agent ILIKE '%%Linux%%' THEN 'Linux'
        WHEN s.user_agent ILIKE '%%Ubuntu%%' THEN 'Linux'
        --RIM OS
        WHEN s.user_agent ILIKE '%%BB10%%' THEN 'RIM OS'
        WHEN s.user_agent ILIKE '%%Blackberry%%' THEN 'RIM OS'
        WHEN s.user_agent ILIKE '%%RIM%%' THEN 'RIM OS'
        --Gaming OS
        WHEN s.user_agent ILIKE '%%Nintendo%%' THEN 'Nintendo'
        WHEN s.user_agent ILIKE '%%Playstation%%' THEN 'Playstation'
        WHEN s.user_agent ILIKE '%%DreamPassport%%' THEN 'DreamCast'
        ELSE 'Unknown'
    END AS os,
    CASE
        WHEN s.device_type_id = 1 THEN 'Desktop'
        WHEN s.device_type_id = 2 THEN 'Mobile'
        WHEN s.device_type_id = 3 THEN 'Tablet'
        --Tablet
        WHEN s.user_agent ILIKE '%%Tablet%%' THEN 'Tablet'
        WHEN s.user_agent ILIKE '%%iPad%%' THEN 'Tablet'
        WHEN s.user_agent ILIKE '%% KF%%' THEN 'Tablet'
        WHEN s.user_agent ILIKE '%%Android%%Safari%%' AND s.user_agent NOT ILIKE '%%mobile%%' THEN 'Tablet'
        WHEN s.user_agent ILIKE '%%Mozilla%%Android%%JustFab%%' AND s.user_agent NOT ILIKE '%%Mobile Safari%%' THEN 'Tablet'
        --Mobile
        WHEN s.user_agent ILIKE '%%iPhone%%' THEN 'Mobile'
        WHEN s.user_agent ILIKE '%%iPod%%' THEN 'Mobile'
        WHEN s.user_agent ILIKE '%%Windows Phone%%' THEN 'Mobile'
        WHEN s.user_agent ILIKE '%%Blackberry%%' THEN 'Mobile'
        WHEN s.user_agent ILIKE '%%Dalvik%%' THEN 'Mobile'
        WHEN s.user_agent ILIKE '%%Mobile%%' THEN 'Mobile'
        WHEN s.user_agent ILIKE '%%Opera Mini%%' THEN 'Mobile'
        WHEN s.user_agent ILIKE '%%Android%%' AND s.user_agent ILIKE '%%safari%%' THEN 'Mobile'
        --Desktop
        WHEN s.user_agent ILIKE '%%Desktop%%' THEN 'Desktop'
        WHEN s.user_agent ILIKE '%%Windows%%' THEN 'Desktop'
        WHEN s.user_agent ILIKE '%% CrOS%%' THEN 'Desktop'
        WHEN s.user_agent ILIKE '%%OS X%%' THEN 'Desktop'
        WHEN s.user_agent ILIKE '%%Darwin%%' THEN 'Desktop'
        WHEN s.user_agent ILIKE '%%Linux%%' THEN 'Desktop'
        WHEN s.user_agent ILIKE '%%MSIE%%' THEN 'Desktop'
        WHEN s.user_agent ILIKE '%%Opera%%' THEN 'Desktop'
        --Android Device
        WHEN s.user_agent ILIKE '%%Android%%' THEN 'Mobile'
        --Other
        WHEN s.user_agent ILIKE '%%Nintendo%%' THEN 'Unknown'
        WHEN s.user_agent ILIKE '%%Playstation Vita%%' THEN 'Unknown'
        WHEN s.user_agent ILIKE '%%Playstation%%' THEN 'Unknown'
        WHEN s.user_agent ILIKE '%%DreamPassport%%' THEN 'Unknown'
        ELSE 'Unknown'
    END AS device,
    up.ccode,
    up.pcode,
    up.scode,
    up.utm_medium,
    up.utm_source,
    up.utm_campaign,
    up.utm_term,
    up.utm_content,
    up.acode,
    up.master_product_id,
	up.uri
FROM _fact_registration__sessions AS s
    JOIN _fact_registration__uri_pivoted AS up
        ON up.session_id = s.session_id;
-- SELECT * FROM _fact_registration__session_attributes;

CREATE OR REPLACE TEMP TABLE _fact_registration__media_attributes AS
SELECT
    rb.membership_event_key,
    rb.session_id,
    pm.code AS pcode,
    cm.code AS ccode,
    sm.code AS scode
FROM _fact_registration__registrations_base AS rb
    JOIN lake_consolidated.ultra_merchant.session_media_data AS smd
        ON smd.session_id = rb.session_id
    LEFT JOIN lake_consolidated.ultra_merchant.media_code AS pm
        ON pm.media_code_id = smd.placement_media_code_id
    LEFT JOIN lake_consolidated.ultra_merchant.media_code AS cm
        ON cm.media_code_id = smd.creative_media_code_id
    LEFT JOIN lake_consolidated.ultra_merchant.media_code AS sm
        ON sm.media_code_id = smd.sub_media_code_id;
-- SELECT * FROM _fact_registration__media_attributes;

CREATE OR REPLACE TEMP TABLE _fact_registration__customer_details AS
SELECT
    pvt.customer_id,
    pvt.lead_type AS cdetail_lead_type,
    pvt.origin AS cdetail_origin,
    pvt.initial_medium AS cdetail_initial_medium,
    pvt.customer_referrer_current_list AS hdyh_values_shown,
    TRY_TO_BOOLEAN(pvt.membership_has_password) AS has_membership_password,
    TRY_TO_NUMBER(pvt.retail_location) AS retail_location
FROM (
    SELECT
        rb.customer_id,
        REPLACE(LOWER(cd.name),' ','_') AS name,
        cd.value
    FROM _fact_registration__registrations_base AS rb
        JOIN lake_consolidated.ultra_merchant.customer_detail AS cd
            ON cd.customer_id = rb.customer_id
    WHERE LOWER(name) IN ('lead_type', 'origin', 'initial medium', 'customer_referrer_current_list', 'membership_has_password', 'retail_location')
    ) AS src
PIVOT (MAX(value) FOR name IN ('lead_type', 'origin', 'initial_medium', 'customer_referrer_current_list', 'membership_has_password', 'retail_location')) AS pvt
    (customer_id, lead_type, origin, initial_medium, customer_referrer_current_list, membership_has_password, retail_location);
-- SELECT * FROM _fact_registration__customer_details;

CREATE OR REPLACE TEMP TABLE _fact_registration__customer_info AS
SELECT
    dc.customer_id,
    dc.is_test_customer,
    dc.how_did_you_hear,
    dc.how_did_you_hear_parent,
    CASE
        WHEN dc.email ILIKE '%%@retail.fabletics%%' THEN TRUE
        WHEN dc.email ILIKE '%%@retail.savagex%%' THEN TRUE
        ELSE FALSE END AS is_retail_registration
FROM _fact_registration__customer_base AS base
    JOIN stg.dim_customer AS dc
        ON dc.customer_id = base.customer_id;
-- SELECT * FROM _fact_registration__customer_info;

CREATE OR REPLACE TEMP TABLE _fact_registration__linked_customers AS
SELECT DISTINCT cl.current_customer_id AS customer_id, -- Could have more than one link, use DISTINCT
                CASE
                    WHEN ds.store_brand_abbr IN ('FL', 'YTY')
                        AND cc.store_id = dc.store_id
                        AND cc.is_scrubs_customer = dc.is_scrubs_customer
                        AND IFF(UPPER(cc.gender) = 'M', 'M', 'F') = IFF(UPPER(dc.gender) = 'M', 'M', 'F')
                        THEN TRUE
                    WHEN ds.store_brand_abbr NOT IN ('FL', 'YTY')
                        THEN TRUE
                    ELSE FALSE
                    END                AS is_match
FROM _fact_registration__customer_base AS base
         JOIN lake_consolidated.ultra_merchant.customer_link AS cl
              ON cl.current_customer_id = base.customer_id
         LEFT JOIN stg.dim_customer cc
                   ON base.customer_id = cc.customer_id
         LEFT JOIN stg.dim_customer dc
                   ON cl.original_customer_id = dc.customer_id
         LEFT JOIN stg.dim_store ds
                   ON cc.store_id = ds.store_id
                       OR dc.store_id = ds.store_id;
-- SELECT * FROM _fact_registration__linked_customers;

CREATE OR REPLACE TEMP TABLE _fact_registration__stg AS
SELECT /* History with no matching values in underlying table */
    fr.membership_event_key,
    fr.store_id,
    fr.customer_id,
    fr.meta_original_customer_id,
    fr.is_test_customer,
    fr.registration_local_datetime,
    fr.session_id,
    fr.registration_channel_type,
    fr.registration_channel,
    fr.registration_subchannel,
    fr.is_reactivated_lead_raw,
    fr.is_reactivated_lead,
    fr.uri,
    fr.hdyh_values_shown,
    fr.cdetail_lead_type,
    fr.cdetail_origin,
    fr.cdetail_initial_medium,
    fr.has_membership_password,
    fr.has_membership_password_first_value,
    fr.is_fake_retail_registration,
    fr.is_retail_registration,
    fr.retail_location,
    fr.how_did_you_hear,
    fr.how_did_you_hear_parent,
    fr.dm_gateway_id,
    fr.dm_site_id,
    fr.utm_medium,
    fr.utm_source,
    fr.utm_campaign,
    fr.utm_content,
    fr.utm_term,
    fr.ccode,
    fr.pcode,
    fr.scode,
    fr.acode,
    fr.master_product_id,
    fr.device,
    fr.browser,

    fr.uri_utm_medium,
    fr.uri_utm_source,
    fr.uri_utm_campaign,
    fr.uri_utm_content,
    fr.uri_utm_term,
    fr.uri_device,
    fr.uri_browser,
    fr.rpt_utm_medium,
    fr.rpt_utm_source,
    fr.rpt_utm_campaign,
    fr.rpt_utm_content,
    fr.rpt_utm_term,
    fr.rpt_device,
    fr.rpt_browser,

    fr.operating_system,
    fr.is_secondary_registration,
    fr.is_secondary_registration_after_vip,
    TRUE AS is_deleted,
    fr.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.fact_registration AS fr
    JOIN _fact_registration__customer_base AS base
        ON base.customer_id = fr.customer_id
WHERE NOT fr.is_deleted
    AND NOT EXISTS (
        SELECT 1
        FROM _fact_registration__registrations_base AS rb
        WHERE rb.customer_id = fr.customer_id
        AND rb.store_id = fr.store_id
        )
UNION ALL
SELECT DISTINCT
    rb.membership_event_key,
    rb.store_id,
    rb.customer_id,
    rb.meta_original_customer_id,
    c.is_test_customer,
    rb.event_start_local_datetime AS registration_local_datetime,
    rb.session_id,
    rc.registration_channel_type,
    rc.registration_channel,
    rc.registration_subchannel,
    IFF(lc.customer_id IS NOT NULL, TRUE, FALSE) AS is_reactivated_lead_raw,
    COALESCE(lc.is_match, is_reactivated_lead_raw) AS is_reactivated_lead,
    sa.uri,
    CASE WHEN ASCII(cd.hdyh_values_shown) = 0 THEN '' ELSE cd.hdyh_values_shown END AS hdyh_values_shown,
    cd.cdetail_lead_type,
    cd.cdetail_origin,
    cd.cdetail_initial_medium,
    cd.has_membership_password,
    COALESCE(fr.has_membership_password_first_value, cd.has_membership_password) AS has_membership_password_first_value,
    IFF(lwa.customer_id IS NOT NULL, FALSE, COALESCE(c.is_retail_registration, FALSE)) AS is_fake_retail_registration,
    CASE /* Two methods of checking for retail leads (DA-22422) */
        WHEN rb.store_id = 241 THEN FALSE
        WHEN COALESCE(c.is_retail_registration, FALSE) THEN TRUE
        WHEN cd.retail_location IS NOT NULL THEN TRUE
        ELSE FALSE END AS is_retail_registration,
    CASE WHEN rb.store_id = 241 THEN NULL
        ELSE cd.retail_location END AS retail_location,
    c.how_did_you_hear,
    c.how_did_you_hear_parent,
    s.dm_gateway_id,
    s.dm_site_id,
    COALESCE(sa.utm_medium, rc.utm_medium) AS utm_medium,
    COALESCE(sa.utm_source, rc.utm_source) AS utm_source,
    COALESCE(sa.utm_campaign, rc.utm_campaign) AS utm_campaign,
    COALESCE(sa.utm_content, rc.utm_content) AS utm_content,
    COALESCE(sa.utm_term, rc.utm_term) AS utm_term,
    COALESCE(sa.ccode, ma.ccode) AS ccode,
    COALESCE(sa.pcode, ma.pcode) AS pcode,
    COALESCE(sa.scode, ma.scode) AS scode,
    sa.acode,
    sa.master_product_id AS master_product_id,
    COALESCE(sa.device, rc.ga_device, 'Unknown') AS device,
    COALESCE(sa.browser, rc.ga_browser, 'Unknown') AS browser,

    /* The following simply saves the raw values for comparison (DA-22414) */
    sa.utm_medium AS uri_utm_medium,
    sa.utm_source AS uri_utm_source,
    sa.utm_campaign AS uri_utm_campaign,
    sa.utm_content AS uri_utm_content,
    sa.utm_term AS uri_utm_term,
    sa.device uri_device,
    sa.browser AS uri_browser,
    rc.utm_medium AS rpt_utm_medium,
    rc.utm_source AS rpt_utm_source,
    rc.utm_campaign AS rpt_utm_campaign,
    rc.utm_content AS rpt_utm_content,
    rc.utm_term AS rpt_utm_term,
    rc.ga_device AS rpt_device,
    rc.ga_browser AS rpt_browser,

    sa.os AS operating_system,
    rb.is_secondary_registration,
    rb.is_secondary_registration_after_vip,
    FALSE AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_registration__registrations_base AS rb
    LEFT JOIN stg.fact_registration AS fr
        ON fr.customer_id = rb.customer_id
    LEFT JOIN _fact_registration__session_attributes AS sa
        ON sa.session_id = rb.session_id
    LEFT JOIN _fact_registration__media_attributes AS ma
        ON ma.session_id = rb.session_id
    LEFT JOIN _fact_registration__sessions AS s
        ON s.session_id = rb.session_id
    LEFT JOIN _fact_registration__customer_details AS cd
        ON cd.customer_id = rb.customer_id
    LEFT JOIN _fact_registration__customer_info AS c
        ON c.customer_id = rb.customer_id
    LEFT JOIN _fact_registration__linked_customers AS lc
        ON lc.customer_id = rb.customer_id
    LEFT JOIN _fact_registration__channel as rc
        ON rc.session_id = rb.session_id
    LEFT JOIN reference.fake_retail_leads_with_activations lwa
        on lwa.customer_id = fr.customer_id;
-- SELECT * FROM _fact_registration__stg;
-- SELECT customer_id, COUNT(1) FROM _fact_registration__stg GROUP BY 1 HAVING COUNT(1) > 1;

INSERT INTO stg.fact_registration_stg (
    membership_event_key,
    store_id,
    customer_id,
    meta_original_customer_id,
    registration_local_datetime,
    session_id,
    registration_channel_type,
    registration_channel,
    registration_subchannel,
    is_reactivated_lead_raw,
    uri,
    hdyh_values_shown,
    cdetail_lead_type,
    cdetail_origin,
    cdetail_initial_medium,
    has_membership_password,
    has_membership_password_first_value,
    is_fake_retail_registration,
    is_retail_registration,
    retail_location,
    how_did_you_hear,
    how_did_you_hear_parent,
    dm_gateway_id,
    dm_site_id,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    ccode,
    pcode,
    scode,
    acode,
    master_product_id,
    device,
    browser,

    uri_utm_medium,
    uri_utm_source,
    uri_utm_campaign,
    uri_utm_content,
    uri_utm_term,
    uri_device,
    uri_browser,
    rpt_utm_medium,
    rpt_utm_source,
    rpt_utm_campaign,
    rpt_utm_content,
    rpt_utm_term,
    rpt_device,
    rpt_browser,

    operating_system,
    is_secondary_registration,
    is_secondary_registration_after_vip,
    is_test_customer,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime,
    is_reactivated_lead
    )
SELECT
    membership_event_key,
    store_id,
    customer_id,
    meta_original_customer_id,
    registration_local_datetime,
    session_id,
    registration_channel_type,
    registration_channel,
    registration_subchannel,
    is_reactivated_lead_raw,
    uri,
    hdyh_values_shown,
    cdetail_lead_type,
    cdetail_origin,
    cdetail_initial_medium,
    has_membership_password,
    has_membership_password_first_value,
    is_fake_retail_registration,
    is_retail_registration,
    retail_location,
    how_did_you_hear,
    how_did_you_hear_parent,
    dm_gateway_id,
    dm_site_id,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    ccode,
    pcode,
    scode,
    acode,
    master_product_id,
    device,
    browser,

    uri_utm_medium,
    uri_utm_source,
    uri_utm_campaign,
    uri_utm_content,
    uri_utm_term,
    uri_device,
    uri_browser,
    rpt_utm_medium,
    rpt_utm_source,
    rpt_utm_campaign,
    rpt_utm_content,
    rpt_utm_term,
    rpt_device,
    rpt_browser,

    operating_system,
    is_secondary_registration,
    is_secondary_registration_after_vip,
    is_test_customer,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime,
    is_reactivated_lead
FROM _fact_registration__stg
ORDER BY
    customer_id;
