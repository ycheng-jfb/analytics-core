SET target_table = 'stg.fact_activation';
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
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_edw_stg_fact_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_membership_event'));
SET wm_lake_ultra_merchant_session = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.session'));
SET wm_lake_ultra_merchant_session_uri = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.session_uri'));
SET wm_lake_ultra_merchant_store = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store'));
SET wm_lake_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_lake_ultra_merchant_customer_link = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer_link'));
SET wm_lake_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership'));
SET wm_lake_ultra_merchant_membership_type = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_type'));
SET wm_reporting_base_shared_session = (SELECT stg.udf_get_watermark($target_table, 'reporting_base_prod.shared.session'));
SET wm_reporting_base_shared_media_source_channel_mapping = (SELECT stg.UDF_GET_WATERMARK($target_table, 'reporting_base_prod.shared.media_source_channel_mapping'));
SET wm_edw_reference_test_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.test_customer'));
SET wm_edw_stg_lkp_membership_cancel_method = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.lkp_membership_cancel_method'));

/*
SELECT
    $wm_self,
    $wm_edw_stg_fact_membership_event,
    $wm_lake_ultra_merchant_session,
    $wm_lake_ultra_merchant_session_uri,
    $wm_lake_ultra_merchant_store,
    $wm_lake_ultra_merchant_order,
    $wm_lake_ultra_merchant_customer_link,
    $wm_lake_ultra_merchant_membership,
    $wm_lake_ultra_merchant_membership_type,
    $wm_reporting_base_shared_session,
    $wm_reporting_base_shared_media_source_channel_mapping,
    $wm_edw_reference_test_customer,
    $wm_edw_stg_lkp_membership_cancel_method;
*/

CREATE OR REPLACE TEMP TABLE _fact_activation__customer_base (customer_id INT);

-- Full Refresh
INSERT INTO _fact_activation__customer_base (customer_id)
SELECT DISTINCT fme.customer_id
FROM stg.fact_membership_event AS fme
WHERE $is_full_refresh = TRUE
ORDER BY fme.customer_id;

-- Incremental Refresh
INSERT INTO _fact_activation__customer_base (customer_id)
SELECT DISTINCT incr.customer_id
FROM (
    /* Self-check for manual updates */
    SELECT fa.customer_id
    FROM stg.fact_activation AS fa
    WHERE fa.meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT fme.customer_id
    FROM stg.fact_membership_event AS fme
    WHERE fme.meta_update_datetime > $wm_edw_stg_fact_membership_event
    UNION ALL
    SELECT fme.customer_id
    FROM stg.fact_membership_event AS fme
        JOIN lake_consolidated.ultra_merchant.session AS s
            ON fme.session_id = s.session_id
    WHERE s.meta_update_datetime > $wm_lake_ultra_merchant_session
    UNION ALL
    SELECT fme.customer_id
    FROM stg.fact_membership_event AS fme
        JOIN lake_consolidated.ultra_merchant.session_uri AS su
            ON fme.session_id = su.session_id
    WHERE su.meta_update_datetime > $wm_lake_ultra_merchant_session_uri
    UNION ALL
    SELECT fme.customer_id
    FROM stg.fact_membership_event AS fme
        JOIN lake_consolidated.ultra_merchant.store AS s
            ON fme.store_id = s.store_id
    WHERE s.meta_update_datetime > $wm_lake_ultra_merchant_store
    UNION ALL
    SELECT fme.customer_id
    FROM stg.fact_membership_event AS fme
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON fme.order_id = o.order_id
    WHERE o.meta_update_datetime > $wm_lake_ultra_merchant_order
    UNION ALL
    SELECT current_customer_id AS customer_id
    FROM lake_consolidated.ultra_merchant.customer_link
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_customer_link
    UNION ALL
    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership AS m
        LEFT JOIN lake_consolidated.ultra_merchant.membership_type AS mt
            ON mt.membership_type_id = m.membership_type_id
    WHERE m.meta_update_datetime > $wm_lake_ultra_merchant_membership
        OR mt.meta_update_datetime > $wm_lake_ultra_merchant_membership_type
    UNION ALL
    SELECT s.customer_id
    FROM reporting_base_prod.shared.session s
        JOIN reporting_base_prod.shared.media_source_channel_mapping m
            ON m.media_source_hash = s.media_source_hash
        JOIN lake_consolidated.ultra_merchant.customer umc
            ON umc.meta_original_customer_id = s.customer_id
    WHERE s.customer_id IS NOT NULL
        AND (m.meta_update_datetime > $wm_reporting_base_shared_media_source_channel_mapping
                OR s.meta_update_datetime > $wm_reporting_base_shared_session)
    UNION ALL
    SELECT customer_id
    FROM reference.test_customer
    WHERE meta_update_datetime > $wm_edw_reference_test_customer
    UNION ALL
    SELECT customer_id
    FROM stg.lkp_membership_cancel_method
    WHERE meta_update_datetime > $wm_edw_stg_lkp_membership_cancel_method
    UNION ALL
    /* Previously errored rows */
    SELECT customer_id
    FROM excp.fact_activation
    WHERE meta_is_current_excp = 1
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.customer_id;
-- SELECT * FROM _fact_activation__customer_base;

-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', CURRENT_WAREHOUSE()) FROM _fact_activation__customer_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

--ALTER SESSION SET USE_CACHED_RESULT = FALSE;

CREATE OR REPLACE TEMP TABLE _fact_activation__all_events AS
SELECT
    membership_event_key,
    meta_original_customer_id,
    customer_id,
    store_id,
    order_id,
    session_id,
    event_start_local_datetime AS activation_local_datetime,
    membership_event_type,
    membership_type_detail,
    is_scrubs_customer,
    cancel_method,
    cancel_reason,
    LAG(membership_event_type) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS prior_event,
    LEAD(membership_event_type) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS next_event,
    LAG(membership_type_detail) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS prior_event_type,
    LEAD(membership_type_detail) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS next_event_type,
    LAG(event_start_local_datetime) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS prior_activation_local_datetime,
    LEAD(event_start_local_datetime) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS next_activation_local_datetime,
    LEAD(cancel_method) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS next_cancel_method,
    LEAD(cancel_reason) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS next_cancel_reason
FROM (
    SELECT
        fme.membership_event_key,
        fme.meta_original_customer_id,
        fme.customer_id,
        fme.store_id,
        fme.order_id,
        fme.session_id,
        fme.event_start_local_datetime,
        fme.membership_event_type,
        fme.membership_type_detail,
        fme.is_scrubs_customer,
        lmcm.cancel_method,
        lmcm.cancel_reason
    FROM stg.fact_membership_event AS fme
        JOIN _fact_activation__customer_base AS base
            ON base.customer_id = fme.customer_id
    LEFT JOIN stg.lkp_membership_cancel_method lmcm
        ON lmcm.membership_event_key = fme.membership_event_key
    WHERE NOT NVL(fme.is_deleted, FALSE)
    ) AS ae;
-- SELECT * FROM _fact_activation__all_events;

CREATE OR REPLACE TEMP TABLE _fact_activation__cancel_events AS
SELECT
    membership_event_key,
    customer_id,
    store_id,
    COALESCE(LAG(DATEADD(ms, 1, activation_local_datetime)) OVER (PARTITION BY customer_id
        ORDER BY activation_local_datetime), '1900-01-01') AS effective_from_cancellation_local_datetime,
    activation_local_datetime AS cancellation_local_datetime,
    membership_event_type,
    membership_type_detail,
    cancel_method,
    cancel_reason
FROM _fact_activation__all_events
WHERE membership_event_type = 'Cancellation';
-- SELECT * FROM _fact_activation__cancel_events;

CREATE OR REPLACE TEMP TABLE _fact_activation__activations_base AS
SELECT
    e.membership_event_key,
    e.meta_original_customer_id,
    e.customer_id,
    e.store_id,
    e.order_id,
    e.session_id,
    e.membership_event_type,
    e.activation_local_datetime,
    CASE
        WHEN e.next_event = 'Cancellation' THEN e.next_activation_local_datetime
        WHEN e.membership_event_type = 'Activation' AND c.membership_event_type = 'Cancellation' THEN c.cancellation_local_datetime
        ELSE '9999-12-31' END AS cancellation_local_datetime,
    CASE
        WHEN e.next_event = 'Cancellation' THEN e.next_event_type
        WHEN e.membership_event_type = 'Activation' AND c.membership_event_type = 'Cancellation' THEN c.membership_type_detail
        ELSE NULL END AS cancel_type,
    ROW_NUMBER() OVER (PARTITION BY e.customer_id ORDER BY e.activation_local_datetime) AS activation_sequence_number,
    COALESCE(LEAD(e.activation_local_datetime) OVER (PARTITION BY e.customer_id ORDER BY e.activation_local_datetime),'9999-12-31') AS next_activation_local_datetime,
    CASE WHEN COALESCE(LAG(e.activation_local_datetime) OVER (PARTITION BY e.customer_id ORDER BY e.activation_local_datetime),'9999-12-31') != '9999-12-31' THEN TRUE
        ELSE FALSE END AS is_reactivated_vip,
    e.is_scrubs_customer AS is_scrubs_vip,
    CASE
        WHEN e.next_event = 'Cancellation' THEN e.next_cancel_method
        WHEN e.membership_event_type = 'Activation' AND c.membership_event_type = 'Cancellation' THEN c.cancel_method
        ELSE NULL END AS cancel_method,
    CASE
        WHEN e.next_event = 'Cancellation' THEN e.next_cancel_reason
        WHEN e.membership_event_type = 'Activation' AND c.membership_event_type = 'Cancellation' THEN c.cancel_reason
        ELSE NULL END AS cancel_reason
FROM _fact_activation__all_events AS e
    LEFT JOIN _fact_activation__cancel_events AS c
        ON c.customer_id = e.customer_id
        AND e.activation_local_datetime BETWEEN c.effective_from_cancellation_local_datetime AND c.cancellation_local_datetime
WHERE e.membership_event_type = 'Activation'
    AND e.membership_type_detail != 'Classic'
    -- Ignore Failed Activations and VIP Level Changes
    AND COALESCE(e.next_event,'') NOT ILIKE '%Failed Activation%'
    AND (COALESCE(e.prior_event,'') != 'Activation'
        OR (COALESCE(e.prior_event,'') = 'Activation' AND e.prior_event_type = 'Classic'));
-- SELECT * FROM _fact_activation__activations_base;

-- If a customer activates, cancels & activates in the same month, this table will only consider their first activation within that month.
-- If a null order_id cancellation happens within 24 hours of activation, we do not want to include the activation event (DA-18489 / DA-18324).
CREATE OR REPLACE TEMP TABLE _fact_activation__valid_events AS
WITH all_events AS (
    SELECT
        customer_id,
        MIN(activation_local_datetime) AS activation_local_datetime,
        MAX(cancellation_local_datetime) AS cancellation_local_datetime
    FROM _fact_activation__activations_base
    WHERE NOT (order_id IS NULL AND cancellation_local_datetime BETWEEN activation_local_datetime AND DATEADD(DAY, 1, activation_local_datetime))
    GROUP BY customer_id, DATE_TRUNC('MONTH', DATEADD(SECOND, -900, activation_local_datetime)::DATE)  -- if 900 second delay causes an activation to spill to the previous month, it won't be counted if they made an activation earlier that month
    )
SELECT
    customer_id,
    activation_local_datetime,
    COALESCE(LEAD(activation_local_datetime) OVER (PARTITION BY customer_id ORDER BY activation_local_datetime), '9999-12-31') AS next_activation_local_datetime,
    cancellation_local_datetime,
    IFF(activation_local_datetime = MAX(activation_local_datetime) OVER (PARTITION BY customer_id), TRUE, FALSE) AS is_current
FROM all_events;
-- SELECT * FROM _fact_activation__valid_events;

CREATE OR REPLACE TEMP TABLE _fact_activation__session_list AS
SELECT
    s.session_id,
    s.dm_gateway_id
FROM lake_consolidated.ultra_merchant.session AS s
    JOIN _fact_activation__activations_base AS ab
        ON ab.session_id = s.session_id
    JOIN _fact_activation__valid_events AS ve
        ON ve.customer_id = ab.customer_id
        AND ve.activation_local_datetime = ab.activation_local_datetime
WHERE ab.session_id IS NOT NULL
ORDER BY s.session_id;
-- SELECT * FROM _fact_activation__session_list;

CREATE OR REPLACE TEMP TABLE _fact_activation__media_source_channel_mapping AS
SELECT media_source_hash,
       event_source,
       channel_type,
       channel,
       subchannel
FROM reporting_base_prod.shared.media_source_channel_mapping
    QUALIFY ROW_NUMBER()
        OVER (PARTITION BY media_source_hash ORDER BY event_source, channel_type, channel, subchannel ) = 1;

CREATE OR REPLACE TEMP TABLE _fact_activation__channel AS
SELECT
    ab.session_id,
    m.channel_type as activation_channel_type,
    m.channel as activation_channel,
    m.subchannel as activation_subchannel,
    s.ga_device,
    s.ga_browser,
    s.utm_medium,
    s.utm_source,
    s.utm_campaign,
    s.utm_content,
    s.utm_term,
    ROW_NUMBER() OVER (PARTITION BY ab.session_id ORDER BY COALESCE(s.session_local_datetime, '1900-01-01') DESC) AS row_num
FROM _fact_activation__activations_base AS ab
    JOIN _fact_activation__valid_events AS ve
        ON ve.customer_id = ab.customer_id
        AND ve.activation_local_datetime = ab.activation_local_datetime
    JOIN reporting_base_prod.shared.session AS s
        ON s.session_id = ab.session_id
    JOIN _fact_activation__media_source_channel_mapping AS m
        ON m.media_source_hash = s.media_source_hash
QUALIFY row_num = 1;
-- SELECT * FROM _fact_activation__channel;

CREATE OR REPLACE TEMP TABLE _fact_activation__sessions AS
SELECT
    s.session_id,
    s.dm_gateway_id,
    su.uri,
    su.user_agent,
    su.device_type_id,
    ROW_NUMBER() OVER (PARTITION BY s.session_id ORDER BY COALESCE(su.datetime_modified, '1900-01-01') DESC) AS row_num
FROM _fact_activation__session_list AS s
    LEFT JOIN lake_consolidated.ultra_merchant.session_uri AS su
        ON su.session_id = s.session_id
        AND LOWER(su.user_agent) <> 'coldfusion'
QUALIFY row_num = 1;
-- SELECT * FROM _fact_activation__sessions;

CREATE OR REPLACE TEMP TABLE _fact_activation__uri AS
SELECT
    session_id,
    LOWER('https://' || public.udf_decode_url(public.udf_decode_url(uri))) AS uri
FROM _fact_activation__sessions
WHERE uri IS NOT NULL;

UPDATE _fact_activation__uri
SET uri = replace(uri, 'utm_medium', '&utm_medium')
WHERE uri ILIKE '%%utm_medium%%';

CREATE OR REPLACE TEMP TABLE _fact_activation__uri_parameters AS
SELECT
    u.session_id,
    f.key,
    f.value :: VARCHAR AS value
FROM _fact_activation__uri AS u,
    LATERAL flatten(INPUT => parse_url(REPLACE(u.uri,'#','?'), 1)['parameters']) AS f
WHERE f.key IN ('ccode', 'pcode', 'scode', 'utm_medium', 'utm_source', 'utm_campaign', 'utm_term', 'utm_content', 'acode');

CREATE OR REPLACE TEMP TABLE _fact_activation__uri_pivoted AS
SELECT
    session_id,
    ccode,
    pcode,
    scode,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_term,
    utm_content,
    acode
FROM _fact_activation__uri_parameters
PIVOT(MAX(value) FOR key IN ('ccode', 'pcode', 'scode', 'utm_medium', 'utm_source', 'utm_campaign', 'utm_term', 'utm_content', 'acode')) AS p
    (session_id, ccode, pcode, scode, utm_medium, utm_source, utm_campaign, utm_term, utm_content, acode);

CREATE OR REPLACE TEMP TABLE _fact_activation__session_attributes AS
SELECT
    s.session_id,
    s.dm_gateway_id,
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
    up.acode
FROM _fact_activation__sessions AS s
    JOIN _fact_activation__uri_pivoted AS up
        ON up.session_id = s.session_id;
-- SELECT * FROM _fact_activation__session_attributes;

CREATE OR REPLACE TEMP TABLE _fact_activation__membership_type_hist AS
SELECT
    s.customer_id,
    s.local_datetime_added,
    s.membership_type,
    s.local_datetime_modified AS eff_from_local_datetime,
    LEAD(DATEADD(MILLISECOND, -1, s.local_datetime_modified), 1, '9999-12-31') OVER (PARTITION BY s.customer_id ORDER BY s.local_datetime_modified) AS eff_to_local_datetime,
    ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.local_datetime_modified) AS row_num
FROM (
    SELECT
        m.customer_id,
        mt.label AS membership_type,
        CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(m.datetime_added, 'America/Los_Angeles')) AS local_datetime_added,
        CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(m.effective_start_datetime, 'America/Los_Angeles')) AS local_datetime_modified,
        HASH(m.customer_id, mt.label, m.datetime_added) AS row_hash,
        LAG(row_hash) OVER (PARTITION BY m.customer_id, m.datetime_added ORDER BY m.effective_start_datetime) AS prev_row_hash
    FROM lake_consolidated.ultra_merchant_history.membership AS m
        JOIN _fact_activation__customer_base AS base
            ON base.customer_id = m.customer_id
        JOIN lake_consolidated.ultra_merchant.membership_type AS mt
            ON mt.membership_type_id = m.membership_type_id
        LEFT JOIN reference.store_timezone AS stz
            ON stz.store_id = m.store_id
    ) AS s
WHERE s.row_hash IS DISTINCT FROM s.prev_row_hash;

UPDATE _fact_activation__membership_type_hist
SET eff_from_local_datetime = local_datetime_added
WHERE row_num = 1;
-- SELECT * FROM _fact_activation__membership_type_hist;

CREATE OR REPLACE TEMP TABLE _fact_activation__membership_type_concat AS
SELECT
    ab.customer_id,
    ab.activation_local_datetime,
    LISTAGG(mth.membership_type, ' | ') WITHIN GROUP (ORDER BY mth.eff_from_local_datetime) AS membership_type
FROM _fact_activation__activations_base AS ab
    JOIN _fact_activation__valid_events AS ve
        ON ve.customer_id = ab.customer_id
        AND ve.activation_local_datetime = ab.activation_local_datetime
    LEFT JOIN _fact_activation__membership_type_hist AS mth
        ON mth.customer_id = ab.customer_id
        AND mth.eff_from_local_datetime < ve.next_activation_local_datetime
        AND mth.eff_to_local_datetime >= ve.activation_local_datetime
GROUP BY
    ab.customer_id,
    ab.activation_local_datetime;
-- SELECT * FROM _fact_activation__membership_type_concat;

CREATE OR REPLACE TEMP TABLE _fact_activation__order_detail_store AS
SELECT
    ab.order_id,
    od.value
FROM _fact_activation__activations_base AS ab
    JOIN _fact_activation__valid_events AS ve
        ON ve.customer_id = ab.customer_id
        AND ve.activation_local_datetime = ab.activation_local_datetime
    JOIN lake_consolidated.ultra_merchant.order_detail AS od
        ON od.order_id = ab.order_id
WHERE od.name='retail_store_id' and od.value != '0';
-- SELECT * FROM _fact_activation__order_detail_store;

CREATE OR REPLACE TEMP TABLE _fact_activation__retail_ship_only_store AS
SELECT
    ab.order_id,
    rso.store_id
FROM _fact_activation__activations_base AS ab
    JOIN _fact_activation__valid_events AS ve
        ON ve.customer_id = ab.customer_id
        AND ve.activation_local_datetime = ab.activation_local_datetime
    JOIN reference.historical_retail_ship_only_vip_store_id AS rso
    ON rso.order_id = ab.order_id;
-- SELECT * FROM _fact_activation__retail_ship_only_store;

CREATE OR REPLACE TEMP TABLE _fact_activation__order_data AS
SELECT
    ab.order_id,
    o.store_id,
    o.discount / NULLIF(o.subtotal, 0) AS order_discount_percent
FROM _fact_activation__activations_base AS ab
    JOIN _fact_activation__valid_events AS ve
        ON ve.customer_id = ab.customer_id
        AND ve.activation_local_datetime = ab.activation_local_datetime
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = ab.order_id;
-- SELECT * FROM _fact_activation__order_data;

CREATE OR REPLACE TEMP TABLE _fact_activation__test_customers AS
SELECT tc.customer_id
FROM _fact_activation__customer_base AS base
    JOIN reference.test_customer AS tc
        ON tc.customer_id = base.customer_id;
-- SELECT * FROM _fact_activation__test_customers;

CREATE OR REPLACE TEMP TABLE _fact_activation__linked_customers AS
SELECT DISTINCT cl.current_customer_id AS customer_id -- Could have more than one link, use DISTINCT
FROM _fact_activation__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.customer_link AS cl
        ON cl.current_customer_id = base.customer_id;
-- SELECT * FROM _fact_activation__linked_customers;

CREATE OR REPLACE TEMP TABLE _fact_activation__stg AS
SELECT /* History with no matching values in underlying table */
    fa.membership_event_key,
    CASE WHEN fa.store_id = 41 THEN 26 ELSE fa.store_id END AS store_id,
    fa.meta_original_customer_id,
    fa.customer_id,
    fa.is_test_customer,
    fa.activation_local_datetime,
    fa.next_activation_local_datetime,
    fa.source_activation_local_datetime,
    fa.source_next_activation_local_datetime,
    fa.cancellation_local_datetime,
    fa.is_reactivated_vip,
    fa.order_id,
    fa.session_id,
    fa.activation_channel_type,
    fa.activation_channel,
    fa.activation_subchannel,
    fa.vip_cohort_month_date,
    fa.source_vip_cohort_month_date,
    fa.membership_event_type,
    fa.membership_type,
    fa.is_vip_activation_from_reactivated_lead,
    fa.dm_gateway_id,
    fa.device,
    fa.browser,
    fa.operating_system,
    fa.utm_medium,
    fa.utm_source,
    fa.utm_campaign,
    fa.utm_content,
    fa.utm_term,
    fa.pcode,
    fa.ccode,
    fa.acode,
    fa.scode,
    CASE WHEN fa.sub_store_id = 41 THEN 26 ELSE fa.sub_store_id END AS sub_store_id,
    fa.is_legging_bar_vip,
    fa.is_kiosk_vip,
    fa.is_scrubs_vip,
    fa.order_discount_percent,
    FALSE AS is_current,
    TRUE AS is_deleted,
    fa.cancel_type,
    fa.cancel_method,
    fa.cancel_reason,
    fa.activation_sequence_number,
    fa.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.fact_activation AS fa
    JOIN _fact_activation__customer_base AS base
        ON base.customer_id = fa.customer_id
WHERE NOT fa.is_deleted
    AND NOT EXISTS (
        SELECT 1
        FROM _fact_activation__valid_events AS ve
        WHERE ve.customer_id = fa.customer_id
            AND ve.activation_local_datetime = fa.source_activation_local_datetime
        )
UNION ALL
SELECT
    ab.membership_event_key,
    CASE WHEN ab.store_id = 41 THEN 26 ELSE ab.store_id END AS store_id,
    ab.meta_original_customer_id,
    ab.customer_id,
    IFF(tc.customer_id IS NOT NULL, TRUE, FALSE) AS is_test_customer,
    DATEADD(SECOND, -900, ve.activation_local_datetime) AS activation_local_datetime,
    DATEADD(SECOND, -900, ve.next_activation_local_datetime) AS next_activation_local_datetime,
    ve.activation_local_datetime AS source_activation_local_datetime,
    ve.next_activation_local_datetime AS source_next_activation_local_datetime,
    ve.cancellation_local_datetime,
    ab.is_reactivated_vip,
    ab.order_id,
    ab.session_id,
    ac.activation_channel_type,
    ac.activation_channel,
    ac.activation_subchannel,
    DATE_TRUNC('MONTH', DATEADD(SECOND, -900, ve.activation_local_datetime)) AS vip_cohort_month_date,
    DATE_TRUNC('MONTH', ve.activation_local_datetime) AS source_vip_cohort_month_date,
    ab.membership_event_type,
    COALESCE(mt.membership_type, 'Unknown') AS membership_type,
    IFF(lc.customer_id IS NOT NULL, TRUE, FALSE) AS is_vip_activation_from_reactivated_lead,
    sa.dm_gateway_id,
    COALESCE(sa.device, ac.ga_device, 'Unknown') AS device,
    COALESCE(sa.browser, ac.ga_browser) AS browser,
    sa.os,
    COALESCE(sa.utm_medium, ac.utm_medium) AS utm_medium,
    COALESCE(sa.utm_source, ac.utm_source) AS utm_source,
    COALESCE(sa.utm_campaign, ac.utm_campaign) AS utm_campaign,
    COALESCE(sa.utm_content, ac.utm_content) AS utm_content,
    COALESCE(sa.utm_term, ac.utm_term) AS utm_term,
    sa.pcode,
    sa.ccode,
    sa.acode,
    sa.scode,
    CASE WHEN COALESCE(TRY_TO_NUMBER(od.value), rso.store_id, o.store_id, ab.store_id) = 41 THEN 26
        ELSE COALESCE(TRY_TO_NUMBER(od.value), rso.store_id, o.store_id, ab.store_id)  END AS sub_store_id,
    CASE WHEN st.code ILIKE '%%varsity%%' THEN TRUE ELSE FALSE END AS is_legging_bar_vip,
    CASE WHEN st.code ILIKE '%%kiosk%%' THEN TRUE ELSE FALSE END AS is_kiosk_vip,
    ab.is_scrubs_vip,
    o.order_discount_percent,
    ve.is_current,
    FALSE AS is_deleted,
    ab.cancel_type,
    ab.cancel_method,
    ab.cancel_reason,
    ab.activation_sequence_number,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_activation__activations_base AS ab
    JOIN _fact_activation__valid_events AS ve
        ON ve.customer_id = ab.customer_id
        AND ve.activation_local_datetime = ab.activation_local_datetime
    LEFT JOIN _fact_activation__membership_type_concat AS mt
        ON mt.customer_id = ab.customer_id
        AND mt.activation_local_datetime = ab.activation_local_datetime
    LEFT JOIN _fact_activation__session_attributes AS sa
        ON sa.session_id = ab.session_id
    LEFT JOIN lake_consolidated.ultra_merchant.store AS st
        ON st.store_id = ab.store_id
    LEFT JOIN _fact_activation__order_detail_store AS od
        ON od.order_id = ab.order_id
    LEFT JOIN _fact_activation__retail_ship_only_store AS rso
        ON rso.order_id = ab.order_id
    LEFT JOIN _fact_activation__order_data AS o
        ON o.order_id = ab.order_id
    LEFT JOIN _fact_activation__test_customers AS tc
        ON tc.customer_id = ab.customer_id
    LEFT JOIN _fact_activation__linked_customers AS lc
        ON lc.customer_id = ab.customer_id
    LEFT JOIN _fact_activation__channel AS ac
        ON ac.session_id = ab.session_id;
-- SELECT * FROM _fact_activation__stg;
-- SELECT customer_id, activation_local_datetime, COUNT(1) FROM _fact_activation__stg  GROUP BY 1, 2 HAVING COUNT(1) > 1;

INSERT INTO stg.fact_activation_stg (
    membership_event_key,
    store_id,
    meta_original_customer_id,
    customer_id,
    is_test_customer,
    activation_local_datetime,
    next_activation_local_datetime,
    source_activation_local_datetime,
    source_next_activation_local_datetime,
    cancellation_local_datetime,
    is_reactivated_vip,
    order_id,
    session_id,
    activation_channel_type,
    activation_channel,
    activation_subchannel,
    vip_cohort_month_date,
    source_vip_cohort_month_date,
    membership_event_type,
    membership_type,
    is_vip_activation_from_reactivated_lead,
    dm_gateway_id,
    device,
    browser,
    operating_system,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    pcode,
    ccode,
    acode,
    scode,
    sub_store_id,
    is_legging_bar_vip,
    is_kiosk_vip,
    is_scrubs_vip,
    order_discount_percent,
    is_current,
    is_deleted,
    cancel_type,
    cancel_method,
    cancel_reason,
    activation_sequence_number,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    membership_event_key,
    store_id,
    meta_original_customer_id,
    customer_id,
    is_test_customer,
    activation_local_datetime,
    next_activation_local_datetime,
    source_activation_local_datetime,
    source_next_activation_local_datetime,
    cancellation_local_datetime,
    is_reactivated_vip,
    order_id,
    session_id,
    activation_channel_type,
    activation_channel,
    activation_subchannel,
    vip_cohort_month_date,
    source_vip_cohort_month_date,
    membership_event_type,
    membership_type,
    is_vip_activation_from_reactivated_lead,
    dm_gateway_id,
    device,
    browser,
    operating_system,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    pcode,
    ccode,
    acode,
    scode,
    sub_store_id,
    is_legging_bar_vip,
    is_kiosk_vip,
    is_scrubs_vip,
    order_discount_percent,
    is_current,
    is_deleted,
    cancel_type,
    cancel_method,
    cancel_reason,
    activation_sequence_number,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_activation__stg
ORDER BY
    customer_id,
    activation_local_datetime;
