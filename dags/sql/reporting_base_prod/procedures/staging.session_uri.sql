SET target_table = 'reporting_base_prod.staging.session_uri';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(edw_prod.stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (
        SELECT
            'reporting_base_prod.staging.session_uri' AS table_name,
            NULLIF(dependent_table_name,'reporting_base_prod.staging.session_uri') AS dependent_table_name,
            high_watermark_datetime AS new_high_watermark_datetime
            FROM (
            SELECT
                'lake_consolidated_view.ultra_merchant.session_uri' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM lake_consolidated_view.ultra_merchant.session_uri
            ) AS h
        ) AS s
    ON t.table_name = s.table_name
        AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
            t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = current_timestamp::timestamp_ltz(3)
    WHEN NOT MATCHED
    THEN INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
        )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime
    );

SET wm_lake_consolidated_view_um_session_uri = public.udf_get_watermark($target_table,'lake_consolidated_view.ultra_merchant.session_uri');

CREATE OR REPLACE TEMP TABLE _session_base AS
SELECT DISTINCT session_id
FROM (SELECT session_id
      FROM lake_consolidated_view.ultra_merchant.session_uri
      WHERE meta_update_datetime > $wm_lake_consolidated_view_um_session_uri
      UNION ALL
      SELECT session_id
      from staging.session_uri_excp)
ORDER BY session_id;

CREATE OR REPLACE TEMP TABLE _session_uri_base AS
SELECT
    base.session_id,
    su.referer,
    CASE
        WHEN su.uri IS NOT NULL THEN 'https://' || reporting_media_base_prod.dbo.udf_decode_url(reporting_media_base_prod.dbo.udf_decode_url(su.uri))
        WHEN su.referer LIKE 'http%%?%%' THEN reporting_media_base_prod.dbo.udf_decode_url(reporting_media_base_prod.dbo.udf_decode_url(su.referer))
    END AS _uri_cleaned,
    IFF(_uri_cleaned LIKE '%%utm_medium%%', lower(replace(_uri_cleaned, 'utm_medium', '&utm_medium')), _uri_cleaned) AS _uri,
    IFF(_uri LIKE '%%?%%', _uri, REGEXP_REPLACE(_uri,'https://','https://?', 1, 1)) AS _uri_with_param,
    parse_url('https:/' || _uri_with_param):"parameters" AS params,
    parse_url('https:/' || _uri):"parameters" AS params_original,
    IFF(_uri_cleaned LIKE '%%utm_medium%%', replace(_uri_cleaned, 'utm_medium', '&utm_medium'), _uri_cleaned) AS _uri_click_id,
    IFF(_uri_click_id LIKE '%%?%%', _uri_click_id, REGEXP_REPLACE(_uri_click_id,'https://','https://?', 1, 1)) AS _uri_with_click_id_param,
    parse_url('https:/' || _uri_with_click_id_param):"parameters" AS params_click_id,
    get_ignore_case(params_click_id, 'clickid') AS clickid,
    get_ignore_case(params, 'sharedid') AS _sharedid,
    IFF(contains(_sharedid, '?'), LEFT(_sharedid, CHARINDEX('?', _sharedid)), _sharedid) AS sharedid,
    get_ignore_case(params, 'irad') AS irad,
    get_ignore_case(params, 'mpid') AS mpid,
    get_ignore_case(params, 'irmp') AS irmp,
    get_ignore_case(params, 'utm_medium') AS utm_medium,
    get_ignore_case(params, 'utm_source') AS utm_source,
    get_ignore_case(params, 'utm_campaign') AS utm_campaign,
    get_ignore_case(params, 'utm_content') AS utm_content,
    get_ignore_case(params, 'utm_term') AS utm_term,
    get_ignore_case(params, 'ccode') AS ccode,
    get_ignore_case(params, 'pcode') AS pcode,
    su.user_agent,
    CASE
        --App
        WHEN left(su.user_agent, 8) = 'JustFab ' THEN 'App'
        WHEN contains(su.user_agent, 'JustFabMobile') THEN 'App'
        --Firefox
        WHEN contains(su.user_agent, 'Firefox') THEN 'Firefox'
        --Chrome
        WHEN contains(su.user_agent, 'Chromium') THEN 'Chrome'
        WHEN contains(su.user_agent, 'Gecko) Ch') THEN 'Chrome'
        WHEN contains(su.user_agent, 'Gecko) Version/4.0') THEN 'Chrome'
        WHEN contains(su.user_agent, 'Chrome') THEN 'Chrome'
        WHEN contains(su.user_agent, ' CriOS') THEN 'Chrome'
        --IE
        WHEN su.user_agent LIKE '%%Win%%rv:11%%' THEN 'Internet Explorer'
        WHEN contains(su.user_agent, 'MSIE') THEN 'Internet Explorer'
        WHEN contains(su.user_agent, 'IEMobile') THEN 'Internet Explorer Mobile'
        --Opera
        WHEN contains(su.user_agent, 'Opera Mini') THEN 'Opera Mini'
        WHEN contains(su.user_agent, 'Opera') THEN 'Opera'
        --Safari
        WHEN contains(su.user_agent, 'Safari') THEN 'Safari'
        WHEN contains(su.user_agent, 'iPhone') THEN 'Safari'
        WHEN contains(su.user_agent, 'iPad') THEN 'Safari'
        --Other
        WHEN contains(su.user_agent, 'Dalvik') THEN 'Other'
        WHEN contains(su.user_agent, 'Silk') THEN 'Other'
        WHEN contains(su.user_agent, 'Nintendo') THEN 'Other'
        WHEN contains(su.user_agent, 'Playstation') THEN 'Other'
        WHEN contains(su.user_agent, 'DreamPassport') THEN 'Other'
        WHEN contains(su.user_agent, 'Mozilla') THEN 'Other'
        ELSE 'Unknown'
    END AS browser,
    CASE
        --Android
        WHEN contains(su.user_agent, ' KF') THEN 'Android'
        WHEN contains(su.user_agent, 'Android') THEN 'Android'
        --Chrome OS
        WHEN contains(su.user_agent, ' CrOS ') THEN 'Chrome OS'
        --iOS
        WHEN contains(su.user_agent, 'iPad') THEN 'iOS'
        WHEN contains(su.user_agent, 'iPhone') THEN 'iOS'
        WHEN contains(su.user_agent, 'iPod') THEN 'iOS'
        --Mac
        WHEN contains(su.user_agent, 'OS X') THEN 'Mac OSX'
        WHEN contains(su.user_agent, 'Darwin') THEN 'Mac OS'
        WHEN contains(su.user_agent, 'Macintosh') THEN 'Mac OS'
        --Windows
        WHEN contains(su.user_agent, 'Windows Phone') THEN 'Windows Mobile'
        WHEN contains(su.user_agent, 'Windows') THEN 'Windows'
        WHEN contains(su.user_agent, 'MSIE') THEN 'Windows'
        --Linux
        WHEN contains(su.user_agent, 'Linux x86_64') THEN 'Linux'
        WHEN contains(su.user_agent, 'Linux') THEN 'Linux'
        WHEN contains(su.user_agent, 'Ubuntu') THEN 'Linux'
        --RIM OS
        WHEN contains(su.user_agent, 'BB10') THEN 'RIM OS'
        WHEN contains(su.user_agent, 'Blackberry') THEN 'RIM OS'
        WHEN contains(su.user_agent, 'RIM') THEN 'RIM OS'
        --Gaming OS
        WHEN contains(su.user_agent, 'Nintendo') THEN 'Nintendo'
        WHEN contains(su.user_agent, 'Playstation') THEN 'Playstation'
        WHEN contains(su.user_agent, 'DreamPassport') THEN 'DreamCast'
        ELSE 'Unknown'
    END AS os,
    nvl(
        CASE
            WHEN su.device_type_id=1 THEN 'Desktop'
            WHEN su.device_type_id=2 THEN 'Mobile'
            WHEN su.device_type_id=3 THEN 'Tablet'
        END,
        CASE
            --Tablet
            WHEN contains(su.user_agent, 'Tablet') THEN 'Tablet'
            WHEN contains(su.user_agent, 'iPad') THEN 'Tablet'
            WHEN contains(su.user_agent, ' KF') THEN 'Tablet'
            WHEN su.user_agent LIKE '%%Android%%Safari%%' AND not contains(su.user_agent, 'mobile') THEN 'Tablet'
            WHEN su.user_agent LIKE '%%Mozilla%%Android%%JustFab%%' AND NOT contains(su.user_agent, 'Mobile Safari') THEN 'Tablet'
            --Mobile
            WHEN contains(su.user_agent, 'iPhone') THEN 'Mobile'
            WHEN contains(su.user_agent, 'iPod') THEN 'Mobile'
            WHEN contains(su.user_agent, 'Windows Phone') THEN 'Mobile'
            WHEN contains(su.user_agent, 'Blackberry') THEN 'Mobile'
            WHEN contains(su.user_agent, 'Dalvik') THEN 'Mobile'
            WHEN contains(su.user_agent, 'Mobile') THEN 'Mobile'
            WHEN contains(su.user_agent, 'Opera Mini') THEN 'Mobile'
            WHEN contains(su.user_agent, 'Android') AND contains(su.user_agent, 'safari') THEN 'Mobile'
            --Desktop
            WHEN contains(su.user_agent, 'Desktop') THEN 'Desktop'
            WHEN contains(su.user_agent, 'Windows') THEN 'Desktop'
            WHEN contains(su.user_agent, ' CrOS') THEN 'Desktop'
            WHEN contains(su.user_agent, 'OS X') THEN 'Desktop'
            WHEN contains(su.user_agent, 'Darwin') THEN 'Desktop'
            WHEN contains(su.user_agent, 'Linux') THEN 'Desktop'
            WHEN contains(su.user_agent, 'MSIE') THEN 'Desktop'
            WHEN contains(su.user_agent, 'Opera') THEN 'Desktop'
            --Android Device
            WHEN contains(su.user_agent, 'Android') THEN 'Mobile'
            --Other
            WHEN contains(su.user_agent, 'Nintendo') THEN 'Unknown'
            WHEN contains(su.user_agent, 'Playstation Vita') THEN 'Unknown'
            WHEN contains(su.user_agent, 'Playstation') THEN 'Unknown'
            WHEN contains(su.user_agent, 'DreamPassport') THEN 'Unknown'
            ELSE 'Unknown'
        END) AS device,
    iff(b.user_agent is not null, 1, 0) AS is_bot_old,
    iff(buad.user_agent is not null, 1, 0) AS is_bot,
    row_number() over(partition by base.session_id order by su.datetime_modified DESC) as rn
FROM _session_base AS base
    JOIN lake_consolidated_view.ultra_merchant.session_uri AS su
        ON su.session_id = base.session_id
    LEFT JOIN lake_consolidated_view.ultra_merchant.session AS s
        ON s.session_id = base.session_id
    LEFT JOIN staging.bots AS b
        ON b.user_agent = su.user_agent
    LEFT JOIN staging.bot_confirmed AS buad
        ON buad.user_agent = su.user_agent
ORDER BY base.session_id;

MERGE INTO staging.session_uri AS t USING (
    SELECT
        session_id,
        referer,
        _uri AS uri,
        user_agent,
        browser,
        os,
        device,
        is_bot_old,
        is_bot,
        clickid,
        sharedid,
        irad,
        mpid,
        irmp,
        utm_medium,
        utm_source,
        utm_campaign,
        utm_content,
        utm_term,
        ccode,
        pcode,
        edw_prod.stg.udf_unconcat_brand(session_id) AS meta_original_session_id,
        HASH(session_id, referer, _uri, user_agent, browser, os, device, is_bot_old, is_bot, clickid, sharedid, irad, mpid, irmp, utm_medium,
             utm_source, utm_campaign, utm_content, utm_term, ccode, pcode) AS meta_row_hash
    FROM _session_uri_base su
    where rn = 1
    ORDER BY su.session_id
    ) AS src
        ON src.session_id = t.session_id
    WHEN NOT MATCHED THEN
    INSERT (
        session_id,
        referer,
        uri,
        user_agent,
        browser,
        os,
        device,
        is_bot_old,
        is_bot,
        clickid,
        sharedid,
        irad,
        mpid,
        irmp,
        utm_medium,
        utm_source,
        utm_campaign,
        utm_content,
        utm_term,
        ccode,
        pcode,
        meta_original_session_id,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
        )
    VALUES (
        src.session_id,
        src.referer,
        src.uri,
        src.user_agent,
        src.browser,
        src.os,
        src.device,
        src.is_bot_old,
        src.is_bot,
        src.clickid,
        src.sharedid,
        src.irad,
        src.mpid,
        src.irmp,
        src.utm_medium,
        src.utm_source,
        src.utm_campaign,
        src.utm_content,
        src.utm_term,
        src.ccode,
        src.pcode,
        src.meta_original_session_id,
        src.meta_row_hash,
        $execution_start_time,
        $execution_start_time
        )
    WHEN MATCHED AND src.meta_row_hash != t.meta_row_hash THEN
    UPDATE SET
        t.referer = src.referer,
        t.uri = src.uri,
        t.user_agent = src.user_agent,
        t.browser = src.browser,
        t.os = src.os,
        t.device = src.device,
        t.is_bot_old = src.is_bot_old,
        t.is_bot = src.is_bot,
        t.clickid = src.clickid,
        t.sharedid = src.sharedid,
        t.irad = src.irad,
        t.mpid = src.mpid,
        t.irmp = src.irmp,
        t.utm_medium = src.utm_medium,
        t.utm_source = src.utm_source,
        t.utm_campaign = src.utm_campaign,
        t.utm_content = src.utm_content,
        t.utm_term = src.utm_term,
        t.ccode = src.ccode,
        t.pcode = src.pcode,
        t.meta_row_hash = src.meta_row_hash,
        t.meta_original_session_id = src.meta_original_session_id,
        t.meta_update_datetime = $execution_start_time;

TRUNCATE TABLE staging.session_uri_excp;

UPDATE public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = current_timestamp::timestamp_ltz(3)
WHERE table_name = $target_table;
