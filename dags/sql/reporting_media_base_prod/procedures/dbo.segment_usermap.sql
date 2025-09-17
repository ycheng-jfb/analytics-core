USE reporting_media_base_prod;

DELETE FROM reporting_media_base_prod.dbo.segment_usermap
    WHERE clid_timestamp < dateadd(DAY, -45, current_date());

SET target_table = 'reporting_media_base_prod.dbo.segment_usermap';

MERGE INTO reporting_media_base_prod.public.meta_table_dependency_watermark AS t
USING
(
    SELECT
        'reporting_media_base_prod.dbo.segment_usermap' AS table_name,
        NULLIF(dependent_table_name,'reporting_media_base_prod.dbo.segment_usermap') AS dependent_table_name,
        high_watermark_datetime AS new_high_watermark_datetime
        FROM(
                SELECT
                    'lake.segment_sxf.javascript_sxf_page' AS dependent_table_name,
                    max(meta_create_datetime) AS high_watermark_datetime
                FROM lake.segment_sxf.javascript_sxf_page
                UNION ALL
                SELECT
                    'lake.segment_fl.javascript_fabletics_page' AS dependent_table_name,
                    max(meta_create_datetime)::TIMESTAMP_NTZ AS high_watermark_datetime
                FROM lake.segment_fl.javascript_fabletics_page
                UNION ALL
                SELECT
                    'lake.segment_gfb.javascript_fabkids_page' AS dependent_table_name,
                    max(meta_create_datetime) AS high_watermark_datetime
                FROM lake.segment_gfb.javascript_fabkids_page
                UNION ALL
                SELECT
                    'lake.segment_gfb.javascript_shoedazzle_page' AS dependent_table_name,
                    max(meta_create_datetime) AS high_watermark_datetime
                FROM lake.segment_gfb.javascript_shoedazzle_page
                UNION ALL
                SELECT
                    'lake.segment_gfb.javascript_justfab_page' AS dependent_table_name,
                    max(meta_create_datetime) AS high_watermark_datetime
                FROM lake.segment_gfb.javascript_justfab_page
            ) h
) AS s
ON t.table_name = s.table_name
    AND equal_null(t.dependent_table_name, s.dependent_table_name)
WHEN MATCHED
    AND not equal_null(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
    THEN UPDATE
        SET t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = current_timestamp::timestamp_ltz(3)
WHEN NOT MATCHED
THEN INSERT
    (
     table_name,
     dependent_table_name,
     high_watermark_datetime,
     new_high_watermark_datetime,
     meta_create_datetime
    )
VALUES
    (
     s.table_name,
     s.dependent_table_name,
     '1900-01-01'::timestamp_ltz,
     s.new_high_watermark_datetime,
     current_timestamp::timestamp_ltz(3)
    );


SET sxf_page_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_sxf.javascript_sxf_page');
SET fabletics_page_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_fl.javascript_fabletics_page');
SET fabkids_page_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.javascript_fabkids_page');
SET shoedazzle_page_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.javascript_shoedazzle_page');
SET justfab_page_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.javascript_justfab_page');


CREATE OR REPLACE TEMPORARY TABLE _segment_user_map AS
SELECT p.anonymousid as anonymousid_clid,
    TRY_TO_NUMBER(IFF(userid is NULL, first_value(userid) ignore nulls
    over (partition by p.anonymousid order by timestamp desc), userid)) AS customer_id,
    parse_url(properties_url,1) as url_json,
    timestamp as clid_timestamp,
    CASE
        WHEN properties_url ILIKE '%%fbclid%%' THEN 'fbclid'
        WHEN properties_url LIKE '%%ScCid=%%' THEN 'ScCid'
        WHEN properties_url ILIKE '%%ttclid%%' THEN 'ttclid'
        ELSE 'other'
    END AS type,
    CASE
        WHEN properties_url ILIKE any ('%%fbclid%%', '%%ttclid%%', '%%ScCid=%%') THEN 1
        ELSE 2
    END AS order_type,
    'SX'||UPPER(country) store,
    context_ip,
    context_useragent,
    context_page_url
FROM lake.segment_sxf.javascript_sxf_page p
WHERE meta_create_datetime >= $sxf_page_watermark

UNION ALL

SELECT p.anonymousid as anonymousid_clid,
    TRY_TO_NUMBER(IFF(userid is NULL, first_value(userid) ignore nulls
    over (partition by p.anonymousid order by timestamp desc), userid)) AS customer_id,
    parse_url(properties_url,1) as url_json,
    timestamp as clid_timestamp,
    CASE
        WHEN properties_url ILIKE '%%fbclid%%' THEN 'fbclid'
        WHEN properties_url LIKE '%%ScCid=%%' THEN 'ScCid'
        WHEN properties_url ILIKE '%%ttclid%%' THEN 'ttclid'
        ELSE 'other'
    END AS type,
    CASE
        WHEN properties_url ILIKE any ('%%fbclid%%', '%%ttclid%%', '%%ScCid=%%') THEN 1
        ELSE 2
    END AS order_type,
    'FL'||UPPER(country) store,
    context_ip,
    context_useragent,
    context_page_url
FROM lake.segment_fl.javascript_fabletics_page p
WHERE meta_create_datetime >= $fabletics_page_watermark

UNION ALL

SELECT p.anonymousid as anonymousid_clid,
    TRY_TO_NUMBER(IFF(userid is NULL, first_value(userid) ignore nulls
    over (partition by p.anonymousid order by timestamp desc), userid)) AS customer_id,
    parse_url(properties_url,1) as url_json,
    timestamp as clid_timestamp,
    CASE
        WHEN properties_url ILIKE '%%fbclid%%' THEN 'fbclid'
        WHEN properties_url LIKE '%%ScCid=%%' THEN 'ScCid'
        WHEN properties_url ILIKE '%%ttclid%%' THEN 'ttclid'
        ELSE 'other'
    END AS type,
    CASE
        WHEN properties_url ILIKE any ('%%fbclid%%', '%%ttclid%%', '%%ScCid=%%') THEN 1
        ELSE 2
    END AS order_type,
    'FK'||UPPER(country) store,
    context_ip,
    context_useragent,
    context_page_url
FROM lake.segment_gfb.javascript_fabkids_page p
WHERE meta_create_datetime >= $fabkids_page_watermark

UNION ALL

SELECT p.anonymousid as anonymousid_clid,
    TRY_TO_NUMBER(IFF(userid is NULL, first_value(userid) ignore nulls
    over (partition by p.anonymousid order by timestamp desc), userid)) AS customer_id,
    parse_url(properties_url,1) as url_json,
    timestamp as clid_timestamp,
    CASE
        WHEN properties_url ILIKE '%%fbclid%%' THEN 'fbclid'
        WHEN properties_url LIKE '%%ScCid=%%' THEN 'ScCid'
        WHEN properties_url ILIKE '%%ttclid%%' THEN 'ttclid'
        ELSE 'other'
    END AS type,
    CASE
        WHEN properties_url ILIKE any ('%%fbclid%%', '%%ttclid%%', '%%ScCid=%%') THEN 1
        ELSE 2
    END AS order_type,
    'SD'||UPPER(country) store,
    context_ip,
    context_useragent,
    context_page_url
FROM lake.segment_gfb.javascript_shoedazzle_page p
WHERE meta_create_datetime >= $shoedazzle_page_watermark

UNION ALL

SELECT p.anonymousid as anonymousid_clid,
    TRY_TO_NUMBER(IFF(userid is NULL, first_value(userid) ignore nulls
    over (partition by p.anonymousid order by timestamp desc), userid)) AS customer_id,
    parse_url(properties_url,1) as url_json,
    timestamp as clid_timestamp,
    CASE
        WHEN properties_url ILIKE '%%fbclid%%' THEN 'fbclid'
        WHEN properties_url LIKE '%%ScCid=%%' THEN 'ScCid'
        WHEN properties_url ILIKE '%%ttclid%%' THEN 'ttclid'
        ELSE 'other'
    END AS type,
    CASE
        WHEN properties_url ILIKE any ('%%fbclid%%', '%%ttclid%%', '%%ScCid=%%') THEN 1
        ELSE 2
    END AS order_type,
    'JF'||UPPER(country) store,
    context_ip,
    context_useragent,
    context_page_url
FROM lake.segment_gfb.javascript_justfab_page p
WHERE meta_create_datetime >= $justfab_page_watermark;


CREATE OR REPLACE TEMPORARY TABLE _segment_user_map_stg AS
SELECT anonymousid_clid,
    um.customer_id customer_id,
    get_path(url_json:"parameters", type):: STRING AS clid,
    clid_timestamp::TIMESTAMP_NTZ as clid_timestamp,
    type,
    store,
    context_ip,
    context_useragent,
    context_page_url
FROM _segment_user_map um
    QUALIFY ROW_NUMBER() OVER(partition by ANONYMOUSID_CLID order by order_type,clid_timestamp desc)=1;


MERGE INTO reporting_media_base_prod.dbo.segment_usermap t
USING (
    SELECT a.*
    FROM (
        SELECT *,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY ANONYMOUSID_CLID, type
                ORDER BY coalesce(clid_timestamp, '1900-01-01') DESC ) AS rn
        FROM _segment_user_map_stg
        ) a
    WHERE a.rn = 1
    ) s ON equal_null(t.ANONYMOUSID_CLID, s.ANONYMOUSID_CLID)
AND equal_null(t.type, s.type)
WHEN NOT MATCHED
THEN INSERT (anonymousid_clid, customer_id, clid, clid_timestamp, type, store, context_ip, context_useragent,
             context_page_url, meta_row_hash, meta_create_datetime, meta_update_datetime)
    VALUES (anonymousid_clid, customer_id, clid, clid_timestamp, type, store, context_ip, context_useragent,
             context_page_url, meta_row_hash, meta_create_datetime, meta_update_datetime)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE
    SET
    t.customer_id = s.customer_id,
    t.clid = s.clid,
    t.clid_timestamp = s.clid_timestamp,
    t.store = s.store,
    t.context_ip = s.context_ip,
    t.context_useragent = s.context_useragent,
    t.context_page_url = s.context_page_url,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = CURRENT_TIMESTAMP;


UPDATE reporting_media_base_prod.public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = current_timestamp::timestamp_ltz(3)
WHERE table_name = $target_table;
