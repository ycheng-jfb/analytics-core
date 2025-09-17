USE reporting_media_base_prod;

DELETE
FROM reporting_media_base_prod.dbo.conversions
WHERE timestamp <= DATEADD(DAY, -15, current_date());

SET target_table = 'reporting_media_base_prod.dbo.conversions';

MERGE INTO reporting_media_base_prod.public.meta_table_dependency_watermark  AS t
USING
    (
        SELECT
            'reporting_media_base_prod.dbo.conversions' AS table_name,
            NULLIF(dependent_table_name,'reporting_media_base_prod.dbo.conversions') AS dependent_table_name,
            high_watermark_datetime AS new_high_watermark_datetime
            FROM(
                    SELECT
                        'lake.segment_sxf.java_sxf_order_completed' AS dependent_table_name,
                        max(meta_create_datetime) AS high_watermark_datetime
                    FROM lake.segment_sxf.java_sxf_order_completed
                    UNION
                    SELECT
                        'lake.segment_sxf.java_sxf_complete_registration',
                        max(meta_create_datetime)
                    FROM lake.segment_sxf.java_sxf_complete_registration
                    UNION
                    SELECT
                        'lake.segment_sxf.java_sxf_product_added',
                        max(meta_create_datetime)
                    FROM lake.segment_sxf.java_sxf_product_added
                    UNION
                    SELECT
                        'lake.segment_sxf.javascript_sxf_product_viewed',
                        max(meta_create_datetime)
                    FROM lake.segment_sxf.javascript_sxf_product_viewed
                    UNION
                    SELECT
                        'lake.segment_fl.java_fabletics_order_completed',
                        max(meta_create_datetime)
                    FROM lake.segment_fl.java_fabletics_order_completed
                    UNION
                    SELECT
                        'lake.segment_fl.java_fabletics_complete_registration',
                        max(meta_create_datetime)
                    FROM lake.segment_fl.java_fabletics_complete_registration
                    UNION
                    SELECT
                        'lake.segment_fl.java_fabletics_product_added',
                        max(meta_create_datetime)
                    FROM lake.segment_fl.java_fabletics_product_added
                    UNION
                    SELECT
                        'lake.segment_fl.javascript_fabletics_product_viewed',
                        max(meta_create_datetime)::timestamp_ntz
                    FROM lake.segment_fl.javascript_fabletics_product_viewed
                    UNION
                    SELECT
                        'lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed',
                        max(meta_create_datetime)::timestamp_ntz
                    FROM lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed
                    UNION
                    SELECT
                        'lake.segment_gfb.java_fabkids_order_completed',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.java_fabkids_order_completed
                    UNION
                    SELECT
                        'lake.segment_gfb.java_fabkids_complete_registration',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.java_fabkids_complete_registration
                    UNION
                    SELECT
                        'lake.segment_gfb.java_fabkids_product_added',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.java_fabkids_product_added
                    UNION
                    SELECT
                        'lake.segment_gfb.javascript_fabkids_product_viewed',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.javascript_fabkids_product_viewed
                    UNION
                    SELECT
                        'lake.segment_gfb.java_justfab_order_completed',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.java_justfab_order_completed
                    UNION
                    SELECT
                        'lake.segment_gfb.java_justfab_complete_registration',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.java_justfab_complete_registration
                    UNION
                    SELECT
                        'lake.segment_gfb.java_justfab_product_added',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.java_justfab_product_added
                    UNION
                    SELECT
                        'lake.segment_gfb.javascript_justfab_product_viewed',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.javascript_justfab_product_viewed
                    UNION
                    SELECT
                        'lake.segment_gfb.java_jf_ecom_app_order_completed',
                        max(meta_create_datetime)::timestamp_ntz
                    FROM lake.segment_gfb.java_jf_ecom_app_order_completed
                    UNION
                    SELECT
                        'lake.segment_gfb.java_shoedazzle_order_completed',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.java_shoedazzle_order_completed
                    UNION
                    SELECT
                        'lake.segment_gfb.java_shoedazzle_complete_registration',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.java_shoedazzle_complete_registration
                    UNION
                    SELECT
                        'lake.segment_gfb.java_shoedazzle_product_added',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.java_shoedazzle_product_added
                    UNION
                    SELECT
                        'lake.segment_gfb.javascript_shoedazzle_product_viewed',
                        max(meta_create_datetime)
                    FROM lake.segment_gfb.javascript_shoedazzle_product_viewed
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
         '2023-01-01'::timestamp_ltz,
         s.new_high_watermark_datetime,
         current_timestamp::timestamp_ltz(3)
        );


SET sxf_order_completed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_sxf.java_sxf_order_completed');
SET sxf_complete_registration_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_sxf.java_sxf_complete_registration');
SET sxf_product_added_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_sxf.java_sxf_product_added');
SET sxf_product_viewed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_sxf.javascript_sxf_product_viewed');
SET fabletics_order_completed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_order_completed');
SET fabletics_complete_registration_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_complete_registration');
SET fabletics_product_added_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_product_added');
SET fabletics_product_viewed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_fl.javascript_fabletics_product_viewed');
SET fabkids_order_completed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_fabkids_order_completed');
SET fabkids_complete_registration_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_fabkids_complete_registration');
SET fabkids_product_added_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_fabkids_product_added');
SET fabkids_product_viewed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.javascript_fabkids_product_viewed');
SET shoedazzle_order_completed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_shoedazzle_order_completed');
SET shoedazzle_complete_registration_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_shoedazzle_complete_registration');
SET shoedazzle_product_added_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_shoedazzle_product_added');
SET shoedazzle_product_viewed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.javascript_shoedazzle_product_viewed');
SET justfab_order_completed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_justfab_order_completed');
SET justfab_complete_registration_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_justfab_complete_registration');
SET justfab_product_added_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_justfab_product_added');
SET justfab_product_viewed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.javascript_justfab_product_viewed');
SET fabletics_ecom_mobile_app_order_completed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed');
SET jf_ecom_app_order_completed_watermark = reporting_media_base_prod.public.udf_get_watermark($target_table, 'lake.segment_gfb.java_jf_ecom_app_order_completed');


CREATE OR REPLACE TEMP TABLE _events AS
SELECT anonymousid,
    IFF(r.properties_order_id='',NULL,r.properties_order_id) AS event_id,
    r.timestamp,
    r.properties_customer_id AS external_id,
    r.properties_sha_email AS email,
    r.properties_currency currency,
    r.PROPERTIES_TOTAL revenue,
    r.properties_customer_gender customer_gender,
    'SX'||UPPER(r.country) store,
    IFF(r.properties_is_activating = 'TRUE', 'order_completed', 'non_activating_order') AS event,
    sha2(IFF(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ','') regexp '[0-9]+'
            and len(ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01)) = 10
            and lower(r.country)='us',
            1||ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01),null)) AS phone,
    '['||listagg( '{ "price": '||r.properties_products_price||
        ',"quantity": 1,"content_type": "product_group","content_id": "'||
        r.properties_products_product_id||'"}', ', ')
        within group (order by r.properties_products_product_id)
        over(partition by r.properties_order_id)||']' as contents,
    r.properties_store_id store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    r.properties_revenue price,
    properties_last_name AS last_name,
    properties_first_name AS first_name
FROM lake.segment_sxf.java_sxf_order_completed r
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = r.properties_store_id
    WHERE r.meta_create_datetime >= $sxf_order_completed_watermark
        and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_customer_id AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    NULL as revenue,
    l.properties_customer_gender AS customer_gender,
    'SX'||UPPER(l.country) store,
    'complete_registration' AS event,
    NULL AS phone,
    NULL AS contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    NULL as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_sxf.java_sxf_complete_registration l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >=$sxf_complete_registration_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    l.properties_price as revenue,
    l.properties_customer_gender AS customer_gender,
    'SX'||UPPER(l.country) store,
    'product_added' event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_sxf.java_sxf_product_added l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >=$sxf_product_added_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp::timestamp_ntz,
    l.properties_customer_id::string AS external_id,
    NULL AS email,
    NULL as currency,
    l.properties_price as revenue,
    NULL AS customer_gender,
    'SX'||UPPER(l.country) store,
    'product_viewed' event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    NULL AS store_id,
    'SX' as store_brand_abbr,
    upper(l.country) as store_country,
    IFF(upper(l.country) in ('US', 'CA'),'NA', 'EU') as store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_sxf.javascript_sxf_product_viewed l
WHERE l.meta_create_datetime::timestamp_ntz >= $sxf_product_viewed_watermark

UNION ALL

SELECT anonymousid,
    IFF(r.properties_order_id='',NULL,r.properties_order_id) AS event_id,
    r.timestamp,
    r.properties_customer_id AS external_id,
    r.properties_sha_email AS email,
    r.properties_currency currency,
    r.PROPERTIES_REVENUE AS revenue,
    r.properties_customer_gender customer_gender,
    'FL'||UPPER(r.country) store,
    CASE WHEN (r.properties_is_activating = 'TRUE' OR
               (r.properties_label = 'eComm' and r.properties_membership_brand_id = 2))
        THEN 'order_completed'
        ELSE 'non_activating_order'
    END AS event,
    sha2(IFF(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ','') regexp '[0-9]+'
            and len(ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01)) = 10
            and lower(r.country)='us',
            1||ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01),null)) AS phone,
    '['||listagg( '{ "price": '||r.properties_products_price||
        ',"quantity": 1,"content_type": "product_group","content_id": "'||
        r.properties_products_product_id||'"}', ', ')
        within group (order by r.properties_products_product_id)
        over(partition by r.properties_order_id)||']' as contents,
    r.properties_store_id store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    r.properties_revenue price,
    properties_last_name AS last_name,
    properties_first_name AS first_name
FROM lake.segment_fl.java_fabletics_order_completed r
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = r.properties_store_id
    WHERE r.meta_create_datetime >=$fabletics_order_completed_watermark
        and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_customer_id AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    NULL as revenue,
    l.properties_customer_gender AS customer_gender,
    'FL'||UPPER(l.country) store,
    'complete_registration' event,
    NULL AS phone,
    NULL AS contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    NULL as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_fl.java_fabletics_complete_registration l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >= $fabletics_complete_registration_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    l.properties_price as revenue,
    l.properties_customer_gender AS customer_gender,
    'FL'||UPPER(l.country) store,
    'product_added' AS event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_fl.java_fabletics_product_added l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >= $fabletics_product_added_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp::timestamp_ntz,
    l.properties_customer_id::string AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    l.properties_price as revenue,
    NULL AS customer_gender,
    'FL'||UPPER(l.country) store,
    'product_viewed' event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    NULL AS store_id,
    case when properties_membership_brand_id = 2 then 'YTY'
        else 'FL' end as store_brand_abbr,
    upper(l.country) as store_country,
    IFF(upper(l.country) in ('US', 'CA'),'NA', 'EU') as store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_fl.javascript_fabletics_product_viewed l
WHERE l.meta_create_datetime::timestamp_ntz >= $fabletics_product_viewed_watermark

UNION ALL

SELECT anonymousid,
    IFF(r.properties_order_id='',NULL,r.properties_order_id) AS event_id,
    r.timestamp,
    r.properties_customer_id AS external_id,
    r.properties_sha_email AS email,
    r.properties_currency currency,
    IFF(COALESCE(r.PROPERTIES_SUBTOTAL - r.PROPERTIES_DISCOUNT, 0) = 0 OR COALESCE(r.PROPERTIES_SHIPPING, 0) = 0,
        coalesce(r.PROPERTIES_SUBTOTAL - r.PROPERTIES_DISCOUNT, 0),
        ROUND((COALESCE(r.PROPERTIES_SUBTOTAL - r.PROPERTIES_DISCOUNT, 0) + COALESCE(r.PROPERTIES_SHIPPING, 0)) * 100) / 100
    ) AS revenue,
    r.properties_customer_gender customer_gender,
    'FK'||UPPER(r.country) store,
    IFF(r.properties_is_activating = 'TRUE', 'order_completed', 'non_activating_order') AS event,
    sha2(IFF(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ','') regexp '[0-9]+'
            and len(ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01)) = 10
            and lower(r.country)='us',
            1||ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01),null)) AS phone,
    '['||listagg( '{ "price": '||r.properties_products_price||
        ',"quantity": 1,"content_type": "product_group","content_id": "'||
        r.properties_products_product_id||'"}', ', ')
        within group (order by r.properties_products_product_id)
        over(partition by r.properties_order_id)||']' as contents,
    r.properties_store_id store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    r.properties_revenue price,
    properties_last_name AS last_name,
    properties_first_name AS first_name
FROM lake.segment_gfb.java_fabkids_order_completed r
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = r.properties_store_id
    WHERE r.meta_create_datetime >= $fabkids_order_completed_watermark
        and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_customer_id AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    NULL as revenue,
    l.properties_customer_gender AS customer_gender,
    'FK'||UPPER(l.country) store,
    'complete_registration' event,
    NULL AS phone,
    NULL AS contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    NULL as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_gfb.java_fabkids_complete_registration l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >=$fabkids_complete_registration_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    l.properties_price as revenue,
    l.properties_customer_gender AS customer_gender,
    'FK'||UPPER(l.country) store,
    'product_added' event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_gfb.java_fabkids_product_added l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >=$fabkids_product_added_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp::timestamp_ntz,
    l.properties_customer_id::string AS external_id,
    NULL AS email,
    NULL as currency,
    l.properties_price as revenue,
    NULL AS customer_gender,
    'FK'||UPPER(l.country) store,
    'product_viewed' event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    NULL AS store_id,
    'FK' as store_brand_abbr,
    upper(l.country) as store_country,
    IFF(upper(l.country) in ('US', 'CA'),'NA', 'EU') as store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_gfb.javascript_fabkids_product_viewed l
WHERE l.meta_create_datetime::timestamp_ntz >= $fabkids_product_viewed_watermark

UNION ALL

SELECT anonymousid,
    IFF(r.properties_order_id='',NULL,r.properties_order_id) AS event_id,
    r.timestamp,
    r.properties_customer_id AS external_id,
    r.properties_sha_email AS email,
    r.properties_currency currency,
    r.PROPERTIES_REVENUE AS revenue,
    r.properties_customer_gender customer_gender,
    'SD'||UPPER(r.country) store,
    IFF(r.properties_is_activating = 'TRUE', 'order_completed', 'non_activating_order') AS event,
    sha2(IFF(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ','') regexp '[0-9]+'
            and len(ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01)) = 10
            and lower(r.country)='us',
            1||ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01),null)) AS phone,
    '['||listagg( '{ "price": '||r.properties_products_price||
        ',"quantity": 1,"content_type": "product_group","content_id": "'||
        r.properties_products_product_id||'"}', ', ')
        within group (order by r.properties_products_product_id)
        over(partition by r.properties_order_id)||']' as contents,
    r.properties_store_id store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    r.properties_revenue price,
    properties_last_name AS last_name,
    properties_first_name AS first_name
FROM lake.segment_gfb.java_shoedazzle_order_completed r
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = r.properties_store_id
    WHERE r.meta_create_datetime >=$shoedazzle_order_completed_watermark
        and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_customer_id AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    NULL as revenue,
    l.properties_customer_gender AS customer_gender,
    'SD'||UPPER(l.country) store,
    'complete_registration' event,
    NULL AS phone,
    NULL AS contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    NULL as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_gfb.java_shoedazzle_complete_registration l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >=$shoedazzle_complete_registration_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    l.properties_price as revenue,
    l.properties_customer_gender AS customer_gender,
    'SD'||UPPER(l.country) store,
    'product_added' event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_gfb.java_shoedazzle_product_added l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >=$shoedazzle_product_added_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp::timestamp_ntz,
    l.properties_customer_id::string AS external_id,
    NULL AS email,
    NULL as currency,
    l.properties_price as revenue,
    NULL AS customer_gender,
    'SD'||UPPER(l.country) store,
    'product_viewed' event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    NULL AS store_id,
    'SD' as store_brand_abbr,
    upper(l.country) as store_country,
    IFF(upper(l.country) in ('US', 'CA'),'NA', 'EU') as store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_gfb.javascript_shoedazzle_product_viewed l
WHERE l.meta_create_datetime::timestamp_ntz >=$shoedazzle_product_viewed_watermark

UNION ALL

SELECT anonymousid,
    IFF(r.properties_order_id='',NULL,r.properties_order_id) AS event_id,
    r.timestamp,
    r.properties_customer_id AS external_id,
    r.properties_sha_email AS email,
    r.properties_currency currency,
    r.PROPERTIES_REVENUE AS revenue,
    r.properties_customer_gender customer_gender,
    'JF'||UPPER(r.country) store,
    IFF(r.properties_is_activating = 'TRUE', 'order_completed', 'non_activating_order') AS event,
    sha2(IFF(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ','') regexp '[0-9]+'
            and len(ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01)) = 10
            and lower(r.country)='us',
            1||ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01),null)) AS phone,
    '['||listagg( '{ "price": '||r.properties_products_price||
        ',"quantity": 1,"content_type": "product_group","content_id": "'||
        r.properties_products_product_id||'"}', ', ')
        within group (order by r.properties_products_product_id)
        over(partition by r.properties_order_id)||']' as contents,
    r.properties_store_id store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    r.properties_revenue price,
    properties_last_name AS last_name,
    properties_first_name AS first_name
FROM lake.segment_gfb.java_justfab_order_completed r
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = r.properties_store_id
    WHERE r.meta_create_datetime >=$justfab_order_completed_watermark
        and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_customer_id AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    NULL as revenue,
    l.properties_customer_gender AS customer_gender,
    'JF'||UPPER(l.country) store,
    'complete_registration' event,
    NULL AS phone,
    NULL AS contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    NULL as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_gfb.java_justfab_complete_registration l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >=$justfab_complete_registration_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp,
    l.properties_customer_id AS external_id,
    l.properties_sha_email AS email,
    NULL as currency,
    l.properties_price as revenue,
    l.properties_customer_gender AS customer_gender,
    'JF'||UPPER(l.country) store,
    'product_added' event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    l.properties_store_id AS store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_gfb.java_justfab_product_added l
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = l.properties_store_id
WHERE l.meta_create_datetime >=$justfab_product_added_watermark
    and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    l.properties_product_id::string AS event_id,
    l.timestamp::timestamp_ntz,
    l.properties_customer_id::string AS external_id,
    NULL AS email,
    NULL as currency,
    l.properties_price as revenue,
    NULL AS customer_gender,
    'JF'||UPPER(l.country) store,
    'product_viewed' event,
    NULL AS phone,
    '[{ "content_type": "product_group","content_id": "'||
        l.properties_product_id||'"}]' as contents,
    NULL AS store_id,
    'JF' as store_brand_abbr,
    upper(l.country) as store_country,
    IFF(upper(l.country) in ('US', 'CA'),'NA', 'EU') as store_region,
    l.properties_price as price,
    NULL AS last_name,
    NULL AS first_name
FROM lake.segment_gfb.javascript_justfab_product_viewed l
WHERE l.meta_create_datetime::timestamp_ntz >=$justfab_product_viewed_watermark

UNION ALL

SELECT anonymousid,
    IFF(r.properties_order_id='',NULL,r.properties_order_id) AS event_id,
    r.timestamp::timestamp_ntz,
    r.properties_customer_id AS external_id,
    r.properties_sha_email AS email,
    r.properties_currency currency,
    r.properties_revenue revenue,
    r.properties_customer_gender customer_gender,
    'FL'||UPPER(r.country) store,
    CASE WHEN (r.properties_is_activating = 'TRUE' OR (
        r.properties_label = 'eComm' and r.properties_membership_brand_id = 2))
        THEN 'order_completed'
        ELSE 'non_activating_order'
    END AS event,
    sha2(IFF(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ','') regexp '[0-9]+'
            and len(ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01)) = 10
            and lower(r.country)='us',
            1||ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01),null)) AS phone,
    '['||listagg( '{ "price": '||r.properties_products_price||
        ',"quantity": 1,"content_type": "product_group","content_id": "'||
        r.properties_products_product_id||'"}', ', ')
        within group (order by r.properties_products_product_id)
        over(partition by r.properties_order_id)||']' as contents,
    r.properties_store_id store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    r.properties_revenue price,
    properties_last_name AS last_name,
    properties_first_name AS first_name
FROM lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed r
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = r.properties_store_id
    WHERE r.meta_create_datetime >=$fabletics_ecom_mobile_app_order_completed_watermark
        and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true

UNION ALL

SELECT anonymousid,
    IFF(r.properties_order_id='',NULL,r.properties_order_id) AS event_id,
    r.timestamp::timestamp_ntz,
    r.properties_customer_id AS external_id,
    r.properties_sha_email AS email,
    r.properties_currency currency,
    r.properties_revenue revenue,
    r.properties_customer_gender customer_gender,
    'JF'||UPPER(r.country) store,
    CASE WHEN r.properties_is_activating = 'TRUE'
        THEN 'order_completed'
        ELSE 'non_activating_order'
    END AS event,
    sha2(IFF(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ','') regexp '[0-9]+'
            and len(ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01)) = 10
            and lower(r.country)='us',
            1||ltrim(regexp_replace(PROPERTIES_PHONE_NUMBER,
                '\\(|\\)|\\+|\\.|-| ',''),01),null)) AS phone,
    '['||listagg( '{ "price": '||r.properties_products_price||
        ',"quantity": 1,"content_type": "product_group","content_id": "'||
        r.properties_products_product_id||'"}', ', ')
        within group (order by r.properties_products_product_id)
        over(partition by r.properties_order_id)||']' as contents,
    r.properties_store_id store_id,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    r.properties_revenue price,
    properties_last_name AS last_name,
    properties_first_name AS first_name
FROM lake.segment_gfb.java_jf_ecom_app_order_completed r
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = r.properties_store_id
    WHERE r.meta_create_datetime >=$jf_ecom_app_order_completed_watermark
        and coalesce(parse_json(_rescued_data)['context']['traits']['email'] ILIKE
             ANY ('%@test.com%', '%@example.com%'), false) <> true;

UPDATE _events
SET anonymousid=NULL
WHERE len(anonymousid) < 5;

CREATE OR REPLACE TEMPORARY TABLE _segment_identify AS
SELECT TRY_TO_NUMERIC(userid) AS customer_id,
        first_value(country) ignore nulls
            over (partition by id.userid order by receivedat desc) as country,
        TO_VARCHAR( first_value(TRY_TO_DATE(traits_birthday, 'MM-DD-YYYY')) ignore nulls
            over (partition by id.userid order by receivedat desc), 'YYYYMMDD') dob,
        first_value(TRY_TO_NUMERIC(LEFT(TRIM(traits_zip_code), 5))) ignore nulls
            over (partition by id.userid order by receivedat desc) as zip_code,
        traits_email ILIKE ANY ('%@test.com%', '%@example.com%') as is_test,
        traits_email AS email
FROM lake_view.segment_sxf.javascript_sxf_identify id
join _events n on n.external_id = id.userid
        and LEFT(n.store, 2) = 'SX'
QUALIFY ROW_NUMBER() OVER(PARTITION BY id.userid ORDER BY receivedat DESC) = 1
    union
SELECT TRY_TO_NUMERIC(userid) AS customer_id,
        first_value(country) ignore nulls
            over (partition by id.userid order by receivedat desc) as country,
        TO_VARCHAR( first_value(TRY_TO_DATE(traits_birthday, 'MM-DD-YYYY')) ignore nulls
            over (partition by id.userid order by receivedat desc), 'YYYYMMDD') dob,
        first_value(TRY_TO_NUMERIC(LEFT(TRIM(traits_zip_code), 5))) ignore nulls
            over (partition by id.userid order by receivedat desc) as zip_code,
        traits_email ILIKE ANY ('%@test.com%', '%@example.com%') as is_test,
        traits_email AS email
FROM lake_view.segment_fl.javascript_fabletics_identify id
join _events n on n.external_id = id.userid
        and LEFT(n.store, 2) = 'FL'
QUALIFY ROW_NUMBER() OVER(PARTITION BY id.userid ORDER BY receivedat DESC) = 1
    union
SELECT TRY_TO_NUMERIC(userid) AS customer_id,
        first_value(country) ignore nulls
            over (partition by id.userid order by receivedat desc) as country,
        NULL AS dob,
        first_value(TRY_TO_NUMERIC(LEFT(TRIM(traits_zip_code), 5))) ignore nulls
            over (partition by id.userid order by receivedat desc) as zip_code,
        traits_email LIKE ANY ('%@test.com%', '%@example.com%') as is_test,
        traits_email AS email
FROM lake_view.segment_gfb.javascript_fabkids_identify id
join _events n on n.external_id = id.userid
        and LEFT(n.store, 2) = 'FK'
QUALIFY ROW_NUMBER() OVER(PARTITION BY id.userid ORDER BY receivedat DESC) = 1
    union
SELECT TRY_TO_NUMERIC(userid) AS customer_id,
        first_value(country) ignore nulls
            over (partition by id.userid order by receivedat desc) as country,
        TO_VARCHAR( first_value(TRY_TO_DATE(traits_birthday, 'MM-DD-YYYY')) ignore nulls
            over (partition by id.userid order by receivedat desc), 'YYYYMMDD') dob,
        first_value(TRY_TO_NUMERIC(LEFT(TRIM(traits_zip_code), 5))) ignore nulls
            over (partition by id.userid order by receivedat desc) as zip_code,
        traits_email ILIKE ANY ('%@test.com%', '%@example.com%') as is_test,
        traits_email AS email
FROM lake_view.segment_gfb.javascript_shoedazzle_identify id
join _events n on n.external_id = id.userid
        and LEFT(n.store, 2) = 'SD'
QUALIFY ROW_NUMBER() OVER(PARTITION BY id.userid ORDER BY receivedat DESC) = 1
    union
SELECT TRY_TO_NUMERIC(userid) AS customer_id,
        first_value(country) ignore nulls
            over (partition by id.userid order by receivedat desc) as country,
        TO_VARCHAR( first_value(TRY_TO_DATE(traits_birthday, 'MM-DD-YYYY')) ignore nulls
            over (partition by id.userid order by receivedat desc), 'YYYYMMDD') dob,
        first_value(TRY_TO_NUMERIC(LEFT(TRIM(traits_zip_code), 5))) ignore nulls
            over (partition by id.userid order by receivedat desc) as zip_code,
        traits_email ILIKE ANY ('%@test.com%', '%@example.com%') as is_test,
        traits_email AS email
FROM lake_view.segment_gfb.javascript_justfab_identify id
join _events n on n.external_id = id.userid
        and LEFT(n.store, 2) = 'JF'
QUALIFY ROW_NUMBER() OVER(PARTITION BY id.userid ORDER BY receivedat DESC) = 1;


UPDATE _events e
SET e.email = upper(sha2(s.email))
FROM _segment_identify s
WHERE e.external_id = s.customer_id::VARCHAR
    AND e.email IS NULL;


CREATE OR REPLACE TEMP TABLE _customers AS
SELECT customer_id,
       clid,
       context_useragent,
       context_ip,
       context_page_url
FROM reporting_media_base_prod.dbo.segment_usermap
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY clid_timestamp DESC) = 1;


CREATE OR REPLACE TEMP TABLE _conversions AS
SELECT
    e.event_id,
    e.timestamp,
    e.external_id,
    e.email,
    e.currency,
    e.revenue,
    e.customer_gender,
    e.store,
    e.store_id,
    e.event,
    e.phone,
    e.contents,
    e.store_brand_abbr,
    e.store_country,
    e.store_region,
    e.price,
    e.last_name,
    e.first_name,
    coalesce(su.clid, c.clid) AS click_id,
    coalesce(su.context_useragent, c.context_useragent) AS user_agent,
    decode(lower(coalesce(su.context_ip, c.context_ip)), 'unknown', NULL, coalesce(su.context_ip, c.context_ip)) AS ip,
    coalesce(su.context_page_url, c.context_page_url) AS page_url,
    lower(trim(si.dob)) AS db,
    lower(trim(si.zip_code)) AS zp,
    lower(trim(si.country)) AS country
from _events e
LEFT JOIN reporting_media_base_prod.dbo.segment_usermap su
    ON su.anonymousid_clid = e.anonymousid
LEFT JOIN _customers c ON c.customer_id::varchar = e.external_id
LEFT JOIN _segment_identify si ON si.customer_id::varchar = e.external_id and coalesce(is_test,false) <> true;


MERGE INTO reporting_media_base_prod.dbo.conversions t
USING (
          SELECT a.*
              FROM (
                       SELECT *,
                              hash(*) AS meta_row_hash,
                              CURRENT_TIMESTAMP AS meta_create_datetime,
                              CURRENT_TIMESTAMP AS meta_update_datetime,
                              row_number() over (partition BY event_id, timestamp
                                        ORDER BY COALESCE(timestamp, '1900-01-01') DESC,
                                            click_id) AS rn
                           FROM _conversions
                           WHERE event_id IS NOT NULL
                   ) a
              WHERE a.rn = 1
      ) s ON equal_null(t.event_id, s.event_id)
    AND equal_null(t.timestamp, s.timestamp)
WHEN NOT MATCHED
    THEN INSERT (
                 event_id,
                 timestamp,
                 external_id,
                 email,
                 currency,
                 revenue,
                 user_agent,
                 ip,
                 customer_gender,
                 store,
                 db,
                 zp,
                 country,
                 event,
                 phone,
                 contents,
                 click_id,
                 page_url,
                 store_id,
                 store_brand_abbr,
                 store_country,
                 store_region,
                 price,
                 last_name,
                 first_name,
                 meta_row_hash,
                 meta_create_datetime,
                 meta_update_datetime)
        VALUES (event_id,
                timestamp,
                external_id,
                email,
                currency,
                revenue,
                user_agent,
                ip,
                customer_gender,
                store,
                db,
                zp,
                country,
                event,
                phone,
                contents,
                click_id,
                page_url,
                store_id,
                store_brand_abbr,
                store_country,
                store_region,
                price,
                last_name,
                first_name,
                meta_row_hash,
                meta_create_datetime,
                meta_update_datetime)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE SET
    t.external_id = s.external_id,
    t.email = s.email,
    t.currency = s.currency,
    t.revenue = s.revenue,
    t.user_agent = s.user_agent,
    t.ip = s.ip,
    t.customer_gender = s.customer_gender,
    t.store = s.store,
    t.db = s.db,
    t.zp = s.zp,
    t.country = s.country,
    t.event = s.event,
    t.phone = s.phone,
    t.contents = s.contents,
    t.click_id = s.click_id,
    t.page_url = s.page_url,
    t.store_id = s.store_id,
    t.store_brand_abbr=s.store_brand_abbr,
    t.store_country=s.store_country,
    t.store_region=s.store_region,
    t.price = s.price,
    t.first_name = s.first_name,
    t.last_name = s.last_name,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;

UPDATE reporting_media_base_prod.public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = current_timestamp::timestamp_ltz(3)
WHERE table_name = $target_table;
