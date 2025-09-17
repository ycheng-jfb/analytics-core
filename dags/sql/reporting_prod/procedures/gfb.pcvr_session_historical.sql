SET first_date_pst = (SELECT COALESCE(DATEADD('day', -2, MAX(max_received)), '2021-08-01')::DATETIME
                  FROM gfb.pcvr_session_historical
                  WHERE platform != 'Mobile App');

SET first_date = CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', $first_date_pst);

SET update_time = CURRENT_TIMESTAMP();

SET r_time = (
    SELECT MIN(t.rtime)::DATETIME
    FROM (
        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.javascript_justfab_product_list_viewed
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.javascript_justfab_product_viewed
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.java_justfab_product_added
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.java_justfab_order_completed
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.javascript_shoedazzle_product_list_viewed
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.javascript_shoedazzle_product_viewed
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.java_shoedazzle_product_added
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.java_shoedazzle_order_completed
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.javascript_fabkids_product_list_viewed
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.javascript_fabkids_product_viewed
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.java_fabkids_product_added
        WHERE receivedat::DATETIME >= $first_date

        UNION ALL

        SELECT MAX(receivedat) AS rtime
        FROM lake.segment_gfb.java_fabkids_order_completed
        WHERE receivedat::DATETIME >= $first_date
    ) t
);

SET first_date_app_pst =(SELECT COALESCE(DATEADD('day', -2, MAX(max_received)), '2023-03-01')::DATETIME
                     FROM gfb.pcvr_session_historical
                     WHERE platform = 'Mobile App');

SET first_date_app = CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', $first_date_app_pst);

SET r_time_app = (SELECT MIN(t.rtime)::DATETIME
                  FROM (SELECT MAX(receivedat) AS rtime
                        FROM lake.segment_gfb.react_native_justfab_product_list_viewed
                        WHERE receivedat::DATETIME >= $first_date_app

                        UNION ALL

                        SELECT MAX(receivedat) AS rtime
                        FROM lake.segment_gfb.react_native_justfab_product_viewed
                        WHERE receivedat::DATETIME >= $first_date_app

                        UNION ALL

                        SELECT MAX(receivedat) AS rtime
                        FROM lake.segment_gfb.java_jf_ecom_app_product_added
                        WHERE receivedat::DATETIME >= $first_date_app

                        UNION ALL

                        SELECT MAX(receivedat) AS rtime
                        FROM lake.segment_gfb.java_jf_ecom_app_order_completed
                        WHERE receivedat::DATETIME >= $first_date_app)
                    t);


CREATE OR REPLACE TEMPORARY TABLE _session_base AS
SELECT DISTINCT
    TRY_TO_NUMBER(properties_session_id) AS session_id,
    store_brand
FROM (SELECT  properties_session_id,
              'JUSTFAB'                    AS store_brand
      FROM lake.segment_gfb.javascript_justfab_product_list_viewed
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'JUSTFAB'                    AS store_brand
      FROM lake.segment_gfb.javascript_justfab_product_viewed
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'JUSTFAB'                    AS store_brand
      FROM lake.segment_gfb.java_justfab_product_added
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'JUSTFAB'                    AS store_brand
      FROM lake.segment_gfb.java_justfab_order_completed
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'SHOEDAZZLE'                 AS store_brand
      FROM lake.segment_gfb.javascript_shoedazzle_product_list_viewed
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'SHOEDAZZLE'                  AS store_brand
      FROM lake.segment_gfb.javascript_shoedazzle_product_viewed
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'SHOEDAZZLE'                         AS store_brand
      FROM lake.segment_gfb.java_shoedazzle_product_added
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'SHOEDAZZLE'                 AS store_brand
      FROM lake.segment_gfb.java_shoedazzle_order_completed
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'FABKIDS'                    AS store_brand
      FROM lake.segment_gfb.javascript_fabkids_product_list_viewed
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'FABKIDS'                    AS store_brand
      FROM lake.segment_gfb.javascript_fabkids_product_viewed
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'FABKIDS'                    AS store_brand
      FROM lake.segment_gfb.java_fabkids_product_added
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time

      UNION ALL

      SELECT  properties_session_id,
              'FABKIDS'                    AS store_brand
      FROM lake.segment_gfb.java_fabkids_order_completed
      WHERE receivedat::DATETIME BETWEEN $first_date AND $r_time) AS a
WHERE TRY_TO_NUMBER(properties_session_id)  IS NOT NULL;

CREATE OR REPLACE TEMPORARY TABLE _usi AS
SELECT
     TRY_TO_NUMBER(properties_session_id)                                           AS session_id,
     COALESCE(properties_customer_id, TRY_TO_NUMBER(userid))                        AS user_id,
     'JUSTFAB'                                                                      AS store_brand,
     CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',TO_TIMESTAMP(timestamp))::DATE   AS session_start_date,
     timestamp,
     country
FROM lake.segment_gfb.javascript_justfab_page
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'JUSTFAB')
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(properties_session_id) ORDER BY timestamp) = 1

UNION ALL

SELECT
    TRY_TO_NUMBER(properties_session_id)                                                     AS session_id,
    COALESCE(properties_customer_id, TRY_TO_NUMBER(userid))                                  AS user_id,
    'SHOEDAZZLE'                                                                             AS store_brand,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',TO_TIMESTAMP(timestamp))::DATE             AS session_start_date,
    timestamp,
    country
FROM lake.segment_gfb.javascript_shoedazzle_page
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'SHOEDAZZLE')
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(properties_session_id) ORDER BY timestamp) = 1

UNION ALL

SELECT
    TRY_TO_NUMBER(properties_session_id)                                                     AS session_id,
    COALESCE(properties_customer_id, TRY_TO_NUMBER(userid))                                  AS user_id,
    'FABKIDS'                                                                                AS store_brand,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp))::DATE              AS session_start_date,
    timestamp,
    country
FROM lake.segment_gfb.javascript_fabkids_page
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'FABKIDS')
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(properties_session_id) ORDER BY timestamp) = 1;

CREATE OR REPLACE TEMPORARY TABLE _session_base_app AS
SELECT DISTINCT
    TRY_TO_NUMBER(properties_session_id) AS session_id,
    store_brand
FROM (SELECT
        properties_session_id,
        'JUSTFAB'   AS store_brand
      FROM lake.segment_gfb.react_native_justfab_product_list_viewed
      WHERE receivedat::DATETIME BETWEEN $first_date_app AND $r_time_app

      UNION ALL

      SELECT
        properties_session_id,
        'JUSTFAB'                            AS store_brand
      FROM lake.segment_gfb.react_native_justfab_product_viewed
      WHERE receivedat::DATETIME BETWEEN $first_date_app AND $r_time_app

      UNION ALL

      SELECT properties_session_id,
             'JUSTFAB' AS store_brand
      FROM lake.segment_gfb.java_jf_ecom_app_product_added
      WHERE receivedat::DATETIME BETWEEN $first_date_app AND $r_time_app

      UNION ALL

      SELECT properties_session_id,
             'JUSTFAB'                            AS store_brand
      FROM lake.segment_gfb.java_jf_ecom_app_order_completed
      WHERE receivedat::DATETIME BETWEEN $first_date_app AND $r_time_app) AS a
WHERE session_id IS NOT NULL;


CREATE OR REPLACE TEMPORARY TABLE _user_app AS
SELECT store_brand,
       session_id,
       MAX(user_id) AS user_id
FROM (SELECT TRY_TO_NUMBER(properties_session_id) AS session_id,
             TRY_TO_NUMBER(userid)                AS user_id,
             'JUSTFAB'                            AS store_brand
      FROM lake.segment_gfb.react_native_justfab_product_list_viewed
      WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
        AND user_id IS NOT NULL

      UNION ALL

      SELECT TRY_TO_NUMBER(properties_session_id)                   AS session_id,
             TRY_TO_NUMBER(COALESCE(userid, context_traits_userid)) AS user_id,
             'JUSTFAB'                                              AS store_brand
      FROM lake.segment_gfb.react_native_justfab_product_viewed
      WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
        AND user_id IS NOT NULL

      UNION ALL

      SELECT TRY_TO_NUMBER(properties_session_id)                    AS session_id,
             TRY_TO_NUMBER(COALESCE(userid, properties_customer_id)) AS user_id,
             'JUSTFAB'                                               AS store_brand
      FROM lake.segment_gfb.java_jf_ecom_app_product_added
      WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
        AND user_id IS NOT NULL

      UNION ALL

      SELECT TRY_TO_NUMBER(properties_session_id)                    AS session_id,
             TRY_TO_NUMBER(COALESCE(userid, properties_customer_id)) AS user_id,
             'JUSTFAB'                                               AS store_brand
      FROM lake.segment_gfb.java_jf_ecom_app_order_completed
      WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
        AND user_id IS NOT NULL

      UNION ALL

      SELECT TRY_TO_NUMBER(properties_session_id)                   AS session_id,
             TRY_TO_NUMBER(COALESCE(userid, context_traits_userid)) AS user_id,
             'JUSTFAB'                                              AS store_brand
      FROM lake.segment_gfb.react_native_justfab_screen
      WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
        AND user_id IS NOT NULL)
GROUP BY store_brand,
         session_id;

CREATE OR REPLACE TEMPORARY TABLE _identify_app AS
SELECT
    TRY_TO_NUMBER(ji.userid)                     AS user_id,
    'JUSTFAB'                                    AS store_brand,
    CASE
        WHEN LOWER(ji.traits_membership_status) LIKE 'elite' THEN 'vip'
        WHEN LOWER(ji.traits_membership_status) LIKE 'visitor' THEN 'prospect'
        WHEN LOWER(ji.traits_membership_status) LIKE '%downgrade%' THEN 'cancelled'
        ELSE LOWER(ji.traits_membership_status) END AS membership_status,
    IFF(ji.traits_email ILIKE ANY('%@test.com','%@example.com'), 'test','') AS test_account,
    TO_TIMESTAMP(ji.timestamp)                      AS memb_start,
    TO_TIMESTAMP(COALESCE(LEAD(ji.timestamp) OVER (PARTITION BY ji.userid ORDER BY ji.timestamp ASC),'9999-12-31')) AS memb_end
FROM lake.segment_gfb.react_native_justfab_identify ji
WHERE ji.userid != '0'
  AND ji.userid NOT LIKE '%-%'
  AND test_account != 'test'
  AND membership_status IS NOT NULL
  AND TRY_TO_NUMBER(ji.userid) IN (SELECT user_id FROM _user_app);


CREATE OR REPLACE TEMPORARY TABLE _usi_app AS
SELECT
    TRY_TO_NUMBER(properties_session_id)                                                AS session_id,
    TRY_TO_NUMBER(COALESCE(userid, context_traits_userid))                              AS user_id,
    'JUSTFAB'                                                                           AS store_brand,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',TO_TIMESTAMP(timestamp))::DATE        AS session_start_date,
    timestamp,
    country
FROM lake.segment_gfb.react_native_justfab_screen
WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(properties_session_id) ORDER BY timestamp) = 1;


CREATE OR REPLACE TEMPORARY TABLE _activation AS
SELECT DISTINCT session_id,
                store_brand,
                COALESCE(UPPER(properties_is_activating), FALSE)::BOOLEAN AS event
FROM(
        SELECT
            TRY_TO_NUMBER(properties_session_id) AS session_id,
            'JUSTFAB'                            AS store_brand,
            properties_is_activating
        FROM lake.segment_gfb.java_justfab_order_completed v
        WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'JUSTFAB')
            AND receivedat::DATETIME <= $r_time
            AND properties_is_activating = TRUE

        UNION ALL

        SELECT
            TRY_TO_NUMBER(properties_session_id) AS session_id,
            'JUSTFAB'                            AS store_brand,
            properties_is_activating
        FROM lake.segment_gfb.java_jf_ecom_app_order_completed v
        WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
            AND  receivedat::DATETIME <= $r_time_app
            AND properties_is_activating = TRUE

        UNION ALL

        SELECT
            TRY_TO_NUMBER(properties_session_id) AS session_id,
            'SHOEDAZZLE'                         AS store_brand,
            properties_is_activating
        FROM lake.segment_gfb.java_shoedazzle_order_completed v
        WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'SHOEDAZZLE')
            AND receivedat::DATETIME <= $r_time
            AND properties_is_activating = TRUE

        UNION ALL

        SELECT
            TRY_TO_NUMBER(properties_session_id)    AS session_id,
            'FABKIDS'                               AS store_brand,
            properties_is_activating
        FROM lake.segment_gfb.java_fabkids_order_completed v
        WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'FABKIDS')
            AND receivedat::DATETIME <= $r_time
            AND properties_is_activating = TRUE
);


CREATE OR REPLACE TEMPORARY TABLE _registration AS
SELECT DISTINCT
    session_id,
    store_brand,
    registration_source
FROM(
        SELECT
            TRY_TO_NUMBER(properties_session_id) AS session_id,
            'JUSTFAB'                            AS store_brand,
            properties_source                    AS registration_source
        FROM lake.segment_gfb.java_justfab_complete_registration
        WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'JUSTFAB')
            AND receivedat::DATETIME <= $r_time

        UNION ALL

        SELECT
            TRY_TO_NUMBER(properties_session_id) AS session_id,
            'SHOEDAZZLE'                         AS store_brand,
            properties_source                    AS registration_source
        FROM lake.segment_gfb.java_shoedazzle_complete_registration
        WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'SHOEDAZZLE')
            AND receivedat::DATETIME <= $r_time

        UNION ALL

        SELECT
            TRY_TO_NUMBER(properties_session_id) AS session_id,
            'FABKIDS'                            AS store_brand,
            properties_source                    AS registration_source
        FROM lake.segment_gfb.java_fabkids_complete_registration
        WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'FABKIDS')
            AND receivedat::DATETIME <= $r_time
  );

CREATE OR REPLACE TEMP TABLE _javascript_justfab_product_viewed AS
SELECT properties_is_bundle,
       TRY_TO_NUMBER(pv.properties_session_id)            AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_list, CONCAT(':', SPLIT_PART(properties_list, ':', -1)), ''),
                        properties_list), ''))            AS psource,
       IFNULL(properties_product_id, '0')::VARCHAR        AS product_id,
       IFNULL(properties_bundle_product_id, '0')::VARCHAR AS bundle_product_id,
       receivedat::DATETIME                               AS receivedat,
       userid,
       properties_customer_id,
       properties_automated_test
FROM lake.segment_gfb.javascript_justfab_product_viewed pv
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'JUSTFAB')
  AND receivedat::DATETIME <= $r_time;

CREATE OR REPLACE TEMP TABLE _javascript_shoedazzle_product_viewed AS
SELECT properties_is_bundle,
       TRY_TO_NUMBER(pv.properties_session_id)            AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_list, CONCAT(':', SPLIT_PART(properties_list, ':', -1)), ''),
                        properties_list), ''))            AS psource,
       IFNULL(properties_product_id, '0')::VARCHAR        AS product_id,
       IFNULL(properties_bundle_product_id, '0')::VARCHAR AS bundle_product_id,
       receivedat::DATETIME                               AS receivedat,
       userid,
       properties_customer_id,
       properties_automated_test
FROM lake.segment_gfb.javascript_shoedazzle_product_viewed pv
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'SHOEDAZZLE')
  AND receivedat::DATETIME <= $r_time;

CREATE OR REPLACE TEMP TABLE _javascript_fabkids_product_viewed AS
SELECT properties_is_bundle,
       TRY_TO_NUMBER(pv.properties_session_id)            AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_list, CONCAT(':', SPLIT_PART(properties_list, ':', -1)), ''),
                        properties_list), ''))            AS psource,
       IFNULL(properties_product_id, '0')::VARCHAR        AS product_id,
       IFNULL(properties_bundle_product_id, '0')::VARCHAR AS bundle_product_id,
       receivedat::DATETIME                               AS receivedat,
       userid,
       properties_customer_id,
       properties_automated_test
FROM lake.segment_gfb.javascript_fabkids_product_viewed pv
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'FABKIDS')
  AND receivedat::DATETIME <= $r_time;


CREATE OR REPLACE TEMPORARY TABLE _pv AS
SELECT IFF(pv.properties_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'JUSTFAB' AS store_brand,
       'Desktop & Mobile Web' AS platform,
       pv.session_id AS session_id,
       pv.psource,
       pv.product_id,
       COUNT(DISTINCT pv.product_id) AS total_views
FROM _javascript_justfab_product_viewed pv
WHERE product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         product_id
UNION ALL

SELECT 'Bundle' AS product_type,
       'JUSTFAB' AS store_brand,
       'Desktop & Mobile Web' AS platform,
       pv.session_id,
       pv.psource,
       pv.bundle_product_id AS product_id,
       COUNT(DISTINCT pv.bundle_product_id) AS total_views
FROM _javascript_justfab_product_viewed pv
WHERE pv.properties_is_bundle
  AND bundle_product_id <> '0'
  AND properties_automated_test = FALSE
  AND pv.receivedat  BETWEEN $first_date AND $r_time
GROUP BY product_type,
         session_id,
         psource,
         bundle_product_id
UNION ALL
SELECT IFF(properties_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'JUSTFAB'                                                    AS store_brand,
       'Mobile App'                                                 AS platform,
       TRY_TO_NUMBER(pv.properties_session_id)                      AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list_id,':',-1) in ('newarrivals','bestsellers','pricel2h','priceh2l','fplasc','toprated')),REPLACE(properties_list_id,CONCAT(':',SPLIT_PART(properties_list_id,':',-1)),''),properties_list_id), '')) AS psource,
       IFNULL(properties_product_id, '0')::VARCHAR                  AS product_id,
       COUNT(DISTINCT product_id)                                   AS total_views
FROM lake.segment_gfb.react_native_justfab_product_viewed pv
WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
  AND receivedat::DATETIME  <= $r_time_app
  AND properties_automated_test = FALSE
  AND product_id <> '0'
GROUP BY product_type,
         platform,
         session_id,
         psource,
         product_id

UNION ALL

SELECT IFF(properties_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'SHOEDAZZLE'                                                 AS store_brand,
       'Desktop & Mobile Web'                                       AS platform,
       pv.session_id,
       pv.psource,
       pv.product_id,
       COUNT(DISTINCT pv.product_id)                                AS total_views
FROM _javascript_shoedazzle_product_viewed pv
WHERE product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         platform,
         session_id,
         psource,
         product_id

UNION ALL

SELECT 'Bundle' AS product_type,
       'SHOEDAZZLE' AS store_brand,
       'Desktop & Mobile Web' AS platform,
       pv.session_id,
       pv.psource,
       pv.bundle_product_id AS product_id,
       COUNT(DISTINCT pv.bundle_product_id) AS total_views
FROM _javascript_shoedazzle_product_viewed pv
WHERE properties_is_bundle
  AND bundle_product_id <> '0'
  AND properties_automated_test = FALSE
  AND receivedat  BETWEEN $first_date AND $r_time
GROUP BY product_type,
         session_id,
         psource,
         pv.bundle_product_id

UNION ALL

SELECT IFF(pv.properties_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'FABKIDS'                                                    AS store_brand,
       'Desktop & Mobile Web'                                       AS platform,
       pv.session_id,
       pv.psource,
       pv.product_id,
       COUNT(DISTINCT pv.product_id)                                AS total_views
FROM _javascript_fabkids_product_viewed pv
WHERE product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         product_id
UNION ALL

SELECT 'Bundle'                                                    AS product_type,
       'FABKIDS'                                                   AS store_brand,
       'Desktop & Mobile Web'                                      AS platform,
       pv.session_id,
       pv.psource,
       pv.bundle_product_id AS product_id,
       COUNT(DISTINCT pv.bundle_product_id)                        AS total_views
FROM _javascript_fabkids_product_viewed pv
WHERE properties_is_bundle
  AND bundle_product_id <> '0'
  AND properties_automated_test = FALSE
  AND receivedat BETWEEN $first_date AND $r_time
GROUP BY product_type,
         session_id,
         psource,
         pv.bundle_product_id;

CREATE OR REPLACE TEMP TABLE _java_justfab_product_added AS
SELECT properties_is_bundle,
       TRY_TO_NUMBER(properties_session_id)               AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list_id, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_list_id, CONCAT(':', SPLIT_PART(properties_list_id, ':', -1)), ''),
                        properties_list_id), ''))         AS psource,
       IFNULL(properties_product_id, '0')::VARCHAR        AS product_id,
       IFNULL(properties_bundle_product_id, '0')::VARCHAR AS bundle_product_id,
       properties_automated_test,
       userid,
       properties_customer_id
FROM lake.segment_gfb.java_justfab_product_added AS pa
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'JUSTFAB')
  AND receivedat::DATETIME <= $r_time;

CREATE OR REPLACE TEMP TABLE _java_shoedazzle_product_added AS
SELECT properties_is_bundle,
       TRY_TO_NUMBER(properties_session_id)               AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list_id, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_list_id, CONCAT(':', SPLIT_PART(properties_list_id, ':', -1)), ''),
                        properties_list_id), ''))         AS psource,
       IFNULL(properties_product_id, '0')::VARCHAR        AS product_id,
       IFNULL(properties_bundle_product_id, '0')::VARCHAR AS bundle_product_id,
       properties_automated_test,
       userid,
       properties_customer_id
FROM lake.segment_gfb.java_shoedazzle_product_added AS pa
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'SHOEDAZZLE')
  AND receivedat::DATETIME <= $r_time;

CREATE OR REPLACE TEMP TABLE _java_fabkids_product_added AS
SELECT properties_is_bundle,
       TRY_TO_NUMBER(properties_session_id)               AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list_id, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_list_id, CONCAT(':', SPLIT_PART(properties_list_id, ':', -1)), ''),
                        properties_list_id), ''))         AS psource,
       IFNULL(properties_product_id, '0')::VARCHAR        AS product_id,
       IFNULL(properties_bundle_product_id, '0')::VARCHAR AS bundle_product_id,
       properties_automated_test,
       userid,
       properties_customer_id
FROM lake.segment_gfb.java_fabkids_product_added AS pa
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'FABKIDS')
  AND receivedat::DATETIME <= $r_time;

CREATE OR REPLACE TEMPORARY TABLE _pa AS
SELECT IFF(properties_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'JUSTFAB'                                                    AS store_brand,
       'Desktop & Mobile Web'                                       AS platform,
       pa.session_id,
       pa.psource,
       pa.product_id,
       COUNT(DISTINCT pa.product_id)                                AS product_adds
FROM _java_justfab_product_added AS pa
WHERE product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         product_id
UNION ALL
SELECT 'Bundle'                                                    AS product_type,
       'JUSTFAB'                                                   AS store_brand,
       'Desktop & Mobile Web'                                      AS platform,
       pa.session_id,
       pa.psource,
       pa.bundle_product_id AS product_id,
       COUNT(DISTINCT pa.bundle_product_id)                        AS product_adds
FROM _java_justfab_product_added AS pa
WHERE properties_is_bundle
AND bundle_product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         bundle_product_id

UNION ALL

SELECT IFF(properties_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'JUSTFAB'                                                    AS store_brand,
       'Mobile App'                                                 AS platform,
       TRY_TO_NUMBER(properties_session_id)                         AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list_id,':',-1) in ('newarrivals','bestsellers','pricel2h','priceh2l','fplasc','toprated')),REPLACE(properties_list_id,CONCAT(':',SPLIT_PART(properties_list_id,':',-1)),''),properties_list_id), '')) AS psource,
       IFNULL(properties_product_id, '0')::VARCHAR                  AS product_id,
       COUNT(DISTINCT product_id)                                   AS product_adds
FROM lake.segment_gfb.java_jf_ecom_app_product_added AS pa
WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
  AND receivedat::DATETIME <= $r_time_app
  AND properties_automated_test = FALSE
  AND product_id <> '0'
GROUP BY product_type,
         platform,
         session_id,
         psource,
         product_id

UNION ALL

SELECT IFF(properties_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'SHOEDAZZLE'                                                    AS store_brand,
       'Desktop & Mobile Web'                                       AS platform,
       pa.session_id,
       pa.psource,
       pa.product_id,
       COUNT(DISTINCT pa.product_id)                                AS product_adds
FROM _java_shoedazzle_product_added AS pa
WHERE product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         product_id

UNION ALL

SELECT 'Bundle'                                                    AS product_type,
       'SHOEDAZZLE'                                                   AS store_brand,
       'Desktop & Mobile Web'                                      AS platform,
       pa.session_id,
       pa.psource,
       pa.bundle_product_id AS product_id,
       COUNT(DISTINCT pa.bundle_product_id)                        AS product_adds
FROM _java_shoedazzle_product_added AS pa
WHERE properties_is_bundle
AND properties_automated_test = FALSE
AND bundle_product_id <> '0'
GROUP BY product_type,
         session_id,
         psource,
         bundle_product_id

UNION ALL

SELECT IFF(properties_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'FABKIDS'                                                    AS store_brand,
       'Desktop & Mobile Web'                                       AS platform,
       pa.session_id,
       pa.psource,
       pa.product_id,
       COUNT(DISTINCT pa.product_id)                                AS product_adds
FROM _java_fabkids_product_added AS pa
WHERE product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         product_id

UNION ALL

SELECT 'Bundle'                                                    AS product_type,
       'FABKIDS'                                                   AS store_brand,
       'Desktop & Mobile Web'                                      AS platform,
       pa.session_id,
       pa.psource,
       pa.bundle_product_id AS product_id,
       COUNT(DISTINCT pa.bundle_product_id)                        AS product_adds
FROM _java_fabkids_product_added AS pa
WHERE properties_is_bundle
AND bundle_product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         bundle_product_id;

CREATE OR REPLACE TEMP TABLE _java_justfab_order_completed AS
SELECT properties_products_is_bundle,
       TRY_TO_NUMBER(properties_session_id)                           AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_products_list_id, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_products_list_id,
                                CONCAT(':', SPLIT_PART(properties_products_list_id, ':', -1)), ''),
                        properties_products_list_id), ''))            AS psource,
       IFNULL(so.properties_products_product_id, '0')::VARCHAR        AS product_id,
       IFNULL(so.properties_products_bundle_product_id, '0')::VARCHAR AS products_bundle_product_id,
       properties_automated_test,
       userid,
       properties_customer_id
FROM lake.segment_gfb.java_justfab_order_completed AS so
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'JUSTFAB')
  AND receivedat::DATETIME <= $r_time;


CREATE OR REPLACE TEMP TABLE _java_shoedazzle_order_completed AS
SELECT properties_products_is_bundle,
       TRY_TO_NUMBER(properties_session_id)                           AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_products_list_id, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_products_list_id,
                                CONCAT(':', SPLIT_PART(properties_products_list_id, ':', -1)), ''),
                        properties_products_list_id), ''))            AS psource,
       IFNULL(so.properties_products_product_id, '0')::VARCHAR        AS product_id,
       IFNULL(so.properties_products_bundle_product_id, '0')::VARCHAR AS products_bundle_product_id,
       properties_automated_test,
       userid,
       properties_customer_id
FROM lake.segment_gfb.java_shoedazzle_order_completed AS so
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'SHOEDAZZLE')
  AND receivedat::DATETIME <= $r_time;


CREATE OR REPLACE TEMP TABLE _java_fabkids_order_completed AS
SELECT properties_products_is_bundle,
       TRY_TO_NUMBER(properties_session_id)                           AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_products_list_id, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_products_list_id,
                                CONCAT(':', SPLIT_PART(properties_products_list_id, ':', -1)), ''),
                        properties_products_list_id), ''))            AS psource,
       IFNULL(so.properties_products_product_id, '0')::VARCHAR        AS product_id,
       IFNULL(so.properties_products_bundle_product_id, '0')::VARCHAR AS products_bundle_product_id,
       properties_automated_test,
       userid,
       properties_customer_id
FROM lake.segment_gfb.java_fabkids_order_completed AS so
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'FABKIDS')
  AND receivedat::DATETIME <= $r_time;

CREATE OR REPLACE TEMPORARY TABLE _oc AS
SELECT IFF(properties_products_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'JUSTFAB'                                                             AS store_brand,
       'Desktop & Mobile Web'                                                AS platform,
       so.session_id,
       so.psource,
       so.product_id,
       COUNT(DISTINCT IFNULL(IFF(so.product_id = '', 0, so.product_id), 0))  AS countproducts
FROM _java_justfab_order_completed AS so
WHERE product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         product_id

UNION ALL

SELECT 'Bundle'                                                              AS product_type,
       'JUSTFAB'                                                             AS store_brand,
       'Desktop & Mobile Web'                                                AS platform,
       so.session_id,
       so.psource,
       so.products_bundle_product_id AS product_id,
       COUNT(DISTINCT IFNULL(IFF(so.products_bundle_product_id = '', 0, so.products_bundle_product_id), 0))  AS countproducts
FROM _java_justfab_order_completed AS so
WHERE properties_products_is_bundle
AND products_bundle_product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         products_bundle_product_id

UNION ALL

SELECT IFF(properties_products_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'JUSTFAB'                                                             AS store_brand,
       'Mobile App'                                                          AS platform,
       TRY_TO_NUMBER(properties_session_id)                                  AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_products_list_id,':',-1) in ('newarrivals','bestsellers','pricel2h','priceh2l','fplasc','toprated')),REPLACE(properties_products_list_id,CONCAT(':',SPLIT_PART(properties_products_list_id,':',-1)),''),properties_products_list_id), '')) AS psource,
       IFNULL(so.properties_products_product_id, '0')::VARCHAR               AS product_id,
       COUNT(DISTINCT IFNULL(IFF(product_id = '', 0, product_id), 0))        AS countproducts
FROM lake.segment_gfb.java_jf_ecom_app_order_completed AS so
WHERE session_id IN (SELECT session_id FROM _session_base_app WHERE store_brand = 'JUSTFAB')
  AND receivedat::DATETIME <=  $r_time_app
  AND properties_automated_test = FALSE
  AND product_id <> '0'
GROUP BY product_type,
         session_id,
         psource,
         product_id

UNION ALL

SELECT IFF(properties_products_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'SHOEDAZZLE'                                                          AS store_brand,
       'Desktop & Mobile Web'                                                AS platform,
       so.session_id,
       so.psource,
       so.product_id,
       COUNT(DISTINCT IFNULL(IFF(so.product_id = '', 0, so.product_id), 0))  AS countproducts
FROM _java_shoedazzle_order_completed AS so
WHERE product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         product_id

UNION ALL

SELECT 'Bundle'                                                              AS product_type,
       'SHOEDAZZLE'                                                          AS store_brand,
       'Desktop & Mobile Web'                                                AS platform,
       so.session_id,
       so.psource,
       so.products_bundle_product_id AS product_id,
       COUNT(DISTINCT IFNULL(IFF(so.products_bundle_product_id = '', 0, so.products_bundle_product_id), 0))  AS countproducts
FROM _java_shoedazzle_order_completed AS so
WHERE properties_products_is_bundle
AND products_bundle_product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         products_bundle_product_id

UNION ALL

SELECT IFF(properties_products_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'FABKIDS'                                                             AS store_brand,
       'Desktop & Mobile Web'                                                AS platform,
       so.session_id,
       so.psource,
       so.product_id,
       COUNT(DISTINCT IFNULL(IFF(so.product_id = '', 0, so.product_id), 0))  AS countproducts
FROM _java_fabkids_order_completed AS so
WHERE product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         product_id

UNION ALL

SELECT 'Bundle'                                                              AS product_type,
       'FABKIDS'                                                             AS store_brand,
       'Desktop & Mobile Web'                                                AS platform,
       so.session_id,
       so.psource,
       so.products_bundle_product_id AS product_id,
       COUNT(DISTINCT IFNULL(IFF(so.products_bundle_product_id = '', 0, so.products_bundle_product_id), 0))  AS countproducts
FROM _java_fabkids_order_completed AS so
WHERE properties_products_is_bundle
AND products_bundle_product_id <> '0'
AND properties_automated_test = FALSE
GROUP BY product_type,
         session_id,
         psource,
         products_bundle_product_id;


CREATE OR REPLACE TEMP TABLE _javascript_justfab_product_list_viewed AS
SELECT properties_products_is_bundle,
       TRY_TO_NUMBER(properties_session_id)                                       AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list_id, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_list_id, CONCAT(':', SPLIT_PART(properties_list_id, ':', -1)), ''),
                        properties_list_id), ''))                                 AS psource,
       IFNULL(TRY_CAST(plv.properties_products_product_id AS BIGINT), 0)::VARCHAR AS product_id,
       IFNULL(plv.properties_products_bundle_product_id, '0')::VARCHAR            AS products_bundle_product_id,
       userid,
       properties_automated_test
FROM lake.segment_gfb.javascript_justfab_product_list_viewed AS plv
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'JUSTFAB')
  AND receivedat::DATETIME <= $r_time;

CREATE OR REPLACE TEMP TABLE _javascript_shoedazzle_product_list_viewed AS
SELECT properties_products_is_bundle,
       TRY_TO_NUMBER(properties_session_id)                                       AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list_id, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_list_id, CONCAT(':', SPLIT_PART(properties_list_id, ':', -1)), ''),
                        properties_list_id), ''))                                 AS psource,
       IFNULL(TRY_CAST(plv.properties_products_product_id AS BIGINT), 0)::VARCHAR AS product_id,
       IFNULL(plv.properties_products_bundle_product_id, '0')::VARCHAR            AS products_bundle_product_id,
       userid::VARCHAR                                                            AS userid,
       properties_automated_test
FROM lake.segment_gfb.javascript_shoedazzle_product_list_viewed AS plv
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'SHOEDAZZLE')
  AND receivedat::DATETIME <= $r_time;


CREATE OR REPLACE TEMP TABLE _javascript_fabkids_product_list_viewed AS
SELECT properties_products_is_bundle,
       TRY_TO_NUMBER(properties_session_id)                                       AS session_id,
       LOWER(IFNULL(IFF((SPLIT_PART(properties_list_id, ':', -1) IN
                         ('newarrivals', 'bestsellers', 'pricel2h', 'priceh2l', 'fplasc', 'toprated')),
                        REPLACE(properties_list_id, CONCAT(':', SPLIT_PART(properties_list_id, ':', -1)), ''),
                        properties_list_id), ''))                                 AS psource,
       IFNULL(TRY_CAST(plv.properties_products_product_id AS BIGINT), 0)::VARCHAR AS product_id,
       IFNULL(plv.properties_products_bundle_product_id, '0')::VARCHAR            AS products_bundle_product_id,
       userid,
       properties_automated_test
FROM lake.segment_gfb.javascript_fabkids_product_list_viewed AS plv
WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'FABKIDS')
  AND receivedat::DATETIME <= $r_time;


CREATE OR REPLACE TEMPORARY TABLE _plv AS
SELECT IFF(properties_products_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'JUSTFAB'                                                             AS store_brand,
       'Desktop & Mobile Web'                                                AS platform,
       plv.session_id,
       plv.psource,
       plv.product_id,
       COUNT(DISTINCT plv.product_id)                                        AS list_views
FROM _javascript_justfab_product_list_viewed plv
WHERE product_id <> '0'
AND properties_automated_test = FALSE
AND plv.product_id <> ''
GROUP BY product_type,
         session_id,
         psource,
         product_id

UNION ALL

SELECT 'Bundle'                                                                 AS product_type,
       'JUSTFAB'                                                                AS store_brand,
       'Desktop & Mobile Web'                                                   AS platform,
       plv.session_id,
       plv.psource,
       plv.products_bundle_product_id AS product_id,
       COUNT(DISTINCT plv.products_bundle_product_id)                           AS list_views
FROM _javascript_justfab_product_list_viewed AS plv
WHERE properties_products_is_bundle
AND products_bundle_product_id <> '0'
AND properties_automated_test = FALSE
AND plv.product_id <> ''
GROUP BY product_type,
         session_id,
         psource,
         products_bundle_product_id

UNION ALL

SELECT IFF(properties_products_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'SHOEDAZZLE'                                                          AS store_brand,
       'Desktop & Mobile Web'                                                AS platform,
       plv.session_id,
       plv.psource,
       plv.product_id,
       COUNT(DISTINCT plv.product_id)                                        AS list_views
FROM _javascript_shoedazzle_product_list_viewed plv
WHERE product_id <> '0'
AND properties_automated_test = FALSE
AND plv.product_id <> ''
GROUP BY product_type,
         session_id,
         psource,
         product_id

UNION ALL

SELECT 'Bundle'                                                                 AS product_type,
       'SHOEDAZZLE'                                                                AS store_brand,
       'Desktop & Mobile Web'                                                   AS platform,
       plv.session_id,
       plv.psource,
       plv.products_bundle_product_id AS product_id,
       COUNT(DISTINCT plv.products_bundle_product_id)                           AS list_views
FROM _javascript_shoedazzle_product_list_viewed AS plv
WHERE properties_products_is_bundle
AND products_bundle_product_id <> '0'
AND properties_automated_test = FALSE
AND plv.product_id <> ''
GROUP BY product_type,
         session_id,
         psource,
         products_bundle_product_id
UNION ALL
SELECT IFF(properties_products_is_bundle, 'Bundle Component', 'Pieced Good') AS product_type,
       'FABKIDS'                                                          AS store_brand,
       'Desktop & Mobile Web'                                                AS platform,
       plv.session_id,
       plv.psource,
       plv.product_id,
       COUNT(DISTINCT plv.product_id)                                        AS list_views
FROM _javascript_fabkids_product_list_viewed plv
WHERE product_id <> '0'
AND properties_automated_test = FALSE
AND plv.product_id <> ''
GROUP BY product_type,
         session_id,
         psource,
         product_id

UNION ALL

SELECT 'Bundle'                                                                 AS product_type,
       'FABKIDS'                                                                AS store_brand,
       'Desktop & Mobile Web'                                                   AS platform,
       plv.session_id,
       plv.psource,
       plv.products_bundle_product_id AS product_id,
       COUNT(DISTINCT plv.products_bundle_product_id)                           AS list_views
FROM _javascript_fabkids_product_list_viewed AS plv
WHERE properties_products_is_bundle
AND products_bundle_product_id <> '0'
AND properties_automated_test = FALSE
AND plv.product_id <> ''
GROUP BY product_type,
         session_id,
         psource,
         products_bundle_product_id;

CREATE OR REPLACE TEMPORARY TABLE _action AS
SELECT product_type,
       store_brand,
       platform,
       session_id,
       psource,
       product_id                 AS product_id,
       SUM(IFNULL(list_views, 0)) AS list_view_total,
       NULL                       AS pdp_view_total,
       NULL                       AS pa_total,
       NULL                       AS oc_total
FROM _plv
GROUP BY product_type,
         store_brand,
         platform,
         session_id,
         psource,
         product_id

UNION ALL

SELECT product_type,
       store_brand,
       platform,
       session_id,
       psource,
       product_id                  AS product_id,
       NULL                        AS list_view_total,
       SUM(IFNULL(total_views, 0)) AS pdp_view_total,
       NULL                        AS pa_total,
       NULL                        AS oc_total
FROM _pv
GROUP BY product_type,
         store_brand,
         platform,
         session_id,
         psource,
         product_id

UNION ALL

SELECT product_type,
       store_brand,
       platform,
       session_id,
       psource,
       product_id                   AS product_id,
       NULL                         AS list_view_total,
       NULL                         AS pdp_view_total,
       SUM(IFNULL(product_adds, 0)) AS pa_total,
       NULL                         AS oc_total
FROM _pa
GROUP BY product_type,
         store_brand,
         platform,
         session_id,
         psource,
         product_id

UNION ALL

SELECT product_type,
       store_brand,
       platform,
       session_id,
       psource,
       product_id                    AS product_id,
       NULL                          AS list_view_total,
       NULL                          AS pdp_view_total,
       NULL                          AS pa_total,
       SUM(IFNULL(countproducts, 0)) AS oc_total
FROM _oc
GROUP BY product_type,
         store_brand,
         platform,
         session_id,
         psource,
         product_id;


CREATE OR REPLACE TEMPORARY TABLE _user AS
SELECT store_brand,
       session_id,
       MAX(user_id) AS user_id
FROM (SELECT session_id,
             TRY_TO_NUMBER(userid) AS user_id,
             'JUSTFAB'             AS store_brand
      FROM _javascript_justfab_product_list_viewed
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id) AS user_id,
             'JUSTFAB'                                                AS store_brand
      FROM _javascript_justfab_product_viewed
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             TRY_TO_NUMBER(COALESCE(userid, properties_customer_id)) AS user_id,
             'JUSTFAB'                                               AS store_brand
      FROM _java_justfab_product_added
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id) AS user_id,
             'JUSTFAB'                                                AS store_brand
      FROM _java_justfab_order_completed
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT TRY_TO_NUMBER(properties_session_id)                     AS session_id,
             COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id) AS user_id,
             'JUSTFAB'                                                AS store_brand
      FROM lake.segment_gfb.javascript_justfab_page
      WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'JUSTFAB')
        AND user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             TRY_TO_NUMBER(userid) AS user_id,
             'SHOEDAZZLE'          AS store_brand
      FROM _javascript_shoedazzle_product_list_viewed
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             TRY_TO_NUMBER(COALESCE(userid, properties_customer_id)) AS user_id,
             'SHOEDAZZLE'                                            AS store_brand
      FROM _javascript_shoedazzle_product_viewed
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             TRY_TO_NUMBER(COALESCE(userid, properties_customer_id)) AS user_id,
             'SHOEDAZZLE'                                            AS store_brand
      FROM _java_shoedazzle_product_added
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id) AS user_id,
             'SHOEDAZZLE'                                             AS store_brand
      FROM _java_shoedazzle_order_completed
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT TRY_TO_NUMBER(properties_session_id)                     AS session_id,
             COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id) AS user_id,
             'SHOEDAZZLE'                                             AS store_brand
      FROM lake.segment_gfb.javascript_shoedazzle_page
      WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'SHOEDAZZLE')
        AND user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             TRY_TO_NUMBER(userid) AS user_id,
             'FABKIDS'             AS store_brand
      FROM _javascript_fabkids_product_list_viewed
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id) AS user_id,
             'FABKIDS'                                                AS store_brand
      FROM _javascript_fabkids_product_viewed
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             TRY_TO_NUMBER(COALESCE(userid, properties_customer_id)) AS user_id,
             'FABKIDS'                                               AS store_brand
      FROM _java_fabkids_product_added
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT session_id,
             COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id) AS user_id,
             'FABKIDS'                                                AS store_brand
      FROM _java_fabkids_order_completed
      WHERE user_id IS NOT NULL

      UNION ALL

      SELECT TRY_TO_NUMBER(properties_session_id)                     AS session_id,
             COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id) AS user_id,
             'FABKIDS'                                                AS store_brand
      FROM lake.segment_gfb.javascript_fabkids_page
      WHERE session_id IN (SELECT session_id FROM _session_base WHERE store_brand = 'FABKIDS')
        AND user_id IS NOT NULL)
GROUP BY store_brand,
         session_id;


CREATE OR REPLACE TEMPORARY TABLE _identify AS
SELECT  TRY_TO_NUMBER(jji.userid)  AS user_id,
       'JUSTFAB' AS store_brand,
       CASE
           WHEN LOWER(jji.traits_membership_status) = 'elite' THEN 'vip'
           WHEN LOWER(jji.traits_membership_status) = 'visitor' THEN 'prospect'
           WHEN LOWER(jji.traits_membership_status) LIKE '%downgrade%' THEN 'cancelled'
           ELSE LOWER(jji.traits_membership_status) END AS membership_status,
       IFF(jji.traits_email ILIKE ANY('%@test.com', '%@example.com'), 'test','') AS test_account,
       TO_TIMESTAMP(jji.timestamp)                      AS memb_start,
       TO_TIMESTAMP(COALESCE(LEAD(jji.timestamp) OVER (PARTITION BY jji.userid ORDER BY jji.timestamp ASC),'9999-12-31')) AS memb_end
FROM lake.segment_gfb.javascript_justfab_identify jji
WHERE jji.userid != '0'
  AND jji.userid NOT LIKE '%-%'
  AND test_account != 'test'
  AND membership_status IS NOT NULL
  AND TRY_TO_NUMBER(jji.userid) IN (SELECT user_id FROM _user WHERE store_brand = 'JUSTFAB')

UNION ALL

SELECT  TRY_TO_NUMBER(jsi.userid)                   AS user_id,
       'SHOEDAZZLE'                                 AS store_brand,
       CASE
           WHEN LOWER(jsi.traits_membership_status) = 'elite' THEN 'vip'
           WHEN LOWER(jsi.traits_membership_status) = 'visitor' THEN 'prospect'
           WHEN LOWER(jsi.traits_membership_status) LIKE '%downgrade%' THEN 'cancelled'
           ELSE LOWER(jsi.traits_membership_status) END AS membership_status,
       IFF(jsi.traits_email ILIKE ANY('%@test.com','%@example.com'), 'test','') AS test_account,
       TO_TIMESTAMP(jsi.timestamp)                      AS memb_start,
       TO_TIMESTAMP(COALESCE(LEAD(jsi.timestamp) OVER (PARTITION BY jsi.userid ORDER BY jsi.timestamp ASC),'9999-12-31')) AS memb_end
FROM lake.segment_gfb.javascript_shoedazzle_identify jsi
WHERE jsi.userid != '0'
  AND jsi.userid NOT LIKE '%-%'
  AND test_account != 'test'
  AND membership_status IS NOT NULL
  AND TRY_TO_NUMBER(jsi.userid) IN (SELECT user_id FROM _user WHERE store_brand = 'SHOEDAZZLE')

UNION ALL

SELECT TRY_TO_NUMBER(jfi.userid)                    AS user_id,
       'FABKIDS'                                    AS store_brand,
       CASE
           WHEN LOWER(jfi.traits_membership_status) = 'elite' THEN 'vip'
           WHEN LOWER(jfi.traits_membership_status) = 'visitor' THEN 'prospect'
           WHEN LOWER(jfi.traits_membership_status) LIKE '%downgrade%' THEN 'cancelled'
           ELSE LOWER(jfi.traits_membership_status) END AS membership_status,
       IFF(jfi.traits_email ILIKE ANY('%@test.com','%@example.com'), 'test','') AS test_account,
       TO_TIMESTAMP(jfi.timestamp)                      AS memb_start,
       TO_TIMESTAMP(COALESCE(LEAD(jfi.timestamp) OVER (PARTITION BY jfi.userid ORDER BY jfi.timestamp ASC),'9999-12-31')) AS memb_end
FROM lake.segment_gfb.javascript_fabkids_identify jfi
WHERE  jfi.userid != '0'
    AND jfi.userid NOT LIKE '%-%'
    AND test_account != 'test'
    AND membership_status IS NOT NULL
    AND TRY_TO_NUMBER(jfi.userid) IN (SELECT user_id FROM _user WHERE store_brand = 'FABKIDS');


CREATE OR REPLACE TEMPORARY TABLE _s1 AS
SELECT
    _usi.session_id,
    u.user_id,
    u.store_brand,
    LOWER(COALESCE(i.membership_status, 'prospect')) AS membership_status,
    _usi.country,
    _usi.session_start_date
FROM _usi
LEFT JOIN _user AS u
    ON _usi.session_id = u.session_id
LEFT JOIN _identify AS i
    ON u.user_id = i.user_id
    AND TO_TIMESTAMP(_usi.timestamp) >= i.memb_start
    AND TO_TIMESTAMP(_usi.timestamp) < i.memb_end
    AND u.store_brand = i.store_brand
WHERE test_account != 'test'
UNION
SELECT
    _usi_app.session_id,
    u.user_id,
    u.store_brand,
    LOWER(COALESCE(i.membership_status, 'prospect')) AS membership_status,
    _usi_app.country,
    _usi_app.session_start_date
FROM _usi_app
LEFT JOIN _user_app AS u
    ON _usi_app.session_id = u.session_id
LEFT JOIN _identify_app AS i
    ON u.user_id = i.user_id
    AND TO_TIMESTAMP(_usi_app.timestamp) >= i.memb_start
    AND TO_TIMESTAMP(_usi_app.timestamp) < i.memb_end
    AND u.store_brand = i.store_brand
WHERE test_account != 'test';

BEGIN;

DELETE
FROM gfb.pcvr_session_historical
WHERE session_id IN (SELECT session_id
                     FROM _session_base
                     WHERE session_id IS NOT NULL);

INSERT INTO gfb.pcvr_session_historical(store_brand,
                                                       platform,
                                                       session_id,
                                                       session_start_date,
                                                       user_id,
                                                       membership_status,
                                                       is_activating,
                                                       registration_session,
                                                       registration_source,
                                                       product_type,
                                                       product_id,
                                                       psource,
                                                       country,
                                                       list_view_total,
                                                       pdp_view_total,
                                                       pa_total,
                                                       oc_total,
                                                       ss_pdp_view,
                                                       ss_product_added,
                                                       ss_product_ordered,
                                                       refresh_datetime,
                                                       max_received,
                                                       max_received_app)
SELECT s1.store_brand,
       r.platform,
       s1.session_id,
       s1.session_start_date::DATE                                  AS session_start_date,
       s1.user_id,
       s1.membership_status,
       COALESCE(a.event, FALSE)                                     AS is_activating,
       IFF(g.registration_source IS NULL, FALSE, TRUE)              AS registration_session,
       COALESCE(g.registration_source, '')                          AS registration_source,
       r.product_type,
       r.product_id::VARCHAR                                        AS product_id,
       r.psource,
       s1.country,
       SUM(IFNULL(r.list_view_total, 0))                            AS list_view_total,
       SUM(IFNULL(r.pdp_view_total, 0))                             AS pdp_view_total,
       SUM(IFNULL(r.pa_total, 0))                                   AS pa_total,
       SUM(IFNULL(r.oc_total, 0))                                   AS oc_total,
       IFF(SUM(IFNULL(r.list_view_total, 0)) > 0 AND SUM(IFNULL(r.pdp_view_total, 0)) > 0,
           SUM(IFNULL(r.pdp_view_total, 0)),
           0)                                                       AS ss_pdp_view,
       IFF(SUM(IFNULL(r.pdp_view_total, 0)) > 0 AND SUM(IFNULL(r.pa_total, 0)) > 0, SUM(IFNULL(r.pa_total, 0)),
           0)                                                       AS ss_product_added,
       IFF(SUM(IFNULL(r.pa_total, 0)) > 0 AND SUM(IFNULL(r.oc_total, 0)) > 0, SUM(IFNULL(r.oc_total, 0)),
           0)                                                       AS ss_product_ordered,
       $update_time::DATETIME                                       AS refresh_datetime,
       TO_VARCHAR(TO_TIMESTAMP(CONVERT_TIMEZONE( 'UTC', 'America/Los_Angeles', $r_time)), 'yyyy-mm-dd hh:mi:ss')    AS max_received,
       TO_VARCHAR(TO_TIMESTAMP(CONVERT_TIMEZONE( 'UTC', 'America/Los_Angeles', $r_time_app)), 'yyyy-mm-dd hh:mi:ss')   AS max_received_app

FROM _s1 AS s1
     LEFT JOIN _action AS r
               ON s1.session_id::VARCHAR = r.session_id::VARCHAR
     LEFT JOIN _activation AS a
               ON s1.session_id::VARCHAR = a.session_id::VARCHAR
     LEFT JOIN _registration g
               ON s1.session_id::VARCHAR = g.session_id::VARCHAR
GROUP BY s1.store_brand,
         platform,
         s1.session_id,
         s1.session_start_date,
         s1.user_id,
         s1.membership_status,
         is_activating,
         registration_session,
         registration_source,
         r.product_type,
         r.product_id::VARCHAR,
         r.psource,
         s1.country
ORDER BY session_id ASC, product_id ASC;

COMMIT;
