set first_date = (select coalesce(dateadd('day', -2, max(max_received)), '2021-08-01')::datetime
    from reporting_prod.fabletics.pcvr_session_historical
     where platform != 'Mobile App');

set update_time = current_timestamp();

SET r_time = (
    SELECT MIN(t.rtime)
    FROM (
        SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp)))::DATETIME) AS rtime
        FROM lake.segment_fl.javascript_fabletics_product_list_viewed

        UNION ALL

        SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp)))::DATETIME) AS rtime
        FROM lake.segment_fl.javascript_fabletics_product_viewed

        UNION ALL

        SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp)))::DATETIME) AS rtime
        FROM lake.segment_fl.java_fabletics_product_added

        UNION ALL

        SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp)))::DATETIME) AS rtime
        FROM lake.segment_fl.java_fabletics_order_completed
    ) t
);


SET first_date_app = (
    SELECT COALESCE(DATEADD('day', -2, MAX(max_received)), '2023-03-01')::DATETIME
    FROM reporting_prod.fabletics.pcvr_session_historical
    where platform = 'Mobile App'
);

SET r_time_app = (
    SELECT MIN(t.rtime)
    FROM (
        SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp)))::DATETIME) AS rtime
        FROM lake.segment_fl.react_native_fabletics_product_list_viewed

        UNION ALL

        SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp)))::DATETIME) AS rtime
        FROM lake.segment_fl.react_native_fabletics_product_viewed

        UNION ALL

        SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp)))::DATETIME) AS rtime
        FROM lake.segment_fl.JAVA_FABLETICS_ECOM_MOBILE_APP_PRODUCT_ADDED

        UNION ALL

        SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp)))::DATETIME) AS rtime
        FROM lake.segment_fl.JAVA_FABLETICS_ECOM_MOBILE_APP_ORDER_COMPLETED
    ) t
);
/***********************************************************************************************************************
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                Desktop query begin
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
***********************************************************************************************************************/
CREATE OR REPLACE TEMPORARY TABLE _session_base AS
SELECT DISTINCT session_id
FROM (
    SELECT DISTINCT try_to_number(properties_session_id::varchar) AS session_id
    FROM lake.segment_fl.javascript_fabletics_product_list_viewed
    WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp))) >= $first_date
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp))) <= $r_time

    UNION ALL

    SELECT DISTINCT try_to_number(properties_session_id::varchar) AS session_id
    FROM lake.segment_fl.javascript_fabletics_product_viewed
    WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp))) >= $first_date
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp))) <= $r_time

    UNION ALL

    SELECT DISTINCT try_to_number(properties_session_id::varchar) AS session_id
    FROM lake.segment_fl.java_fabletics_product_added
    WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp))) >= $first_date
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp))) <= $r_time

    UNION ALL

    SELECT DISTINCT try_to_number(properties_session_id::varchar) AS session_id
    FROM lake.segment_fl.java_fabletics_order_completed
    WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp))) >= $first_date
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(coalesce(meta_create_datetime,timestamp))) <= $r_time
     ) AS a
WHERE session_id IS NOT NULL
ORDER BY session_id ASC;

CREATE OR REPLACE TEMPORARY TABLE _identify AS
SELECT try_to_number(userid::varchar) AS user_id,
    CASE WHEN LOWER(traits_membership_status) LIKE 'elite' THEN 'vip'
        WHEN LOWER(traits_membership_status) LIKE 'visitor' THEN 'prospect'
        WHEN LOWER(traits_membership_status) LIKE '%downgrade%' THEN 'cancelled'
    ELSE LOWER(traits_membership_status) END AS membership_status,
   -- IFF(traits_email ILIKE '%@test.com' OR traits_email ILIKE '%@example.com', 'test', '') AS test_account, --note - do we want to filter out employees too?
    IFF(t.meta_original_customer_id IS NOT NULL, 'test','') AS test_account,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) AS memb_start,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(COALESCE(LEAD(timestamp) OVER (PARTITION BY userid ORDER BY timestamp ASC), '9999-12-31'))) AS memb_end
FROM lake.segment_fl.javascript_fabletics_identify as i
LEFT JOIN edw_prod.reference.test_customer as t ON (i.userid::varchar = t.meta_original_customer_id::varchar)
WHERE user_id is not null and  userid != '0' and not contains(userid,'-')
  --and test_account != 'test'--filter out bad user_id
    AND membership_status IS NOT NULL --Remove null membership state
ORDER BY userid ASC, memb_start ASC
  ;


CREATE OR REPLACE TEMPORARY TABLE _user AS
WITH _session_data
         AS (SELECT DISTINCT try_to_number(properties_session_id::varchar) AS session_id, /*COALESCE(userid, properties_user_id)*/
                             try_to_number(USERID::varchar)                AS user_id
             FROM lake.segment_fl.javascript_fabletics_product_list_viewed
             WHERE session_id IN (SELECT * FROM _session_base)
               AND user_id IS NOT NULL

             UNION ALL

             SELECT DISTINCT try_to_number(properties_session_id::varchar)                             AS session_id,
                             try_to_number(COALESCE(userid::varchar, properties_customer_id::varchar)) AS user_id
             FROM lake.segment_fl.javascript_fabletics_product_viewed
             WHERE session_id IN (SELECT * FROM _session_base)
               AND user_id IS NOT NULL

             UNION ALL

             SELECT DISTINCT try_to_number(properties_session_id::varchar)                             AS session_id,
                             try_to_number(COALESCE(userid::varchar, properties_customer_id::varchar)) AS user_id
             FROM lake.segment_fl.java_fabletics_product_added
             WHERE session_id IN (SELECT * FROM _session_base)
               AND user_id IS NOT NULL

             UNION ALL

             SELECT DISTINCT try_to_number(properties_session_id::varchar)                    AS session_id,
                             try_to_number(COALESCE(userid, properties_customer_id::varchar)) AS user_id
             FROM lake.segment_fl.java_fabletics_order_completed
             WHERE session_id IN (SELECT * FROM _session_base)
               AND user_id IS NOT NULL

             UNION ALL

             SELECT DISTINCT try_to_number(properties_session_id::varchar)                             AS session_id,
                             try_to_number(coalesce(properties_customer_id::varchar, userid::varchar)) AS user_id
             FROM lake.segment_fl.javascript_fabletics_page
             WHERE session_id IN (SELECT * FROM _session_base)
               AND user_id IS NOT NULL)
SELECT session_id,
       MAX(user_id) AS user_id
FROM _session_data
GROUP BY session_id;



CREATE OR REPLACE TEMPORARY TABLE _usi AS
WITH _session_data AS (
    SELECT try_to_number(properties_session_id::varchar) AS session_id,
        try_to_number(coalesce(properties_customer_id::varchar, userid::varchar)) AS u_id,
        context_useragent as user_agent,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp))::DATE AS session_start_date,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) as timestamp,
        country,
        ROW_NUMBER() OVER (PARTITION BY try_to_number(properties_session_id) ORDER BY timestamp) AS rn
    FROM lake.segment_fl.javascript_fabletics_page
    WHERE session_id IN (SELECT session_id FROM _session_base)

)
SELECT s.session_id,
    try_to_number(coalesce(s.u_id::varchar,u.user_id::varchar)) AS user_id,
    s.session_start_date,
    s.timestamp,
    s.country
FROM _session_data as s
LEFT JOIN _user AS u ON (u.session_id=s.session_id)
LEFT JOIN reporting_base_prod.staging.bot_confirmed as b ON (lower(NVL(s.user_agent,'')) = lower(NVL(b.user_agent,'')))
WHERE rn = 1
and b.user_agent is null;


/***********************************************************************************************************************
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                Activation and Registration query begin
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
***********************************************************************************************************************/
CREATE OR REPLACE TEMPORARY TABLE _activation AS
SELECT DISTINCT try_to_number(properties_session_id::varchar) AS session_id,
    COALESCE(UPPER(properties_is_activating), FALSE)::BOOLEAN AS event
FROM lake.segment_fl.java_fabletics_order_completed v
WHERE session_id IN (SELECT session_id FROM _session_base)
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
    AND properties_is_activating = TRUE;

CREATE OR REPLACE TEMPORARY TABLE _registration AS
SELECT DISTINCT try_to_number(properties_session_id::varchar) AS session_id,
    properties_source AS registration_source
FROM lake.segment_fl.java_fabletics_complete_registration
WHERE session_id in (SELECT session_id FROM _session_base)
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
ORDER BY session_id ASC;

/***********************************************************************************************************************
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                Combined query begin
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
***********************************************************************************************************************/

CREATE OR REPLACE TEMPORARY TABLE _pv AS
SELECT iff(PROPERTIES_IS_BUNDLE, 'Bundle Component', 'Pieced Good') as product_type,
       'Desktop & Mobile Web' as platform,
       try_to_number(pv.properties_session_id::varchar) AS session_id,
    LOWER(IFNULL(properties_list, '')) AS psource, --todo - clean/categorize?
    IFNULL(properties_product_id::VARCHAR, '0')::VARCHAR AS product_id,
    COUNT(DISTINCT product_id) AS total_views
FROM lake.segment_fl.javascript_fabletics_product_viewed pv
WHERE session_id IN (SELECT session_id FROM _session_base)
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
    AND properties_automated_test = FALSE
    AND product_id <> '0'
GROUP BY product_type,
         session_id,
         psource,
         product_id
UNION
--ALL
SELECT 'Bundle'                                          as product_type,
       'Desktop & Mobile Web' as platform,
       try_to_number(pv.properties_session_id::varchar) AS session_id,
       LOWER(IFNULL(properties_list, ''))                AS psource,
       IFNULL(properties_bundle_product_id::VARCHAR, '0')::VARCHAR AS product_id, --bundle_product_id,
       COUNT(DISTINCT product_id)                        AS total_views
FROM lake.segment_fl.javascript_fabletics_product_viewed pv
WHERE PROPERTIES_IS_BUNDLE
  and session_id IN (SELECT session_id FROM _session_base)
  AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
  AND properties_automated_test = FALSE
  AND product_id <> '0'
GROUP BY product_type,
         session_id,
         psource,
         product_id;


CREATE OR REPLACE TEMPORARY TABLE _pa AS
    SELECT iff(PROPERTIES_IS_BUNDLE, 'Bundle Component', 'Pieced Good') as product_type,
       'Desktop & Mobile Web' as platform,
       try_to_number(properties_session_id::varchar) AS session_id,
    LOWER(IFNULL(properties_list_id, '')) AS psource,
    IFNULL(properties_product_id::VARCHAR, '0')::VARCHAR AS product_id,
    COUNT(DISTINCT product_id) AS product_adds
FROM lake.segment_fl.java_fabletics_product_added AS pa
WHERE session_id IN (SELECT session_id FROM _session_base)
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
    AND properties_automated_test = FALSE
    AND product_id <> '0'
GROUP BY product_type,
    session_id,
    psource,
    product_id
union
SELECT 'Bundle' as product_type,
       'Desktop & Mobile Web' as platform,
       try_to_number(properties_session_id::varchar) AS session_id,
    LOWER(IFNULL(properties_list_id, '')) AS psource,
    IFNULL(properties_bundle_product_id::VARCHAR, '0')::VARCHAR AS product_id,
    COUNT(DISTINCT product_id) AS product_adds
FROM lake.segment_fl.java_fabletics_product_added AS pa
WHERE PROPERTIES_IS_BUNDLE
    AND session_id IN (SELECT session_id FROM _session_base)
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
    AND properties_automated_test = FALSE
    AND product_id <> '0'
GROUP BY product_type,
    session_id,
    psource,
    product_id
;


CREATE OR REPLACE TEMPORARY TABLE _oc AS
SELECT  iff(PROPERTIES_PRODUCTS_IS_BUNDLE, 'Bundle Component', 'Pieced Good') as product_type,
       'Desktop & Mobile Web' as platform,
        try_to_number(properties_session_id::varchar) AS session_id,
    LOWER(IFNULL(properties_products_list_id, '')) AS psource,
    IFNULL(so.properties_products_product_id::VARCHAR, '0')::VARCHAR AS product_id,
    COUNT(DISTINCT IFNULL(IFF(product_id = '', 0, product_id), 0)) AS countproducts
FROM lake.segment_fl.java_fabletics_order_completed AS so
WHERE session_id IN (SELECT session_id FROM _session_base)
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
    AND properties_automated_test = FALSE
    AND product_id <> '0'
GROUP BY product_type,
    session_id,
    psource,
    product_id
union
SELECT 'Bundle' as product_type,
       'Desktop & Mobile Web' as platform,
      try_to_number(properties_session_id::varchar) AS session_id,
    LOWER(IFNULL(properties_products_list_id, '')) AS psource,
    IFNULL(so.properties_products_bundle_product_id::VARCHAR, '0')::VARCHAR AS product_id,
    --SUM(IFNULL(so.properties_products_quantity, 0)) AS quantity,
    COUNT(DISTINCT IFNULL(IFF(product_id = '', 0, product_id), 0)) AS countproducts
FROM lake.segment_fl.java_fabletics_order_completed AS so
WHERE PROPERTIES_PRODUCTS_IS_BUNDLE
and session_id IN (SELECT session_id FROM _session_base)
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
    AND properties_automated_test = FALSE
    AND product_id <> '0'
GROUP BY product_type,
    session_id,
    psource,
    product_id
;




CREATE OR REPLACE TEMPORARY TABLE _plv AS
SELECT iff(PROPERTIES_PRODUCTS_IS_BUNDLE, 'Bundle Component', 'Pieced Good') as product_type,
       'Desktop & Mobile Web' as platform,
    try_to_number(properties_session_id::varchar) AS session_id,--NOTE - LOOK AT POPERTIES_PRODUCT_CATEGORY TO VALIDATE PSOURCE MAPPING
    LOWER(IFNULL(properties_list_id, '')) AS psource,
    IFNULL(plv.properties_products_product_id::BIGINT, 0)::VARCHAR AS product_id,
    COUNT(DISTINCT product_id) AS list_views
FROM lake.segment_fl.javascript_fabletics_product_list_viewed AS plv
WHERE session_id IN (SELECT session_id FROM _session_base)
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
    AND properties_automated_test = FALSE
    AND product_id <> '0'
GROUP BY product_type,
    session_id,
    psource,
    product_id
union
SELECT 'Bundle' as product_type,
       'Desktop & Mobile Web' as platform,
    try_to_number(properties_session_id::varchar) AS session_id,
    LOWER(IFNULL(properties_list_id, '')) AS psource,
    IFNULL(plv.properties_products_bundle_product_id::VARCHAR, '0')::VARCHAR AS product_id,
    COUNT(DISTINCT product_id) AS list_views
FROM lake.segment_fl.javascript_fabletics_product_list_viewed AS plv
WHERE PROPERTIES_PRODUCTS_IS_BUNDLE
and session_id IN (SELECT session_id FROM _session_base)
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) <= $r_time
    AND properties_automated_test = FALSE
    AND product_id <> '0'
GROUP BY product_type,
    session_id,
    psource,
    product_id
;


CREATE OR REPLACE TEMPORARY TABLE _action AS
SELECT product_type,
       platform,
       session_id,
       psource,
       product_id::VARCHAR        AS product_id,
       SUM(IFNULL(list_views, 0)) AS list_view_total,
       NULL                       AS pdp_view_total,
       NULL                       AS pa_total,
       NULL                       AS oc_total
FROM _plv
GROUP BY product_type,
         platform,
         session_id,
         psource,
         product_id

UNION ALL

SELECT product_type,
       platform,
       session_id,
       psource,
       product_id::VARCHAR         AS product_id,
       NULL                        AS list_view_total,
       SUM(IFNULL(total_views, 0)) AS pdp_view_total, -- test count distinct instead
       NULL                        AS pa_total,
       NULL                        AS oc_total
FROM _pv
GROUP BY product_type,
         platform,
         session_id,
         psource,
         product_id::VARCHAR

UNION ALL

SELECT product_type,
       platform,
       session_id,
       psource,
       product_id::VARCHAR          AS product_id,
       NULL                         AS list_view_total,
       NULL                         AS pdp_view_total,
       SUM(IFNULL(product_adds, 0)) AS pa_total, --change to quantity?
       NULL                         AS oc_total
FROM _pa
GROUP BY product_type,
         platform,
         session_id,
         psource,
         product_id::VARCHAR

UNION ALL

SELECT product_type,
       platform,
       session_id,
       psource,
       product_id::VARCHAR           AS product_id,
       NULL                          AS list_view_total,
       NULL                          AS pdp_view_total,
       NULL                          AS pa_total,
       SUM(IFNULL(countproducts, 0)) AS oc_total --maybe use quantity here instead?
FROM _oc
GROUP BY product_type,
         platform,
         session_id,
         psource,
         product_id::VARCHAR;

;
CREATE OR REPLACE TEMPORARY TABLE _s1 AS
(SELECT _usi.session_id,
    _usi.user_id,
    LOWER(COALESCE(i.membership_status, 'prospect')) AS membership_status, --prospects do not have identify calls, so we define them here
    _usi.country,
    _usi.session_start_date
FROM _usi
LEFT JOIN _identify AS i ON (_usi.user_id = i.user_id)
    AND TO_TIMESTAMP(_usi.timestamp) >= TO_TIMESTAMP(i.memb_start)
    AND TO_TIMESTAMP(_usi.timestamp) < TO_TIMESTAMP(i.memb_end)
WHERE NVL(test_account, '') != 'test'--remove test accounts
GROUP BY _usi.session_id,
    _usi.user_id,
    membership_status,
    country,
    session_start_date)
;


BEGIN;

DELETE
FROM reporting_prod.fabletics.pcvr_session_historical --UPDATE THIS TO NEW TABLE

WHERE session_id IN (
    SELECT DISTINCT session_id
    FROM _session_base
    WHERE session_id IS NOT NULL
);


INSERT INTO reporting_prod.fabletics.pcvr_session_historical(
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
    max_received_app

)

SELECT r.platform,
    s1.session_id,
    s1.session_start_date::DATE AS session_start_date,
    s1.user_id,
    s1.membership_status,
    COALESCE(a.event, FALSE) AS is_activating,
    IFF(g.registration_source IS NULL, FALSE, TRUE) AS registration_session,
    COALESCE(g.registration_source, '') AS registration_source,
    r.product_type,
    r.product_id::VARCHAR AS product_id, --master product id!
    r.psource,
    s1.country,
    SUM(IFNULL(r.list_view_total, 0)) AS list_view_total,
    SUM(IFNULL(r.pdp_view_total, 0)) AS pdp_view_total,
    SUM(IFNULL(r.pa_total, 0)) AS pa_total,
    SUM(IFNULL(r.oc_total, 0)) AS oc_total,
    IFF(SUM(IFNULL(r.list_view_total, 0)) > 0 AND SUM(IFNULL(r.pdp_view_total, 0)) > 0, SUM(IFNULL(r.pdp_view_total, 0)), 0) AS ss_pdp_view,
    IFF(SUM(IFNULL(r.pdp_view_total, 0)) > 0 AND SUM(IFNULL(r.pa_total, 0)) > 0, SUM(IFNULL(r.pa_total, 0)), 0) AS ss_product_added,
    IFF(SUM(IFNULL(r.pa_total, 0)) > 0 AND SUM(IFNULL(r.oc_total, 0)) > 0, SUM(IFNULL(r.oc_total, 0)), 0) AS ss_product_ordered,
    $update_time::DATETIME AS refresh_datetime,
    TO_VARCHAR(TO_TIMESTAMP($r_time), 'yyyy-mm-dd hh:mi:ss') AS max_received,
    TO_VARCHAR(TO_TIMESTAMP($r_time_app), 'yyyy-mm-dd hh:mi:ss') AS max_received_app
FROM _s1 AS s1
LEFT JOIN _action AS r ON s1.session_id::VARCHAR = r.session_id::VARCHAR
LEFT JOIN _activation AS a
    ON s1.session_id::VARCHAR = a.session_id::VARCHAR
LEFT JOIN _registration g ON s1.session_id::VARCHAR = g.session_id::VARCHAR
GROUP BY platform,
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
ORDER BY session_id ASC, product_id ASC
;

COMMIT;

