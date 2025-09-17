SET first_date = ((SELECT CONVERT_TIMEZONE('America/Los_Angeles', 'UTC',
                                           COALESCE(DATEADD('day', -1, MAX(max_recieved)), '2021-09-01')::DATETIME)
                   FROM REPORTING_BASE_PROD.sxf.pcvr_session_psource));
SET update_time = CURRENT_TIMESTAMP();
SET r_time = (SELECT MIN(t.rtime)
              FROM (
                       SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                                                   TO_TIMESTAMP(receivedat))::DATETIME) AS rtime
                       FROM lake.segment_sxf.javascript_sxf_product_list_viewed
                       UNION ALL
                       SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                                                   TO_TIMESTAMP(receivedat))::DATETIME) AS rtime
                       FROM lake.segment_sxf.javascript_sxf_product_viewed
                       UNION ALL
                       SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                                                   TO_TIMESTAMP(receivedat))::DATETIME) AS rtime
                       FROM lake.segment_sxf.java_sxf_product_added
                       UNION ALL
                       SELECT MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                                                   TO_TIMESTAMP(receivedat))::DATETIME) AS rtime
                       FROM lake.segment_sxf.java_sxf_order_completed
                   ) t);;

CREATE OR REPLACE TEMPORARY TABLE _session_base AS (
    SELECT DISTINCT session_id
    FROM (
             SELECT DISTINCT properties_session_id::VARCHAR AS session_id
             FROM lake.segment_sxf.javascript_sxf_product_list_viewed
             WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) >= $first_date
               AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time

             UNION ALL
             SELECT DISTINCT properties_session_id::VARCHAR AS session_id
             FROM lake.segment_sxf.javascript_sxf_product_viewed
             WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) >= $first_date
               AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time

             UNION ALL
             SELECT DISTINCT properties_session_id::VARCHAR AS session_id
             FROM lake.segment_sxf.java_sxf_product_added
             WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) >= $first_date
               AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time

             UNION ALL
             SELECT DISTINCT properties_session_id::VARCHAR AS session_id
             FROM lake.segment_sxf.java_sxf_order_completed
             WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) >= $first_date
               AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time
         ) AS a
    WHERE session_id IS NOT NULL
);



CREATE OR REPLACE TEMPORARY TABLE _identify AS (
    SELECT userid                               AS user_id,
           IFF(LOWER(traits_membership_status) LIKE 'payg', 'paygo',
               LOWER(traits_membership_status)) AS membership_status,
           IFF(traits_email LIKE '%@test.com' OR traits_email LIKE '%@example.com', 'test',
               '')                              AS test_account,
           TO_TIMESTAMP(timestamp)              AS memb_start,
           TO_TIMESTAMP(COALESCE(LEAD(timestamp) OVER (PARTITION BY userid ORDER BY timestamp),
                                 '9999-12-31')) AS memb_end
    FROM lake.segment_sxf.javascript_sxf_identify
);


CREATE OR REPLACE TEMPORARY TABLE _user AS (
    WITH s AS (SELECT DISTINCT properties_session_id, COALESCE(userid, properties_user_id) AS user_id
               FROM lake.segment_sxf.javascript_sxf_product_list_viewed plv
    JOIN _session_base sb on plv.properties_session_id::VARCHAR = sb.session_id::VARCHAR
               WHERE user_id IS NOT NULL
               UNION ALL
               SELECT DISTINCT properties_session_id, COALESCE(userid, properties_customer_id) AS user_id
               FROM lake.segment_sxf.javascript_sxf_product_viewed pv
    JOIN _session_base sb on pv.properties_session_id::VARCHAR = sb.session_id::VARCHAR
               WHERE user_id IS NOT NULL
               UNION ALL
               SELECT DISTINCT properties_session_id, COALESCE(userid, properties_customer_id) AS user_id
               FROM lake.segment_sxf.java_sxf_product_added pa
    JOIN _session_base sb on pa.properties_session_id::VARCHAR = sb.session_id::VARCHAR
               WHERE user_id IS NOT NULL
               UNION ALL
               SELECT DISTINCT properties_session_id, COALESCE(userid, properties_customer_id) AS user_id
               FROM lake.segment_sxf.java_sxf_order_completed oc
    JOIN _session_base sb on oc.properties_session_id::VARCHAR = sb.session_id::VARCHAR
               WHERE user_id IS NOT NULL
               UNION ALL
               SELECT properties_session_id, COALESCE(userid, properties_user_id) AS user_id
               FROM lake.segment_sxf.javascript_sxf_page p
    JOIN _session_base sb on p.properties_session_id::VARCHAR = sb.session_id::VARCHAR
               WHERE user_id IS NOT NULL
    )
    SELECT properties_session_id::VARCHAR AS session_id,
           MAX(user_id)          AS user_id
    FROM s
    GROUP BY session_id
);

CREATE OR REPLACE TEMPORARY TABLE _usi AS (
SELECT properties_session_id::VARCHAR as session_id,
                       properties_user_status_initial as user_status_initial,
                       COALESCE(userid, properties_user_id)                                      AS user_id,
                       CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                                        TO_TIMESTAMP(timestamp))::DATE                           AS session_start_date,
                       timestamp,
                       country
                FROM lake.segment_sxf.javascript_sxf_page p
                join _session_base sb on p.properties_session_id::VARCHAR = sb.session_id::VARCHAR
QUALIFY ROW_NUMBER() OVER (PARTITION BY properties_session_id ORDER BY timestamp, receivedat) = 1
);


CREATE OR REPLACE TEMPORARY TABLE _activation AS (
    SELECT DISTINCT v.properties_session_id::VARCHAR                          AS session_id,
                    COALESCE(UPPER(properties_is_activating), FALSE)::BOOLEAN AS event
    FROM lake.segment_sxf.java_sxf_order_completed v
    join _session_base sb on v.properties_session_id::VARCHAR=sb.session_id::VARCHAR
    WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time
      AND properties_is_activating = TRUE
);

CREATE OR REPLACE TEMPORARY TABLE _registration AS (
    SELECT DISTINCT properties_session_id::VARCHAR AS session_id,
                    properties_source     AS registration_source
    FROM lake.segment_sxf.java_sxf_complete_registration r
    JOIN _session_base sb on r.properties_session_id::VARCHAR = sb.session_id::VARCHAR
    WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time
);


CREATE OR REPLACE TEMPORARY TABLE _pv AS (
    SELECT pv.properties_session_id::VARCHAR                             AS session_id,
           LOWER(CASE
                     WHEN IFNULL(properties_list, '') ILIKE 'customer_also_viewed%' THEN 'CAV'
                     WHEN IFNULL(properties_list, '') ILIKE 'pair_it_with%' THEN 'YMAL'
                     WHEN IFNULL(properties_list, '') ILIKE 'you_may_also%' THEN 'YMAL'
                     ELSE IFNULL(properties_list, '') END)      AS psource,
           IFNULL(properties_product_id::VARCHAR, '0')::VARCHAR AS product_id,
           properties_is_bundle                                 AS is_bundle,
           IFNULL(properties_bundle_product_id, '')             AS bundle_product_id,
           COUNT(*)                                             AS total_views
    FROM lake.segment_sxf.javascript_sxf_product_viewed pv
    JOIN _session_base sb on pv.properties_session_id::VARCHAR=sb.session_id::VARCHAR
    WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time
      AND properties_automated_test = FALSE
      AND product_id <> '0'
    GROUP BY pv.properties_session_id::VARCHAR,
             psource,
             IFNULL(properties_product_id::VARCHAR, '0')::VARCHAR,
             properties_is_bundle,
             IFNULL(properties_bundle_product_id, '')
);


CREATE OR REPLACE TEMPORARY TABLE _pa AS (
    SELECT pa.properties_session_id::VARCHAR                             AS session_id,
           LOWER(CASE
                     WHEN IFNULL(properties_list_id, '') ILIKE 'customer_also_viewed%' THEN 'CAV'
                     WHEN IFNULL(properties_list_id, '') ILIKE 'pair_it_with%' THEN 'YMAL'
                     WHEN IFNULL(properties_list_id, '') ILIKE 'you_may_also%' THEN 'YMAL'
                     ELSE IFNULL(properties_list_id, '') END)   AS psource,
           IFNULL(properties_product_id::VARCHAR, '0')::VARCHAR AS product_id,
           properties_is_bundle                                 AS is_bundle,
           IFNULL(properties_bundle_product_id, '')::VARCHAR    AS bundle_product_id,
           SUM(IFNULL(properties_quantity, 0))                  AS quantity,
           COUNT(*)                                             AS product_adds
    FROM lake.segment_sxf.java_sxf_product_added AS pa
    JOIN _session_base sb on pa.properties_session_id::VARCHAR = sb.session_id::VARCHAR
    WHERE pa.properties_session_id != ''
      AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time
      AND properties_automated_test = FALSE
      AND product_id <> '0'
    GROUP BY pa.properties_session_id::VARCHAR ,
             psource,
             IFNULL(properties_product_id::VARCHAR, '0')::VARCHAR,
             properties_is_bundle,
             IFNULL(properties_bundle_product_id, '')::VARCHAR
);

CREATE OR REPLACE TEMPORARY TABLE _oc AS (
    SELECT so.properties_session_id::VARCHAR                                               AS session_id,
           LOWER(CASE
                     WHEN IFNULL(properties_products_list_id, '') ILIKE 'customer_also_viewed%' THEN 'CAV'
                     WHEN IFNULL(properties_products_list_id, '') ILIKE 'pair_it_with%' THEN 'YMAL'
                     WHEN IFNULL(properties_products_list_id, '') ILIKE 'you_may_also%' THEN 'YMAL'
                     ELSE IFNULL(properties_products_list_id, '') END)            AS psource,
           IFNULL(so.properties_products_product_id::VARCHAR, '0')::VARCHAR       AS product_id,
           so.properties_products_is_bundle::VARCHAR                              AS is_bundle,
           IFNULL(so.properties_products_bundle_product_id::VARCHAR, '')::VARCHAR AS bundle_product_id,
           SUM(IFNULL(so.properties_products_quantity, 0))                        AS quantity,
           COUNT(DISTINCT IFNULL(IFF(product_id = '', 0, product_id), 0))         AS countproducts
    FROM lake.segment_sxf.java_sxf_order_completed AS so
    JOIN _session_base sb on so.properties_session_id::VARCHAR=sb.session_id::VARCHAR
    WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time
      AND properties_automated_test = FALSE
      AND product_id <> '0'
    GROUP BY so.properties_session_id::VARCHAR,
             psource,
             IFNULL(so.properties_products_product_id::VARCHAR, '0')::VARCHAR,
             so.properties_products_is_bundle::VARCHAR,
             IFNULL(so.properties_products_bundle_product_id::VARCHAR, '')::VARCHAR
);


CREATE OR REPLACE TEMPORARY TABLE _plv AS (
    SELECT plv.properties_session_id::VARCHAR                                               AS session_id,
           LOWER(CASE
                     WHEN IFNULL(properties_list_id, '') ILIKE 'customer_also_viewed%' THEN 'CAV'
                     WHEN IFNULL(properties_list_id, '') ILIKE 'pair_it_with%' THEN 'YMAL'
                     WHEN IFNULL(properties_list_id, '') ILIKE 'you_may_also%' THEN 'YMAL'
                     ELSE IFNULL(properties_list_id, '') END)                      AS psource,
           IFNULL(plv.properties_products_product_id::BIGINT, 0)::VARCHAR          AS product_id,
           IFNULL(plv.properties_products_is_bundle::VARCHAR, FALSE)               AS is_bundle,
           IFNULL(plv.properties_products_bundle_product_id::VARCHAR, '')::VARCHAR AS bundle_product_id,
           COUNT(*)                                                                AS list_views
    FROM lake.segment_sxf.javascript_sxf_product_list_viewed AS plv
    JOIN _session_base sb on plv.properties_session_id::VARCHAR = sb.session_id::VARCHAR
    WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(receivedat)) <= $r_time
      AND properties_automated_test = FALSE
      AND product_id <> '0'
    GROUP BY plv.properties_session_id::VARCHAR,
             psource,
             IFNULL(plv.properties_products_product_id::BIGINT, 0)::VARCHAR,
             IFNULL(plv.properties_products_is_bundle::VARCHAR, FALSE),
             IFNULL(plv.properties_products_bundle_product_id::VARCHAR, '')::VARCHAR
);

CREATE OR REPLACE TEMPORARY TABLE _action AS (
    SELECT session_id,
           psource,
           product_id::VARCHAR        AS product_id,
           is_bundle,
           bundle_product_id,
           SUM(IFNULL(list_views, 0)) AS list_view_total,
           NULL                       AS pdp_view_total,
           NULL                       AS pa_total,
           NULL                       AS oc_total
    FROM _plv
    GROUP BY session_id,
             psource,
             product_id::VARCHAR,
             is_bundle,
             bundle_product_id

    UNION ALL
    SELECT session_id,
           psource,
           product_id::VARCHAR         AS product_id,
           is_bundle,
           bundle_product_id,
           NULL                        AS list_view_total,
           SUM(IFNULL(total_views, 0)) AS pdp_view_total,
           NULL                        AS pa_total,
           NULL                        AS oc_total
    FROM _pv
    GROUP BY session_id,
             psource,
             product_id::VARCHAR,
             is_bundle,
             bundle_product_id

    UNION ALL
    SELECT session_id,
           psource,
           product_id::VARCHAR          AS product_id,
           is_bundle,
           bundle_product_id,
           NULL                         AS list_view_total,
           NULL                         AS pdp_view_total,
           SUM(IFNULL(product_adds, 0)) AS pa_total,
           NULL                         AS oc_total
    FROM _pa
    GROUP BY session_id,
             psource,
             product_id::VARCHAR,
             is_bundle,
             bundle_product_id

    UNION ALL
    SELECT session_id,
           psource,
           product_id::VARCHAR           AS product_id,
           is_bundle,
           bundle_product_id,
           NULL                          AS list_view_total,
           NULL                          AS pdp_view_total,
           NULL                          AS pa_total,
           SUM(IFNULL(countproducts, 0)) AS oc_total
    FROM _oc
    GROUP BY session_id,
             psource,
             product_id::VARCHAR,
             is_bundle,
             bundle_product_id
);

CREATE OR REPLACE TEMPORARY TABLE _s1 AS (
    SELECT _usi.session_id,
           u.user_id,
           LOWER(COALESCE(i.membership_status, 'prospect')) AS membership_status,
           _usi.user_status_initial,
           _usi.country,
           _usi.session_start_date
    FROM _usi
             LEFT JOIN _user AS u
                       ON _usi.session_id = u.session_id
             LEFT JOIN _identify AS i
                       ON u.user_id = i.user_id
                           AND TO_TIMESTAMP(_usi.timestamp) >= TO_TIMESTAMP(i.memb_start)
                           AND TO_TIMESTAMP(_usi.timestamp) < TO_TIMESTAMP(i.memb_end)
    GROUP BY _usi.session_id,
             u.user_id,
             LOWER(COALESCE(i.membership_status, 'prospect')),
             _usi.user_status_initial,
             country,
             session_start_date
);

DELETE
FROM REPORTING_BASE_PROD.sxf.pcvr_session_psource
WHERE session_id IN (
    SELECT DISTINCT session_id
    FROM _session_base
    WHERE session_id IS NOT NULL)
;

INSERT INTO REPORTING_BASE_PROD.sxf.pcvr_session_psource (session_id,
                                           user_id,
                                           membership_status,
                                           user_status_initial,
                                           is_activating,
                                           registration_session,
                                           registration_source,
                                           product_id,
                                           is_bundle,
                                           bundle_product_id,
                                           session_start_date,
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
                                           max_recieved)
SELECT s1.session_id,
       s1.user_id,
       s1.membership_status,
       s1.user_status_initial,
       COALESCE(a.event, FALSE)                                      AS is_activating,
       IFF(g.registration_source IS NULL, FALSE, TRUE)               AS registration_session,
       COALESCE(g.registration_source, '')                           AS registration_source,
       r.product_id::VARCHAR                                         AS product_id,
       r.is_bundle,
       IFF(is_bundle = 'false', '', IFNULL(r.bundle_product_id, '')) AS bundle_product_id,
       s1.session_start_date::DATE                                   AS session_start_date,
       r.psource,
       s1.country,
       SUM(IFNULL(r.list_view_total, 0))                             AS list_view_total,
       SUM(IFNULL(r.pdp_view_total, 0))                              AS pdp_view_total,
       SUM(IFNULL(r.pa_total, 0))                                    AS pa_total,
       SUM(IFNULL(r.oc_total, 0))                                    AS oc_total,
       IFF(SUM(IFNULL(r.list_view_total, 0)) > 0 AND SUM(IFNULL(r.pdp_view_total, 0)) > 0,
           SUM(IFNULL(r.pdp_view_total, 0)),0)                       AS ss_pdp_view,
       IFF(SUM(IFNULL(r.pdp_view_total, 0)) > 0 AND SUM(IFNULL(r.pa_total, 0)) > 0,
           SUM(IFNULL(r.pa_total, 0)), 0)                            AS ss_product_added,
       IFF(SUM(IFNULL(r.pa_total, 0)) > 0 AND SUM(IFNULL(r.oc_total, 0)) > 0,
           SUM(IFNULL(r.oc_total, 0)),0)                             AS ss_product_ordered,
       $update_time::DATETIME                                        AS refresh_datetime,
       TO_VARCHAR(to_timestamp($r_time), 'yyyy-mm-dd hh:mi:ss')      AS max_recieved
FROM _s1 AS s1
         LEFT JOIN _action AS r
                   ON s1.session_id::VARCHAR = r.session_id::VARCHAR
         LEFT JOIN _activation AS a
                   ON s1.session_id::VARCHAR = a.session_id::VARCHAR
         LEFT JOIN _registration g
                   ON s1.session_id::VARCHAR = g.session_id::VARCHAR
where s1.session_start_date>='2021-09-01'
GROUP BY s1.session_id,
         s1.user_id,
         s1.membership_status,
         s1.user_status_initial,
         is_activating,
         IFF(g.registration_source IS NULL, FALSE, TRUE),
         COALESCE(g.registration_source, ''),
         r.product_id::VARCHAR,
         r.is_bundle,
         IFF(is_bundle = 'false', '', IFNULL(r.bundle_product_id, '')),
         s1.session_start_date,
         r.psource,
         s1.country;



CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.sxf.pcvr_product_psource AS
SELECT session_start_date,
       psource,
       country,
       ses.product_id,
       is_bundle,
       ses.bundle_product_id,
       is_activating,
       registration_session,
       registration_source,
       INITCAP(membership_status)                                          AS membership_status,
       IFNULL(INITCAP(user_status_initial), '')                            AS user_status_initial,
       SUM(ses.list_view_total)                                            AS list_view_total,
       SUM(ses.pdp_view_total)                                             AS pdp_view_total,
       SUM(ses.pa_total)                                                   AS pa_total,
       SUM(ses.oc_total)                                                   AS oc_total,
       SUM(ses.ss_pdp_view)                                                AS ss_pdp_view,
       SUM(ses.ss_product_added)                                           AS ss_product_added,
       SUM(ses.ss_product_ordered)                                         AS ss_product_ordered,
       $update_time                                                        AS refresh_datetime,
       $r_time                                                             AS max_received
FROM REPORTING_BASE_PROD.sxf.pcvr_session_psource AS ses
where ses.product_id is not null and ses.product_id <>''
GROUP BY session_start_date,
         psource,
         country,
         ses.product_id,
         is_bundle,
         ses.bundle_product_id,
         is_activating,
         registration_session,
         registration_source,
         INITCAP(membership_status),
         IFNULL(INITCAP(user_status_initial), '');
