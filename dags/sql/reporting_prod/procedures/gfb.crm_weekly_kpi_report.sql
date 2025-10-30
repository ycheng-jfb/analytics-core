SET start_date =(
    SELECT IFNULL(
               DATEADD('day', -14, (SELECT MAX(send_date)::DATE
                                    FROM gfb.crm_weekly_kpi_report
               )), '2024-06-30')
);

SET end_date = DATEADD('day', -1, CURRENT_DATE());

CREATE OR REPLACE TEMPORARY TABLE _sms_sends_campaigns AS
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM campaign_event_data.org_3223.sms_sends_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM iterable.org_3620.sms_sends_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _sms_sends_delta
AS
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       sms.project_id                            AS source_store_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       sms.from_phone_number_id                  AS contact_id,
       'sms'                                     AS type,
       sms.created_at                            AS event_time,
       'lake.iterable.sms_sends'                 AS source_table,
       sms.template_id                           AS campaign_id,
       sms.template_name                         AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM campaign_event_data.org_3223.sms_sends_view AS sms
         LEFT JOIN campaign_event_data.org_3223.campaigns c
                   ON sms.campaign_id = c.id
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON sms.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE sms.created_at > $start_date
  AND sms.created_at <= $end_date
  AND sms.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       sms.project_id                            AS source_store_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       sms.from_phone_number_id                  AS contact_id,
       'sms'                                     AS type,
       sms.created_at                            AS event_time,
       'lake.iterable.sms_sends'                 AS source_table,
       sms.template_id                           AS campaign_id,
       sms.template_name                         AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.sms_sends_view AS sms
         LEFT JOIN iterable.org_3620.campaigns c
                   ON sms.campaign_id = c.id
         LEFT JOIN iterable.org_3620.users u
                   ON sms.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE sms.created_at > $start_date
  AND sms.created_at <= $end_date
  AND sms.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _sms_sends AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(sms.campaign_name, m.campaign_name)                      AS campaign_name,
       m.campaign_id                                                     AS campaign_id,
       subject,
       version_name,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS send_count
FROM _sms_sends_delta m
         LEFT JOIN _sms_sends_campaigns sms
                   ON m.campaign_id = sms.campaign_id
WHERE store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'), store_brand),
         COALESCE(sms.campaign_name, m.campaign_name),
         m.campaign_id,
         subject,
         version_name,
         TO_DATE(event_time);

CREATE OR REPLACE TEMPORARY TABLE _sms_clicks_campaigns AS
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM campaign_event_data.org_3223.sms_clicks_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM iterable.org_3620.sms_clicks_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _sms_clicks_delta
AS
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       sms.from_phone_number_id::VARCHAR         AS contact_id,
       'sms'                                     AS type,
       sms.created_at                            AS event_time,
       'lake.iterable.sms_clicks'                AS source_table,
       sms.template_id                           AS campaign_id,
       sms.template_name                         AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM campaign_event_data.org_3223.sms_clicks_view AS sms
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON sms.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE sms.created_at > $start_date
  AND sms.created_at <= $end_date
  AND sms.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       sms.from_phone_number_id::VARCHAR         AS contact_id,
       'sms'                                     AS type,
       sms.created_at                            AS event_time,
       'lake.iterable.sms_clicks'                AS source_table,
       sms.template_id                           AS campaign_id,
       sms.template_name                         AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.sms_clicks_view AS sms
         LEFT JOIN iterable.org_3620.users u
                   ON sms.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE sms.created_at > $start_date
  AND sms.created_at <= $end_date
  AND sms.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _sms_clicks AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(sms.campaign_name, m.campaign_name)                      AS campaign_name,
       m.campaign_id,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS click_count,
FROM _sms_clicks_delta m
         LEFT JOIN _sms_clicks_campaigns sms
                   ON m.campaign_id = sms.campaign_id
WHERE store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'), store_brand),
         COALESCE(sms.campaign_name, m.campaign_name),
         m.campaign_id,
         TO_DATE(event_time);

CREATE OR REPLACE TEMPORARY TABLE _attentive_sms_metrics AS
SELECT s.campaign_name,
       s.store_brand,
       s.campaign_id,
       s.send_date,
       s.subject,
       IFNULL(s.send_count, 0)  AS send_count,
       IFNULL(c.click_count, 0) AS click_count,
       0                        AS optouts,
       0                        AS optin
FROM _sms_sends s
         LEFT JOIN _sms_clicks c
                   ON s.campaign_id = c.campaign_id AND s.send_date = c.send_date AND s.store_brand = c.store_brand;

CREATE OR REPLACE TEMPORARY TABLE _attentive_sms_metrics_mkt AS
SELECT s.campaign_name,
       s.store_brand,
       s.campaign_id,
       s.send_date,
       s.subject,
       IFNULL(s.send_count, 0)  AS send_count,
       IFNULL(c.click_count, 0) AS click_count,
       0                        AS optouts,
       0                        AS optin
FROM (SELECT store_brand,
             campaign_id,
             campaign_name,
             subject,
             MIN(send_date)  AS send_date,
             SUM(send_count) AS send_count
      FROM _sms_sends
      WHERE campaign_name ILIKE 'mkt_%'
      GROUP BY store_brand,
               campaign_id,
               campaign_name,
               subject) s
         LEFT JOIN (SELECT store_brand,
                           campaign_id,
                           SUM(click_count) AS click_count
                    FROM _sms_clicks
                    GROUP BY store_brand,
                             campaign_id) c
                   ON s.campaign_id = c.campaign_id AND s.store_brand = c.store_brand;


CREATE OR REPLACE TEMPORARY TABLE _email_sends_campaigns AS
SELECT DISTINCT s.template_id                AS campaign_id,
                CASE
                    WHEN s.template_name ILIKE '%mkt%'
                        AND (s.template_name ILIKE '%control'
                            OR s.template_name ILIKE '%test') THEN s.template_name
                    ELSE s.campaign_name END AS campaign_name,
                c.start_at::DATE             AS start_date
FROM campaign_event_data.org_3223.email_blast_sends_view s
         JOIN campaign_event_data.org_3223.campaigns c ON c.id = s.campaign_id
WHERE s.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT DISTINCT s.template_id                AS campaign_id,
                CASE
                    WHEN s.template_name ILIKE '%mkt%'
                        AND (s.template_name ILIKE '%control'
                            OR s.template_name ILIKE '%test') THEN s.template_name
                    ELSE s.campaign_name END AS campaign_name,
                c.start_at::DATE             AS start_date
FROM iterable.org_3620.email_blast_sends_view s
         JOIN iterable.org_3620.campaigns c ON c.id = s.campaign_id
WHERE s.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT DISTINCT template_id AS campaign_id,
                campaign_name,
                NULL        AS start_date
FROM campaign_event_data.org_3223.email_triggered_sends_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT DISTINCT template_id AS campaign_id,
                campaign_name,
                NULL        AS start_date
FROM iterable.org_3620.email_triggered_sends_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _email_sends_delta
AS
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'email'                                   AS type,
       ebs.created_at                            AS event_time,
       'lake.iterable.email_blast_sends'         AS source_table,
       ebs.template_id                           AS campaign_id,
       ebs.template_name                         AS campaign_name,
       ebs.email_subject                         AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(ebs.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(ebs.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM campaign_event_data.org_3223.email_blast_sends_view ebs
         JOIN campaign_event_data.org_3223.campaigns c ON c.id = ebs.campaign_id
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON ebs.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE ebs.created_at > $start_date
  AND ebs.created_at <= $end_date
  AND ebs.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'email'                                   AS type,
       ebs.created_at                            AS event_time,
       'lake.iterable.email_blast_sends'         AS source_table,
       ebs.template_id                           AS campaign_id,
       ebs.template_name                         AS campaign_name,
       ebs.email_subject                         AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(ebs.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(ebs.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.email_blast_sends_view ebs
         JOIN iterable.org_3620.campaigns c ON c.id = ebs.campaign_id
         LEFT JOIN iterable.org_3620.users u
                   ON ebs.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE ebs.created_at > $start_date
  AND ebs.created_at <= $end_date
  AND ebs.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'email'                                   AS type,
       ets.created_at                            AS event_time,
       'lake.iterable.email_triggered_sends'     AS source_table,
       ets.template_id                           AS campaign_id,
       ets.campaign_name                         AS campaign_name,
       ets.email_subject                         AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(ets.campaign_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(ets.campaign_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM campaign_event_data.org_3223.email_triggered_sends_view ets
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON ets.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE ets.created_at > $start_date
  AND ets.created_at <= $end_date
  AND ets.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'email'                                   AS type,
       ets.created_at                            AS event_time,
       'lake.iterable.email_triggered_sends'     AS source_table,
       ets.template_id                           AS campaign_id,
       ets.campaign_name                         AS campaign_name,
       ets.email_subject                         AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(ets.campaign_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(ets.campaign_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.email_triggered_sends_view ets
         LEFT JOIN iterable.org_3620.users u
                   ON ets.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE ets.created_at > $start_date
  AND ets.created_at <= $end_date
  AND ets.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _email_sends AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(e.campaign_name, m.campaign_name)                        AS campaign_name,
       m.campaign_id,
       subject,
       version_name,
       COALESCE(TO_DATE(e.start_date), TO_DATE(event_time))              AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS send_count
FROM _email_sends_delta m
         LEFT JOIN _email_sends_campaigns e
                   ON m.campaign_id = e.campaign_id
WHERE store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'), store_brand),
         COALESCE(e.campaign_name, m.campaign_name),
         m.campaign_id,
         subject,
         version_name,
         COALESCE(TO_DATE(e.start_date), TO_DATE(event_time));

CREATE OR REPLACE TEMPORARY TABLE _email_clicks_campaigns AS
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM campaign_event_data.org_3223.email_clicks_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM iterable.org_3620.email_clicks_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _email_clicks_delta
AS
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'email'                                   AS type,
       es.created_at                             AS event_time,
       'lake.iterable.email_clicks'              AS source_table,
       es.template_id                            AS campaign_id,
       es.template_name                          AS campaign_name,
       es.email_subject                          AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(es.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(es.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM campaign_event_data.org_3223.email_clicks_view AS es
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON es.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE es.created_at > $start_date
  AND es.created_at <= $end_date
  AND es.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'email'                                   AS type,
       es.created_at                             AS event_time,
       'lake.iterable.email_clicks'              AS source_table,
       es.template_id                            AS campaign_id,
       es.template_name                          AS campaign_name,
       es.email_subject                          AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(es.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(es.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.email_clicks_view AS es
         LEFT JOIN iterable.org_3620.users u
                   ON es.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE es.created_at > $start_date
  AND es.created_at <= $end_date
  AND es.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _email_clicks AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(e.campaign_name, m.campaign_name)                        AS campaign_name,
       m.campaign_id,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS click_count
FROM _email_clicks_delta m
         LEFT JOIN _email_clicks_campaigns e
                   ON m.campaign_id = e.campaign_id
WHERE store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'), store_brand),
         COALESCE(e.campaign_name, m.campaign_name),
         m.campaign_id,
         TO_DATE(event_time);

CREATE OR REPLACE TEMPORARY TABLE _email_opens AS
SELECT IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
           REPLACE(LOWER(s.store_name), ' ', '_')) AS store_group,
       ebs.template_id                             AS campaign_id,
       ebs.campaign_name                           AS campaign_name,
       TO_DATE(ebs.created_at)                     AS send_date,
       COUNT(DISTINCT u.user_id)                   AS open_count
FROM campaign_event_data.org_3223.email_opens_view ebs
         LEFT JOIN campaign_event_data.org_3223.campaigns c
                   ON ebs.campaign_id = c.id
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON ebs.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model.dim_store AS s
                   ON cust.store_id = s.store_id
WHERE TO_DATE(ebs.created_at) > $start_date
  AND TO_DATE(ebs.created_at) <= $end_date
  AND s.store_name ILIKE ANY ('ShoeDazzle%', 'JustFab%', 'FabKids%')
  AND LOWER(c.message_medium) = 'email'
GROUP BY IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
             REPLACE(LOWER(s.store_name), ' ', '_')),
         ebs.template_id,
         ebs.campaign_name,
         TO_DATE(ebs.created_at)
UNION
SELECT IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
           REPLACE(LOWER(s.store_name), ' ', '_')) AS store_group,
       ebs.template_id                             AS campaign_id,
       ebs.campaign_name                           AS campaign_name,
       TO_DATE(ebs.created_at)                     AS send_date,
       COUNT(DISTINCT u.user_id)                   AS open_count
FROM iterable.org_3620.email_opens_view ebs
         LEFT JOIN iterable.org_3620.campaigns c
                   ON ebs.campaign_id = c.id
         LEFT JOIN iterable.org_3620.users u
                   ON ebs.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model.dim_store AS s
                   ON cust.store_id = s.store_id
WHERE TO_DATE(ebs.created_at) > $start_date
  AND TO_DATE(ebs.created_at) <= $end_date
  AND s.store_name ILIKE ANY ('ShoeDazzle%', 'JustFab%', 'FabKids%')
  AND LOWER(c.message_medium) = 'email'
GROUP BY IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
             REPLACE(LOWER(s.store_name), ' ', '_')),
         ebs.template_id,
         ebs.campaign_name,
         TO_DATE(ebs.created_at);

CREATE OR REPLACE TEMPORARY TABLE _email_bounces AS
SELECT IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
           REPLACE(LOWER(s.store_name), ' ', '_'))                                                 AS store_group,
       ebs.template_id                                                                             AS campaign_id,
       ebs.campaign_name                                                                           AS campaign_name,
       TO_DATE(ebs.created_at)                                                                     AS send_date,
       COUNT(DISTINCT CASE WHEN LOWER(ebs.recipient_state) ILIKE 'SoftBounce%' THEN u.user_id END) AS soft_bounce_count,
       COUNT(DISTINCT CASE WHEN LOWER(ebs.recipient_state) = 'HardBounce' THEN u.user_id END)      AS hard_bounce_count,
       COUNT(DISTINCT CASE WHEN LOWER(ebs.recipient_state) ILIKE 'MailBlock%' THEN u.user_id END)  AS block_count
FROM campaign_event_data.org_3223.email_bounces_view ebs
         LEFT JOIN campaign_event_data.org_3223.campaigns c
                   ON ebs.campaign_id = c.id
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON ebs.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model.dim_store AS s
                   ON cust.store_id = s.store_id
WHERE TO_DATE(ebs.created_at) > $start_date
  AND TO_DATE(ebs.created_at) <= $end_date
  AND s.store_name ILIKE ANY ('ShoeDazzle%', 'JustFab%', 'FabKids%')
  AND LOWER(c.message_medium) = 'email'
GROUP BY IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
             REPLACE(LOWER(s.store_name), ' ', '_')),
         ebs.template_id,
         ebs.campaign_name,
         TO_DATE(ebs.created_at)
UNION
SELECT IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
           REPLACE(LOWER(s.store_name), ' ', '_'))                                                 AS store_group,
       ebs.template_id                                                                             AS campaign_id,
       ebs.campaign_name                                                                           AS campaign_name,
       TO_DATE(ebs.created_at)                                                                     AS send_date,
       COUNT(DISTINCT CASE WHEN LOWER(ebs.recipient_state) ILIKE 'SoftBounce%' THEN u.user_id END) AS soft_bounce_count,
       COUNT(DISTINCT CASE WHEN LOWER(ebs.recipient_state) = 'HardBounce' THEN u.user_id END)      AS hard_bounce_count,
       COUNT(DISTINCT CASE WHEN LOWER(ebs.recipient_state) ILIKE 'MailBlock%' THEN u.user_id END)  AS block_count
FROM iterable.org_3620.email_bounces_view ebs
         LEFT JOIN iterable.org_3620.campaigns c
                   ON ebs.campaign_id = c.id
         LEFT JOIN iterable.org_3620.users u
                   ON ebs.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model.dim_store AS s
                   ON cust.store_id = s.store_id
WHERE TO_DATE(ebs.created_at) > $start_date
  AND TO_DATE(ebs.created_at) <= $end_date
  AND s.store_name ILIKE ANY ('ShoeDazzle%', 'JustFab%', 'FabKids%')
  AND LOWER(c.message_medium) = 'email'
GROUP BY IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
             REPLACE(LOWER(s.store_name), ' ', '_')),
         ebs.template_id,
         ebs.campaign_name,
         TO_DATE(ebs.created_at);

CREATE OR REPLACE TEMPORARY TABLE _email_unsubscribes AS
SELECT IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
           REPLACE(LOWER(s.store_name), ' ', '_')) AS store_group,
       ebs.template_id                             AS campaign_id,
       ebs.campaign_name                           AS campaign_name,
       TO_DATE(ebs.created_at)                     AS send_date,
       COUNT(DISTINCT u.user_id)                   AS unsubscribe_count
FROM campaign_event_data.org_3223.email_unsubscribes_view ebs
         LEFT JOIN campaign_event_data.org_3223.campaigns c
                   ON ebs.campaign_id = c.id
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON ebs.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model.dim_store AS s
                   ON cust.store_id = s.store_id
WHERE TO_DATE(ebs.created_at) > $start_date
  AND TO_DATE(ebs.created_at) <= $end_date
  AND s.store_name ILIKE ANY ('ShoeDazzle%', 'JustFab%', 'FabKids%')
  AND LOWER(c.message_medium) = 'email'
GROUP BY IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
             REPLACE(LOWER(s.store_name), ' ', '_')),
         ebs.template_id,
         ebs.campaign_name,
         TO_DATE(ebs.created_at)
UNION
SELECT IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
           REPLACE(LOWER(s.store_name), ' ', '_')) AS store_group,
       ebs.template_id                             AS campaign_id,
       ebs.campaign_name                           AS campaign_name,
       TO_DATE(ebs.created_at)                     AS send_date,
       COUNT(DISTINCT u.user_id)                   AS unsubscribe_count
FROM iterable.org_3620.email_unsubscribes_view ebs
         LEFT JOIN iterable.org_3620.campaigns c
                   ON ebs.campaign_id = c.id
         LEFT JOIN iterable.org_3620.users u
                   ON ebs.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model.dim_store AS s
                   ON cust.store_id = s.store_id
WHERE TO_DATE(ebs.created_at) > $start_date
  AND TO_DATE(ebs.created_at) <= $end_date
  AND s.store_name ILIKE ANY ('ShoeDazzle%', 'JustFab%', 'FabKids%')
  AND LOWER(c.message_medium) = 'email'
GROUP BY IFF(LOWER(s.store_name) IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(LOWER(s.store_name), '_', 'us'),
             REPLACE(LOWER(s.store_name), ' ', '_')),
         ebs.template_id,
         ebs.campaign_name,
         TO_DATE(ebs.created_at);

CREATE OR REPLACE TEMPORARY TABLE _email_metrics AS
SELECT s.store_brand,
       s.campaign_name,
       s.campaign_id,
       s.subject,
       s.version_name,
       s.send_date,
       IFNULL(s.send_count, 0)        AS send_count,
       IFNULL(o.open_count, 0)        AS open_count,
       IFNULL(c.click_count, 0)       AS click_count,
       IFNULL(b.soft_bounce_count, 0) AS soft_bounce_count,
       IFNULL(b.hard_bounce_count, 0) AS hard_bounce_count,
       IFNULL(b.block_count, 0)       AS block_count,
       IFNULL(u.unsubscribe_count, 0) AS unsubscribe_count
FROM _email_sends s
         LEFT JOIN _email_clicks c
                   ON s.campaign_id = c.campaign_id AND s.send_date = c.send_date AND s.store_brand = c.store_brand
         LEFT JOIN _email_opens o
                   ON s.campaign_id = o.campaign_id AND s.store_brand = o.store_group AND s.send_date = o.send_date
         LEFT JOIN _email_bounces b
                   ON s.campaign_id = b.campaign_id AND s.store_brand = b.store_group AND s.send_date = b.send_date
         LEFT JOIN _email_unsubscribes u
                   ON s.campaign_id = u.campaign_id AND s.store_brand = u.store_group AND s.send_date = u.send_date;

CREATE OR REPLACE TEMPORARY TABLE _email_metrics_mkt AS
SELECT s.store_brand,
       s.campaign_name,
       s.campaign_id,
       s.subject,
       s.version_name,
       s.send_date,
       s.send_count,
       o.open_count,
       c.click_count,
       b.soft_bounce_count,
       b.hard_bounce_count,
       b.block_count,
       u.unsubscribe_count
FROM (SELECT store_brand,
             campaign_id,
             campaign_name,
             subject,
             version_name,
             MIN(send_date)  AS send_date,
             SUM(send_count) AS send_count
      FROM _email_sends
      WHERE campaign_name ILIKE 'mkt_%'
      GROUP BY store_brand,
               campaign_id,
               campaign_name,
               subject,
               version_name) s
         LEFT JOIN (SELECT store_brand,
                           campaign_id,
                           campaign_name,
                           SUM(click_count) AS click_count
                    FROM _email_clicks
                    WHERE campaign_name ILIKE 'mkt_%'
                    GROUP BY store_brand,
                             campaign_id,
                             campaign_name) c
                   ON s.campaign_id = c.campaign_id AND s.store_brand = c.store_brand
         LEFT JOIN (SELECT store_group,
                           campaign_id,
                           SUM(open_count) AS open_count
                    FROM _email_opens
                    WHERE campaign_name ILIKE 'mkt_%'
                    GROUP BY store_group,
                             campaign_id) o
                   ON s.campaign_id = o.campaign_id AND s.store_brand = o.store_group
         LEFT JOIN (SELECT store_group,
                           campaign_id,
                           SUM(soft_bounce_count) AS soft_bounce_count,
                           SUM(hard_bounce_count) AS hard_bounce_count,
                           SUM(block_count)       AS block_count
                    FROM _email_bounces
                    WHERE campaign_name ILIKE 'mkt_%'
                    GROUP BY store_group,
                             campaign_id) b
                   ON s.campaign_id = b.campaign_id AND s.store_brand = b.store_group
         LEFT JOIN (SELECT store_group,
                           campaign_id,
                           SUM(unsubscribe_count) AS unsubscribe_count
                    FROM _email_unsubscribes
                    WHERE campaign_name ILIKE 'mkt_%'
                    GROUP BY store_group,
                             campaign_id) u
                   ON s.campaign_id = u.campaign_id AND s.store_brand = u.store_group;


CREATE OR REPLACE TEMPORARY TABLE _push_sends_campaigns AS
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM campaign_event_data.org_3223.push_sends_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM iterable.org_3620.push_sends_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _push_sends_delta
AS
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'push'                                    AS type,
       ps.created_at                             AS event_time,
       'lake.iterable.push_sends'                AS source_table,
       ps.template_id                            AS campaign_id,
       ps.template_name                          AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(ps.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(ps.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM campaign_event_data.org_3223.push_sends_view AS ps
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON ps.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE ps.created_at > $start_date
  AND ps.created_at <= $end_date
  AND ps.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'push'                                    AS type,
       ps.created_at                             AS event_time,
       'lake.iterable.push_sends'                AS source_table,
       ps.template_id                            AS campaign_id,
       ps.template_name                          AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(ps.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(ps.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.push_sends_view AS ps
         LEFT JOIN iterable.org_3620.users u
                   ON ps.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE ps.created_at > $start_date
  AND ps.created_at <= $end_date
  AND ps.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _push_sends AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(p.campaign_name, m.campaign_name)                        AS campaign_name,
       m.campaign_id,
       subject,
       version_name,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS send_count
FROM _push_sends_delta m
         LEFT JOIN _push_sends_campaigns p
                   ON m.campaign_id = p.campaign_id
WHERE type = 'push'
  AND store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND event_time > $start_date
  AND event_time <= $end_date
GROUP BY IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'), store_brand),
         COALESCE(p.campaign_name, m.campaign_name),
         m.campaign_id,
         subject,
         version_name,
         TO_DATE(event_time);

CREATE OR REPLACE TEMPORARY TABLE _push_clicks_campaigns AS
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM campaign_event_data.org_3223.push_opens_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM iterable.org_3620.push_opens_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _push_opens_delta
AS
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'push'                                    AS type,
       po.created_at                             AS event_time,
       'lake.iterable.push_opens'                AS source_table,
       po.template_id                            AS campaign_id,
       po.template_name                          AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(po.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(po.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM campaign_event_data.org_3223.push_opens_view AS po
         LEFT JOIN campaign_event_data.org_3223.campaigns c
                   ON po.campaign_id = c.id
         LEFT JOIN campaign_event_data.org_3223.users u
                   ON po.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON edw_prod.stg.udf_unconcat_brand(cust.customer_id) = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE po.created_at > $start_date
  AND po.created_at <= $end_date
  AND po.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'push'                                    AS type,
       po.created_at                             AS event_time,
       'lake.iterable.push_opens'                AS source_table,
       po.template_id                            AS campaign_id,
       po.template_name                          AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(po.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(po.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.push_opens_view AS po
         LEFT JOIN iterable.org_3620.campaigns c
                   ON po.campaign_id = c.id
         LEFT JOIN iterable.org_3620.users u
                   ON po.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON edw_prod.stg.udf_unconcat_brand(cust.customer_id) = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE po.created_at > $start_date
  AND po.created_at <= $end_date
  AND po.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _push_clicks AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(p.campaign_name, m.campaign_name)                        AS campaign_name,
       m.campaign_id,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS click_count
FROM _push_opens_delta m
         LEFT JOIN _push_clicks_campaigns p
                   ON m.campaign_id = p.campaign_id
WHERE type = 'push'
  AND store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND event_time > $start_date
  AND event_time <= $end_date
GROUP BY IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'), store_brand),
         COALESCE(p.campaign_name, m.campaign_name),
         m.campaign_id,
         TO_DATE(event_time);

CREATE OR REPLACE TEMPORARY TABLE _push_campaign_metrics AS
SELECT s.store_brand,
       s.campaign_name,
       s.campaign_id,
       s.subject,
       s.version_name,
       s.send_date,
       IFNULL(s.send_count, 0)  AS send_count,
       IFNULL(c.click_count, 0) AS click_count
FROM _push_sends s
         LEFT JOIN _push_clicks c
                   ON s.campaign_id = c.campaign_id AND s.send_date = c.send_date AND s.store_brand = c.store_brand;

CREATE OR REPLACE TEMPORARY TABLE _push_campaign_metrics_mkt AS
SELECT s.store_brand,
       s.campaign_name,
       s.campaign_id,
       s.subject,
       s.version_name,
       s.send_date,
       s.send_count,
       c.click_count
FROM (SELECT store_brand,
             campaign_id,
             campaign_name,
             subject,
             version_name,
             MIN(send_date)  AS send_date,
             SUM(send_count) AS send_count
      FROM _push_sends
      WHERE campaign_name ILIKE 'mkt_%'
      GROUP BY store_brand,
               campaign_id,
               campaign_name,
               subject,
               version_name) s
         LEFT JOIN (SELECT store_brand,
                           campaign_id,
                           campaign_name,
                           SUM(click_count) AS click_count
                    FROM _push_clicks
                    WHERE campaign_name ILIKE 'mkt_%'
                    GROUP BY store_brand,
                             campaign_id,
                             campaign_name) c
                   ON s.campaign_id = c.campaign_id AND s.store_brand = c.store_brand;

CREATE OR REPLACE TEMPORARY TABLE _new_clicks AS
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'email'                                   AS type,
       es.created_at                             AS event_time,
       'lake.iterable.email_clicks'              AS source_table,
       es.template_id                            AS campaign_id,
       es.template_name                          AS campaign_name,
       es.email_subject                          AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(es.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(es.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.email_clicks_view AS es
         LEFT JOIN iterable.org_3620.users u
                   ON es.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE es.created_at > $start_date
  AND es.created_at <= $end_date
  AND es.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       NULL                                      AS contact_id,
       'push'                                    AS type,
       po.created_at                             AS event_time,
       'lake.iterable.push_opens'                AS source_table,
       po.template_id                            AS campaign_id,
       po.template_name                          AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(po.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(po.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.push_opens_view AS po
         LEFT JOIN iterable.org_3620.campaigns c
                   ON po.campaign_id = c.id
         LEFT JOIN iterable.org_3620.users u
                   ON po.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON edw_prod.stg.udf_unconcat_brand(cust.customer_id) = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE po.created_at > $start_date
  AND po.created_at <= $end_date
  AND po.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')
UNION
SELECT TRY_TO_NUMBER(u.user_id)                  AS customer_id,
       s.store_group_id                          AS store_group_id,
       REPLACE(LOWER(csm.store_group), ' ', '_') AS store_brand,
       sms.from_phone_number_id::VARCHAR         AS contact_id,
       'sms'                                     AS type,
       sms.created_at                            AS event_time,
       'lake.iterable.sms_clicks'                AS source_table,
       sms.template_id                           AS campaign_id,
       sms.template_name                         AS campaign_name,
       NULL                                      AS subject,
       IFF(LENGTH(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')) = 1,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(sms.template_name, '[^\\x20-\\x7E]', '', 1), '[^_]*$')),
           NULL)                                 AS version_name
FROM iterable.org_3620.sms_clicks_view AS sms
         LEFT JOIN iterable.org_3620.users u
                   ON sms.itbl_user_id = u.itbl_user_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cust
                   ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
         LEFT JOIN edw_prod.data_model_jfb.dim_store s
                   ON cust.store_id = s.store_id
         LEFT JOIN reporting_prod.gfb.customer_store_mapping csm
                   ON csm.store_group_id = s.store_group_id
WHERE sms.created_at > $start_date
  AND sms.created_at <= $end_date
  AND sms.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _crm_orders AS
SELECT DISTINCT olp.order_id,
                CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ                           AS order_datetime,
                olp.customer_id,
                FIRST_VALUE(c.type)
                            OVER (PARTITION BY olp.order_id ORDER BY CAST(c.event_time AS DATETIME) DESC) AS campaign_source,
                FIRST_VALUE(c.campaign_id)
                            OVER (PARTITION BY olp.order_id ORDER BY CAST(c.event_time AS DATETIME) DESC) AS campaign_id,
                FIRST_VALUE(CAST(c.event_time AS DATETIME))
                            OVER (PARTITION BY olp.order_id ORDER BY CAST(c.event_time AS DATETIME) DESC) AS campaign_event_time,
                IFF(c.store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(c.store_brand, '_', 'us'),
                    c.store_brand)                                                                        AS store_name,
                fo.unit_count                                                                             AS quantity,
                IFNULL(fo.subtotal_excl_tariff_local_amount, 0) + IFNULL(fo.tariff_revenue_local_amount, 0) -
                IFNULL(fo.product_discount_local_amount, 0) +
                IFNULL(fo.shipping_revenue_before_discount_local_amount, 0) -
                IFNULL(fo.shipping_discount_local_amount, 0) - IFNULL(fo.non_cash_credit_local_amount, 0) AS revenue,
                IFF(token_count > 0, 1, 0)                                                                AS token_order,
                IFF(token_count > 0, revenue, 0)                                                          AS token_revenue,
                IFF(token_count = 0, 1, 0)                                                                AS cash_order,
                IFF(token_count = 0, revenue, 0)                                                          AS cash_revenue
FROM gfb.gfb_order_line_data_set_place_date olp
         JOIN edw_prod.data_model_jfb.fact_order fo
              ON fo.order_id = olp.order_id
         JOIN (SELECT campaign_id,
                      customer_id,
                      type,
                      store_brand,
                      CASE
                          WHEN source_table = 'med_db_staging.attentive.vw_attentive_sms'
                              THEN CONVERT_TIMEZONE('UTC', event_time)
                          ELSE event_time END AS event_time
               FROM shared.marketing_channels_customer_clicks
               WHERE source_table <> 'lake.sailthru.data_exporter_message_blast'
                 AND event_time > $start_date
                 AND event_time <= $end_date
               UNION
               SELECT campaign_id,
                      customer_id,
                      type,
                      store_brand,
                      event_time
               FROM _new_clicks
               WHERE event_time > $start_date
                 AND event_time <= $end_date
) c
              ON c.customer_id = olp.customer_id
                  AND CAST(c.event_time AS DATETIME) < CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ
                  AND (DATEDIFF(DAY, CAST(c.event_time AS DATETIME),
                                CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ) BETWEEN 0 AND 6)
WHERE olp.order_classification = 'product order';

CREATE OR REPLACE TEMPORARY TABLE _bill_me_now AS
SELECT DISTINCT fo.customer_id,
                CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ                          AS order_datetime,
                dosc.is_bill_me_now_online,
                dosc.is_bill_me_now_gms,
                FIRST_VALUE(c.type)
                            OVER (PARTITION BY fo.order_id ORDER BY CAST(c.event_time AS DATETIME) DESC) AS campaign_source,
                FIRST_VALUE(c.campaign_id)
                            OVER (PARTITION BY fo.order_id ORDER BY CAST(c.event_time AS DATETIME) DESC) AS campaign_id,
                FIRST_VALUE(CAST(c.event_time AS DATETIME))
                            OVER (PARTITION BY fo.order_id ORDER BY CAST(c.event_time AS DATETIME) DESC) AS campaign_event_time,
                IFF(c.store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(c.store_brand, '_', 'us'),
                    c.store_brand)                                                                       AS store_name,
FROM gfb.gfb_order_line_data_set_place_date olp
         JOIN edw_prod.data_model_jfb.fact_order fo
              ON fo.order_id = olp.order_id
         JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
              ON dosc.order_sales_channel_key = fo.order_sales_channel_key
                  AND dosc.is_border_free_order = 0
                  AND dosc.is_ps_order = 0
                  AND dosc.is_test_order = 0
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = fo.store_id
         JOIN (SELECT campaign_id,
                      customer_id,
                      type,
                      store_brand,
                      CASE
                          WHEN source_table = 'med_db_staging.attentive.vw_attentive_sms'
                              THEN CONVERT_TIMEZONE('UTC', event_time)
                          ELSE event_time END AS event_time
               FROM reporting_prod.shared.marketing_channels_customer_clicks
               WHERE source_table <> 'lake.sailthru.data_exporter_message_blast'
                 AND event_time > $start_date
                 AND event_time <= $end_date) c
              ON c.customer_id = fo.customer_id
                  AND CAST(c.event_time AS DATETIME) < CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ
                  AND (DATEDIFF(DAY, CAST(c.event_time AS DATETIME),
                                CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ) BETWEEN 0 AND 5)
WHERE olp.order_classification = 'credit billing'
  AND (dosc.is_bill_me_now_gms = TRUE OR dosc.is_bill_me_now_online = TRUE)
  AND (DAY(fo.order_local_datetime) BETWEEN 1 AND 5);

CREATE OR REPLACE TEMPORARY TABLE _cancellations AS
SELECT *
FROM (SELECT fme.customer_id,
             CONVERT_TIMEZONE('UTC', fme.event_start_local_datetime::DATE)::TIMESTAMP_NTZ                                      AS cancellation_date,
             c.type                                                                                                            AS campaign_source,
             c.campaign_id,
             CAST(c.event_time AS DATETIME)                                                                                    AS campaign_event_time,
             IFF(c.store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(c.store_brand, '_', 'us'),
                 c.store_brand)                                                                                                AS store_name,
             ROW_NUMBER() OVER (PARTITION BY fme.customer_id, fme.event_start_local_datetime::DATE ORDER BY c.event_time DESC) AS rn
      FROM edw_prod.data_model_jfb.fact_membership_event fme
               JOIN gfb.vw_store st
                    ON st.store_id = fme.store_id
               JOIN (SELECT campaign_id,
                            customer_id,
                            type,
                            store_brand,
                            CASE
                                WHEN source_table = 'med_db_staging.attentive.vw_attentive_sms'
                                    THEN CONVERT_TIMEZONE('UTC', event_time)
                                ELSE event_time
                                END AS event_time
                     FROM reporting_prod.shared.marketing_channels_customer_clicks
                     WHERE source_table <> 'lake.sailthru.data_exporter_message_blast'
                       AND event_time >= $start_date
                       AND event_time <= $end_date) c
                    ON c.customer_id = fme.customer_id
                        AND CAST(c.event_time AS DATETIME) <= CONVERT_TIMEZONE('UTC', fme.event_start_local_datetime)::TIMESTAMP_NTZ
                        AND (DATEDIFF(DAY, CAST(c.event_time AS DATETIME),
                                      CONVERT_TIMEZONE('UTC', fme.event_start_local_datetime)::TIMESTAMP_NTZ) BETWEEN 0 AND 6)
      WHERE fme.membership_event_type = 'Cancellation') a
WHERE rn = 1;

CREATE OR REPLACE TEMPORARY TABLE _billed_through_credit AS
SELECT DISTINCT fo.order_id,
                CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ                          AS order_datetime,
                fo.customer_id,
                FIRST_VALUE(c.type)
                            OVER (PARTITION BY fo.order_id ORDER BY CAST(c.event_time AS DATETIME) DESC) AS campaign_source,
                FIRST_VALUE(c.campaign_id)
                            OVER (PARTITION BY fo.order_id ORDER BY CAST(c.event_time AS DATETIME) DESC) AS campaign_id,
                FIRST_VALUE(CAST(c.event_time AS DATETIME))
                            OVER (PARTITION BY fo.order_id ORDER BY CAST(c.event_time AS DATETIME) DESC) AS campaign_event_time,
                IFF(c.store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(c.store_brand, '_', 'us'),
                    c.store_brand)                                                                       AS store_name,
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         JOIN edw_prod.data_model_jfb.fact_order fo
              ON fo.order_id = olp.order_id
         JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
              ON dosc.order_sales_channel_key = fo.order_sales_channel_key
                  AND dosc.is_border_free_order = 0
                  AND dosc.is_ps_order = 0
                  AND dosc.is_test_order = 0
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = fo.store_id
         JOIN (SELECT campaign_id,
                      customer_id,
                      type,
                      store_brand,
                      CASE
                          WHEN source_table = 'med_db_staging.attentive.vw_attentive_sms'
                              THEN CONVERT_TIMEZONE('UTC', event_time)
                          ELSE event_time END AS event_time
               FROM reporting_prod.shared.marketing_channels_customer_clicks
               WHERE source_table <> 'lake.sailthru.data_exporter_message_blast'
                 AND event_time > $start_date
                 AND event_time <= $end_date) c
              ON c.customer_id = fo.customer_id
                  AND CAST(c.event_time AS DATETIME) < CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ
                  AND (DATEDIFF(DAY, CAST(c.event_time AS DATETIME),
                                CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ) BETWEEN 0 AND 6)
WHERE (dosc.is_bill_me_now_gms = FALSE AND dosc.is_bill_me_now_online = FALSE)
  AND olp.order_classification = 'credit billing'
  AND DAY(fo.order_local_datetime) >= 6;

CREATE OR REPLACE TEMPORARY TABLE _email_campaign_orders AS
SELECT store_name,
       campaign_id,
       campaign_event_time::DATE AS date,
       COUNT(order_id)           AS orders,
       SUM(revenue)              AS revenue,
       SUM(token_order)          AS token_sales,
       SUM(token_revenue)        AS token_revenue,
       SUM(cash_order)           AS cash_sales,
       SUM(cash_revenue)         AS cash_revenue
FROM _crm_orders
WHERE campaign_source = 'email'
  AND TO_DATE(campaign_event_time) > $start_date
GROUP BY store_name,
         campaign_id,
         campaign_event_time::DATE;

CREATE OR REPLACE TEMPORARY TABLE _sms_orders AS
SELECT store_name,
       campaign_id,
       campaign_event_time::DATE AS date,
       COUNT(order_id)           AS purchase,
       SUM(revenue)              AS revenue,
       SUM(token_order)          AS token_sales,
       SUM(token_revenue)        AS token_revenue,
       SUM(cash_order)           AS cash_sales,
       SUM(cash_revenue)         AS cash_revenue
FROM _crm_orders
WHERE campaign_source = 'sms'
  AND TO_DATE(campaign_event_time) > $start_date
GROUP BY store_name,
         campaign_id,
         campaign_event_time::DATE;

CREATE OR REPLACE TEMPORARY TABLE _push_campaign_orders AS
SELECT store_name,
       campaign_id,
       campaign_event_time::DATE AS date,
       COUNT(order_id)           AS orders,
       SUM(revenue)              AS revenue,
       SUM(token_order)          AS token_sales,
       SUM(token_revenue)        AS token_revenue,
       SUM(cash_order)           AS cash_sales,
       SUM(cash_revenue)         AS cash_revenue
FROM _crm_orders
WHERE campaign_source = 'push'
  AND TO_DATE(campaign_event_time) > $start_date
GROUP BY store_name,
         campaign_id,
         campaign_event_time::DATE;

CREATE OR REPLACE TEMPORARY TABLE billed_me_now AS
SELECT store_name,
       campaign_id,
       campaign_source,
       campaign_event_time::DATE   AS date,
       COUNT(DISTINCT customer_id) AS billed_me_now,
FROM _bill_me_now
WHERE TO_DATE(campaign_event_time) > $start_date
GROUP BY store_name,
         campaign_id,
         campaign_source,
         campaign_event_time::DATE;

CREATE OR REPLACE TEMPORARY TABLE cancelled AS
SELECT store_name,
       campaign_id,
       campaign_source,
       campaign_event_time::DATE   AS date,
       COUNT(DISTINCT customer_id) AS cancelled,
FROM _cancellations
WHERE TO_DATE(campaign_event_time) > $start_date
GROUP BY store_name,
         campaign_id,
         campaign_source,
         campaign_event_time::DATE;

CREATE OR REPLACE TEMPORARY TABLE credit_billings AS
SELECT store_name,
       campaign_id,
       campaign_source,
       campaign_event_time::DATE   AS date,
       COUNT(DISTINCT customer_id) AS credit_billings,
FROM _billed_through_credit
WHERE TO_DATE(campaign_event_time) > $start_date
GROUP BY store_name,
         campaign_id,
         campaign_source,
         campaign_event_time::DATE;

DELETE
FROM gfb.crm_weekly_kpi_report
WHERE send_date > $start_date;


INSERT INTO gfb.crm_weekly_kpi_report

/*
!!! UNCOMMENT IF YOU NEED TO LOAD THE TABLE FIRST TIME!!!
!!! HISTORICAL DATA!!!

SELECT CURRENT_TIMESTAMP() AS update_time,
       store_name,
       campaign_id,
       campaign_name,
       subject,
       email_list,
       abtest_segment,
       send_date,
       revenue,
       unique_clicks,
       opened_user_count,
       estimated_opens,
       sent,
       opt_out,
       purchases,
       hard_bounce,
       soft_bounce,
       spam_reports,
       activating_orders,
       repeat_orders,
       activating_rev,
       repeat_rev,
       type,
       branch,
       token_revenue,
       cash_revenue,
       token_sales,
       cash_sales,
       null            AS billed_me_now,
       null            AS cancelled,
       null            AS credit_billings
FROM gfb.crm_weekly_kpi_report_historical

UNION ALL

SELECT CURRENT_TIMESTAMP() AS update_time,
       store_name,
       campaign_id,
       campaign_name,
       subject,
       email_list,
       abtest_segment,
       send_date,
       revenue,
       unique_clicks,
       opened_user_count,
       estimated_opens,
       sent,
       opt_out,
       purchases,
       hard_bounce,
       soft_bounce,
       spam_reports,
       activating_orders,
       repeat_orders,
       activating_rev,
       repeat_rev,
       type,
       'branch 1'          AS branch,
       null                AS token_revenue,
       null                AS cash_revenue,
       null                AS token_sales,
       null                AS cash_sales,
       null                AS billed_me_now,
       null                AS cancelled,
       null                AS credit_billings
FROM gfb.sailthru_campaign_data
UNION ALL
*/
SELECT DISTINCT CURRENT_TIMESTAMP()                                                 AS update_time,
                m.store_brand                                                       AS store_name,
                m.campaign_id,
                COALESCE(f.new_campaign_name, r.new_campaign_name, m.campaign_name) AS campaign_name,
                m.subject,
                'Email_List'                                                        AS email_list,
                m.version_name                                                      AS abtest_segment,
                m.send_date,
                NVL(a.revenue, 0)                                                   AS revenue,
                m.click_count                                                       AS unique_clicks,
                m.open_count                                                        AS opened_user_count,
                m.open_count                                                        AS estimated_opens,
                m.send_count                                                        AS sent,
                m.unsubscribe_count                                                 AS opt_out,
                NVL(a.orders, 0)                                                    AS purchases,
                m.hard_bounce_count                                                 AS hard_bounce,
                m.soft_bounce_count                                                 AS soft_bounce,
                m.block_count                                                       AS spam_reports,
                0                                                                   AS activating_orders,
                0                                                                   AS repeat_orders,
                0                                                                   AS activating_rev,
                0                                                                   AS repeat_rev,
                'Email'                                                             AS type,
                'branch 2'                                                          AS branch,
                NVL(a.token_revenue, 0)                                             AS token_revenue,
                NVL(a.cash_revenue, 0)                                              AS cash_revenue,
                NVL(a.token_sales, 0)                                               AS token_sales,
                NVL(a.cash_sales, 0)                                                AS cash_sales,
                NVL(bmw.billed_me_now, 0)                                           AS billed_me_now,
                NVL(ca.cancelled, 0)                                                AS cancelled,
                NVL(bc.credit_billings, 0)                                          AS credit_billings

FROM _email_metrics_mkt m
         JOIN (SELECT ma.campaign_id,
                      ma.send_date,
                      SUM(NVL(revenue, 0))       AS revenue,
                      SUM(NVL(orders, 0))        AS orders,
                      SUM(NVL(token_sales, 0))   AS token_sales,
                      SUM(NVL(cash_sales, 0))    AS cash_sales,
                      SUM(NVL(token_revenue, 0)) AS token_revenue,
                      SUM(NVL(cash_revenue, 0))  AS cash_revenue
               FROM _email_metrics_mkt ma
                        LEFT JOIN _email_campaign_orders c
                                  ON ma.campaign_id::STRING = c.campaign_id::STRING
                                      AND ma.send_date <= c.date
                                      AND (DATEDIFF(DAY, ma.send_date, c.date) BETWEEN 0 AND 6)
                                      AND LOWER(ma.store_brand) = LOWER(c.store_name)
               WHERE ma.send_date > $start_date
               GROUP BY ma.campaign_id, ma.send_date) a
              ON a.campaign_id = m.campaign_id AND a.send_date = m.send_date
         LEFT JOIN billed_me_now bmw
                   ON m.campaign_id = bmw.campaign_id
                       AND m.send_date = bmw.date
                       AND bmw.campaign_source = 'email'
                       AND LOWER(m.store_brand) = LOWER(bmw.store_name)
         LEFT JOIN cancelled ca
                   ON m.campaign_id = ca.campaign_id
                       AND m.send_date = ca.date
                       AND ca.campaign_source = 'email'
                       AND LOWER(m.store_brand) = LOWER(ca.store_name)
         LEFT JOIN credit_billings bc
                   ON m.campaign_id = bc.campaign_id
                       AND m.send_date = bc.date
                       AND bc.campaign_source = 'email'
                       AND LOWER(m.store_brand) = LOWER(bc.store_name)
         LEFT JOIN gfb.crm_master_campaign_lookup r
                   ON m.campaign_name = r.old_campaign_name
         LEFT JOIN gfb.fk_crm_master_campaign_lookup f
                   ON m.campaign_name = f.old_campaign_name
WHERE m.campaign_name ILIKE 'mkt_%'
  AND m.send_date > $start_date

UNION ALL

SELECT DISTINCT CURRENT_TIMESTAMP()                                                 AS update_time,
                m.store_brand                                                       AS store_name,
                m.campaign_id,
                COALESCE(f.new_campaign_name, r.new_campaign_name, m.campaign_name) AS campaign_name,
                m.subject,
                'Email_List'                                                        AS email_list,
                m.version_name                                                      AS abtest_segment,
                m.send_date,
                NVL(revenue, 0)                                                     AS revenue,
                m.click_count                                                       AS unique_clicks,
                m.open_count                                                        AS opened_user_count,
                m.open_count                                                        AS estimated_opens,
                m.send_count                                                        AS sent,
                m.unsubscribe_count                                                 AS opt_out,
                NVL(orders, 0)                                                      AS purchases,
                m.hard_bounce_count                                                 AS hard_bounce,
                m.soft_bounce_count                                                 AS soft_bounce,
                m.block_count                                                       AS spam_reports,
                0                                                                   AS activating_orders,
                0                                                                   AS repeat_orders,
                0                                                                   AS activating_rev,
                0                                                                   AS repeat_rev,
                'Email'                                                             AS type,
                'branch 2'                                                          AS branch,
                NVL(token_revenue, 0)                                               AS token_revenue,
                NVL(cash_revenue, 0)                                                AS cash_revenue,
                NVL(token_sales, 0)                                                 AS token_sales,
                NVL(cash_sales, 0)                                                  AS cash_sales,
                NVL(bmw.billed_me_now, 0)                                           AS billed_me_now,
                NVL(ca.cancelled, 0)                                                AS cancelled,
                NVL(bc.credit_billings, 0)                                          AS credit_billings
FROM _email_metrics m
         LEFT JOIN _email_campaign_orders c
                   ON m.campaign_id::STRING = c.campaign_id::STRING AND m.send_date = c.date
                       AND LOWER(m.store_brand) = LOWER(c.store_name)
         LEFT JOIN billed_me_now bmw
                   ON m.campaign_id = bmw.campaign_id
                       AND m.send_date = bmw.date
                       AND bmw.campaign_source = 'email'
                       AND LOWER(m.store_brand) = LOWER(bmw.store_name)
         LEFT JOIN cancelled ca
                   ON m.campaign_id = ca.campaign_id
                       AND m.send_date = ca.date
                       AND ca.campaign_source = 'email'
                       AND LOWER(m.store_brand) = LOWER(ca.store_name)
         LEFT JOIN credit_billings bc
                   ON m.campaign_id = bc.campaign_id
                       AND m.send_date = bc.date
                       AND bc.campaign_source = 'email'
                       AND LOWER(m.store_brand) = LOWER(bc.store_name)
         LEFT JOIN gfb.crm_master_campaign_lookup r
                   ON m.campaign_name = r.old_campaign_name
         LEFT JOIN gfb.fk_crm_master_campaign_lookup f
                   ON m.campaign_name = f.old_campaign_name
WHERE m.campaign_name NOT ILIKE 'mkt_%'
  AND m.send_date > $start_date

UNION ALL
/*
   !!! UNCOMMENT IF YOU NEED TO LOAD THE TABLE FIRST TIME!!!
SELECT CURRENT_TIMESTAMP() AS update_time,
       store_name,
       template_id,
       template_name,
       subject,
       email_list,
       abtest_segment,
       date,
       rev,
       unique_clicks,
       opened_user_count,
       estopens,
       delivered,
       optout,
       purchase,
       hard_bounce,
       soft_bounce,
       spam_reports,
       activating_orders,
       repeat_orders,
       activating_rev,
       repeat_rev,
       type,
       'branch 3'          AS branch,
       null                AS token_revenue,
       null                AS cash_revenue,
       null                AS token_sales,
       null                AS cash_sales,
       null                AS billed_me_now,
       null                AS cancelled,
       null                AS credit_billings
FROM gfb.sailthru_trigger_data

UNION ALL
*/
SELECT CURRENT_TIMESTAMP()                                                  AS update_time,
       store_brand                                                          AS store_name,
       sc.campaign_id,
       COALESCE(f.new_campaign_name, r.new_campaign_name, sc.campaign_name) AS campaign_name,
       sc.subject,
       'EMAIL_LIST'                                                         AS email_list,
       'ABTEST_SEGMENT'                                                     AS abtest_segment,
       TO_DATE(sc.send_date)                                                AS date,
       a.revenue                                                            AS rev,
       sc.click_count                                                       AS unique_click,
       0                                                                    AS opened_user_count,
       0                                                                    AS estopens,
       sc.send_count                                                        AS delivered,
       sc.optouts                                                           AS optout,
       a.purchase                                                           AS purchase,
       0                                                                    AS hard_bounce,
       0                                                                    AS soft_bounce,
       0                                                                    AS spam_reports,
       0                                                                    AS activating_orders,
       0                                                                    AS repeat_orders,
       0                                                                    AS activating_rev,
       0                                                                    AS repeat_rev,
       'SMS'                                                                AS type,
       'branch 4'                                                           AS branch,
       NVL(a.token_revenue, 0)                                              AS token_revenue,
       NVL(a.cash_revenue, 0)                                               AS cash_revenue,
       NVL(a.token_sales, 0)                                                AS token_sales,
       NVL(a.cash_sales, 0)                                                 AS cash_sales,
       NVL(bmw.billed_me_now, 0)                                            AS billed_me_now,
       NVL(ca.cancelled, 0)                                                 AS cancelled,
       NVL(bc.credit_billings, 0)                                           AS credit_billings
FROM _attentive_sms_metrics_mkt sc
         JOIN (SELECT s.campaign_id,
                      s.send_date,
                      SUM(revenue)               AS revenue,
                      SUM(purchase)              AS purchase,
                      SUM(NVL(token_sales, 0))   AS token_sales,
                      SUM(NVL(cash_sales, 0))    AS cash_sales,
                      SUM(NVL(token_revenue, 0)) AS token_revenue,
                      SUM(NVL(cash_revenue, 0))  AS cash_revenue
               FROM _attentive_sms_metrics_mkt s
                        LEFT JOIN _sms_orders so
                                  ON TRY_TO_NUMBER(s.campaign_id) = TRY_TO_NUMBER(so.campaign_id)
                                      AND s.send_date <= so.date
                                      AND (DATEDIFF(DAY, s.send_date, so.date) BETWEEN 0 AND 6)
                                      AND s.store_brand = LOWER(so.store_name)
               WHERE s.send_date > $start_date
               GROUP BY s.campaign_id, s.send_date) a
              ON sc.campaign_id = a.campaign_id AND sc.send_date = a.send_date
         LEFT JOIN billed_me_now bmw
                   ON sc.campaign_id = bmw.campaign_id
                       AND sc.send_date = bmw.date
                       AND bmw.campaign_source = 'sms'
                       AND LOWER(sc.store_brand) = LOWER(bmw.store_name)
         LEFT JOIN cancelled ca
                   ON sc.campaign_id = ca.campaign_id
                       AND sc.send_date = ca.date
                       AND ca.campaign_source = 'sms'
                       AND LOWER(sc.store_brand) = LOWER(ca.store_name)
         LEFT JOIN credit_billings bc
                   ON sc.campaign_id = bc.campaign_id
                       AND sc.send_date = bc.date
                       AND bc.campaign_source = 'sms'
                       AND LOWER(sc.store_brand) = LOWER(bc.store_name)
         LEFT JOIN gfb.crm_master_campaign_lookup r
                   ON sc.campaign_name = r.old_campaign_name
         LEFT JOIN gfb.fk_crm_master_campaign_lookup f
                   ON sc.campaign_name = f.old_campaign_name
WHERE sc.campaign_name ILIKE 'mkt_%'
  AND sc.send_date > $start_date

UNION ALL

SELECT CURRENT_TIMESTAMP()                                                  AS update_time,
       store_brand                                                          AS store_name,
       sc.campaign_id,
       COALESCE(f.new_campaign_name, r.new_campaign_name, sc.campaign_name) AS campaign_name,
       sc.subject,
       'EMAIL_LIST'                                                         AS email_list,
       'ABTEST_SEGMENT'                                                     AS abtest_segment,
       TO_DATE(sc.send_date)                                                AS date,
       so.revenue                                                           AS rev,
       sc.click_count                                                       AS unique_click,
       0                                                                    AS opened_user_count,
       0                                                                    AS estopens,
       sc.send_count                                                        AS delivered,
       sc.optouts                                                           AS optout,
       so.purchase                                                          AS purchase,
       0                                                                    AS hard_bounce,
       0                                                                    AS soft_bounce,
       0                                                                    AS spam_reports,
       0                                                                    AS activating_orders,
       0                                                                    AS repeat_orders,
       0                                                                    AS activating_rev,
       0                                                                    AS repeat_rev,
       'SMS'                                                                AS type,
       'branch 4'                                                           AS branch,
       NVL(so.token_revenue, 0)                                             AS token_revenue,
       NVL(so.cash_revenue, 0)                                              AS cash_revenue,
       NVL(so.token_sales, 0)                                               AS token_sales,
       NVL(so.cash_sales, 0)                                                AS cash_sales,
       NVL(bmw.billed_me_now, 0)                                            AS billed_me_now,
       NVL(ca.cancelled, 0)                                                 AS cancelled,
       NVL(bc.credit_billings, 0)                                           AS credit_billings
FROM _attentive_sms_metrics sc
         LEFT JOIN _sms_orders so
                   ON TRY_TO_NUMBER(sc.campaign_id) = TRY_TO_NUMBER(so.campaign_id)
                       AND sc.send_date = so.date AND sc.store_brand = LOWER(so.store_name)
         LEFT JOIN billed_me_now bmw
                   ON sc.campaign_id = bmw.campaign_id
                       AND sc.send_date = bmw.date
                       AND bmw.campaign_source = 'sms'
                       AND LOWER(sc.store_brand) = LOWER(bmw.store_name)
         LEFT JOIN cancelled ca
                   ON sc.campaign_id = ca.campaign_id
                       AND sc.send_date = ca.date
                       AND ca.campaign_source = 'sms'
                       AND LOWER(sc.store_brand) = LOWER(ca.store_name)
         LEFT JOIN credit_billings bc
                   ON sc.campaign_id = bc.campaign_id
                       AND sc.send_date = bc.date
                       AND bc.campaign_source = 'sms'
                       AND LOWER(sc.store_brand) = LOWER(bc.store_name)
         LEFT JOIN gfb.crm_master_campaign_lookup r
                   ON sc.campaign_name = r.old_campaign_name
         LEFT JOIN gfb.fk_crm_master_campaign_lookup f
                   ON sc.campaign_name = f.old_campaign_name
WHERE sc.campaign_name NOT ILIKE 'mkt_%'
  AND sc.send_date > $start_date

UNION ALL

SELECT DISTINCT CURRENT_TIMESTAMP()                                                 AS update_time,
                store_brand                                                         AS store_name,
                m.campaign_id,
                COALESCE(f.new_campaign_name, r.new_campaign_name, m.campaign_name) AS campaign_name,
                m.subject,
                'Email_List'                                                        AS email_list,
                'ABTEST_SEGMENT'                                                    AS abtest_segment,
                TO_DATE(m.send_date)                                                AS send_date,
                NVL(revenue, 0)                                                     AS revenue,
                click_count                                                         AS unique_clicks,
                0                                                                   AS opened_user_count,
                0                                                                   AS estimated_opens,
                m.send_count                                                        AS sent,
                0                                                                   AS opt_out,
                NVL(orders, 0)                                                      AS purchases,
                0                                                                   AS hard_bounce,
                0                                                                   AS soft_bounce,
                0                                                                   AS spam_reports,
                0                                                                   AS activating_orders,
                0                                                                   AS repeat_orders,
                0                                                                   AS activating_rev,
                0                                                                   AS repeat_rev,
                'Push'                                                              AS type,
                'branch 6'                                                          AS branch,
                NVL(token_revenue, 0)                                               AS token_revenue,
                NVL(cash_revenue, 0)                                                AS cash_revenue,
                NVL(token_sales, 0)                                                 AS token_sales,
                NVL(cash_sales, 0)                                                  AS cash_sales,
                NVL(bmw.billed_me_now, 0)                                           AS billed_me_now,
                NVL(ca.cancelled, 0)                                                AS cancelled,
                NVL(bc.credit_billings, 0)                                          AS credit_billings
FROM _push_campaign_metrics_mkt m
         JOIN (SELECT ma.campaign_id,
                      ma.send_date,
                      SUM(NVL(revenue, 0))       AS revenue,
                      SUM(NVL(orders, 0))        AS orders,
                      SUM(NVL(token_sales, 0))   AS token_sales,
                      SUM(NVL(cash_sales, 0))    AS cash_sales,
                      SUM(NVL(token_revenue, 0)) AS token_revenue,
                      SUM(NVL(cash_revenue, 0))  AS cash_revenue
               FROM _push_campaign_metrics_mkt ma
                        LEFT JOIN _push_campaign_orders c
                                  ON ma.campaign_id::STRING = c.campaign_id::STRING
                                      AND ma.send_date <= c.date
                                      AND (DATEDIFF(DAY, ma.send_date, c.date) BETWEEN 0 AND 6)
                                      AND LOWER(ma.store_brand) = LOWER(c.store_name)
               WHERE ma.send_date > $start_date
               GROUP BY ma.campaign_id, ma.send_date) a
              ON m.campaign_id = a.campaign_id AND m.send_date = a.send_date
         LEFT JOIN billed_me_now bmw
                   ON m.campaign_id = bmw.campaign_id
                       AND m.send_date = bmw.date
                       AND bmw.campaign_source = 'push'
                       AND LOWER(m.store_brand) = LOWER(bmw.store_name)
         LEFT JOIN cancelled ca
                   ON m.campaign_id = ca.campaign_id
                       AND m.send_date = ca.date
                       AND ca.campaign_source = 'push'
                       AND LOWER(m.store_brand) = LOWER(ca.store_name)
         LEFT JOIN credit_billings bc
                   ON m.campaign_id = bc.campaign_id
                       AND m.send_date = bc.date
                       AND bc.campaign_source = 'push'
                       AND LOWER(m.store_brand) = LOWER(bc.store_name)
         LEFT JOIN gfb.crm_master_campaign_lookup r
                   ON m.campaign_name = r.old_campaign_name
         LEFT JOIN gfb.fk_crm_master_campaign_lookup f
                   ON m.campaign_name = f.old_campaign_name
WHERE m.campaign_name ILIKE 'mkt_%'
  AND m.send_date > $start_date

UNION ALL

SELECT DISTINCT CURRENT_TIMESTAMP()                                                 AS update_time,
                store_brand                                                         AS store_name,
                m.campaign_id,
                COALESCE(f.new_campaign_name, r.new_campaign_name, m.campaign_name) AS campaign_name,
                m.subject,
                'Email_List'                                                        AS email_list,
                'ABTEST_SEGMENT'                                                    AS abtest_segment,
                m.send_date,
                NVL(revenue, 0)                                                     AS revenue,
                click_count                                                         AS unique_clicks,
                0                                                                   AS opened_user_count,
                0                                                                   AS estimated_opens,

                send_count                                                          AS sent,
                0                                                                   AS opt_out,
                NVL(orders, 0)                                                      AS purchases,
                0                                                                   AS hard_bounce,
                0                                                                   AS soft_bounce,
                0                                                                   AS spam_reports,
                0                                                                   AS activating_orders,
                0                                                                   AS repeat_orders,
                0                                                                   AS activating_rev,
                0                                                                   AS repeat_rev,
                'Push'                                                              AS type,
                'branch 6'                                                          AS branch,
                NVL(token_revenue, 0)                                               AS token_revenue,
                NVL(cash_revenue, 0)                                                AS cash_revenue,
                NVL(token_sales, 0)                                                 AS token_sales,
                NVL(cash_sales, 0)                                                  AS cash_sales,
                NVL(bmw.billed_me_now, 0)                                           AS billed_me_now,
                NVL(ca.cancelled, 0)                                                AS cancelled,
                NVL(bc.credit_billings, 0)                                          AS credit_billings
FROM _push_campaign_metrics m
         LEFT JOIN _push_campaign_orders c
                   ON m.campaign_id::STRING = c.campaign_id::STRING AND m.send_date = c.date AND
                      LOWER(m.store_brand) = LOWER(c.store_name)
         LEFT JOIN billed_me_now bmw
                   ON m.campaign_id = bmw.campaign_id
                       AND m.send_date = bmw.date
                       AND bmw.campaign_source = 'push'
                       AND LOWER(m.store_brand) = LOWER(bmw.store_name)
         LEFT JOIN cancelled ca
                   ON m.campaign_id = ca.campaign_id
                       AND m.send_date = ca.date
                       AND ca.campaign_source = 'push'
                       AND LOWER(m.store_brand) = LOWER(ca.store_name)
         LEFT JOIN credit_billings bc
                   ON m.campaign_id = bc.campaign_id
                       AND m.send_date = bc.date
                       AND bc.campaign_source = 'push'
                       AND LOWER(m.store_brand) = LOWER(bc.store_name)
         LEFT JOIN gfb.crm_master_campaign_lookup r
                   ON m.campaign_name = r.old_campaign_name
         LEFT JOIN gfb.fk_crm_master_campaign_lookup f
                   ON m.campaign_name = f.old_campaign_name
WHERE m.campaign_name NOT ILIKE 'mkt_%'
  AND m.send_date > $start_date;
