SET start_date =(
    SELECT IFNULL(
        DATEADD('day',-14,(SELECT MAX(send_date)::DATE
        from gfb.crm_weekly_kpi_report
    )), '1990-01-01')
);

SET end_date =(
        SELECT MAX(event_time)::DATE
        from shared.marketing_channels_customer_communications

);

CREATE OR REPLACE TEMPORARY TABLE _sms_sends_campaigns AS
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM campaign_event_data.org_3223.sms_sends_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _sms_sends AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(sms.campaign_name, m.campaign_name)                      AS campaign_name,
       m.campaign_id                                                     AS campaign_id,
       subject,
       version_name,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS send_count
FROM shared.marketing_channels_customer_communications m
         LEFT JOIN _sms_sends_campaigns sms
                   ON m.campaign_id = sms.campaign_id
WHERE type = 'sms'
  AND store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
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
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _sms_clicks AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(sms.campaign_name, m.campaign_name)                      AS campaign_name,
       m.campaign_id,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS click_count,
FROM shared.marketing_channels_customer_clicks m
         LEFT JOIN _sms_clicks_campaigns sms
                   ON m.campaign_id = sms.campaign_id
WHERE type = 'sms'
  AND store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'), store_brand),
         COALESCE(sms.campaign_name, m.campaign_name),
         m.campaign_id,
         TO_DATE(event_time);

CREATE OR REPLACE TEMPORARY TABLE _sms_optin_optouts AS
SELECT IFF(store_group IN ('JustFab', 'ShoeDazzle', 'FabKids'), CONCAT(LOWER(store_group), '_', 'us'),
           REPLACE(LOWER(store_group), ' ', '_'))                                AS store_group,
       message_id,
       TO_DATE(timestamp)                                                        AS send_date,
       COUNT(DISTINCT CASE WHEN type = 'OPT_OUT' AND phone != '' THEN phone END) AS optouts,
       COUNT(DISTINCT CASE WHEN type = 'JOIN' AND phone != '' THEN phone END)    AS optin
FROM gfb.gfb_vw_attentive_sms
WHERE TO_DATE(timestamp) > $start_date
  AND TO_DATE(timestamp) <= $end_date
GROUP BY IFF(store_group IN ('JustFab', 'ShoeDazzle', 'FabKids'), CONCAT(LOWER(store_group), '_', 'us'),
             REPLACE(LOWER(store_group), ' ', '_')),
         message_id,
         TO_DATE(timestamp);

CREATE OR REPLACE TEMPORARY TABLE _attentive_sms_metrics AS
SELECT s.campaign_name,
       s.store_brand,
       s.campaign_id,
       s.send_date,
       s.subject,
       IFNULL(s.send_count, 0)  AS send_count,
       IFNULL(c.click_count, 0) AS click_count,
       IFNULL(o.optouts, 0)     AS optouts,
       IFNULL(o.optin, 0)       AS optin
FROM _sms_sends s
         LEFT JOIN _sms_clicks c
                   ON s.campaign_id = c.campaign_id AND s.send_date = c.send_date AND s.store_brand = c.store_brand
         LEFT JOIN _sms_optin_optouts o
                   ON s.campaign_id::STRING = o.message_id::STRING AND s.send_date = o.send_date AND
                      s.store_brand = o.store_group;

CREATE OR REPLACE TEMPORARY TABLE _attentive_sms_metrics_mkt AS
SELECT s.campaign_name,
       s.store_brand,
       s.campaign_id,
       s.send_date,
       s.subject,
       IFNULL(s.send_count, 0)  AS send_count,
       IFNULL(c.click_count, 0) AS click_count,
       IFNULL(o.optouts, 0)     AS optouts,
       IFNULL(o.optin, 0)       AS optin
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
                   ON s.campaign_id = c.campaign_id AND s.store_brand = c.store_brand
         LEFT JOIN (SELECT store_group,
                           message_id,
                           SUM(optouts) AS optouts,
                           SUM(optin)   AS optin
                    FROM _sms_optin_optouts
                    GROUP BY store_group,
                             message_id) o
                   ON s.campaign_id::STRING = o.message_id::STRING AND s.store_brand = o.store_group;


CREATE OR REPLACE TEMPORARY TABLE _email_sends_campaigns AS
SELECT DISTINCT s.template_id AS campaign_id,
                CASE
                    WHEN s.template_name ILIKE '%mkt%'
                        AND (s.template_name ILIKE '%control'
                            OR s.template_name ILIKE '%test') THEN s.template_name
                    ELSE s.campaign_name END AS campaign_name,
                c.start_at::DATE AS start_date
FROM campaign_event_data.org_3223.email_blast_sends_view s
join CAMPAIGN_EVENT_DATA.ORG_3223.CAMPAIGNS c on c.ID = s.CAMPAIGN_ID
WHERE s.project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%')

UNION

SELECT DISTINCT template_id AS campaign_id,
                campaign_name,
                NULL AS start_date
FROM campaign_event_data.org_3223.email_triggered_sends_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _email_sends AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(e.campaign_name, m.campaign_name)                        AS campaign_name,
       m.campaign_id,
       subject,
       version_name,
       COALESCE(TO_DATE(e.start_date),TO_DATE(event_time))                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS send_count
FROM shared.marketing_channels_customer_communications m
         LEFT JOIN _email_sends_campaigns e
                   ON m.campaign_id = e.campaign_id
WHERE type = 'email'
  AND source_table <> 'lake.sailthru.data_exporter_message_blast'
  AND store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'), store_brand),
         COALESCE(e.campaign_name, m.campaign_name),
         m.campaign_id,
         subject,
         version_name,
         COALESCE(TO_DATE(e.start_date),TO_DATE(event_time));


CREATE OR REPLACE TEMPORARY TABLE _email_clicks_campaigns AS
SELECT DISTINCT template_id AS campaign_id,
                campaign_name
FROM campaign_event_data.org_3223.email_clicks_view
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _email_clicks AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(e.campaign_name, m.campaign_name)                        AS campaign_name,
       m.campaign_id,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS click_count
FROM shared.marketing_channels_customer_clicks m
         LEFT JOIN _email_clicks_campaigns e
                   ON m.campaign_id = e.campaign_id
WHERE type = 'email'
  AND source_table <> 'lake.sailthru.data_exporter_message_blast'
  AND store_brand ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'), store_brand),
         COALESCE(e.campaign_name, m.campaign_name),
         m.campaign_id,
         TO_DATE(event_time);

CREATE OR REPLACE TEMPORARY TABLE _email_opens AS
SELECT store_group,
       campaign_id,
       NULL                       AS campaign_name,
       TO_DATE(event_time)        AS send_date,
       COUNT(DISTINCT contact_id) AS open_count
FROM lake_view.emarsys.email_opens
WHERE store_group ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY store_group,
         campaign_id,
         TO_DATE(event_time)

UNION

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
         TO_DATE(ebs.created_at);

CREATE OR REPLACE TEMPORARY TABLE _email_bounces AS
SELECT store_group,
       campaign_id,
       NULL                                                                       AS campaign_name,
       TO_DATE(event_time)                                                        AS send_date,
       COUNT(DISTINCT CASE WHEN LOWER(bounce_type) = 'soft' THEN contact_id END)  AS soft_bounce_count,
       COUNT(DISTINCT CASE WHEN LOWER(bounce_type) = 'hard' THEN contact_id END)  AS hard_bounce_count,
       COUNT(DISTINCT CASE WHEN LOWER(bounce_type) = 'block' THEN contact_id END) AS block_count
FROM lake_view.emarsys.email_bounces
WHERE store_group ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY store_group,
         campaign_id,
         TO_DATE(event_time)

UNION

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
         TO_DATE(ebs.created_at);

CREATE OR REPLACE TEMPORARY TABLE _email_unsubscribes AS
SELECT store_group,
       campaign_id,
       NULL                       AS campaign_name,
       TO_DATE(event_time)        AS send_date,
       COUNT(DISTINCT contact_id) AS unsubscribe_count
FROM lake_view.emarsys.email_unsubscribes
WHERE store_group ILIKE ANY ('shoedazzle%', 'fabkids%', 'justfab%')
  AND TO_DATE(event_time) > $start_date
  AND TO_DATE(event_time) <= $end_date
GROUP BY store_group,
         campaign_id,
         TO_DATE(event_time)

UNION

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
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');


CREATE OR REPLACE TEMPORARY TABLE _push_sends AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(p.campaign_name, m.campaign_name)                        AS campaign_name,
       m.campaign_id,
       subject,
       version_name,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS send_count
FROM shared.marketing_channels_customer_communications m
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
WHERE project_name ILIKE ANY ('%ShoeDazzle%', '%JustFab%', '%FabKids%');

CREATE OR REPLACE TEMPORARY TABLE _push_clicks AS
SELECT IFF(store_brand IN ('justfab', 'shoedazzle', 'fabkids'), CONCAT(store_brand, '_', 'us'),
           store_brand)                                                  AS store_brand,
       COALESCE(p.campaign_name, m.campaign_name)                        AS campaign_name,
       m.campaign_id,
       TO_DATE(event_time)                                               AS send_date,
       COUNT(DISTINCT COALESCE(customer_id::STRING, contact_id::STRING)) AS click_count
FROM shared.marketing_channels_customer_clicks m
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
                fo.product_gross_revenue_local_amount                                                     AS revenue
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
                 AND event_time <= $end_date) c
              ON c.customer_id = olp.customer_id
                  AND CAST(c.event_time AS DATETIME) < CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ
                  AND (DATEDIFF(DAY, CAST(c.event_time AS DATETIME),
                                CONVERT_TIMEZONE('UTC', fo.order_local_datetime)::TIMESTAMP_NTZ) BETWEEN 0 AND 6)
WHERE olp.order_classification = 'product order';

CREATE OR REPLACE TEMPORARY TABLE _email_campaign_orders AS
SELECT store_name,
       campaign_id,
       campaign_event_time::DATE AS date,
       COUNT(order_id)           AS orders,
       SUM(revenue)              AS revenue
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
       SUM(revenue)              AS revenue
FROM _crm_orders
WHERE campaign_source = 'sms'
  AND TO_DATE(campaign_event_time) > $start_date
GROUP BY store_name,
         campaign_id,
         campaign_event_time::DATE;

CREATE OR REPLACE TEMPORARY TABLE _sms_emarsys_orders AS
SELECT DISTINCT campaign_id,
                event_time::DATE                  date,
                SPLIT_PART(store_name, '_', 1) AS store_group,
                store_name,
                COUNT(r.order_id)              AS purchase,
                SUM(revenue)                   AS revenue
FROM gfb.gfb_vw_crm_email_ga_orders_all_region r
WHERE campaign_source = 'SMS_CAMPAIGNS'
  AND TO_DATE(event_time) > $start_date
GROUP BY campaign_id,
         event_time::DATE,
         store_group,
         store_name;

CREATE OR REPLACE TEMPORARY TABLE _push_campaign_orders AS
SELECT store_name,
       campaign_id,
       campaign_event_time::DATE AS date,
       COUNT(order_id)           AS orders,
       SUM(revenue)              AS revenue
FROM _crm_orders
WHERE campaign_source = 'push'
  AND TO_DATE(campaign_event_time) > $start_date
GROUP BY store_name,
         campaign_id,
         campaign_event_time::DATE;

DELETE
FROM gfb.crm_weekly_kpi_report
WHERE send_date > $start_date;

INSERT INTO gfb.crm_weekly_kpi_report
/*
   !!! UNCOMMENT IF YOU NEED TO LOAD THE TABLE FIRST TIME!!!
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
       'branch 1'          AS branch
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
                'branch 2'                                                          AS branch
FROM _email_metrics_mkt m
         JOIN (SELECT ma.campaign_id,
                      ma.send_date,
                      SUM(NVL(revenue, 0)) AS revenue,
                      SUM(NVL(orders, 0))  AS orders
               FROM _email_metrics_mkt ma
                        LEFT JOIN _email_campaign_orders c
                                  ON ma.campaign_id::STRING = c.campaign_id::STRING
                                      AND ma.send_date <= c.date
                                      AND (DATEDIFF(DAY, ma.send_date, c.date) BETWEEN 0 AND 6)
                                      AND LOWER(ma.store_brand) = LOWER(c.store_name)
               WHERE ma.send_date > $start_date
               GROUP BY ma.campaign_id, ma.send_date) a
              ON a.campaign_id = m.campaign_id AND a.send_date = m.send_date
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
                'branch 2'                                                          AS branch
FROM _email_metrics m
         LEFT JOIN _email_campaign_orders c
                   ON m.campaign_id::STRING = c.campaign_id::STRING AND m.send_date = c.date
                       AND LOWER(m.store_brand) = LOWER(c.store_name)
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
       'branch 3'          AS branch
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
       'branch 4'                                                           AS branch
FROM _attentive_sms_metrics_mkt sc
         JOIN (SELECT s.campaign_id,
                      s.send_date,
                      SUM(revenue)  AS revenue,
                      SUM(purchase) AS purchase
               FROM _attentive_sms_metrics_mkt s
                        LEFT JOIN _sms_orders so
                                  ON TRY_TO_NUMBER(s.campaign_id) = TRY_TO_NUMBER(so.campaign_id)
                                      AND s.send_date <= so.date
                                      AND (DATEDIFF(DAY, s.send_date, so.date) BETWEEN 0 AND 6)
                                      AND s.store_brand = LOWER(so.store_name)
               WHERE s.send_date > $start_date
               GROUP BY s.campaign_id, s.send_date) a
              ON sc.campaign_id = a.campaign_id AND sc.send_date = a.send_date
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
       'branch 4'                                                           AS branch
FROM _attentive_sms_metrics sc
         LEFT JOIN _sms_orders so
                   ON TRY_TO_NUMBER(sc.campaign_id) = TRY_TO_NUMBER(so.campaign_id)
                       AND sc.send_date = so.date AND sc.store_brand = LOWER(so.store_name)
         LEFT JOIN gfb.crm_master_campaign_lookup r
                   ON sc.campaign_name = r.old_campaign_name
         LEFT JOIN gfb.fk_crm_master_campaign_lookup f
                   ON sc.campaign_name = f.old_campaign_name
WHERE sc.campaign_name NOT ILIKE 'mkt_%'
  AND sc.send_date > $start_date

UNION ALL

SELECT CURRENT_TIMESTAMP()                                                  AS update_time,
       LOWER(sc.store_group)                                                AS store_name,
       sc.campaign_id                                                       AS template_id,
       COALESCE(f.new_campaign_name, r.new_campaign_name, sc.campaign_name) AS template_name,
       sc.subject                                                           AS subject,
       'EMAIL_LIST'                                                         AS email_list,
       'ABTEST_SEGMENT'                                                     AS abtest_segment,
       TO_DATE(sc.send_date)                                                AS date,
       a.revenue                                                            AS rev,
       click_count                                                          AS unique_clicks,
       0                                                                    AS opened_user_count,
       0                                                                    AS estopens,
       send_count                                                           AS delivered,
       unsubscribe_count                                                    AS optout,
       a.purchase                                                           AS purchase,
       0                                                                    AS hard_bounce,
       0                                                                    AS soft_bounce,
       0                                                                    AS spam_reports,
       0                                                                    AS activating_orders,
       0                                                                    AS repeat_orders,
       0                                                                    AS activating_rev,
       0                                                                    AS repeat_rev,
       'SMS'                                                                AS type,
       'branch 5'                                                           AS branch
FROM gfb.gfb_vw_emarsys_sms_campaign_metrics_all_region_mkt sc
         JOIN (SELECT s.campaign_id,
                      s.send_date,
                      SUM(revenue)  AS revenue,
                      SUM(purchase) AS purchase
               FROM gfb.gfb_vw_emarsys_sms_campaign_metrics_all_region_mkt s
                        LEFT JOIN _sms_emarsys_orders so
                                  ON s.campaign_id::STRING = so.campaign_id::STRING
                                      AND s.send_date <= so.date
                                      AND (DATEDIFF(DAY, s.send_date, so.date) BETWEEN 0 AND 6)
                                      AND LOWER(s.store_group) = LOWER(so.store_name)
               WHERE s.send_date > $start_date
               GROUP BY s.campaign_id, s.send_date) a
              ON sc.campaign_id = a.campaign_id AND sc.send_date = a.send_date
         LEFT JOIN gfb.crm_master_campaign_lookup r
                   ON sc.campaign_name = r.old_campaign_name
         LEFT JOIN gfb.fk_crm_master_campaign_lookup f
                   ON sc.campaign_name = f.old_campaign_name
WHERE sc.campaign_name ILIKE 'mkt_%'
  AND sc.send_date > $start_date

UNION ALL

SELECT CURRENT_TIMESTAMP()                                                  AS update_time,
       LOWER(sc.store_group)                                                AS store_name,
       sc.campaign_id                                                       AS template_id,
       COALESCE(f.new_campaign_name, r.new_campaign_name, sc.campaign_name) AS template_name,
       sc.subject                                                           AS subject,
       'EMAIL_LIST'                                                         AS email_list,
       'ABTEST_SEGMENT'                                                     AS abtest_segment,
       TO_DATE(COALESCE(sc.send_date, so.date))                             AS date,
       revenue                                                              AS rev,
       click_count                                                          AS unique_clicks,
       0                                                                    AS opened_user_count,
       0                                                                    AS estopens,
       send_count                                                           AS delivered,
       unsubscribe_count                                                    AS optout,
       purchase                                                             AS purchase,
       0                                                                    AS hard_bounce,
       0                                                                    AS soft_bounce,
       0                                                                    AS spam_reports,
       0                                                                    AS activating_orders,
       0                                                                    AS repeat_orders,
       0                                                                    AS activating_rev,
       0                                                                    AS repeat_rev,
       'SMS'                                                                AS type,
       'branch 5'                                                           AS branch
FROM gfb.gfb_vw_emarsys_sms_campaign_metrics_all_region sc
         LEFT JOIN _sms_emarsys_orders so
                   ON sc.campaign_id::STRING = so.campaign_id::STRING
                       AND sc.send_date = so.date AND LOWER(sc.store_group) = LOWER(so.store_name)
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
                'branch 6'                                                          AS branch
FROM _push_campaign_metrics_mkt m
         JOIN (SELECT ma.campaign_id,
                      ma.send_date,
                      SUM(NVL(revenue, 0)) AS revenue,
                      SUM(NVL(orders, 0))  AS orders
               FROM _push_campaign_metrics_mkt ma
                        LEFT JOIN _push_campaign_orders c
                                  ON ma.campaign_id::STRING = c.campaign_id::STRING
                                      AND ma.send_date <= c.date
                                      AND (DATEDIFF(DAY, ma.send_date, c.date) BETWEEN 0 AND 6)
                                      AND LOWER(ma.store_brand) = LOWER(c.store_name)
               WHERE ma.send_date > $start_date
               GROUP BY ma.campaign_id, ma.send_date) a
              ON m.campaign_id = a.campaign_id AND m.send_date = a.send_date
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
                'branch 6'                                                          AS branch
FROM _push_campaign_metrics m
         LEFT JOIN _push_campaign_orders c
                   ON m.campaign_id::STRING = c.campaign_id::STRING AND m.send_date = c.date AND
                      LOWER(m.store_brand) = LOWER(c.store_name)
         LEFT JOIN gfb.crm_master_campaign_lookup r
                   ON m.campaign_name = r.old_campaign_name
         LEFT JOIN gfb.fk_crm_master_campaign_lookup f
                   ON m.campaign_name = f.old_campaign_name
WHERE m.campaign_name NOT ILIKE 'mkt_%'
  AND m.send_date > $start_date;
