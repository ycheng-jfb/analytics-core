-- REPORTING_PROD.SXF.CRM_OPTIMIZATION = SXF CRM Campaign Level Reporting
-- CRM Attribution = 7 day last click

-- Create base list of all online orders, joined to session single view media to pull in last 7 day clicks and utms

CREATE OR REPLACE TEMP TABLE  seven_day_click_order AS (
    SELECT DISTINCT fo.customer_id,
                    ds.store_brand,
                    ds.store_country,
                    fo.order_id,
                    fo.order_local_datetime,
                    mc.membership_order_type_l2                    AS order_type,
                    LOWER(COALESCE(smd.utmsource, 'n/a'))          AS source,
                    LOWER(COALESCE(smd.utmmedium, 'n/a'))          AS medium,
                    LOWER(COALESCE(smd.utmcampaign, 'n/a'))        AS campaign,
                    DATE_TRUNC('minute', smd.sessionlocaldatetime) AS session_minute
                    -- EDW_PROD.DATA_MODEL_SXF tables are unconcatenated, no store_id at the end of customer_id
    FROM edw_prod.data_model_sxf.fact_order fo
    JOIN edw_prod.data_model_sxf.dim_store ds
            ON fo.store_id = ds.store_id
    JOIN edw_prod.data_model_sxf.dim_order_sales_channel oc
            ON oc.order_sales_channel_key = fo.order_sales_channel_key
            AND LOWER(oc.order_classification_l2) = 'product order'
    JOIN edw_prod.data_model_sxf.dim_order_status os
             ON os.order_status_key = fo.order_status_key
             AND LOWER(os.order_status) IN ('success', 'pending')
    --REPORTING_BASE_PROD.shared.session_single_view_media is concatenated ~ session_id, customer_id, and visitor_id have 30 attached to end
    LEFT JOIN reporting_base_prod.shared.session_single_view_media smd
            ON CONCAT(fo.customer_id, '30') = smd.customer_id
            AND fo.order_local_datetime::DATE - smd.sessionlocaldatetime::DATE <= 6 -- seven day look back window
            AND smd.sessionlocaldatetime::DATETIME <= fo.order_local_datetime::DATETIME
    JOIN edw_prod.data_model_sxf.dim_order_membership_classification mc
             ON mc.order_membership_classification_key = fo.order_membership_classification_key
    WHERE 1 = 1
    AND oc.is_border_free_order = 0
    AND oc.is_ps_order = 0
    AND oc.is_test_order = FALSE
    AND ds.store_brand = 'Savage X'
    AND ds.store_type <> 'Retail'
    AND order_local_datetime >= '2022-03-01'
    AND smd.sessionlocaldatetime >= '2022-03-01'
);

-- Clean Orders + Sessions Table:
-- Row for every customer + order + session within each order
-- clean up utm campaigns format
-- for every customer and order, rank each session by time. most recent session = 1.

CREATE OR REPLACE TEMP TABLE order_session_clean AS (
    SELECT o.*,
            REGEXP_REPLACE(REGEXP_REPLACE(campaign, '%20', ' '), '%2f', '/')     AS campaign_clean,
            ROW_NUMBER() OVER (PARTITION BY o.customer_id, o.order_local_datetime, o.order_id ORDER BY session_minute DESC, order_local_datetime DESC) AS rno
    FROM seven_day_click_order AS o
);




--- Aggregated Order Session Table:
--- Row for every customer and order: type, utm sources, utm mediums, utm campaigns, and session times

CREATE OR REPLACE TEMP TABLE aggregated_order_session AS (
       SELECT s.customer_id,
                 s.store_brand,
                 s.store_country,
                 s.order_id,
                 s.order_local_datetime,
                 s.order_type,
                 LISTAGG(s.source, ';') WITHIN GROUP (ORDER BY rno)         AS source_list,
                 LISTAGG(s.medium, ';') WITHIN GROUP (ORDER BY rno)         AS medium_list,
                 LISTAGG(s.campaign_clean, ';') WITHIN GROUP (ORDER BY rno) AS campaign_list,
                 LISTAGG(s.session_minute, ';') WITHIN GROUP (ORDER BY rno) AS session_time_list
       FROM order_session_clean AS s
       GROUP BY 1, 2, 3, 4, 5, 6
);


--- Last Touch Order Session Table:
--- Row for each customer + order: count of utm sources, mediums, and campaigns + most recent / second most recent

CREATE OR REPLACE TEMP TABLE last_touch_order_session AS (
       SELECT a.customer_id,
                 a.store_brand,
                 a.store_country,
                 a.order_id,
                 a.order_local_datetime,
                 a.order_type,
                 REGEXP_COUNT(source_list, ';') + 1   AS source_count,
                 REGEXP_COUNT(medium_list, ';') + 1   AS medium_count,
                 REGEXP_COUNT(campaign_list, ';') + 1 AS campaign_count,
                 IFF(campaign_count > 1,
                     SPLIT_PART(REGEXP_REPLACE(campaign_list, 'n/a;', ''), ';', 1),
                     campaign_list)                   AS last_campaign,
                 IFF(campaign_count > 1,
                     SPLIT_PART(REGEXP_REPLACE(campaign_list, 'n/a;', ''), ';', 2),
                     campaign_list)                   AS second_last_campaign,
                 IFF(source_count > 1,
                     SPLIT_PART(REGEXP_REPLACE(source_list, 'n/a;', ''), ';', 1),
                     source_list)                     AS last_source,
                 IFF(source_count > 1,
                     SPLIT_PART(REGEXP_REPLACE(source_list, 'n/a;', ''), ';', 2),
                     source_list)                     AS second_last_source,
                 IFF(medium_count > 1,
                     SPLIT_PART(REGEXP_REPLACE(medium_list, 'n/a;', ''), ';', 1),
                     medium_list)                     AS last_medium,
                 IFF(medium_count > 1,
                     SPLIT_PART(REGEXP_REPLACE(medium_list, 'n/a;', ''), ';', 2),
                     medium_list)                     AS second_last_medium,
                 IFF(campaign_count > 1,
                     SPLIT_PART(REGEXP_REPLACE(session_time_list, 'n/a;', ''), ';', 1),
                     session_time_list)               AS last_session_time,
                 IFF(medium_count > 1,
                     SPLIT_PART(REGEXP_REPLACE(session_time_list, 'n/a;', ''), ';', 2),
                     session_time_list)               AS second_last_session_time,
                 a.campaign_list,
                 a.medium_list,
                 a.source_list,
                 a.session_time_list
          FROM aggregated_order_session AS a
);

-- Second Orders: identify which orders = customer's 2nd purchase

CREATE OR REPLACE TEMP TABLE _sxf_second_orders AS (
       SELECT fo.order_id,
              fo.customer_id,
              fo.order_local_datetime,
              ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_local_datetime ASC) AS rno
        FROM edw_prod.data_model_sxf.fact_order fo
        JOIN edw_prod.data_model_sxf.dim_store ds ON fo.store_id = ds.store_id
        JOIN edw_prod.data_model_sxf.dim_order_sales_channel oc
            ON oc.order_sales_channel_key = fo.order_sales_channel_key
        JOIN edw_prod.data_model_sxf.dim_order_status os ON os.order_status_key = fo.order_status_key
        WHERE LOWER(oc.order_classification_l2) = 'product order'
       AND LOWER(os.order_status) IN ('success', 'pending')
       AND ds.store_brand = 'Savage X'
       AND order_local_datetime >= '2022-03-01'
       QUALIFY rno = 2
);

-- Credits per order - calc number of credits redeemed on each order
-- REPORTING_PROD.SXF.ORDER_LINE_DATASET is not concatenated
CREATE OR REPLACE TEMP TABLE _credits_per_order AS (
       SELECT order_id,
               customer_id,
               order_local_datetime,
               is_ecomm                           order_type,
               MIN(num_credits_redeemed_on_order) num_credits_redeemed
       FROM reporting_prod.sxf.order_line_dataset
       WHERE order_local_datetime >= '2022-03-01'
       GROUP BY 1, 2, 3, 4
);

--- Orders Plus Revenue Metrics Table:
-- Combine Last Tounch Order Sessions table to Fact Order
-- Row for every customer + order with type, units, gaap, product margin, product cash margin

CREATE OR REPLACE TEMP TABLE  aggregated_order_session_plus_dollars_units AS (
       SELECT l.*,
                fo.unit_count,
                num_credits_redeemed AS token_count,
                fo.product_gross_revenue_excl_shipping_local_amount,
                fo.product_margin_pre_return_excl_shipping_local_amount,
                fo.product_order_cash_margin_pre_return_local_amount,
                so.order_id          AS second_order_id
       FROM last_touch_order_session AS l
       JOIN edw_prod.data_model_sxf.fact_order fo
                ON l.customer_id = fo.customer_id
                AND l.order_id = fo.order_id
                AND l.order_local_datetime = fo.order_local_datetime
       LEFT JOIN _sxf_second_orders so
                ON so.order_id = fo.order_id
                AND so.customer_id = fo.customer_id
       LEFT JOIN _credits_per_order cpo
                ON cpo.order_id = fo.order_id
);

-- Customer Level Attribution Email Table:
-- Filter: last medium = email
-- Row for every Customer + campaign name + time: count of orders + sum of revenue metrics
-- last_campaign = CAMPAIGN NAME

CREATE OR REPLACE TEMP TABLE customer_level_attribution AS (
       SELECT f.customer_id,
               f.store_brand                                                            AS store_brand,
               f.store_country                                                          AS store_country,
               f.last_campaign                                                          AS campaign_name,
               f.last_source                                                            AS utm_source,
               f.last_medium                                                            AS utm_medium,
               f.last_session_time                                                      AS event_time,
               f.order_type                                                             AS order_type,
               LOWER(f.last_medium)                                                     AS Email_or_SMS,
               COUNT(DISTINCT f.order_id)                                               AS orders,
               COUNT(DISTINCT f.second_order_id)                                        AS second_orders,
               SUM(COALESCE(f.unit_count, 0))                                           AS units,
               SUM(COALESCE(f.token_count, 0))                                          AS credits_redeemed,
               SUM(COALESCE(f.product_gross_revenue_excl_shipping_local_amount, 0))     AS gaap,
               SUM(COALESCE(f.product_margin_pre_return_excl_shipping_local_amount, 0)) AS product_margin,
               SUM(COALESCE(f.product_order_cash_margin_pre_return_local_amount, 0))    AS product_cash_margin
       FROM aggregated_order_session_plus_dollars_units AS f
       WHERE (LOWER(last_medium) = 'email') OR (LOWER(last_medium) ='sms' and last_source = 'iterable')
       GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
);



----------------------------------- CRM ATTRIBUTION GROWTH/BLAST SECTION - Aggregate ordres/revenue to campaign id-----------------------

-- Email Blast Campaign Attribution Table:
-- For every campaign ID: count of orders = count of customer_ids, sum of units, gaap, product margin, and product cash margin
-- Campaign ID pulled from Campaign Name
-- Blast = one row per campaign, orders/revenue aggregated by campaign id

CREATE OR REPLACE TEMP TABLE email_utm_attribution_model_growth AS (
       SELECT c.store_brand,
               c.store_country,
               SPLIT_PART(c.campaign_name, 'cid-', -1)                                         AS campaign_id,
               EMAIL_OR_SMS,
               SUM(COALESCE(c.orders, 0))                                                      AS orders,
               SUM(COALESCE(c.second_orders, 0))                                               AS second_orders,
               SUM(COALESCE(c.units, 0))                                                       AS units,
               SUM(COALESCE(c.credits_redeemed, 0))                                            AS credits_redeemed,
               SUM(COALESCE(c.gaap, 0))                                                        AS gaap,
               SUM(COALESCE(c.product_margin, 0))                                              AS product_margin,
               SUM(COALESCE(c.product_cash_margin, 0))                                         AS product_cash_margin,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN c.orders ELSE NULL END)        AS repeat_vip_orders,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN c.orders ELSE NULL END)        AS activating_vip_orders,
               SUM(CASE WHEN order_type = 'Guest'          THEN c.orders ELSE NULL END)        AS guest_orders,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN c.second_orders ELSE NULL END)      AS repeat_vip_second_orders,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN c.second_orders ELSE NULL END)      AS activating_vip_second_orders,
               SUM(CASE WHEN order_type = 'Guest'          THEN c.second_orders ELSE NULL END)      AS guest_second_orders,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.units, 0) ELSE NULL END)    AS repeat_vip_units,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.units, 0) ELSE NULL END)    AS activating_vip_units,
               SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.units, 0) ELSE NULL END)    AS guest_units,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END)  AS repeat_vip_credits_redeemed,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END)  AS activating_vip_credits_redeemed,
               SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END)  AS guest_credits_redeemed,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.gaap, 0) ELSE NULL END)  AS repeat_vip_gaap,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.gaap, 0) ELSE NULL END)  AS activating_vip_gaap,
               SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.gaap, 0) ELSE NULL END)  AS guest_gaap,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.product_margin, 0)  ELSE NULL END) AS repeat_vip_product_margin,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.product_margin, 0)  ELSE NULL END) AS activating_vip_product_margin,
               SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.product_margin, 0)  ELSE NULL END) AS guest_product_margin,

               SUM(CASE  WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.product_margin, 0) ELSE NULL END)  AS repeat_vip_product_cash_margin,
               SUM(CASE  WHEN order_type = 'Activating VIP' THEN COALESCE(c.product_margin, 0)  ELSE NULL END) AS activating_vip_product_cash_margin,
               SUM(CASE WHEN order_type = 'Guest'           THEN COALESCE(c.product_margin, 0) ELSE NULL END)  AS guest_product_cash_margin

        FROM customer_level_attribution c
        WHERE EMAIL_OR_SMS = 'email'
        GROUP BY 1, 2, 3, 4
);

--- SMS Campaign Attribution Table:
-- For every campaign ID: count of orders = count of customer_ids, sum of units, gaap, product margin, and product cash margin
-- Campaign ID pulled from Campaign Name

CREATE OR REPLACE TEMP TABLE sms_utm_attribution_model_growth AS (
       SELECT c.store_brand,
               c.store_country,
               SPLIT_PART(c.campaign_name, 'cid-', -1)                                         AS campaign_id,
               EMAIL_OR_SMS,

               SUM(COALESCE(c.orders, 0))                                                      AS orders,
               SUM(COALESCE(c.second_orders, 0))                                               AS second_orders,
               SUM(COALESCE(c.units, 0))                                                       AS units,
               SUM(COALESCE(c.credits_redeemed, 0))                                            AS credits_redeemed,
               SUM(COALESCE(c.gaap, 0))                                                        AS gaap,
               SUM(COALESCE(c.product_margin, 0))                                              AS product_margin,
               SUM(COALESCE(c.product_cash_margin, 0))                                         AS product_cash_margin,

               SUM(CASE WHEN order_type = 'Repeat VIP' THEN c.orders ELSE NULL END)            AS repeat_vip_orders,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN c.orders ELSE NULL END)        AS activating_vip_orders,
               SUM(CASE WHEN order_type = 'Guest' THEN c.orders ELSE NULL END)                 AS guest_orders,

               SUM(CASE WHEN order_type = 'Repeat VIP' THEN c.second_orders ELSE NULL END)     AS repeat_vip_second_orders,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN c.second_orders ELSE NULL END) AS activating_vip_second_orders,
               SUM(CASE WHEN order_type = 'Guest' THEN c.second_orders ELSE NULL END)          AS guest_second_orders,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.units, 0)  ELSE NULL END) AS repeat_vip_units,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.units, 0)  ELSE NULL END) AS activating_vip_units,
               SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.units, 0) ELSE NULL END)     AS guest_units,

               SUM(CASE WHEN order_type = 'Repeat VIP'      THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END) AS repeat_vip_credits_redeemed,
               SUM(CASE WHEN order_type = 'Activating VIP'  THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END) AS activating_vip_credits_redeemed,
               SUM(CASE WHEN order_type = 'Guest'           THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END) AS guest_credits_redeemed,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.gaap, 0) ELSE NULL END) AS repeat_vip_gaap,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.gaap, 0) ELSE NULL END) AS activating_vip_gaap,
               SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.gaap, 0) ELSE NULL END) AS guest_gaap,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.product_margin, 0) ELSE NULL END) AS repeat_vip_product_margin,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.product_margin, 0) ELSE NULL END) AS activating_vip_product_margin,
               SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.product_margin, 0) ELSE NULL END) AS guest_product_margin,

               SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.product_margin, 0) ELSE NULL END) AS repeat_vip_product_cash_margin,
               SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.product_margin, 0) ELSE NULL END) AS activating_vip_product_cash_margin,
               SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.product_margin, 0) ELSE NULL END) AS guest_product_cash_margin

        FROM customer_level_attribution c
        WHERE EMAIL_OR_SMS = 'sms'
        GROUP BY 1, 2, 3, 4
);



----------------------------------- CRM CAMPAIGN METRICS SECTIONS - BLAST AND TRIGGERED: PULL EMAIL/SMS SENDS, CLICKS, ETC. -----------------------------------------------

-- Emarsys: launched March 2022
-- Iterable: launched July 2024


---------------- Campaign Dimensions: Pull campaign dimensions/attributes from emarsys and iterable tables ----------------------

-- cte to match store group with emarsys store_group
CREATE OR REPLACE TEMP TABLE _iterable_customer_store_mapping AS (
       SELECT itbl_user_id,
            store_brand,
            CASE
                WHEN store_name ILIKE 'Savage X US' THEN 'savagex_us'
                WHEN store_name ILIKE 'Savage X CA' THEN 'savagex_ca'
                WHEN store_name ILIKE 'Savage X DE' THEN 'savagex_de'
                WHEN store_name ILIKE 'Savage X ES' THEN 'savagex_es'
                WHEN store_name ILIKE 'Savage X FR' THEN 'savagex_fr'
                WHEN store_name ILIKE 'Savage X EUREM' THEN 'savagex_eu'
                WHEN store_name ILIKE 'Savage X UK' THEN 'savagex_uk'
            END store_group
     FROM campaign_event_data.org_3223.users u
    LEFT JOIN edw_prod.data_model_sxf.dim_customer cust ON cust.customer_id = TRY_TO_NUMBER(u.user_id)
    LEFT JOIN edw_prod.data_model_sxf.dim_store s ON cust.store_id = s.store_id
    WHERE store_brand = 'Savage X'
);

-- Emarsys campaign dimensions (Row for each campaign + event time. Most recent = 1)
CREATE OR REPLACE TEMP TABLE campaign_dim_rno_emarsys AS (
       SELECT DISTINCT e.campaign_id,
                     e.name::STRING AS campaign_name,
                     e.campaign_type,
                     e.category_name,
                     e.subject,
                     e.store_group,
                     e.store_group as project_name,
                     ROW_NUMBER() OVER (PARTITION BY e.store_group, e.campaign_id ORDER BY event_time DESC) AS rno
     FROM lake_view.emarsys.email_campaigns_v2 AS e
     WHERE NOT LOWER(name) IN ('test')
       AND NOT LOWER(name) IN ('keya')
       AND NOT LOWER(name) IN ('copy')
       AND store_group LIKE 'savagex%'
     QUALIFY rno = 1
     ORDER BY 1, 2 DESC
);

-- Iterable Email campaign dimensions (Row for each campaign + event time. Most recent = 1)
CREATE OR REPLACE TEMP TABLE  campaign_dim_rno_iterable AS (
       SELECT DISTINCT c.id             AS campaign_id,
                     c.name::STRING     AS campaign_name,
                     c.type             campaign_type,
                     'ITERABLE'         category_name,
                     es.email_subject   AS subject,
                     sm.store_group,
                     es.project_name,
                     ROW_NUMBER()  OVER (PARTITION BY sm.store_group, c.id ORDER BY c.created_at DESC) AS rno
     FROM campaign_event_data.org_3223.campaigns AS c
     LEFT JOIN campaign_event_data.org_3223.email_clicks_view AS es ON es.campaign_id = c.id
     LEFT JOIN _iterable_customer_store_mapping sm
               ON es.itbl_user_id = sm.itbl_user_id --AND store_group NOT ILIKE 'Savage X CA'
     WHERE NOT LOWER(c.name) IN ('test')
       AND NOT LOWER(c.name) IN ('keya')
       AND NOT LOWER(c.name) IN ('copy')
       AND store_brand ILIKE 'Savage X%'
     QUALIFY rno = 1
     ORDER BY 1, 2 DESC
);


-- Iterable SMS campaign dimensions (Row for each campaign + event time. Most recent = 1)
CREATE OR REPLACE TEMP TABLE sms_campaign_dim_rno_iterable AS (
       SELECT DISTINCT c.id                                   AS campaign_id,
                     c.name::STRING                           AS campaign_name,
                     c.type                                      campaign_type,
                     'ITERABLE'                                  category_name,
                     null                                     AS subject,
                     sm.store_group,
                     es.project_name,
                     ROW_NUMBER()
                         OVER (PARTITION BY sm.store_group,
                             c.id ORDER BY c.created_at DESC) AS rno
     FROM campaign_event_data.org_3223.campaigns AS c
     LEFT JOIN campaign_event_data.org_3223.sms_clicks_view AS es ON es.campaign_id = c.id
     LEFT JOIN _iterable_customer_store_mapping sm
               ON es.itbl_user_id = sm.itbl_user_id --AND store_group NOT ILIKE 'Savage X CA'
     WHERE NOT LOWER(c.name) IN ('test')
       AND NOT LOWER(c.name) IN ('keya')
       AND NOT LOWER(c.name) IN ('copy')
       AND store_brand ILIKE 'Savage X%'
       QUALIFY rno = 1
     ORDER BY 1, 2 DESC
);


-- All campaign dimensions = Union Emarsys, Iterable Email, and Iterable SMS campaign dimensions
CREATE OR REPLACE TEMP TABLE  campaign_dim AS  (
     SELECT * FROM campaign_dim_rno_emarsys
     UNION
     SELECT * FROM campaign_dim_rno_iterable
     UNION
     SELECT * FROM sms_campaign_dim_rno_iterable
);

-- For every SMS campaign, get min send date and max click date to get date range of campaign
-- Start Date = first send date of campaign
-- End Date = last day with send or click  = if max send date is greater than max click date, return max send date. if not return max click date
-- SMS Campaigns = iterable only
CREATE OR REPLACE TEMP TABLE  sms_campaign_min_max_dates as (
select distinct cd.campaign_id,
                cd.campaign_name,
                cd.campaign_type,
                cd.category_name,
                cd.subject,
                cd.store_group,
                cd.PROJECT_NAME,
               min(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', s.created_at)::DATE ) as start_date,
               iff(max(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', s.created_at)::DATE ) >= max(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', c.created_at)::DATE)
                   , max(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', s.created_at)::DATE ), max(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', c.created_at)::DATE ))  as end_date
from CAMPAIGN_EVENT_DATA.ORG_3223.SMS_SENDS_VIEW s
join CAMPAIGN_EVENT_DATA.ORG_3223.SMS_CLICKS_VIEW c  ON s.campaign_id = c.campaign_id
                                                     AND s.PROJECT_NAME = c.PROJECT_NAME
JOIN campaign_dim AS cd ON  s.campaign_id = cd.campaign_id
                        AND s.PROJECT_NAME = cd.PROJECT_NAME
WHERE S.PROJECT_NAME ilike 'PRD-SavageX%'
GROUP BY 1, 2, 3, 4, 5, 6, 7
);


-- SMS Campaign Base table - for every campaign, row for each date between campaign start and end date
CREATE OR REPLACE TEMP TABLE sms_campaign_dates_base as (
SELECT dd.full_date, a.*
FROM sms_campaign_min_max_dates a
JOIN edw_prod.data_model.dim_date dd ON dd.full_date BETWEEN a.start_date AND a.end_date
order by dd.full_date
);

-- For every Email campaign, get min send date and max click date to get date range of campaign
-- Start Date = first send date of campaign
-- End Date = last day with open

CREATE OR REPLACE TEMP TABLE  email_campaign_min_max_dates_iterable_triggered as (
select distinct cd.campaign_id,
                cd.campaign_name,
                cd.campaign_type,
                cd.category_name,
                cd.subject,
                cd.store_group,
                cd.PROJECT_NAME,
               min(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', s.created_at)::DATE ) as start_date,
               iff(max(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', s.created_at)::DATE ) >= max(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', o.created_at)::DATE)
                   , max(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', s.created_at)::DATE ), max(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', o.created_at)::DATE ))  as end_date
FROM campaign_event_data.org_3223.email_triggered_sends_view s
join campaign_event_data.org_3223.email_opens_view o ON o.campaign_id = s.campaign_id
                                                     AND s.PROJECT_NAME ilike 'PRD-SavageX%'
JOIN campaign_dim AS cd ON  s.campaign_id = cd.campaign_id
                        AND s.PROJECT_NAME = cd.PROJECT_NAME
WHERE S.PROJECT_NAME ilike 'PRD-SavageX%'
GROUP BY 1, 2, 3, 4, 5, 6, 7
);



-- Email Campaign Base table - for every campaign, row for each date between campaign start and end date
CREATE OR REPLACE TEMP TABLE email_campaign_dates_base_iterable_triggered as (
SELECT dd.full_date, a.*
FROM email_campaign_min_max_dates_iterable_triggered a
JOIN edw_prod.data_model.dim_date dd ON dd.full_date BETWEEN a.start_date AND a.end_date
order by dd.full_date
);



---------------- Email Sends - Emarsys + Iterable Blast + Iterable Triggered  ----------------------
-- Email Sends Tables: Row for every email send  --- calc volume of sends for every campaign and date
-- Joined to Campaign Dimensions

-- Emarsys email sends
CREATE OR REPLACE TEMP TABLE sends_emarsys AS (
       SELECT s.campaign_id,
              c.campaign_name,
              c.campaign_type,
              c.category_name,
              c.subject,
              s.store_group,
              s.store_group as project_name,
              s.event_time::DATE           AS send_date,
              COUNT(DISTINCT s.contact_id) AS send_count
       FROM lake_view.emarsys.email_sends AS s
       LEFT JOIN campaign_dim AS c
                    ON s.campaign_id = c.campaign_id
                    AND s.store_group = c.store_group
       WHERE s.store_group LIKE 'savagex%'
       AND s.event_time::DATE  >= '2022-03-01'
       GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
);

-- Iterable email blast sends
CREATE OR REPLACE TEMP TABLE sends_iterable AS (
       SELECT s.campaign_id,
               c.campaign_name,
               c.campaign_type,
               c.category_name,
               c.subject,
               sm.store_group,
               s.project_name,
               CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', s.created_at)::DATE AS send_date,
               COUNT(DISTINCT s.message_id)                                       AS send_count
        FROM campaign_event_data.org_3223.email_blast_sends_view AS s
        LEFT JOIN _iterable_customer_store_mapping sm
                           ON s.itbl_user_id = sm.itbl_user_id
        LEFT JOIN campaign_dim AS c
                          ON s.campaign_id = c.campaign_id
                          AND sm.store_group = c.store_group
        WHERE sm.store_brand ILIKE 'Savage X%'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
);


-- Iterable email triggered sends
-- Email triggered Sends = Base table = campaign_dates_base = for every campaign, row for each date between campaign start and end date
-- when joining to click data, join on date - dates that have clicks and no sends will still be included

CREATE OR REPLACE TEMP TABLE  sends_iterable_triggered AS (
       SELECT cb.campaign_id,
                 cb.campaign_name,
                 cb.campaign_type,
                 cb.category_name,
                 cb.subject,
                 cb.store_group,
                 cb.PROJECT_NAME,
                 cb.full_date as send_date,
                 COUNT(DISTINCT s.message_id)                                       AS send_count
          FROM email_campaign_dates_base_iterable_triggered cb
          LEFT JOIN campaign_event_data.org_3223.email_triggered_sends_view AS s ON CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', s.created_at)::DATE = cb.full_date
                                                                                 AND s.campaign_id = cb.campaign_id
                                                                                 AND s.PROJECT_NAME = cb.PROJECT_NAME
          LEFT JOIN _iterable_customer_store_mapping sm ON s.itbl_user_id = sm.itbl_user_id
          LEFT JOIN campaign_dim AS c ON s.campaign_id = c.campaign_id
                                      AND sm.store_group = c.store_group
          WHERE cb.PROJECT_NAME ilike 'PRD-SavageX%'
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
);


-- all email sends = union emarsys, iterable blast, and iterable triggered
CREATE OR REPLACE TEMP TABLE sends AS (
       SELECT * FROM sends_emarsys
       UNION
       SELECT * FROM sends_iterable
       UNION
       SELECT * FROM sends_iterable_triggered
);


---------------- Email Bounces - Emarsys + Iterable ----------------------
-- Row for every campaign + date + count of total bounces, soft bounces, hard bounces, and blocks

CREATE OR REPLACE TEMP TABLE bounces AS (
 -- Emarsys bounces:
       SELECT b.store_group,
            b.campaign_id,
            b.email_sent_at::DATE                                                      AS send_date,
            COUNT(DISTINCT b.contact_id)                                               AS total_bounce_count,
            COUNT(DISTINCT CASE WHEN LOWER(bounce_type) = 'soft' THEN contact_id END)  AS soft_bounce_count,
            COUNT(DISTINCT CASE WHEN LOWER(bounce_type) = 'hard' THEN contact_id END)  AS hard_bounce_count,
            COUNT(DISTINCT CASE WHEN LOWER(bounce_type) = 'block' THEN contact_id END) AS block_count
     FROM lake_view.emarsys.email_bounces b
     WHERE b.store_group LIKE 'savagex%'
    AND b.event_time::DATE >= '2022-03-01'
     GROUP BY 1, 2, 3

     UNION
      -- Iterable bounces:
     SELECT sm.store_group,
            b.campaign_id,
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', b.created_at)::DATE                         AS send_date,
            COUNT(DISTINCT b.message_id)                                                               AS total_bounce_count,
            COUNT(DISTINCT CASE WHEN LOWER(recipient_state) ILIKE '%soft%' THEN b.message_id END)      AS soft_bounce_count,
            COUNT(DISTINCT CASE WHEN LOWER(recipient_state) ILIKE '%hard%' THEN b.message_id END)      AS hard_bounce_count,
            COUNT(DISTINCT CASE WHEN (LOWER(recipient_state) NOT ILIKE '%soft%' AND LOWER(recipient_state) NOT ILIKE '%hard%') THEN b.message_id END) AS block_count
     FROM campaign_event_data.org_3223.email_bounces_view b
              LEFT JOIN _iterable_customer_store_mapping sm ON b.itbl_user_id = sm.itbl_user_id
     WHERE sm.store_brand ILIKE 'Savage X%'
     GROUP BY 1, 2, 3
);


---------------------- Email Opens - Emarsys + Iterable ----------------------
-- Row for every campaign + date + count of opens

CREATE OR REPLACE TEMP TABLE opens AS (
    -- Emarsys email opens:
       SELECT o.campaign_id,
            o.store_group,
            o.email_sent_at::DATE      AS send_date,
            COUNT(DISTINCT contact_id) AS open_count
     FROM lake_view.emarsys.email_opens AS o
     WHERE o.store_group LIKE 'savagex%'
    AND o.event_time::DATE >= '2022-03-01'
     GROUP BY 1, 2, 3

     UNION
   -- iterable email opens:
     SELECT o.campaign_id,
            sm.store_group,
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', o.created_at)::DATE AS send_date,
            COUNT(DISTINCT o.message_id)                                       AS open_count
     FROM campaign_event_data.org_3223.email_opens_view AS o
              LEFT JOIN _iterable_customer_store_mapping sm ON o.itbl_user_id = sm.itbl_user_id
     WHERE sm.store_brand ILIKE 'Savage X%'
     GROUP BY 1, 2, 3
);

---------------------- Email Clicks - Emarsys + Iterable ----------------------
-- Row for every campaign + date + count of clicks

CREATE OR REPLACE TEMP TABLE clicks AS (
     -- emarsys email clicks:
       SELECT c.campaign_id,
            c.store_group,
            c.email_sent_at::DATE        AS send_date,
            COUNT(DISTINCT c.contact_id) AS click_count
     FROM lake_view.emarsys.email_clicks AS c
     WHERE c.store_group LIKE 'savagex%'
    AND c.event_time::DATE  >= '2022-03-01'
     GROUP BY 1, 2, 3

     UNION
    -- iterable email clicks:
     SELECT c.campaign_id,
            sm.store_group,
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', c.created_at)::DATE AS send_date,
            COUNT(DISTINCT c.message_id)                                       AS click_count
     FROM campaign_event_data.org_3223.email_clicks_view AS c
              LEFT JOIN _iterable_customer_store_mapping sm
                        ON c.itbl_user_id = sm.itbl_user_id
     WHERE sm.store_brand ILIKE 'Savage X%'
     GROUP BY 1, 2, 3
);

---------------------- Email Unsubscribes - Emarsys + Iterable ----------------------
-- Row for every campaign + date + count of unsubscribes


CREATE OR REPLACE TEMP TABLE unsubscribes AS (
    -- emarsys unsubscribes:
     SELECT u.campaign_id,
            u.store_group,
            u.email_sent_at::DATE        AS send_date,
            COUNT(DISTINCT u.contact_id) AS unsubscribe_count
     FROM lake_view.emarsys.email_unsubscribes AS u
     WHERE u.store_group LIKE 'savagex%'
    AND u.event_time::DATE  >= '2022-03-01'
     GROUP BY 1, 2, 3
     UNION
    -- iterable unsubscribes:
     SELECT u.campaign_id,
            sm.store_group,
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', u.created_at)::DATE AS send_date,
            COUNT(DISTINCT u.message_id)                                       AS unsubscribe_count
     FROM campaign_event_data.org_3223.email_unsubscribes_view AS u
              LEFT JOIN _iterable_customer_store_mapping sm ON u.itbl_user_id = sm.itbl_user_id
     WHERE sm.store_brand ILIKE 'Savage X%'
     GROUP BY 1, 2, 3
);

---------------- SMS Sends - Iterable ----------------------

-- SMS Sends = Base table = campaign_dates_base = for every campaign, row for each date between campaign start and end date
-- when joining to click data, join on date - dates that have clicks and no sends will still be included

CREATE OR REPLACE temp TABLE sms_sends as (
       SELECT distinct  cb.campaign_id,
                               cb.campaign_name,
                               cb.campaign_type,
                               cb.category_name,
                               cb.subject,
                               cb.store_group,
                               cb.PROJECT_NAME,
                               cb.full_date as send_date,
                               COUNT(DISTINCT s.message_id)                                       AS send_count
         FROM sms_campaign_dates_base cb
         LEFT JOIN CAMPAIGN_EVENT_DATA.ORG_3223.SMS_SENDS_VIEW s ON CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', s.created_at)::DATE = cb.full_date
                                                                 AND s.campaign_id = cb.campaign_id
                                                                 AND s.PROJECT_NAME = cb.PROJECT_NAME
         LEFT JOIN campaign_dim AS c ON  s.campaign_id = c.campaign_id
         WHERE cb.PROJECT_NAME ilike 'PRD-SavageX%'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
         order by cb.campaign_id, cb.full_date
);



---------------- SMS Clicks - Iterable ----------------------

CREATE OR REPLACE TEMP TABLE sms_clicks as (
       SELECT c.campaign_id,
                 cd.campaign_name,
                 cd.campaign_type,
                 cd.category_name,
                 cd.subject,
                 cd.store_group,
                 c.PROJECT_NAME,
                 CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', c.created_at)::DATE AS send_date,
                 COUNT(DISTINCT c.message_id)                                       AS click_count
       FROM CAMPAIGN_EVENT_DATA.ORG_3223.SMS_CLICKS_VIEW c
       LEFT JOIN campaign_dim AS cd ON  c.campaign_id = cd.campaign_id
       WHERE c.PROJECT_NAME ilike 'PRD-SavageX%'
       GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
);


---------------- SMS Unsubscribes - Iterable ----------------------

CREATE OR REPLACE TEMP TABLE sms_unsubscribes as (
       SELECT u.campaign_id,
            U.PROJECT_NAME,
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', u.created_at)::DATE   AS send_date,
            COUNT(DISTINCT u.itbl_user_id)                                       AS unsubscribe_count
     FROM campaign_event_data.org_3223.email_unsubscribes_view AS u
     where u.PROJECT_NAME ilike 'PRD-SavageX%'
     and u.TEMPLATE_ID is not null
     GROUP BY 1, 2, 3
);


---------------- SMS Bounces - Iterable ----------------------
CREATE OR REPLACE TEMP TABLE sms_bounces as (
       SELECT b.campaign_id,
                B.PROJECT_NAME,
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', b.created_at)::DATE                         AS send_date,
                COUNT(DISTINCT b.message_id)                                                               AS total_bounce_count,
                NULL AS soft_bounce_count,
                NULL AS hard_bounce_count,
                NULL AS block_count
         from CAMPAIGN_EVENT_DATA.ORG_3223.SMS_BOUNCES_VIEW b
             -- LEFT JOIN _iterable_customer_store_mapping sm ON b.itbl_user_id = sm.itbl_user_id
         WHERE b.PROJECT_NAME ilike 'PRD-SavageX%'
         GROUP BY 1, 2, 3
);



----------------------------------- GROWTH METRICS SECTION - Email and SMS -----------------------------------------------
-- Growth metrics section = blast campaigns - one row per campaign - send data is aggregated by campaign id to one send date
-- Opens/clicks/unsubs/bounces aggregated to campaign id before joined with sends


-- SMS Growth/Blast Metrics: One row for every campaign: sum of sends, opens, clicks, etc. - aggregated to send date
-- Modify store_country: EU --> EUREM to match fact order
CREATE OR REPLACE TEMP TABLE savagex_emarsys_iterable_metrics_growth_sms as (
       SELECT MIN(s.send_date)                                     AS send_date,
            'sms'                                                  AS channel,
            s.campaign_id,
            s.campaign_name,
            s.campaign_type,
            s.category_name,
            s.subject,
            'Savage X'                                             AS store_brand,
            s.store_group,
            CASE WHEN UPPER(SPLIT_PART(s.store_group, '_', -1)) = 'EU' THEN 'EUREM'
                 ELSE UPPER(SPLIT_PART(s.store_group, '_', -1)) END AS store_country,
            SUM(COALESCE(s.send_count, 0))                         AS send_count,
            NULL                                                   AS unique_open_count,
            max(COALESCE(c.click_count, 0))                        AS unique_click_count,
            max(COALESCE(u.unsubscribe_count, 0))                  AS unique_unsubscribe_count,
            max(COALESCE(b.total_bounce_count, 0))                 AS unique_total_bounce_count,
            max(COALESCE(b.soft_bounce_count, 0))                  AS unique_soft_bounce_count,
            max(COALESCE(b.hard_bounce_count, 0))                  AS unique_hard_bounce_count,
            max(COALESCE(b.block_count, 0))                        AS unique_block_count
     FROM sms_sends AS s
              LEFT JOIN (SELECT DISTINCT PROJECT_NAME
                                       , campaign_id
                                       , SUM(total_bounce_count) AS total_bounce_count
                                       , SUM(soft_bounce_count)  AS soft_bounce_count
                                       , SUM(hard_bounce_count)  AS hard_bounce_count
                                       , SUM(block_count)        AS block_count
                         FROM sms_bounces
                         GROUP BY 1,2
                        ) AS b ON s.campaign_id = b.campaign_id AND s.PROJECT_NAME = b.PROJECT_NAME

              LEFT JOIN (SELECT DISTINCT PROJECT_NAME
                                       , campaign_id
                                       , SUM(click_count) AS click_count
                         FROM sms_clicks
                         GROUP BY 1,2
                        ) AS c ON s.campaign_id = c.campaign_id AND s.PROJECT_NAME = c.PROJECT_NAME

              LEFT JOIN (SELECT DISTINCT PROJECT_NAME
                                       , campaign_id
                                       , SUM(unsubscribe_count) AS unsubscribe_count
                         FROM sms_unsubscribes
                         GROUP BY 1, 2
                        ) AS u ON s.campaign_id = u.campaign_id AND s.PROJECT_NAME = u.PROJECT_NAME
     WHERE LOWER(s.campaign_type) IN ('blast')
     GROUP BY 2, 3, 4, 5, 6, 7, 8, 9
);



-- Email Blast/Growth Metrics: Row for every campaign: send date, sum of sends, opens, clicks, etc.
-- Modify store_country: EU --> EUREM to match fact order
CREATE OR REPLACE TEMP TABLE savagex_emarsys_iterable_metrics_growth_email AS (
       SELECT MIN(s.send_date)                                       AS send_date,
            'email'                                                AS channel,
            (CASE
                 WHEN s.campaign_id = '273736' AND LOWER(s.store_group) = 'savagex_us' THEN '273730'
                 ELSE s.campaign_id END)                           AS campaign_id,
            --s.campaign_id,
            s.campaign_name,
            s.campaign_type,
            s.category_name,
            s.subject,
            'Savage X'                                             AS store_brand,
            s.store_group,
            CASE
                WHEN UPPER(SPLIT_PART(s.store_group, '_', -1)) = 'EU' THEN 'EUREM'
                ELSE UPPER(SPLIT_PART(s.store_group, '_', -1)) END AS store_country,
            SUM(COALESCE(s.send_count, 0))                         AS send_count,
            SUM(COALESCE(o.open_count, 0))                         AS unique_open_count,
            SUM(COALESCE(c.click_count, 0))                        AS unique_click_count,
            SUM(COALESCE(u.unsubscribe_count, 0))                  AS unique_unsubscribe_count,
            SUM(COALESCE(b.total_bounce_count, 0))                 AS unique_total_bounce_count,
            SUM(COALESCE(b.soft_bounce_count, 0))                  AS unique_soft_bounce_count,
            SUM(COALESCE(b.hard_bounce_count, 0))                  AS unique_hard_bounce_count,
            SUM(COALESCE(b.block_count, 0))                        AS unique_block_count
     FROM sends AS s
              LEFT JOIN (SELECT DISTINCT store_group
                                       , campaign_id
                                       , SUM(total_bounce_count) AS total_bounce_count
                                       , SUM(soft_bounce_count)  AS soft_bounce_count
                                       , SUM(hard_bounce_count)  AS hard_bounce_count
                                       , SUM(block_count)        AS block_count
                         FROM bounces
                         GROUP BY 1, 2
                        ) AS b ON s.campaign_id = b.campaign_id AND s.store_group = b.store_group

              LEFT JOIN (SELECT DISTINCT store_group
                                       , campaign_id
                                       , SUM(open_count) AS open_count
                         FROM opens
                         GROUP BY 1, 2
                        ) AS o ON s.campaign_id = o.campaign_id AND s.store_group = o.store_group

              LEFT JOIN (SELECT DISTINCT store_group
                                       , campaign_id
                                       , SUM(click_count) AS click_count
                         FROM clicks
                         GROUP BY 1, 2
                        ) AS c ON s.campaign_id = c.campaign_id AND s.store_group = c.store_group

              LEFT JOIN (SELECT DISTINCT store_group
                                       , campaign_id
                                       , SUM(unsubscribe_count) AS unsubscribe_count
                         FROM unsubscribes
                         GROUP BY 1, 2
                        ) AS u ON s.campaign_id = u.campaign_id AND s.store_group = u.store_group
     WHERE LOWER(s.campaign_type) IN ('batch', 'blast')
     GROUP BY 2, 3, 4, 5, 6, 7, 8, 9
);

-- Email Blast + SMS Blast Attribution: Join Email to attribution and join sms to attribution
-- Row for every campaign: campaign info + email metrics + revenue metrics

CREATE OR REPLACE TEMP TABLE crm_attribution_growth AS (
    -- email blast metrics joined to email attribution:
       SELECT e.store_group                                  AS store_name,
               e.store_country,
               e.store_group,
               'email'                                        AS channel,
               e.campaign_id,
               e.campaign_name,
               e.campaign_type,
               e.category_name,
               e.subject,
               e.send_date,
               EMAIL_OR_SMS,
               COALESCE(m.gaap, 0)                            AS revenue,
               COALESCE(m.repeat_vip_gaap, 0)                 AS repeat_vip_revenue,
               COALESCE(m.activating_vip_gaap, 0)             AS activating_vip_revenue,
               COALESCE(m.guest_gaap, 0)                      AS guest_revenue,

               e.send_count,
               e.unique_open_count                            AS unique_opens,
               e.unique_click_count                           AS unique_clicks,
               e.unique_unsubscribe_count                     AS unique_unsubscribes,

               COALESCE(m.orders, 0)                          AS purchases,
               COALESCE(m.repeat_vip_orders, 0)               AS repeat_vip_purchases,
               COALESCE(m.activating_vip_orders, 0)           AS activating_vip_purchases,
               COALESCE(m.guest_orders, 0)                    AS guest_purchases,

               COALESCE(m.second_orders, 0)                   AS second_purchases,
               COALESCE(m.repeat_vip_second_orders, 0)        AS repeat_vip_second_purchases,
               COALESCE(m.activating_vip_second_orders, 0)    AS activating_vip_second_purchases,
               COALESCE(m.guest_second_orders, 0)             AS guest_second_purchases,

               COALESCE(m.units, 0)                           AS units,
               COALESCE(m.repeat_vip_units, 0)                AS repeat_vip_units,
               COALESCE(m.activating_vip_units, 0)            AS activating_vip_units,
               COALESCE(m.guest_units, 0)                     AS guest_units,

               COALESCE(m.credits_redeemed, 0)                AS credits_redeemed,
               COALESCE(m.repeat_vip_credits_redeemed, 0)     AS repeat_vip_credits_redeemed,
               COALESCE(m.activating_vip_credits_redeemed, 0) AS activating_vip_credits_redeemed,
               COALESCE(m.guest_credits_redeemed, 0)          AS guest_credits_redeemed,

               COALESCE(m.product_margin, 0)                  AS product_margin,
               COALESCE(m.repeat_vip_product_margin, 0)       AS repeat_vip_product_margin,
               COALESCE(m.activating_vip_product_margin, 0)   AS activating_vip_product_margin,
               COALESCE(m.guest_product_margin, 0)            AS guest_product_margin,

               e.unique_total_bounce_count                    AS unique_bounces,
               e.unique_hard_bounce_count                     AS unique_hard_bounces,
               e.unique_soft_bounce_count                     AS unique_soft_bounces,
               e.unique_block_count                           AS unique_blocks

        FROM savagex_emarsys_iterable_metrics_growth_email AS e
                 LEFT JOIN email_utm_attribution_model_growth AS m
                               ON  e.campaign_id::VARCHAR = m.campaign_id::VARCHAR
                               AND e.store_country = m.store_country
      UNION ALL
    -- sms blast metrics joined to sms attribution:
       SELECT
               e.store_group                                  AS store_name,
               e.store_country,
               e.store_group,
               'sms'                                        AS channel,
               e.campaign_id,
               e.campaign_name,
               e.campaign_type,
               e.category_name,
               e.subject,
               e.send_date,
               EMAIL_OR_SMS,
               COALESCE(m.gaap, 0)                            AS revenue,
               COALESCE(m.repeat_vip_gaap, 0)                 AS repeat_vip_revenue,
               COALESCE(m.activating_vip_gaap, 0)             AS activating_vip_revenue,
               COALESCE(m.guest_gaap, 0)                      AS guest_revenue,

               e.send_count,
               e.unique_open_count                            AS unique_opens,
               e.unique_click_count                           AS unique_clicks,
               e.unique_unsubscribe_count                     AS unique_unsubscribes,

               COALESCE(m.orders, 0)                          AS purchases,
               COALESCE(m.repeat_vip_orders, 0)               AS repeat_vip_purchases,
               COALESCE(m.activating_vip_orders, 0)           AS activating_vip_purchases,
               COALESCE(m.guest_orders, 0)                    AS guest_purchases,

               COALESCE(m.second_orders, 0)                   AS second_purchases,
               COALESCE(m.repeat_vip_second_orders, 0)        AS repeat_vip_second_purchases,
               COALESCE(m.activating_vip_second_orders, 0)    AS activating_vip_second_purchases,
               COALESCE(m.guest_second_orders, 0)             AS guest_second_purchases,

               COALESCE(m.units, 0)                           AS units,
               COALESCE(m.repeat_vip_units, 0)                AS repeat_vip_units,
               COALESCE(m.activating_vip_units, 0)            AS activating_vip_units,
               COALESCE(m.guest_units, 0)                     AS guest_units,

               COALESCE(m.credits_redeemed, 0)                AS credits_redeemed,
               COALESCE(m.repeat_vip_credits_redeemed, 0)     AS repeat_vip_credits_redeemed,
               COALESCE(m.activating_vip_credits_redeemed, 0) AS activating_vip_credits_redeemed,
               COALESCE(m.guest_credits_redeemed, 0)          AS guest_credits_redeemed,

               COALESCE(m.product_margin, 0)                  AS product_margin,
               COALESCE(m.repeat_vip_product_margin, 0)       AS repeat_vip_product_margin,
               COALESCE(m.activating_vip_product_margin, 0)   AS activating_vip_product_margin,
               COALESCE(m.guest_product_margin, 0)            AS guest_product_margin,

               e.unique_total_bounce_count                    AS unique_bounces,
               e.unique_hard_bounce_count                     AS unique_hard_bounces,
               e.unique_soft_bounce_count                     AS unique_soft_bounces,
               e.unique_block_count                           AS unique_blocks

       FROM savagex_emarsys_iterable_metrics_growth_sms AS e
        LEFT JOIN sms_utm_attribution_model_growth AS m
               ON e.campaign_id::VARCHAR = m.campaign_id::VARCHAR
               AND e.store_country = m.store_country
);


----------------------------------- CRM ATTRIBUTION TRIGGERED SECTION - Aggregate orders/revenue to campaign id and date -----------------------
-- Triggered metrics section = triggered campaigns - one row per campaign and send date - data is aggregated to send date
-- Opens/clicks/unsubs/bounces joined on send date


---------------- Email Triggered - Attribution Table + Email Metrics table ----------------------

-- Email Triggered Campaign Attribution Table: Row per campaign and send date, orders/revenue aggregated by campaign id and date
-- For every campaign ID and send date: count of orders
-- Campaign ID pulled from Campaign Name

CREATE OR REPLACE TEMP TABLE  email_utm_attribution_model_triggered AS (
       SELECT c.store_brand,
              c.store_country,
              SPLIT_PART(c.campaign_name, 'cid-', -1)                                         AS campaign_id,
              c.event_time::DATE                                                              AS event_time,
              EMAIL_OR_SMS,
              SUM(COALESCE(c.orders, 0))                                                      AS orders,
              SUM(COALESCE(c.second_orders, 0))                                               AS second_orders,
              SUM(COALESCE(c.units, 0))                                                       AS units,
              SUM(COALESCE(c.credits_redeemed, 0))                                            AS credits_redeemed,
              SUM(COALESCE(c.gaap, 0))                                                        AS gaap,
              SUM(COALESCE(c.product_margin, 0))                                              AS product_margin,
              SUM(COALESCE(c.product_cash_margin, 0))                                         AS product_cash_margin,

              SUM(CASE WHEN order_type = 'Repeat VIP' THEN c.orders ELSE NULL END)            AS repeat_vip_orders,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN c.orders ELSE NULL END)        AS activating_vip_orders,
              SUM(CASE WHEN order_type = 'Guest' THEN c.orders ELSE NULL END)                 AS guest_orders,

              SUM(CASE WHEN order_type = 'Repeat VIP' THEN c.second_orders ELSE NULL END)     AS repeat_vip_second_orders,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN c.second_orders ELSE NULL END) AS activating_vip_second_orders,
              SUM(CASE WHEN order_type = 'Guest' THEN c.second_orders ELSE NULL END)          AS guest_second_orders,

              SUM(CASE WHEN order_type = 'Repeat VIP'      THEN COALESCE(c.units, 0) ELSE NULL END)     AS repeat_vip_units,
              SUM(CASE WHEN order_type = 'Activating VIP'  THEN COALESCE(c.units, 0) ELSE NULL END)     AS activating_vip_units,
              SUM(CASE WHEN order_type = 'Guest'           THEN COALESCE(c.units, 0) ELSE NULL END)     AS guest_units,

              SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END)   AS repeat_vip_credits_redeemed,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END)   AS activating_vip_credits_redeemed,
              SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END)   AS guest_credits_redeemed,

              SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.gaap, 0) ELSE NULL END)   AS repeat_vip_gaap,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.gaap, 0) ELSE NULL END)   AS activating_vip_gaap,
              SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.gaap, 0) ELSE NULL END)   AS guest_gaap,

              SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.product_margin, 0) ELSE NULL END) AS repeat_vip_product_margin,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.product_margin, 0) ELSE NULL END) AS activating_vip_product_margin,
              SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.product_margin, 0) ELSE NULL END) AS guest_product_margin,

              SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.product_margin, 0) ELSE NULL END)     AS repeat_vip_product_cash_margin,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.product_margin, 0) ELSE NULL END)     AS activating_vip_product_cash_margin,
              SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.product_margin, 0) ELSE NULL END)     AS guest_product_cash_margin

       FROM customer_level_attribution c
       GROUP BY 1, 2, 3, 4, 5
);


-- Email Triggered Metrics: row for every campaign and send date
CREATE OR REPLACE TEMP TABLE savagex_emarsys_iterable_metrics_triggered AS (
       SELECT s.send_date                                          AS send_date,
            'email'                                                AS channel,
            s.campaign_id,
            s.campaign_name,
            s.campaign_type,
            s.category_name,
            s.subject,
            'Savage X'                                             AS store_brand,
            s.store_group,
            CASE WHEN UPPER(SPLIT_PART(s.store_group, '_', -1)) = 'EU' THEN 'EUREM'
                ELSE UPPER(SPLIT_PART(s.store_group, '_', -1)) END AS store_country,
            SUM(COALESCE(s.send_count, 0))                         AS send_count,
            SUM(COALESCE(o.open_count, 0))                         AS unique_open_count,
            SUM(COALESCE(c.click_count, 0))                        AS unique_click_count,
            SUM(COALESCE(u.unsubscribe_count, 0))                  AS unique_unsubscribe_count,
            SUM(COALESCE(b.total_bounce_count, 0))                 AS unique_total_bounce_count,
            SUM(COALESCE(b.soft_bounce_count, 0))                  AS unique_soft_bounce_count,
            SUM(COALESCE(b.hard_bounce_count, 0))                  AS unique_hard_bounce_count,
            SUM(COALESCE(b.block_count, 0))                        AS unique_block_count
     FROM sends AS s
              LEFT JOIN bounces AS b ON
                 s.campaign_id = b.campaign_id AND
                 s.store_group = b.store_group AND
                 s.send_date = b.send_date
              LEFT JOIN opens AS o ON
                 s.campaign_id = o.campaign_id AND
                 s.store_group = o.store_group AND
                 s.send_date = o.send_date
              LEFT JOIN clicks AS c ON
                 s.campaign_id = c.campaign_id AND
                 s.store_group = c.store_group AND
                 s.send_date = c.send_date
              LEFT JOIN unsubscribes AS u ON
                 s.campaign_id = u.campaign_id AND
                 s.store_group = u.store_group AND
                 s.send_date = u.send_date
     WHERE LOWER(s.campaign_type) NOT IN ('batch', 'blast')
     GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
     ORDER BY 1, 2
);


---------------- SMS Triggered - Attribution Table + Email Metrics table ----------------------

-- SMS Triggered Campaign Attribution Table: Row per campaign and send date, orders/revenue aggregated by campaign id and date
-- For every campaign ID and send date: count of orders
-- Campaign ID pulled from Campaign Name

CREATE OR REPLACE TEMP TABLE sms_utm_attribution_model_triggered AS (
       SELECT c.store_brand,
              c.store_country,
              SPLIT_PART(c.campaign_name, 'cid-', -1)                                         AS campaign_id,
              c.event_time::DATE                                                              AS event_time,
              EMAIL_OR_SMS,
              SUM(COALESCE(c.orders, 0))                                                      AS orders,
              SUM(COALESCE(c.second_orders, 0))                                               AS second_orders,
              SUM(COALESCE(c.units, 0))                                                       AS units,
              SUM(COALESCE(c.credits_redeemed, 0))                                            AS credits_redeemed,
              SUM(COALESCE(c.gaap, 0))                                                        AS gaap,
              SUM(COALESCE(c.product_margin, 0))                                              AS product_margin,
              SUM(COALESCE(c.product_cash_margin, 0))                                         AS product_cash_margin,

              SUM(CASE WHEN order_type = 'Repeat VIP' THEN c.orders ELSE NULL END)            AS repeat_vip_orders,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN c.orders ELSE NULL END)        AS activating_vip_orders,
              SUM(CASE WHEN order_type = 'Guest' THEN c.orders ELSE NULL END)                 AS guest_orders,

              SUM(CASE WHEN order_type = 'Repeat VIP' THEN c.second_orders ELSE NULL END)     AS repeat_vip_second_orders,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN c.second_orders ELSE NULL END) AS activating_vip_second_orders,
              SUM(CASE WHEN order_type = 'Guest' THEN c.second_orders ELSE NULL END)          AS guest_second_orders,

              SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.units, 0) ELSE NULL END)  AS repeat_vip_units,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.units, 0) ELSE NULL END)  AS activating_vip_units,
              SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.units, 0) ELSE NULL END)  AS guest_units,

              SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END)   AS repeat_vip_credits_redeemed,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END)   AS activating_vip_credits_redeemed,
              SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.credits_redeemed, 0) ELSE NULL END)   AS guest_credits_redeemed,

              SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.gaap, 0) ELSE NULL END)   AS repeat_vip_gaap,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.gaap, 0) ELSE NULL END)   AS activating_vip_gaap,
              SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.gaap, 0) ELSE NULL END)   AS guest_gaap,

              SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.product_margin, 0) ELSE NULL END)    AS repeat_vip_product_margin,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.product_margin, 0) ELSE NULL END)    AS activating_vip_product_margin,
              SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.product_margin, 0) ELSE NULL END)    AS guest_product_margin,

              SUM(CASE WHEN order_type = 'Repeat VIP'     THEN COALESCE(c.product_margin, 0) ELSE NULL END)    AS repeat_vip_product_cash_margin,
              SUM(CASE WHEN order_type = 'Activating VIP' THEN COALESCE(c.product_margin, 0) ELSE NULL END)    AS activating_vip_product_cash_margin,
              SUM(CASE WHEN order_type = 'Guest'          THEN COALESCE(c.product_margin, 0) ELSE NULL END)    AS guest_product_cash_margin
       FROM customer_level_attribution c
       GROUP BY 1, 2, 3, 4, 5
);




-- SMS Triggered Metrics: row for every campaign and send date
CREATE OR REPLACE TEMP TABLE  savagex_iterable_metrics_triggered_sms AS (
       SELECT s.send_date                                          AS send_date,
            'sms'                                                  AS channel,
            s.campaign_id,
            s.campaign_name,
            s.campaign_type,
            s.category_name,
            s.subject,
            'Savage X'                                             AS store_brand,
            s.store_group,
            CASE WHEN UPPER(SPLIT_PART(s.store_group, '_', -1)) = 'EU' THEN 'EUREM'
                 ELSE UPPER(SPLIT_PART(s.store_group, '_', -1)) END AS store_country,
            SUM(COALESCE(s.send_count, 0))                         AS send_count,
            NULL                                                   AS unique_open_count,
            SUM(COALESCE(c.click_count, 0))                        AS unique_click_count,
            SUM(COALESCE(u.unsubscribe_count, 0))                  AS unique_unsubscribe_count,
            SUM(COALESCE(b.total_bounce_count, 0))                 AS unique_total_bounce_count,
            SUM(COALESCE(b.soft_bounce_count, 0))                  AS unique_soft_bounce_count,
            SUM(COALESCE(b.hard_bounce_count, 0))                  AS unique_hard_bounce_count,
            SUM(COALESCE(b.block_count, 0))                        AS unique_block_count
     FROM sms_sends AS s
             LEFT JOIN sms_clicks AS c
                       ON s.campaign_id = c.campaign_id
                       AND s.PROJECT_NAME = c.PROJECT_NAME
                       AND s.send_date = c.send_date

              LEFT JOIN sms_bounces AS b
                       ON s.campaign_id = b.campaign_id
                       AND s.PROJECT_NAME = b.PROJECT_NAME
                       AND s.send_date = b.send_date

             LEFT JOIN sms_unsubscribes AS u
                       ON s.campaign_id = u.campaign_id
                       AND s.PROJECT_NAME = u.PROJECT_NAME
                       AND s.send_date = u.send_date
     WHERE LOWER(s.campaign_type) IN ('triggered')
     GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
);



-- Email Triggered + SMS Triggered Attribution: Join Email to attribution and join sms to attribution
-- Row for every campaign: campaign info + sms metrics + revenue metrics

CREATE OR REPLACE TEMP TABLE  crm_attribution_triggered AS (
    -- email triggered:
       SELECT LOWER(e.store_group)                           AS store_name,
            e.store_country,
            e.store_group,
            'email'                                        AS channel,
            e.campaign_id,
            e.campaign_name,
            e.campaign_type,
            e.category_name,
            e.subject,
            e.send_date,
            EMAIL_OR_SMS,

            COALESCE(m.gaap, 0)                            AS revenue,
            COALESCE(m.repeat_vip_gaap, 0)                 AS repeat_vip_revenue,
            COALESCE(m.activating_vip_gaap, 0)             AS activating_vip_revenue,
            COALESCE(m.guest_gaap, 0)                      AS guest_revenue,

            e.send_count,
            e.unique_open_count                            AS unique_opens,
            e.unique_click_count                           AS unique_clicks,
            e.unique_unsubscribe_count                     AS unique_unsubscribes,

            COALESCE(m.orders, 0)                          AS purchases,
            COALESCE(m.repeat_vip_orders, 0)               AS repeat_vip_purchases,
            COALESCE(m.activating_vip_orders, 0)           AS activating_vip_purchases,
            COALESCE(m.guest_orders, 0)                    AS guest_purchases,

            COALESCE(m.second_orders, 0)                   AS second_purchases,
            COALESCE(m.repeat_vip_second_orders, 0)        AS repeat_vip_second_purchases,
            COALESCE(m.activating_vip_second_orders, 0)    AS activating_vip_second_purchases,
            COALESCE(m.guest_second_orders, 0)             AS guest_second_purchases,

            COALESCE(m.units, 0)                           AS units,
            COALESCE(m.repeat_vip_units, 0)                AS repeat_vip_units,
            COALESCE(m.activating_vip_units, 0)            AS activating_vip_units,
            COALESCE(m.guest_units, 0)                     AS guest_units,

            COALESCE(m.credits_redeemed, 0)                AS credits_redeemed,
            COALESCE(m.repeat_vip_credits_redeemed, 0)     AS repeat_vip_credits_redeemed,
            COALESCE(m.activating_vip_credits_redeemed, 0) AS activating_vip_credits_redeemed,
            COALESCE(m.guest_credits_redeemed, 0)          AS guest_credits_redeemed,

            COALESCE(m.product_margin, 0)                  AS product_margin,
            COALESCE(m.repeat_vip_product_margin, 0)       AS repeat_vip_product_margin,
            COALESCE(m.activating_vip_product_margin, 0)   AS activating_vip_product_margin,
            COALESCE(m.guest_product_margin, 0)            AS guest_product_margin,


            e.unique_total_bounce_count                    AS unique_bounces,
            e.unique_hard_bounce_count                     AS unique_hard_bounces,
            e.unique_soft_bounce_count                     AS unique_soft_bounces,
            e.unique_block_count                           AS unique_blocks

     FROM savagex_emarsys_iterable_metrics_triggered AS e
              LEFT JOIN email_utm_attribution_model_triggered AS m
                        ON e.campaign_id::VARCHAR = m.campaign_id::VARCHAR
                        AND e.store_country = m.store_country
                        AND e.send_date = m.event_time

     UNION ALL

  -- SMS triggered:
     SELECT LOWER(e.store_group)                           AS store_name,
            e.store_country,
            e.store_group,
            'sms'                                        AS channel,
            e.campaign_id,
            e.campaign_name,
            e.campaign_type,
            e.category_name,
            e.subject,
            e.send_date,
            EMAIL_OR_SMS,

            COALESCE(m.gaap, 0)                            AS revenue,
            COALESCE(m.repeat_vip_gaap, 0)                 AS repeat_vip_revenue,
            COALESCE(m.activating_vip_gaap, 0)             AS activating_vip_revenue,
            COALESCE(m.guest_gaap, 0)                      AS guest_revenue,

            e.send_count,
            e.unique_open_count                            AS unique_opens,
            e.unique_click_count                           AS unique_clicks,
            e.unique_unsubscribe_count                     AS unique_unsubscribes,

            COALESCE(m.orders, 0)                          AS purchases,
            COALESCE(m.repeat_vip_orders, 0)               AS repeat_vip_purchases,
            COALESCE(m.activating_vip_orders, 0)           AS activating_vip_purchases,
            COALESCE(m.guest_orders, 0)                    AS guest_purchases,

            COALESCE(m.second_orders, 0)                   AS second_purchases,
            COALESCE(m.repeat_vip_second_orders, 0)        AS repeat_vip_second_purchases,
            COALESCE(m.activating_vip_second_orders, 0)    AS activating_vip_second_purchases,
            COALESCE(m.guest_second_orders, 0)             AS guest_second_purchases,

            COALESCE(m.units, 0)                           AS units,
            COALESCE(m.repeat_vip_units, 0)                AS repeat_vip_units,
            COALESCE(m.activating_vip_units, 0)            AS activating_vip_units,
            COALESCE(m.guest_units, 0)                     AS guest_units,

            COALESCE(m.credits_redeemed, 0)                AS credits_redeemed,
            COALESCE(m.repeat_vip_credits_redeemed, 0)     AS repeat_vip_credits_redeemed,
            COALESCE(m.activating_vip_credits_redeemed, 0) AS activating_vip_credits_redeemed,
            COALESCE(m.guest_credits_redeemed, 0)          AS guest_credits_redeemed,

            COALESCE(m.product_margin, 0)                  AS product_margin,
            COALESCE(m.repeat_vip_product_margin, 0)       AS repeat_vip_product_margin,
            COALESCE(m.activating_vip_product_margin, 0)   AS activating_vip_product_margin,
            COALESCE(m.guest_product_margin, 0)            AS guest_product_margin,

            e.unique_total_bounce_count                    AS unique_bounces,
            e.unique_hard_bounce_count                     AS unique_hard_bounces,
            e.unique_soft_bounce_count                     AS unique_soft_bounces,
            e.unique_block_count                           AS unique_blocks

     FROM savagex_iterable_metrics_triggered_sms AS e
              LEFT JOIN sms_utm_attribution_model_triggered AS m
                    ON e.campaign_id::VARCHAR = m.campaign_id::VARCHAR
                    AND e.store_country = m.store_country
                    AND e.send_date = m.event_time
     ORDER BY 9, 4 DESC
);

----------------------------------- FINAL TABLE - COMBINE GROWTH AND TRIGGERED  -----------------------

-- Union Growth + Triggered
CREATE OR REPLACE TEMP TABLE crm_attribution AS (
       SELECT * FROM crm_attribution_growth
       UNION ALL
       SELECT * FROM crm_attribution_triggered
);

---final select statement:
-- Join to Email CRM Campaign Name Generator Sharepoint doc and to SMS CRM Campaign Name Generator Sharepoint doc
-- Naming Generators live in Sharepoint TFG/Data Integrations --> Media --> Acquisition: https://techstyleinc.sharepoint.com/sites/TFGDataIntegrations/?CID=4c9905ba-ae44-4302-90a0-f40fbdf274d6
CREATE OR REPLACE TRANSIENT TABLE reporting_prod.sxf.crm_optimization_dataset AS
SELECT a.store_name,
       a.store_country,
       a.store_group,
       a.channel,
       a.campaign_id,
       n.campaign_name,
       a.campaign_type,
       a.subject,
       a.email_or_sms,
       a.send_date,
       revenue,
       send_count,
       unique_opens,
       unique_clicks,
       unique_unsubscribes,
       purchases,
       second_purchases,
       units,
       credits_redeemed,
       unique_bounces,
       unique_hard_bounces,
       unique_soft_bounces,
       unique_blocks,
       product_margin,

       repeat_vip_revenue,
       activating_vip_revenue,
       guest_revenue,

       repeat_vip_purchases,
       activating_vip_purchases,
       guest_purchases,


       repeat_vip_second_purchases,
       activating_vip_second_purchases,
       guest_second_purchases,

       repeat_vip_units,
       activating_vip_units,
       guest_units,

       repeat_vip_credits_redeemed,
       activating_vip_credits_redeemed,
       guest_credits_redeemed,

       repeat_vip_product_margin,
       activating_vip_product_margin,
       guest_product_margin,

       email_type as message_type,
       'email' as sms_or_mms,
       'email'  as text_to_join,
       segment,
       subsegment,
       version,

       track,
       creative_concept,
       online_or_retail,
       welcome_drip,
       offer_1,
       offer_2,

       imagery_type,
       offer_in_subject,
       emoji_in_subject,
       cta_above_fold,
       test,

       email_length as message_length,
       number_of_pdp_shown,
       gif_in_hero,
       email_objective as message_objective,

       sub_department,
       promo_series,
       event,
       'email'  as sms_emarsys_attentive_joint_campaign,
       'email'  as landing_page

FROM crm_attribution a
        LEFT JOIN lake_view.sharepoint.med_sxf_crm_dimensions n ON
            n.campaign_id = a.campaign_id AND
            n.country = a.store_country
            WHERE channel = 'email'
UNION ALL

SELECT a.store_name,
       a.store_country,
       a.store_group,
       a.channel,
       a.campaign_id,
       n.campaign_name,
       a.campaign_type,
       a.subject,
       a.email_or_sms,
       a.send_date,
       revenue,
       send_count,
       unique_opens,
       unique_clicks,
       unique_unsubscribes,
       purchases,
       second_purchases,
       units,
       credits_redeemed,
       unique_bounces,
       unique_hard_bounces,
       unique_soft_bounces,
       unique_blocks,
       product_margin,

       repeat_vip_revenue,
       activating_vip_revenue,
       guest_revenue,

       repeat_vip_purchases,
       activating_vip_purchases,
       guest_purchases,


       repeat_vip_second_purchases,
       activating_vip_second_purchases,
       guest_second_purchases,

       repeat_vip_units,
       activating_vip_units,
       guest_units,

       repeat_vip_credits_redeemed,
       activating_vip_credits_redeemed,
       guest_credits_redeemed,

       repeat_vip_product_margin,
       activating_vip_product_margin,
       guest_product_margin,

       message_type,
       sms_or_mms,
       text_to_join,
       segment,
       subsegment,
       version,

       track,
       creative_concept,
       online_or_retail,
       welcome_drip,
       offer_1,
       offer_2,

       'sms' as imagery_type,
       'sms' as offer_in_subject,
       emoji_in_subject,
       'sms' as cta_above_fold,

       test,
       sms_length as message_length,
       'sms' as number_of_pdp_shown,
       gif_in_hero,
       message_objective,

       sub_department,
       promo_series,
       event,
       sms_emarsys_attentive_joint_campaign,
       landing_page

FROM crm_attribution a
        LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_SXF_CRM_SMS_DIMENSIONS n ON
            n.campaign_id = a.campaign_id AND
            n.country = a.store_country
WHERE channel = 'sms'

;

-- UNION
--
-- -- Union to Historical CRM Data (before march 2022)
--
-- SELECT store_name,
--        store_country,
--        store_group,
--        channel,
--        campaign_id,
--        campaign_name,
--        campaign_type,
--        subject,
--        send_date,
--        revenue,
--        send_count,
--        unique_opens,
--        unique_clicks,
--        unique_unsubscribes,
--        purchases,
--        second_purchases,
--        units,
--        credits_redeemed,
--        unique_bounces,
--        unique_hard_bounces,
--        unique_soft_bounces,
--        unique_blocks,
--
--        repeat_vip_revenue,
--        activating_vip_revenue,
--        guest_revenue,
--
--        repeat_vip_purchases,
--        activating_vip_purchases,
--        guest_purchases,
--
--
--        repeat_vip_second_purchases,
--        activating_vip_second_purchases,
--        guest_second_purchases,
--
--        repeat_vip_units,
--        activating_vip_units,
--        guest_units,
--
--        repeat_vip_credits_redeemed,
--        activating_vip_credits_redeemed,
--        guest_credits_redeemed,
--
--        email_type,
--        segment,
--        subsegment,
--        version,
--        track,
--        creative_concept,
--        online_or_retail,
--
--        welcome_drip,
--        offer_1,
--        offer_2,
--        imagery_type,
--
--        offer_in_subject,
--        emoji_in_subject,
--        cta_above_fold,
--        test,
--        email_length,
--        number_of_pdp_shown,
--        gif_in_hero,
--        email_objective,
--        sub_department,
--        '' promo_series,
--        '' event
-- FROM reporting_prod.sxf.crm_historical_optimization_dataset;
