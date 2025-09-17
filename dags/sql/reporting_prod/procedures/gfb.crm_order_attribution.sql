CREATE OR REPLACE TEMPORARY TABLE _email_click_campaign AS
SELECT dc.customer_id
     , s.campaign_id
     , 'EMAIL_CAMPAIGNS'              AS event_category
     , CAST(s.event_time AS DATETIME) AS event_datetime
FROM lake_view.emarsys.email_clicks s
         JOIN lake_view.emarsys.email_subscribes es
              ON es.emarsys_user_id = s.contact_id
                  AND es.brand = s.store_group
         JOIN reporting_prod.gfb.vw_store st
              ON LOWER(st.store_brand) = LOWER(SPLIT_PART(es.brand, '_', 1))
                  AND LOWER(st.store_country) = LOWER(SPLIT_PART(es.brand, '_', 2))
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = es.customer_id
                  AND dc.store_id = st.store_id;

CREATE OR REPLACE TEMPORARY TABLE _push_campaign AS
SELECT dc.customer_id
     , a.campaign_id
     , 'PUSH_CAMPAIGNS'               AS campaign_category
     , CAST(a.event_time AS DATETIME) AS event_datetime
FROM lake_view.emarsys.push_opens a
         JOIN lake_view.emarsys.email_subscribes es
              ON es.emarsys_user_id = a.contact_id
                  AND es.brand = a.store_group
         JOIN reporting_prod.gfb.vw_store st
              ON LOWER(st.store_brand) = LOWER(SPLIT_PART(es.brand, '_', 1))
                  AND LOWER(st.store_country) = LOWER(SPLIT_PART(es.brand, '_', 2))
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = es.customer_id
                  AND dc.store_id = st.store_id;


CREATE OR REPLACE TEMPORARY TABLE _sms_campaign AS
SELECT (CASE
            WHEN CONTAINS(LOWER(a.client_id), 'c:') THEN TRY_TO_NUMERIC(LTRIM(SPLIT(a.client_id, '-')[0], 'c:'))
            WHEN CONTAINS(LOWER(a.client_id), '_') THEN TRY_TO_NUMERIC(LTRIM(SPLIT(a.client_id, '_')[0], ' '))
            ELSE TRY_TO_NUMERIC(a.client_id)
    END)                             AS customer_id
     , a.message_id
     , 'ATTENTIVE_SMS'               AS campaign_category
     , CAST(a.timestamp AS DATETIME) AS event_datetime
FROM lake.media.attentive_attentive_sms_legacy a
         JOIN lake_view.sharepoint.med_account_mapping_media am
              ON am.source_id = a.company_id
                  AND am.source ILIKE 'attentive'
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = am.store_id
WHERE a.type = 'MESSAGE_LINK_CLICK'
  AND customer_id IS NOT NULL
UNION
SELECT dc.customer_id
     , a.campaign_id
     , 'EMARSYS_SMS'  AS campaign_category
     , CAST(a.event_time AS DATETIME) AS event_datetime
FROM lake.emarsys.sms_clicks a
         JOIN lake.emarsys.customer_store_mapping csm on csm.customer_id = a.customer_id
         JOIN lake_view.emarsys.email_subscribes es
              ON es.emarsys_user_id = a.contact_id
                  AND es.brand = csm.store_group
         JOIN reporting_prod.gfb.vw_store st
              ON LOWER(st.store_brand) = LOWER(SPLIT_PART(es.brand, '_', 1))
                  AND LOWER(st.store_country) = LOWER(SPLIT_PART(es.brand, '_', 2))
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = es.customer_id
                  AND dc.store_id = st.store_id
UNION
SELECT dc.customer_id
     , c.campaign_id
     , 'EMARSYS_SMS'  AS campaign_category
     , CAST(s.sessionlocaldatetime AS DATETIME) AS event_datetime
FROM reporting_base_prod.shared.session_single_view_media s
        JOIN lake.emarsys.sms_campaigns c
            ON lower(c.name) = lower(utmcampaign)
        JOIN edw_prod.data_model_jfb.dim_customer dc
            ON dc.customer_id = edw_prod.stg.udf_unconcat_brand(s.customer_id)
            AND dc.store_id = s.store_id
        WHERE utmcampaign ilike '%orphan%';

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.crm_order_attribution AS
SELECT DISTINCT a.order_id
              , a.order_datetime
              , a.customer_id
              , (CASE
                     WHEN GREATEST(
                              COALESCE(a.email_click_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.push_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.sms_message_event_datetime, '1900-01-01')
                              ) = a.email_click_campaign_event_datetime
                         THEN CAST(a.email_click_campaign_id AS INT)
                     WHEN GREATEST(
                              COALESCE(a.email_click_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.push_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.sms_message_event_datetime, '1900-01-01')
                              ) = a.push_campaign_event_datetime
                         THEN CAST(a.push_campaign_id AS INT)
                     WHEN GREATEST(
                              COALESCE(a.email_click_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.push_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.sms_message_event_datetime, '1900-01-01')
                              ) = a.sms_message_event_datetime
                         THEN CAST(a.sms_message_id AS INT)
    END) AS campaign_id
              , (CASE
                     WHEN GREATEST(
                              COALESCE(a.email_click_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.push_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.sms_message_event_datetime, '1900-01-01')
                              ) = a.email_click_campaign_event_datetime
                         THEN 'EMAIL_CAMPAIGNS'
                     WHEN GREATEST(
                              COALESCE(a.email_click_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.push_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.sms_message_event_datetime, '1900-01-01')
                              ) = a.push_campaign_event_datetime
                         THEN 'PUSH_CAMPAIGNS'
                     WHEN GREATEST(
                              COALESCE(a.email_click_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.push_campaign_event_datetime, '1900-01-01')
                              , COALESCE(a.sms_message_event_datetime, '1900-01-01')
                              ) = a.sms_message_event_datetime
                         THEN 'SMS_CAMPAIGNS'
    END) AS campaign_source
              , GREATEST(
    COALESCE(a.email_click_campaign_event_datetime, '1900-01-01')
    , COALESCE(a.push_campaign_event_datetime, '1900-01-01')
    , COALESCE(a.sms_message_event_datetime, '1900-01-01')
    )    AS campaign_event_date_time
FROM (SELECT DISTINCT olp.order_id
                    , CAST(fo.order_local_datetime AS DATETIME)                                                       AS order_datetime
                    , olp.customer_id
                    , FIRST_VALUE(ecc.campaign_id)
                                  OVER (PARTITION BY olp.order_id ORDER BY ecc.event_datetime DESC)                   AS email_click_campaign_id
                    , FIRST_VALUE(pc.campaign_id)
                                  OVER (PARTITION BY olp.order_id ORDER BY pc.event_datetime DESC)                    AS push_campaign_id
                    , FIRST_VALUE(sc.message_id)
                                  OVER (PARTITION BY olp.order_id ORDER BY sc.event_datetime DESC)                    AS sms_message_id
                    , FIRST_VALUE(ecc.event_datetime)
                                  OVER (PARTITION BY olp.order_id ORDER BY ecc.event_datetime DESC)                   AS email_click_campaign_event_datetime
                    , FIRST_VALUE(pc.event_datetime)
                                  OVER (PARTITION BY olp.order_id ORDER BY pc.event_datetime DESC)                    AS push_campaign_event_datetime
                    , FIRST_VALUE(sc.event_datetime)
                                  OVER (PARTITION BY olp.order_id ORDER BY sc.event_datetime DESC)                    AS sms_message_event_datetime
      FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
               JOIN edw_prod.data_model_jfb.fact_order fo
                    ON fo.order_id = olp.order_id
               LEFT JOIN _email_click_campaign ecc
                         ON ecc.customer_id = olp.customer_id
                             AND ecc.event_datetime < CAST(fo.order_local_datetime AS DATETIME)
                             AND (DATEDIFF(DAY, ecc.event_datetime, olp.order_date) BETWEEN 0 AND 6)
               LEFT JOIN _push_campaign pc
                         ON pc.customer_id = olp.customer_id
                             AND pc.event_datetime < CAST(fo.order_local_datetime AS DATETIME)
                             AND (DATEDIFF(DAY, pc.event_datetime, olp.order_date) BETWEEN 0 AND 6)
               LEFT JOIN _sms_campaign sc
                         ON sc.customer_id = olp.customer_id
                             AND sc.event_datetime < CAST(fo.order_local_datetime AS DATETIME)
                             AND (DATEDIFF(DAY, sc.event_datetime, olp.order_date) BETWEEN 0 AND 6)
      WHERE olp.order_classification = 'product order'
        AND COALESCE(ecc.campaign_id, pc.campaign_id, sc.message_id) IS NOT NULL) a;
