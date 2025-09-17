 CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.sxf.crm_contribution AS

--email_utm_attribution_model --
WITH seven_day_click_order AS (
    SELECT DISTINCT
         fo.customer_id,
         ds.store_brand,
         ds.store_country,
         mc.membership_order_type_l2 AS order_type,
         fo.order_id,
         fo.order_local_datetime,
         CASE WHEN COALESCE(smd.utmcampaign, 'n/a') ILIKE '%forgotpassword%'
             OR COALESCE(smd.utmcampaign, 'n/a') ILIKE '%shippingconfirmation%'
             OR COALESCE(smd.utmcampaign, 'n/a') ILIKE '%orderconfirmation%'
                  THEN 'n/a'
              ELSE LOWER(COALESCE(smd.utmsource, 'n/a'))
         END AS source,
         CASE WHEN COALESCE(smd.utmcampaign, 'n/a') ILIKE '%forgotpassword%'
             OR COALESCE(smd.utmcampaign, 'n/a') ILIKE '%shippingconfirmation%'
             OR COALESCE(smd.utmcampaign, 'n/a') ILIKE '%orderconfirmation%'
                  THEN 'n/a'
              ELSE LOWER(COALESCE(smd.utmmedium, 'n/a'))
         END AS medium,
         CASE WHEN COALESCE(smd.utmcampaign, 'n/a') ILIKE '%forgotpassword%'
             OR COALESCE(smd.utmcampaign, 'n/a') ILIKE '%shippingconfirmation%'
             OR COALESCE(smd.utmcampaign, 'n/a') ILIKE '%orderconfirmation%'
                  THEN 'n/a'
              ELSE LOWER(COALESCE(smd.utmcampaign, 'n/a'))
         END AS campaign,

         DATE_TRUNC('minute', smd.sessionlocaldatetime) AS session_minute

      -- EDW_PROD.DATA_MODEL_SXF tables are unconcatenated, no store_id at the end of customer_id
      FROM EDW_PROD.DATA_MODEL_SXF.fact_order fo

      JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds ON fo.store_id = ds.store_id

      JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel oc ON oc.order_sales_channel_key = fo.order_sales_channel_key
                                                     AND LOWER(oc.order_classification_l2) = 'product order'

      JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = fo.order_status_key
                                              AND LOWER(os.order_status) IN ('success', 'pending')

      JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification AS mc  ON fo.order_membership_classification_key = mc.order_membership_classification_key

      -- REPORTING_BASE_PROD.shared.session_single_view_media is concatenated ~ session_id, customer_id, and visitor_id have 30 attached to end, meta_original_session_id is unconcatenated

      LEFT JOIN REPORTING_BASE_PROD.shared.session_single_view_media smd ON CONCAT(fo.customer_id,'30') = smd.customer_id
                                                                         AND fo.order_local_datetime::DATE - smd.sessionlocaldatetime::DATE <= 6
                                                                         AND smd.sessionlocaldatetime::DATETIME   <= fo.order_local_datetime::DATETIME -- seven day look back window
      WHERE 1 = 1
        AND oc.is_border_free_order = 0
        AND oc.is_ps_order = 0
        AND oc.is_test_order = FALSE
        AND ds.store_brand = 'Savage X'
        AND ds.store_type <> 'Retail'
        AND order_local_datetime >= '2019-01-01'
)

 ,order_session_clean AS (
    SELECT o.*,
       REGEXP_REPLACE(REGEXP_REPLACE(campaign, '%20', ' '), '%2f', '/') AS campaign_clean,
       ROW_NUMBER()
           OVER (PARTITION BY o.customer_id, o.order_local_datetime, o.order_id ORDER BY session_minute DESC) AS rno
    FROM seven_day_click_order AS o
 )


,aggregated_order_session AS (
    SELECT s.customer_id,
        s.store_brand,
        s.store_country,
        s.order_type,
        s.order_id,
        s.order_local_datetime,
        LISTAGG(s.source, ';') WITHIN GROUP (ORDER BY rno) AS source_list,
        LISTAGG(s.medium, ';') WITHIN GROUP (ORDER BY rno) AS medium_list,
        LISTAGG(s.campaign_clean, ';') WITHIN GROUP (ORDER BY rno) AS campaign_list,
        LISTAGG(s.session_minute, ';') WITHIN GROUP (ORDER BY rno) AS session_time_list
     FROM order_session_clean AS s
     GROUP BY 1, 2, 3, 4, 5, 6
 )


,last_touch_order_session AS (
    SELECT a.customer_id,
        a.store_brand,
        a.store_country,
        a.order_type,
        a.order_id,
        a.order_local_datetime,
        REGEXP_COUNT(source_list, ';') + 1 AS source_count,
        REGEXP_COUNT(medium_list, ';') + 1 AS medium_count,
        REGEXP_COUNT(campaign_list, ';') + 1 AS campaign_count,
        IFF(campaign_count > 1,
            SPLIT_PART(REGEXP_REPLACE(campaign_list, 'n/a;', ''), ';', 1),
            campaign_list) AS last_campaign,

        IFF(campaign_count > 1,
            SPLIT_PART(REGEXP_REPLACE(campaign_list, 'n/a;', ''), ';', 2),
            campaign_list) AS second_last_campaign,

        IFF(source_count > 1,
            SPLIT_PART(REGEXP_REPLACE(source_list, 'n/a;', ''), ';', 1),
            source_list) AS last_source,

        IFF(source_count > 1,
            SPLIT_PART(REGEXP_REPLACE(source_list, 'n/a;', ''), ';', 2),
            source_list) AS second_last_source,

        IFF(medium_count > 1,
            SPLIT_PART(REGEXP_REPLACE(medium_list, 'n/a;', ''), ';', 1),
            medium_list) AS last_medium,

        IFF(medium_count > 1,
            SPLIT_PART(REGEXP_REPLACE(medium_list, 'n/a;', ''), ';', 2),
            medium_list) AS second_last_medium,

        IFF(campaign_count > 1,
            SPLIT_PART(REGEXP_REPLACE(session_time_list, 'n/a;', ''), ';', 1),
            session_time_list) AS last_session_time,

        IFF(medium_count > 1,
            SPLIT_PART(REGEXP_REPLACE(session_time_list, 'n/a;', ''), ';', 2),
            session_time_list) AS second_last_session_time,

        a.campaign_list,
        a.medium_list,
        a.source_list,
        a.session_time_list
     FROM aggregated_order_session AS a
)


,_all_sxf_orders AS (
    SELECT fo.order_id,
        fo.customer_id,
        fo.order_local_datetime,
        membership_order_type_l2 order_type,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_local_datetime ASC) AS rno_all_orders,
        ROW_NUMBER() OVER (PARTITION BY customer_id,membership_order_type_l2 ORDER BY order_local_datetime ASC) AS rno_order_type
     FROM EDW_PROD.DATA_MODEL_SXF.fact_order fo

              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds ON fo.store_id = ds.store_id

              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel oc ON oc.order_sales_channel_key = fo.order_sales_channel_key

              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification mc  ON mc.order_membership_classification_key = fo.order_membership_classification_key

              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = fo.order_status_key

     WHERE LOWER(oc.order_classification_l2) = 'product order'
       AND LOWER(os.order_status) IN ('success', 'pending')
       AND ds.store_brand = 'Savage X'
 )


,_sxf_second_orders AS (
    SELECT order_id,
        customer_id,
        order_local_datetime,
        order_type,
        CASE WHEN rno_all_orders = 2 THEN '1' ELSE 0 END second_order,
        CASE WHEN rno_order_type = 2 AND order_type = 'Activating VIP' THEN '1'
             ELSE 0
        END activating_vip_second_order,
        CASE WHEN rno_order_type = 2 AND order_type = 'Repeat VIP' THEN '1'
             ELSE 0
        END repeat_vip_second_order,
        CASE WHEN rno_order_type = 2 AND order_type = 'Guest' THEN '1' ELSE 0 END guest_second_order
     FROM _all_sxf_orders
 )


,_credits_per_order AS(
     SELECT order_id,
         customer_id,
         order_local_datetime,
         is_ecomm order_type,
         MIN(num_credits_redeemed_on_order) num_credits_redeemed_per_order

     --REPORTING_PROD.SXF.ORDER_LINE_DATASET is not concatenated
     FROM REPORTING_PROD.SXF.ORDER_LINE_DATASET
         GROUP BY 1, 2, 3, 4
 )
,aggregated_order_session_plus_dollars_units AS (
    SELECT l.*,
           fo.unit_count,
           fo.product_gross_revenue_excl_shipping_local_amount,
           fo.product_margin_pre_return_excl_shipping_local_amount,
           fo.product_order_cash_margin_pre_return_local_amount,
           so.second_order,
           so.activating_vip_second_order,
           so.repeat_vip_second_order,
           so.guest_second_order,
           cr.num_credits_redeemed_per_order

        FROM last_touch_order_session AS l
        JOIN EDW_PROD.DATA_MODEL_SXF.fact_order  fo ON l.customer_id = fo.customer_id
                                                  AND l.order_id = fo.order_id
                                                  AND l.order_local_datetime = fo.order_local_datetime
        LEFT JOIN _sxf_second_orders so  ON l.customer_id = so.customer_id
                                         AND l.order_id = so.order_id
                                         AND l.order_local_datetime = so.order_local_datetime
       LEFT JOIN _credits_per_order cr ON l.customer_id = cr.customer_id
                                       AND l.order_id = cr.order_id
                                       AND l.order_local_datetime = cr.order_local_datetime
)


,customer_level_attribution AS (
    SELECT f.customer_id,
          f.order_local_datetime::DATE AS order_date,
          f.order_id,
          f.order_type,
          f.store_brand AS store_brand,
          f.store_country AS store_country,
          f.campaign_list,
          f.last_medium AS utm_medium,
          f.last_campaign AS utm_campaign,
          MAX(f.order_local_datetime) AS max_order_datetime,
          COUNT(DISTINCT f.order_id) AS orders,
          SUM(COALESCE(f.second_order, 0)) second_order,
          SUM(COALESCE(f.activating_vip_second_order, 0)) activating_vip_second_order,
          SUM(COALESCE(f.repeat_vip_second_order, 0)) repeat_vip_second_order,
          SUM(COALESCE(f.guest_second_order, 0)) guest_second_order,
          SUM(COALESCE(num_credits_redeemed_per_order, 0)) credits_redeemed,
          SUM(COALESCE(f.unit_count, 0)) AS units,
          SUM(COALESCE(f.product_gross_revenue_excl_shipping_local_amount, 0)) AS revenue,
          SUM(COALESCE(f.product_margin_pre_return_excl_shipping_local_amount, 0)) AS product_margin_prereturn,
          SUM(COALESCE(f.product_order_cash_margin_pre_return_local_amount, 0)) AS product_cash_margin
       FROM aggregated_order_session_plus_dollars_units AS f
       GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
       ORDER BY 2 DESC
 )
, customer_level_attribution_per_date AS (
    SELECT c.store_brand,
           c.store_country,
           DATE_TRUNC('day', order_date) AS date_day,

           CASE WHEN c.utm_medium = 'n/a' THEN 'other referral'
                WHEN c.utm_medium = '(none)' THEN 'direct'
                ELSE c.utm_medium
           END AS medium,

           MAX(max_order_datetime) AS max_order_datetime,

           SUM(COALESCE(c.orders, 0)) AS orders,
           SUM(COALESCE(c.second_order, 0)) AS second_orders,
           SUM(COALESCE(c.units, 0)) AS units,
           SUM(COALESCE(c.credits_redeemed, 0)) AS credits_redeemed,
           SUM(COALESCE(c.revenue, 0)) AS revenue,
           SUM(COALESCE(c.product_margin_prereturn, 0)) AS product_margin_prereturn,
           SUM(COALESCE(c.product_cash_margin, 0)) AS product_cash_margin,


           SUM(CASE WHEN order_type = 'Repeat VIP' THEN c.orders ELSE NULL END) AS repeat_vip_orders,
           SUM(CASE WHEN order_type = 'Activating VIP' THEN c.orders ELSE NULL END) AS activating_vip_orders,
           SUM(CASE WHEN order_type = 'Guest' THEN c.orders ELSE NULL END) AS guest_orders,

           SUM(repeat_vip_second_order) AS repeat_vip_second_orders,
           SUM(activating_vip_second_order) AS activating_vip_second_orders,
           SUM(guest_second_order) AS guest_second_orders,

           SUM(CASE WHEN order_type = 'Repeat VIP' THEN COALESCE(c.units, 0)
                    ELSE NULL
               END) AS repeat_vip_units,
           SUM(CASE WHEN order_type = 'Activating VIP'
                        THEN COALESCE(c.units, 0)
                    ELSE NULL
               END) AS activating_vip_units,
           SUM(CASE WHEN order_type = 'Guest' THEN COALESCE(c.units, 0) ELSE NULL END) AS guest_units,

           SUM(CASE WHEN order_type = 'Repeat VIP'
                        THEN COALESCE(c.credits_redeemed, 0)
                    ELSE NULL
               END) AS repeat_vip_credits_redeemed,
           SUM(CASE WHEN order_type = 'Activating VIP'
                        THEN COALESCE(c.credits_redeemed, 0)
                    ELSE NULL
               END) AS activating_vip_credits_redeemed,
           SUM(CASE WHEN order_type = 'Guest'
                        THEN COALESCE(c.credits_redeemed, 0)
                    ELSE NULL
               END) AS guest_credits_redeemed,

           SUM(CASE WHEN order_type = 'Repeat VIP'
                        THEN COALESCE(c.revenue, 0)
                    ELSE NULL
               END) AS repeat_vip_revenue,
           SUM(CASE WHEN order_type = 'Activating VIP'
                        THEN COALESCE(c.revenue, 0)
                    ELSE NULL
               END) AS activating_vip_revenue,
           SUM(CASE WHEN order_type = 'Guest' THEN COALESCE(c.revenue, 0) ELSE NULL END) AS guest_revenue,

           sum(case when order_type = 'Repeat VIP' then coalesce(c.product_margin_prereturn,0) else null end) as     Repeat_VIP_product_margin_prereturn,
           sum(case when order_type = 'Activating VIP' then coalesce(c.product_margin_prereturn,0) else null end) as Activating_VIP_product_margin_prereturn,
           sum(case when order_type = 'Guest' then coalesce(c.product_margin_prereturn,0) else null end) as          Guest_product_margin_prereturn,

           --sum(case when order_type = 'Repeat VIP' then coalesce(c.product_margin_prereturn,0) else null end) as Repeat_VIP_product_cash_margin,
           --sum(case when order_type = 'Activating VIP' then coalesce(c.product_margin_prereturn,0) else null end) as Activating_VIP_product_cash_margin,
           --sum(case when order_type = 'Guest' then coalesce(c.product_margin_prereturn,0) else null end) as Guest_product_cash_margin

        FROM customer_level_attribution c
        GROUP BY 1, 2, 3, 4
 )

-- Sesssion Note: REPORTING_BASE_PROD.shared.session_single_view_media begins 2021-01-01

,session_data AS (
     SELECT DISTINCT
            smd.customer_id,
            smd.session_id,
            smd.sessionlocaldatetime,
            DATE_TRUNC('day', date(smd.sessionlocaldatetime)) AS date_day,
            ds.store_brand,
            ds.store_country,
            CASE WHEN COALESCE(smd.utmcampaign, 'n/a') ILIKE '%forgotpassword%'
                OR COALESCE(smd.utmcampaign, 'n/a') ILIKE '%shippingconfirmation%'
                OR COALESCE(smd.utmcampaign, 'n/a') ILIKE '%orderconfirmation%'
                     THEN 'n/a'
                 ELSE LOWER(COALESCE(smd.utmmedium, 'n/a'))
            END AS utmmedium,
            DATE_TRUNC('minute', smd.sessionlocaldatetime) AS session_minute
         FROM REPORTING_BASE_PROD.shared.session_single_view_media smd
         JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds ON smd.store_id = ds.store_id

         WHERE 1 = 1
           AND ds.store_brand = 'Savage X'
           AND sessionlocaldatetime >= '2019-01-01'
 )


,session_attribution AS (
    SELECT store_brand,
           store_country,
           date_day,
           CASE WHEN utmmedium = 'n/a' THEN 'other referral'
                WHEN utmmedium = '(none)' THEN 'direct'
                ELSE utmmedium
           END AS medium,
           COUNT(DISTINCT session_id) AS sessions
        FROM session_data
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2, 3, 4
 )

--final contribution model output, daily

SELECT c.store_brand,
       c.store_country,
       c.date_day,
       c.max_order_datetime,
       c.medium,
       sessions,
       orders,
       revenue,
       second_orders,
       units,
       credits_redeemed,
       product_margin_prereturn,

       repeat_vip_revenue,
       activating_vip_revenue,
       guest_revenue,

       repeat_vip_orders,
       activating_vip_orders,
       guest_orders,

       repeat_vip_second_orders,
       activating_vip_second_orders,
       guest_second_orders,

       repeat_vip_units,
       activating_vip_units,
       guest_units,

       repeat_vip_credits_redeemed,
       activating_vip_credits_redeemed,
       guest_credits_redeemed,

       repeat_vip_product_margin_prereturn,
       activating_vip_product_margin_prereturn,
       guest_product_margin_prereturn

    FROM customer_level_attribution_per_date c
    LEFT JOIN session_attribution s ON s.date_day = c.date_day
        AND s.store_brand = c.store_brand
        AND s.store_country = c.store_country
        AND s.medium = c.medium

    ORDER BY 3 DESC, 5, 4
;
