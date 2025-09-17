SET meta_update_datetime = (SELECT MAX(meta_update_datetime) FROM lake_view.facebook.ad_insights_by_age_gender);

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.facebook.advertising_spend_metrics_daily_by_age_gender AS
SELECT account_id,
       store_id,
       ad_id,
       date,
       --IFNULL(SPEND*100/NULLIF(SUM(spend) OVER (PARTITION BY ad_id ORDER BY (SELECT 0)),0),0) as spend_allocation_pct,
       --IFNULL(impressions*100/NULLIF(SUM(spend) OVER (PARTITION BY ad_id ORDER BY (SELECT 0)),0),0) as impressions_allocation_pct,
       --IFNULL(clicks*100/NULLIF(SUM(spend) OVER (PARTITION BY ad_id ORDER BY (SELECT 0)),0),0) as clicks_allocation_pct,
       age,
       gender,
       spend_account_currency,
       spend_usd,
       spend_local,
       impressions,
       clicks,
       $meta_update_datetime AS pixel_spend_meta_update_datetime,
       CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS meta_create_datetime,
       CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS meta_update_datetime
FROM (
         SELECT account_id,
                st.store_id,
                ad_id,
                age,
                gender,
                date AS date,
                SUM(spend) AS spend_account_currency,
                SUM((spend) * COALESCE(lkpusd.exchange_rate, 1)) AS spend_usd,
                SUM((spend) * COALESCE(lkplocal.exchange_rate, 1)) AS spend_local,
                SUM(impressions) AS impressions,
                SUM(COALESCE(outbound_clicks, inline_link_clicks)) AS clicks
         FROM lake_view.facebook.ad_insights_by_age_gender fb
                  LEFT JOIN lake_view.sharepoint.med_account_mapping_media am ON lower(am.source_id) = lower(fb.account_id)
             AND am.source ILIKE '%facebook%'
                  LEFT JOIN edw_prod.data_model.dim_store st ON st.store_id = am.store_id
                  LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date lkpusd ON am.currency = lkpusd.src_currency
             AND fb.date = lkpusd.rate_date_pst
             AND lkpusd.dest_currency = 'USD'
                  LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date lkplocal
                            ON am.currency = lkplocal.src_currency
                                AND fb.date = lkplocal.rate_date_pst
                                AND lkplocal.dest_currency = st.store_currency
         GROUP BY 1, 2, 3, 4, 5, 6
     );
