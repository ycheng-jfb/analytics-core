-- Facebook Country Mappings

CREATE OR REPLACE TEMPORARY TABLE _fb_country_mappings
(
    store_id        INT,
    start_date      DATE,
    end_date        DATE,
    fb_country_code VARCHAR(15),
    store_id_map_to INT
);

INSERT INTO _fb_country_mappings
VALUES (26, '2010-01-01', '2018-07-01', 'CA', 41),
       --(55, '2010-01-01', '2079-01-01', 'CA', 55),
       (52, '2010-01-01', '2018-06-01', 'CA', 79),
       --(36, '2010-01-01', '2018-06-01', 'AT', 36), -- jfde to jfat

       (38, '2019-03-01', '2079-01-01', 'DK', 61),   -- jfuk to jfdk
       (38, '2019-03-01', '2079-01-01', 'SE', 63),   -- jfuk to jfse

       --(65, '2010-01-01', '2018-06-01', 'AT', 65), -- flde to flat
       (139, '2019-01-01', '2079-01-01', 'GB', 133), -- sxeu to sxuk -- it's 'GB' in the FB feed
       (139, '2019-01-01', '2079-01-01', 'FR', 125), -- sxeu to sxfr
       (139, '2019-01-01', '2079-01-01', 'ES', 131), -- sxeu to sxes
       (139, '2019-01-01', '2079-01-01', 'DE', 128), -- sxeu to sxde
       (139, '2019-01-01', '2079-01-01', 'AT', 128), -- sxeu to sxat (under sxde) -- should be 12801, but doesn't exist yet
       (139, '2019-01-01', '2079-01-01', 'BE', 137);
-- sxeu to sxbe (under sxnl) -- should be 13701, but doesnt exist yet

--------------------------------------------------------------------

SET high_watermark_datetime = (SELECT MAX(meta_update_datetime)
                               FROM lake_view.facebook.ad_insights_by_country);

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.facebook.advertising_spend_metrics_daily_by_country AS
SELECT account_id,
       ad_id,
       store_id,
       date,
       spend_account_currency,
       spend_usd,
       spend_local,
       impressions,
       video_play_impressions,
       clicks,
       video_views_2s,
       video_views_p75,
       avg_video_view,
       $high_watermark_datetime AS pixel_spend_meta_update_datetime,
       CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS meta_create_datetime,
       CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS meta_update_datetime
FROM (
         SELECT fb.account_id,
                fb.ad_id,
                COALESCE(c.store_id_map_to, st.store_id) AS store_id,
                fb.date AS date,
                SUM(spend) AS spend_account_currency,
                SUM((spend) * COALESCE(lkpusd.exchange_rate, 1)) AS spend_usd,
                SUM((spend) * COALESCE(lkplocal.exchange_rate, 1)) AS spend_local,
                SUM(fb.impressions) AS impressions,
                sum(video_play_impressions) as video_play_impressions,
                SUM(COALESCE(outbound_clicks, inline_link_clicks)) AS clicks,
                sum(video_views_2s) as video_views_2s,
                sum(video_views_p75) as video_views_p75,
                sum(avg_video_view) as avg_video_view

         FROM LAKE_VIEW.FACEBOOK.AD_INSIGHTS_BY_COUNTRY fb
                  JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = fb.account_id
             AND LOWER(am.source) = 'facebook'
                  JOIN edw_prod.data_model.dim_store st ON st.store_id = am.store_id
                  LEFT JOIN _fb_country_mappings c ON c.store_id = st.store_id
             AND c.fb_country_code = fb.country
             AND fb.date >= c.start_date
             AND fb.date < c.end_date
                  LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date lkpusd
                            ON am.currency = lkpusd.src_currency
                                AND fb.date = lkpusd.rate_date_pst
                                AND lkpusd.dest_currency = 'USD'
                  LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date lkplocal
                            ON am.currency = lkplocal.src_currency
                                AND fb.date = lkplocal.rate_date_pst
                                AND lkplocal.dest_currency = st.store_currency
         GROUP BY 1, 2, 3, 4
     );
