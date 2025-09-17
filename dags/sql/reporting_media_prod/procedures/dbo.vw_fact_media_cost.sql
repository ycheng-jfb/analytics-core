CREATE OR REPLACE VIEW reporting_media_prod.dbo.vw_fact_media_cost AS
SELECT LOWER(TRIM(channel)) AS channel,
       LOWER(TRIM(subchannel)) AS subchannel,
       LOWER(TRIM(vendor)) AS vendor,
       LOWER(TRIM(spend_type)) AS spend_type,
       media_cost_date,
       targeting,
       source,
       cost,
       impressions,
       clicks,
       spend_iso_currency_code,
       store_iso_currency_code,
       spend_date_eur_conv_rate,
       spend_date_usd_conv_rate,
       local_store_conv_rate,
       store_id,
       is_specialty_store,
       specialty_store,
       is_mens_flag,
       is_scrubs_flag
    FROM (
             SELECT media_cost_date,
                    channel,
                    subchannel,
                    vendor,
                    spend_type,
                    targeting,
                    source,
                    cost,
                    impressions,
                    clicks,
                    spend_iso_currency_code,
                    store_iso_currency_code,
                    spend_date_eur_conv_rate,
                    spend_date_usd_conv_rate,
                    local_store_conv_rate,
                    s.store_id,
                    is_specialty_store,
                    specialty_store,
                    is_mens_flag,
                    is_scrubs_flag
                 FROM reporting_media_prod.dbo.fact_media_cost fmc
                          JOIN edw_prod.data_model_jfb.dim_store s ON s.store_id = fmc.store_id
                 WHERE fmc.media_cost_date >= '2018-01-01'
--
--             UNION ALL
--
--             SELECT media_cost_date,
--                    channel,
--                    subchannel,
--                    vendor,
--                    spend_type,
--                    targeting,
--                    source,
--                    cost,
--                    impressions,
--                    clicks,
--                    c.iso_currency_code AS spend_iso_currency_code,
--                    c2.iso_currency_code AS store_iso_currency_code,
--                    spend_date_eur_conv_rate,
--                    spend_date_usd_conv_rate,
--                    local_store_conv_rate,
--                    s.store_id,
--                    is_specialty_store,
--                    specialty_store,
--                    is_mens_flag,
--                    0 AS is_scrubs_flag
--                 FROM reporting_media_prod.dbo.fact_media_cost_historical_specialty_store fmc
--                          JOIN edw_prod.data_model.dim_currency c ON c.currency_name = fmc.spend_currency_name
--                          JOIN edw_prod.data_model.dim_currency c2 ON c2.currency_name = fmc.store_currency_name
--                          JOIN edw_prod.data_model.dim_store s ON s.store_id = fmc.store_id
--                 WHERE fmc.media_cost_date < '2018-01-01'
         );
