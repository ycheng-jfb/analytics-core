CREATE TABLE IF NOT EXISTS REPORTING_MEDIA_PROD.DBO.FACT_MEDIA_COST_HISTORICAL_SPECIALTY_STORE (
    MEDIA_COST_DATE	TIMESTAMP_NTZ(3),
    STORE_ID NUMBER(38,0),
    CHANNEL VARCHAR(200),
    SUBCHANNEL VARCHAR(200),
    VENDOR VARCHAR(200),
    SOURCE	VARCHAR(200),
    SPEND_TYPE VARCHAR(200),
    SPEND_CURRENCY_NAME	VARCHAR(200),
    STORE_CURRENCY_NAME	VARCHAR(200),
    COST	NUMBER(38,4),
    IMPRESSIONS	NUMBER(38,0),
    CLICKS	NUMBER(38,0),
    TYPE	VARCHAR(200),
    TARGETING VARCHAR(200),
    IS_MENS_FLAG INT,
    SPEND_DATE_USD_CONV_RATE	NUMBER(10,6),
    SPEND_DATE_EUR_CONV_RATE	NUMBER(10,6),
    IS_SPECIALTY_STORE BOOLEAN,
    SPECIALTY_STORE VARCHAR(10),
    LOCAL_STORE_CONV_RATE	NUMBER(10,6),
    META_CREATE_DATETIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP,
	META_UPDATE_DATETIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO REPORTING_MEDIA_PROD.DBO.FACT_MEDIA_COST_HISTORICAL_SPECIALTY_STORE
with _fmc_channel_mapped as
(
select media_cost_date,
       ds.store_id,
       lower(trim(dmp.channel))               AS _channel,
       lower(trim(dmp.subchannel))            AS _subchannel,
       lower(trim(dmp.vendor))                AS _vendor,
       lower(trim(dmp.spend_type))            AS _spend_type,
       CASE
           --update fb+ig subchannels to fb+ig
           WHEN _channel = 'social' and _subchannel in ('instagram', 'facebook')
               THEN 'fb+ig>fb+ig>facebook>media_cost'
           WHEN _channel = 'lead retargeting'
               THEN 'fb+ig>fb+ig>facebook>media_cost'
           --map all pinterest channels to pinterest subchannel
           WHEN _channel = 'pinterest' OR _subchannel ilike 'pinterest%'
               THEN 'pinterest>pinterest' || '>' || _vendor || '>' || _spend_type
           WHEN _subchannel = 'powerspace'
               THEN 'programmatic>content/native' || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'content/native' AND
                _subchannel IN ('outbrain', 'powerspace', 'other', 'taboola')
               THEN 'programmatic>content/native' || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'content/native' AND _subchannel = 'tradedesk'
               THEN 'programmatic>ttdnative' || '>' || _vendor || '>' || _spend_type
           --map snapchat to snapchat
           WHEN _subchannel = 'snapchat'
               THEN 'snapchat>' || _subchannel || '>' || _vendor || '>' || _spend_type
           --map tier 1 influencer subchannel to collaborator subchannel
           WHEN _channel = 'influencers' AND _subchannel = 'tier 1'
               THEN 'influencers>collaborator>gsheet' || '>' || _spend_type
           --map youtube subchannels
           WHEN _channel = 'youtube'
               THEN 'youtube>youtube' || '>' || _vendor || '>' || _spend_type
           --map affiliate _subchannels to networks (leaving only networks and emailing)
           WHEN _channel = 'affiliate' AND _subchannel IN ('dqna', 'other', 'coupons/vouchers')
               THEN 'affiliate>networks' || '>' || _vendor || '>' || _spend_type
           -- update display channels
           WHEN _vendor = 'criteo'
               THEN 'programmatic>criteo' || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'display' AND _subchannel = 'tradedesk'
               THEN 'programmatic>ttddisplay' || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'video' AND _vendor = 'tradedesk'
               THEN 'programmatic>ttdvideo' || '>' || 'tradedesk' || '>' || _spend_type
           WHEN _channel = 'radio/podcast' AND _vendor = 'tradedesk'
               THEN 'programmatic>ttdaudio' || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'display' AND _vendor = 'rokt'
               THEN 'programmatic>rokt' || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'display' AND _subchannel NOT IN ('gdn', 'yahoo')
               THEN 'programmatic>display' || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'display' AND _subchannel = 'yahoo'
               THEN 'programmatic' || '>' || _subchannel || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'display' AND _subchannel = 'gdn'
               THEN 'programmatic-gdn' || '>' || _subchannel || '>' || _vendor || '>' || _spend_type
           -- map all radio/podcast subchannels to other
           WHEN _channel = 'radio/podcast'
               THEN 'radio/podcast>other' || '>' || _vendor || '>' || _spend_type
           -- map popsugar print to other
           WHEN _channel = 'print' AND _vendor = 'epsilon'
               THEN 'print>catalog' || '>' || _vendor || '>' || _spend_type
           --update tv/video
           WHEN _channel = 'tv'
               THEN 'tv+streaming>tv>' || _vendor || '>' || _spend_type
           WHEN _channel = 'video'
               THEN 'tv+streaming>streaming>' || _vendor || '>' || _spend_type
           -- map misc. social subchannels
           WHEN _channel = 'social' AND _vendor in ('twitter', 'tumblr')
               THEN 'fb+ig>other' || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'social' AND _vendor = 'reddit'
                THEN 'reddit>reddit' || '>' || _vendor || '>' || _spend_type
           WHEN _channel = 'social'
               THEN 'fb+ig' || '>' || _subchannel || '>' || _vendor || '>' || _spend_type
           ELSE _channel || '>' || replace(_subchannel, ' ', '') || '>' || _vendor || '>' || _spend_type
           END                            AS classification,
       split_part(classification, '>', 1) AS channel,
       split_part(classification, '>', 2) AS subchannel,
       split_part(classification, '>', 3) AS vendor,
       split_part(classification, '>', 4) AS spend_type,
       source,
       spend_currency_name,
       store_currency_name,
       cost,
       impressions,
       clicks,
       type,
       targeting,
       0 as is_mens_flag,
       spend_date_usd_conv_rate,
       spend_date_eur_conv_rate,
       is_specialty_store,
       specialty_store,
       local_store_curr_conv_rate as local_store_conv_rate,
       fmc.meta_create_datetime,
       fmc.meta_update_datetime
from reporting_media_prod.dbo.fact_media_cost_20211215 fmc
join edw_prod.data_model.dim_store ds
on fmc.store_name = ds.store_name
join reporting_media_prod.dbo.dim_media_partner_20200817 dmp
on fmc.media_partner_key = dmp.media_partner_key
where media_cost_date < '2018-01-01'
)
select media_cost_date,
       store_id,
       channel,
       subchannel,
       vendor,
       source,
       spend_type,
       spend_currency_name,
       store_currency_name,
       cost,
       impressions,
       clicks,
       type,
       targeting,
       is_mens_flag,
       spend_date_usd_conv_rate,
       spend_date_eur_conv_rate,
       is_specialty_store,
       specialty_store,
       local_store_conv_rate,
       meta_create_datetime,
       meta_update_datetime
from _fmc_channel_mapped;
