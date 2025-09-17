SET low_watermark_ltz = dateadd(month, -3, current_date) :: TIMESTAMP_LTZ;


CREATE OR REPLACE TEMPORARY TABLE _facebook_spend AS
SELECT date::DATE AS spend_date,
    CASE WHEN ds.store_brand || ' ' || ds.store_country = 'JustFab US' AND aibh.campaign_name ILIKE '%XAC%' THEN 41
         WHEN ds.store_brand || ' ' || ds.store_country = 'Fabletics US' AND aibh.campaign_name ILIKE '%XAC%' THEN 80
         WHEN aibh.campaign_name ilike  any ('%flmus_canada%', '%flmca_%') THEN 79
         ELSE ds.store_id
	END AS store_id,
    ds.store_brand || CASE WHEN (ds.store_brand || ' ' || ds.store_country) IN ('JustFab US', 'Fabletics US') AND aibh.campaign_name ILIKE '%XAC%' THEN 'CA'
                           WHEN aibh.campaign_name ilike any ('%flmus_canada%', '%flmca_%') THEN 'CA'
						        ELSE ds.store_country
						   END AS business_unit,
    ds.store_region AS store_region,
    CASE WHEN (ds.store_brand || ' ' || ds.store_country) IN ('JustFab US', 'Fabletics US') AND aibh.campaign_name ILIKE '%XAC%' THEN 'CA'
         WHEN aibh.campaign_name ilike any ('%flmus_canada%', '%flmca_%') THEN 'CA'
         ELSE ds.store_country
    END AS country,
    ds.store_country,
    'FB+IG' AS channel,
    'API' AS source,
    CASE
         WHEN aibh.account_name ILIKE '%1180%' THEN '1180'
         WHEN account_id='338056876952277' and date::DATE>='2023-02-01' THEN 'Volt'
         WHEN aibh.account_name ILIKE '%peoplehype%' THEN 'Peoplehype'
         WHEN aibh.account_name ILIKE '%agencywithin%' THEN 'Agencywithin'
         WHEN aibh.account_name ILIKE '%tubescience%' OR aibh.campaign_name ILIKE '%tubescience%' THEN 'Tubescience'
         WHEN aibh.campaign_name ILIKE '%Gassed%' THEN 'Gassed'
         WHEN aibh.Campaign_name ILIKE '%DonutDigital%' THEN 'DonutDigital'
         WHEN aibh.account_name ILIKE '%narrative%' or CAMPAIGN_NAME ILIKE '%narrative%' THEN 'Narrative'
         WHEN aibh.account_name ILIKE '%geistm%' THEN 'geistm'
         when aibh.account_name ILIKE '%Sandbox%' THEN 'Sandbox'
         WHEN aibh.account_name ILIKE ANY ('%influencers', '%collaborator%')
                  OR (aibh.account_name ILIKE 'fabletics_us_promo'
                     and aibh.adset_name ILIKE ANY ('%infl_krowland%','%infl_mziegler%','%infl_dlovato%')) THEN 'PaidInfluencers'
         WHEN (aibh.campaign_name ILIKE '%sapphire%' OR aibh.ad_name ILIKE '%sapphire%' OR aibh.adset_name ILIKE '%sapphire%') and CAMPAIGN_NAME not ilike '%CreativeExchange%' THEN 'Sapphire'
         WHEN lower(aibh.account_name) ILIKE '%admk%' THEN 'ADMK'
         WHEN adset_name ilike '%schema%' OR campaign_name ilike '%schema%' THEN 'Schema'
         WHEN aibh.campaign_name ILIKE '%MySubscriptionAddiction%' THEN 'MySubscriptionAddiction'
         ELSE 'FB+IG'
    END AS subchannel,
    'facebook' AS vendor,
    aibh.adset_name,
    CASE WHEN aibh.adset_name ILIKE '%%_PROSPECT%%'
					  THEN 'Prospecting'
					  WHEN aibh.adset_name ILIKE '%%_SITERET%%'
					  THEN 'Site Retargeting'
					  WHEN aibh.adset_name ILIKE '%%_LEADRET%%'
						OR aibh.adset_name ILIKE '%%AgedLead%%'
						OR aibh.account_name ILIKE '%Retargeting%'
						OR aibh.account_name ILIKE '%RTG%'
					  THEN 'Lead Retargeting'
					  WHEN aibh.adset_name ILIKE '%%_VIPRet%%'
					  THEN 'VIP Retargeting'
					  ELSE 'Prospecting'
				END AS Targeting,
    am.MENS_ACCOUNT_FLAG as is_mens_flag,
	am.IS_SCRUBS_FLAG as is_scrubs_flag,
    aibh.spend,
    am.currency AS spend_currency,
    aibh.impressions,
    aibh.inline_link_clicks AS clicks,
    account_id,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake_view.facebook.ad_insights_by_hour aibh
JOIN lake_view.sharepoint.med_account_mapping_media am ON aibh.account_id=am.source_id
 AND am.source='Facebook'
 AND am.reference_column='account_id'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
WHERE am.include_in_cpa_report = 1
    AND (aibh.account_id <> '867502650349544' or date::DATE < '2020-12-01' or date::DATE >= '2021-11-01')--SXUS removed for Afterpay
    AND not ((((lower(aibh.adset_name) like '%afterpay%' or lower(aibh.campaign_name) like '%afterpay%') AND lower(aibh.campaign_name) not in ('sxus_fb_ig_afterpay_prospecting', 'sxus_fb_ig_afterpay_daba_prospecting'))
              or (lower(aibh.campaign_name) in ('sxus_fb_ig_afterpay_prospecting', 'sxus_fb_ig_afterpay_daba_prospecting', 'sxus_fb_ig_excluded_reach') and date::DATE < '2022-05-23'))
            and lower(ds.STORE_BRAND_ABBR||ds.STORE_REGION) in ('sxna', 'fkna', 'jfna', 'sdna')
            and date::DATE >= '2021-05-01') --removed SXNA adset, campaigns that contains afterpay
	AND aibh.date::DATE >= $low_watermark_ltz;


CREATE OR REPLACE TEMPORARY TABLE _facebook_spend_final AS
SELECT
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
    SUM(IFNULL(impressions::FLOAT,0)) AS impressions_count,
    SUM(IFNULL(clicks::FLOAT,0)) AS clicks_count,
    spend_currency,
    'MEDIACOST' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _facebook_spend
GROUP BY
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;

---------------------pixis FEES------------------
CREATE OR REPLACE TEMPORARY TABLE _facebook_pixis_fees_Final AS
SELECT
    business_unit,
    country,
    spend_date,
    store_id,
    'fb+ig' as channel,
    source,
    'fb+ig' as subchannel,
    'pixis' as vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL((0.02*spend) :: FLOAT, 0)) AS media_cost,
    SUM(IFNULL(impressions::FLOAT,0)) AS impressions_count,
    SUM(IFNULL(clicks::FLOAT,0)) AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _facebook_spend
where adset_name ilike '%pixis%' and spend_date::DATE >= '2022-06-15'
GROUP BY
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;


--------------------- Kargo FEES for FB+IG Ads ------------------

CREATE OR REPLACE TEMPORARY TABLE _facebook_kargo_fees_final AS
SELECT
    ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    date AS spend_date,
    ds.store_id AS store_id,
    'fb+ig' AS channel,
    'API' AS source,
    'fb+ig' AS subchannel,
    'kargo' AS vendor,
    'Prospecting' AS targeting,
    am.MENS_ACCOUNT_FLAG AS is_mens_flag,
    am.IS_SCRUBS_FLAG AS is_scrubs_flag,
    CASE WHEN date >= '2024-12-07' THEN SUM(IFNULL(spend::FLOAT, 0)) * .08
        WHEN date >='2024-11-07' AND SUM(SUM(IFNULL(spend::FLOAT, 0))) OVER (PARTITION BY channel ORDER BY date) > 200000 THEN SUM(IFNULL(spend::FLOAT, 0)) * .06
        ELSE 0 END AS media_cost,
    0 AS impressions_count,
    0 AS clicks_count,
    am.currency AS spend_currency,
    'FEES' AS spend_type,
    IFF(am.specialty_store IS NOT NULL,'TRUE','FALSE') as is_specialty_store,
    am.specialty_store
FROM lake_view.facebook.ad_insights_by_hour aibh
JOIN lake_view.sharepoint.med_account_mapping_media am ON aibh.account_id = am.source_id
    AND am.source = 'Facebook'
    AND am.reference_column = 'account_id'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = am.store_id
WHERE aibh.account_id in ('1248016452888951','2018839018546535','1260019478408379')
    AND aibh.date::DATE >= $low_watermark_ltz
GROUP BY
        ds.store_brand || ds.store_country,
        ds.store_country,
        date,
	    ds.store_id,
        am.MENS_ACCOUNT_FLAG,
	    am.IS_SCRUBS_FLAG,
        am.currency,
        is_specialty_store,
        am.specialty_store;

--------------------- Kargo FEES for Snapchat Ads ------------------

CREATE OR REPLACE TEMPORARY TABLE _snapchat_kargo_fees_final AS
SELECT
    ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    pd.start_time::date as spend_date,
    ds.store_id AS store_id,
    'Snapchat' AS channel,
    'API' AS source,
    'Snapchat' AS subchannel,
    'kargo' AS vendor,
    'Prospecting' AS targeting,
    am.MENS_ACCOUNT_FLAG AS is_mens_flag,
    am.IS_SCRUBS_FLAG AS is_scrubs_flag,
    CASE WHEN pd.start_time::date >= '2024-12-07' THEN SUM(IFNULL(pd.spend::FLOAT, 0) / 1000000) * .08
        WHEN pd.start_time::date >= '2024-11-27' THEN SUM(IFNULL(pd.spend::FLOAT, 0) / 1000000) * .06
        ELSE 0 END AS media_cost,
    0 AS impressions_count,
    0 AS clicks_count,
    am.currency AS spend_currency,
    'FEES' AS spend_type,
    IFF(am.SPECIALTY_STORE IS NOT null,'TRUE','FALSE') AS IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake_view.snapchat.pixel_data_1day pd
JOIN lake_view.snapchat.ad_metadata amd ON pd.ad_id = amd.ad_id
JOIN lake_view.snapchat.adsquad_metadata asmd ON amd.ad_squad_id = asmd.adsquad_id
JOIN lake_view.snapchat.campaign_metadata cmd ON cmd.campaign_id = asmd.campaign_id
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = cmd.ad_account_id
    AND am.reference_column = 'account_id'
    AND am.source = 'Snapchat'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = am.store_id
WHERE pd.start_time >= '2024-11-01'
    AND pd.start_time >= $low_watermark_ltz
    AND ad_account_id IN ('5af03be2-9d34-48d0-946a-5175cb2912c3','b06997a6-5f5b-4301-9051-bf933d0a45bd',
                          'be3fd0b3-27a8-4069-9a6d-5bd4daa38741','349c8286-ebc0-4096-835a-a6bb9d632b69')
    AND asmd.name ILIKE '%kargo%'
GROUP BY business_unit,
    country,
    spend_date,
    ds.store_id,
    am.MENS_ACCOUNT_FLAG,
    am.IS_SCRUBS_FLAG,
    am.currency,
    is_specialty_store,
    am.specialty_store;

/************************** google doubleclick api ad insights by hour
2. With we are trying to capture the day level spend

****/
CREATE OR REPLACE TEMPORARY TABLE _google_doubleclick_spend AS
SELECT spend_date,
      CASE WHEN store_brand = 'JustFab' AND campaign_labels ILIKE '%%CA |%%' THEN 41
          WHEN store_brand = 'Fabletics' AND campaign_labels ILIKE '%%CA |%%' THEN 79
          WHEN store_brand = 'ShoeDazzle' AND campaign_labels ILIKE '%%CA |%%' THEN 55
          WHEN store_brand = 'JustFab' and store_country = 'FR' and campaign_labels ILIKE '%%BE | %%' THEN 48
          WHEN store_brand = 'Fabletics' and store_country = 'FR' and campaign_labels ILIKE '%%BE | %%' THEN 69
          WHEN store_brand = 'JustFab' and store_country = 'DE' and campaign_labels ILIKE '%%AT | %%' THEN 36
          WHEN store_brand = 'Fabletics' and store_country = 'DE' and campaign_labels ILIKE '%%AT | %%' THEN 65
          ELSE store_id
				END AS store_id,
	store_brand || CASE WHEN store_brand IN ('JustFab' , 'Fabletics') AND campaign_labels ILIKE '%%CA |%%' THEN 'CA'
												WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'FR' and campaign_labels ILIKE '%%BE | %%' THEN 'BE'
												WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'DE' and campaign_labels ILIKE '%%AT | %%' THEN 'AT'
												ELSE store_country
										   END AS business_unit,
	store_region,
	CASE WHEN store_brand IN ('JustFab' , 'Fabletics','ShoeDazzle') AND campaign_labels ILIKE '%%CA |%%' THEN 'CA'
					WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'FR' and campaign_labels ILIKE '%%BE | %%' THEN 'BE'
					WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'FR' and campaign_labels ILIKE '%%ES | %%' THEN 'ES'
					WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'DE' and campaign_labels ILIKE '%%AT | %%' THEN 'AT'
					ELSE store_country
					END AS country,
    CASE WHEN store_brand IN ('JustFab' , 'Fabletics') AND campaign_labels ILIKE '%%CA |%%' THEN 'CA'
						  WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'FR' and campaign_labels ILIKE '%%BE | %%' THEN 'BE'
						  WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'DE' and campaign_labels ILIKE '%%AT | %%' THEN 'AT'
						  ELSE store_country
					 END AS store_country,
	CASE WHEN campaign_labels ILIKE '%%| NonBrand%%'  OR campaign_labels ILIKE '%%|NonBrand%%' OR campaign_labels ILIKE '%%Non-Brand%%' THEN 'Non Branded Search'
					WHEN campaign_labels ILIKE '%%| Brand%%' OR campaign_labels ILIKE '%%|Brand%%' OR campaign = 'Techstyle' THEN 'Branded Search'
                    WHEN campaign_labels ILIKE '%%| GDN%%' OR account ILIKE '%%GDN%%' THEN 'Programmatic-GDN'
					WHEN campaign ilike  '%%$_PLA$_%%' ESCAPE '$' OR Campaign ILIKE '%%Shopping%%' OR (campaign_labels ILIKE '%%| PLA%%' OR campaign_labels ILIKE '%%|PLA%%') THEN 'Shopping'
					WHEN campaign ILIKE '%%_Brand_%%' THEN 'Branded Search'
					WHEN campaign ILIKE '%%_NB_%%' THEN 'Non Branded Search'
					Else 'Non Branded Search'
			  END AS channel,
	'API' AS source,
	CASE WHEN account ILIKE '%%Bing%%' OR account ILIKE '%%BNG%%'OR campaign_labels ILIKE '%%Bing%%' THEN 'Bing'
					   WHEN account ILIKE '%%Gemini%%' OR campaign_labels ILIKE '%%Gemini%%' OR campaign_labels ILIKE '%%Yahoo%%' THEN 'Yahoo'
					   WHEN campaign_labels ILIKE '%%GDN%%' OR account ILIKE '%%GDN%%' THEN 'GDN'
					   WHEN account ILIKE '%%Google%%' OR account ILIKE '%%GDN%%' OR campaign_labels ILIKE '%%Google%%' OR account ILIKE '%%Search%%' OR account ILIKE '%%Shopping%%' THEN 'Google'
					   ELSE 'Google'
				   END AS subchannel,
    campaign,       -- Populated to calculate MSA FEES
	'Doubleclick' AS vendor,
    CASE WHEN campaign_labels ILIKE '%%VIPRET%%' THEN 'VIP Retargeting'
					ELSE 'Prospecting'
				 END as targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend,
	spend_currency,
	impressions,
	clicks,
    CASE WHEN store_brand IN ('ShoeDazzle') AND campaign_labels ILIKE '%%CA |%%' THEN TRUE
        WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'FR' and campaign_labels ILIKE '%%BE | %%' THEN TRUE
        WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'DE' and campaign_labels ILIKE '%%AT | %%' THEN TRUE
        WHEN SPECIALTY_STORE is not null THEN TRUE
        ELSE FALSE
    END AS is_specialty_store,
    CASE WHEN store_brand IN ('ShoeDazzle') AND campaign_labels ILIKE '%%CA |%%' THEN 'CA'
        WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'FR' and campaign_labels ILIKE '%%BE | %%' THEN 'BE'
        WHEN store_brand IN ('JustFab','Fabletics') and store_country= 'DE' and campaign_labels ILIKE '%%AT | %%' THEN 'AT'
        ELSE specialty_store
        END AS specialty_store
FROM lake.media.google_search_ads_360_campaign_stats_legacy
    WHERE CAST(spend_date AS DATE) < '2021-08-01'
	AND CAST(spend_date AS DATE) >= $low_watermark_ltz

UNION ALL

select pmax.date :: DATE AS spend_date,
     CASE WHEN ds.store_brand = 'JustFab' AND l.label_names ILIKE '%%CA |%%' THEN 41
          WHEN ds.store_brand = 'Fabletics' AND l.label_names ILIKE '%%CA |%%' THEN 79
          WHEN ds.store_brand = 'ShoeDazzle' AND l.label_names ILIKE '%%CA |%%' THEN 55
          WHEN ds.store_brand = 'JustFab' and ds.store_country = 'FR' and l.label_names ILIKE '%%BE |%%' THEN 48
          WHEN ds.store_brand = 'Fabletics' and ds.store_country = 'FR' and l.label_names ILIKE '%%BE |%%' THEN 69
          WHEN ds.store_brand = 'JustFab' and ds.store_country = 'DE' and l.label_names ILIKE '%%AT |%%' THEN 36
          WHEN ds.store_brand = 'Fabletics' and ds.store_country = 'DE' and l.label_names ILIKE '%%AT |%%' THEN 65
          ELSE ds.store_id
     END AS store_id,
    ds.store_brand || CASE WHEN ds.store_brand IN ('JustFab' , 'Fabletics') AND l.label_names ILIKE '%%CA$_%%' ESCAPE '$' THEN 'CA'
                           WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%BE |%%' THEN 'BE'
                           WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and l.label_names ILIKE '%%AT |%%' THEN 'AT'
                           ELSE ds.store_country
                      END AS business_unit,
    ds.store_region AS store_region,
     CASE WHEN ds.store_brand IN ('JustFab' , 'Fabletics','ShoeDazzle') AND l.label_names ILIKE '%%CA |%%' THEN 'CA'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%BE | %%' THEN 'BE'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%ES | %%' THEN 'ES'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and l.label_names ILIKE '%%AT | %%' THEN 'AT'
          ELSE ds.store_country
          END AS country,
     CASE WHEN ds.store_brand IN ('JustFab','Fabletics') AND l.label_names ILIKE '%%CA |%%' THEN 'CA'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%BE | %%' THEN 'BE'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and l.label_names ILIKE '%%AT | %%' THEN 'AT'
          ELSE ds.store_country
          END AS store_country,
     CASE WHEN c.campaign_name ilike '%%$_PLA$_%%' ESCAPE '$' OR c.campaign_name ILIKE '%%Shopping%%' OR l.label_names ILIKE 'PLA' OR l.label_names ILIKE '%% PLA%%' OR l.label_names ILIKE '%%| PLA%%' THEN 'Shopping'
          WHEN l.label_names ILIKE '%%NonBrand%%' OR l.label_names ILIKE '%%Non-Brand%%' THEN 'Non Branded Search'
          WHEN l.label_names ILIKE ' Brand %%' OR l.label_names ILIKE '| Brand%%' OR c.campaign_name = 'Techstyle' THEN 'Branded Search'
          WHEN l.label_names ILIKE '%%| GDN%%' OR a.accountdescriptivename ILIKE '%%GDN%%' THEN 'Programmatic-GDN'
          WHEN c.campaign_name ILIKE '%%NonBrand%%' OR c.campaign_name ILIKE '%%_NB_%%' THEN 'Non Branded Search'
          WHEN c.campaign_name ILIKE '%%Brand%%' OR c.campaign_name ILIKE '%%_Brand%%' THEN 'Branded Search'
          Else 'Non Branded Search'
     END AS channel,
	'API' AS source,
	CASE WHEN a.accountdescriptivename ILIKE '%%Bing%%' OR a.accountdescriptivename ILIKE '%%BNG%%'OR l.label_names ILIKE '%%Bing%%' THEN 'Bing'
					   WHEN a.accountdescriptivename ILIKE '%%Gemini%%' OR l.label_names ILIKE '%%Gemini%%' OR l.label_names ILIKE '%%Yahoo%%' THEN 'Yahoo'
					   WHEN l.label_names ILIKE '%%GDN%%' OR a.accountdescriptivename ILIKE '%%GDN%%' THEN 'GDN'
					   WHEN a.accountdescriptivename ILIKE '%%Google%%' OR a.accountdescriptivename ILIKE '%%GDN%%' OR l.label_names ILIKE '%%Google%%' OR a.accountdescriptivename ILIKE '%%Search%%' OR a.accountdescriptivename ILIKE '%%Shopping%%' THEN 'Google'
					   ELSE 'Google'
				   END AS subchannel,
    c.campaign_name,
    'Doubleclick' AS vendor,
    CASE WHEN l.label_names ILIKE '%%VIPRET%%' OR c.campaign_name ILIKE '%%VIPRET%%' THEN 'VIP Retargeting'
		 ELSE 'Prospecting'
		 END as targeting,
    am.MENS_ACCOUNT_FLAG as is_mens_flag,
    am.is_scrubs_flag,
    IFF(ds.store_id in (75,77), (pmax.cost_micros/1000000)*eur_order_er.exchange_rate, pmax.cost_micros/1000000) AS spend,
    am.currency AS spend_currency,
    pmax.impressions AS Impressions,
    pmax.clicks AS Clicks,
    CASE WHEN ds.store_brand IN ('ShoeDazzle') AND l.label_names ILIKE '%%CA |%%' THEN TRUE
        WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%BE | %%' THEN TRUE
        WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and l.label_names ILIKE '%%AT | %%' THEN TRUE
        WHEN am.SPECIALTY_STORE is not null THEN TRUE
        ELSE FALSE
    END AS is_specialty_store,
    CASE WHEN ds.store_brand IN ('ShoeDazzle') AND l.label_names ILIKE '%%CA |%%' THEN 'CA'
        WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%BE | %%' THEN 'BE'
        WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and l.label_names ILIKE '%%AT | %%' THEN 'AT'
        ELSE am.specialty_store
        END AS specialty_store
from lake_view.google_ads.performance_max_spend_by_campaign pmax
join LAKE_VIEW.GOOGLE_ADS.CAMPAIGN_METADATA c on pmax.campaign_id = c.campaign_id
LEFT join (
    SELECT
        clh.campaign_id,
        LISTAGG(l.name, ' | ') WITHIN GROUP (ORDER BY l.name) AS label_names
    FROM LAKE_VIEW.GOOGLE_ADS.CAMPAIGN_LABEL_HISTORY clh
    JOIN LAKE_VIEW.GOOGLE_ADS.LABEL l ON clh.label_id = l.id
    GROUP BY
        clh.campaign_id
) l on pmax.campaign_id = l.campaign_id
join lake_view.google_ads.account_metadata a on a.external_customer_id=pmax.customer_id
join lake_view.sharepoint.med_account_mapping_media am on try_to_number(am.source_id)=pmax.customer_id
      and am.source = 'Doubleclick Account'
join edw_prod.data_model.dim_store ds on am.store_id = ds.store_id
LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date eur_order_er ON eur_order_er.rate_date_pst = DATEADD(DAY,-1,spend_date)
  AND eur_order_er.src_currency = 'GBP'
  AND eur_order_er.dest_currency = spend_currency
where am.include_in_cpa_report = 1
    and pmax.date::date >='2021-08-01'
    and pmax.date::date >=$low_watermark_ltz

union all

SELECT  ap.date :: DATE AS spend_date,
    CASE WHEN ds.store_brand = 'JustFab' AND (l.label_names ILIKE '%%CA |%%' OR l.label_names ILIKE '%% CA %%') THEN 41
          WHEN ds.store_brand = 'Fabletics' AND (l.label_names ILIKE '%%CA |%%' OR l.label_names ILIKE '%% CA %%') THEN 79
          WHEN ds.store_brand = 'ShoeDazzle' AND (l.label_names ILIKE '%%CA |%%' OR l.label_names ILIKE '%% CA %%') THEN 55
          WHEN ds.store_brand = 'JustFab' and ds.store_country = 'FR' and (l.label_names ILIKE '%%BE |%%' OR l.label_names ILIKE '%% BE%%') THEN 48
          WHEN ds.store_brand = 'Fabletics' and ds.store_country = 'FR' and (l.label_names ILIKE '%%BE |%%' OR l.label_names ILIKE '%% BE%%') THEN 69
          WHEN ds.store_brand = 'JustFab' and ds.store_country = 'DE' and (l.label_names ILIKE '%%AT |%%' OR l.label_names ILIKE '%% AT%%') THEN 36
          WHEN ds.store_brand = 'Fabletics' and ds.store_country = 'DE' and (l.label_names ILIKE '%%AT |%%' OR l.label_names ILIKE '%% AT%%') THEN 65
          ELSE ds.store_id
     END AS store_id,
    ds.store_brand || CASE WHEN ds.store_brand IN ('JustFab' , 'Fabletics') AND l.label_names ILIKE '%%CA$_%%' ESCAPE '$' THEN 'CA'
                           WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and (l.label_names ILIKE '%%BE |%%' OR l.label_names ILIKE '%% BE%%') THEN 'BE'
                           WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and (l.label_names ILIKE '%%AT |%%' OR l.label_names ILIKE '%% AT%%') THEN 'AT'
                           ELSE ds.store_country
                      END AS business_unit,
    ds.store_region AS store_region,
     CASE WHEN ds.store_brand IN ('JustFab' , 'Fabletics','ShoeDazzle') AND l.label_names ILIKE '%%CA |%%' THEN 'CA'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%BE | %%' THEN 'BE'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%ES | %%' THEN 'ES'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and l.label_names ILIKE '%%AT | %%' THEN 'AT'
          ELSE ds.store_country
          END AS country,
     CASE WHEN ds.store_brand IN ('JustFab','Fabletics') AND l.label_names ILIKE '%%CA |%%' THEN 'CA'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%BE | %%' THEN 'BE'
          WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and l.label_names ILIKE '%%AT | %%' THEN 'AT'
          ELSE ds.store_country
          END AS store_country,
    CASE
        WHEN l.label_names ILIKE '%%NonBrand%%' OR l.label_names ILIKE '%%Non%Brand%%' THEN 'Non Branded Search'
        WHEN l.label_names ILIKE 'Brand%%' OR l.label_names ILIKE '| Brand%%' OR ch.name = 'Techstyle' THEN 'Branded Search'
        WHEN l.label_names ILIKE '%%GDN%%' OR ah.name ILIKE '%%GDN%%' THEN 'Programmatic-GDN'
        WHEN ch.name ilike  '%%$_PLA$_%%' ESCAPE '$' OR ch.name ILIKE '%%Shopping%%' OR l.label_names ILIKE 'PLA' OR l.label_names ILIKE '%% PLA%%' OR l.label_names ILIKE '%%| PLA%%' THEN 'Shopping'
        WHEN ch.name ILIKE '%%_Brand%%' THEN 'Branded Search'
        WHEN ch.name ILIKE '%%NonBrand%%' OR ch.name ILIKE '%%_NB_%%' THEN 'Non Branded Search'
        Else 'Non Branded Search'
     END AS channel,
    'API' AS source,
    'Bing' AS subchannel,
    ch.name campaign_name,
    -- ah.name account_name,
    'Microsoft Ads' AS vendor,
    CASE WHEN l.label_names ILIKE '%%VIPRET%%' OR ch.name ILIKE '%%VIPRET%%' THEN 'VIP Retargeting'
                    ELSE 'Prospecting'
                 END as targeting,
    am.MENS_ACCOUNT_FLAG as is_mens_flag,
    am.is_scrubs_flag,
    ap.spend AS spend,
    am.currency AS spend_currency,
    ap.IMPRESSIONS AS Impressions,
    ap.CLICKS AS Clicks,
    CASE WHEN ds.store_brand IN ('ShoeDazzle') AND l.label_names ILIKE '%%CA |%%' THEN TRUE
        WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%BE | %%' THEN TRUE
        WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and l.label_names ILIKE '%%AT | %%' THEN TRUE
        WHEN am.SPECIALTY_STORE is not null THEN TRUE
        ELSE FALSE
    END AS is_specialty_store,
    CASE WHEN ds.store_brand IN ('ShoeDazzle') AND l.label_names ILIKE '%%CA |%%' THEN 'CA'
        WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'FR' and l.label_names ILIKE '%%BE | %%' THEN 'BE'
        WHEN ds.store_brand IN ('JustFab','Fabletics') and ds.store_country= 'DE' and l.label_names ILIKE '%%AT | %%' THEN 'AT'
        ELSE am.specialty_store
        END AS specialty_store
from LAKE_VIEW.MICROSOFT_ADS.AD_PERFORMANCE_DAILY_REPORT ap
         LEFT JOIN (SELECT id, name
                    FROM lake_view.microsoft_ads.account_history
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_modified_time DESC) = 1
         ) ah ON ap.account_id = ah.id
         LEFT JOIN (SELECT id, name
                    FROM lake_view.microsoft_ads.campaign_history
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified_time DESC) = 1
         ) ch ON ap.campaign_id = ch.id
         LEFT join (
            SELECT
                clh.campaign_id,
                LISTAGG(lh.name, ' | ') WITHIN GROUP (ORDER BY lh.modified_time desc) AS label_names
            FROM LAKE_VIEW.MICROSOFT_ADS.CAMPAIGN_LABEL_HISTORY clh
            JOIN LAKE_VIEW.MICROSOFT_ADS.LABEL_HISTORY lh ON clh.label_id = lh.id
            GROUP BY
                clh.campaign_id
        ) l on ap.campaign_id = l.campaign_id
join lake_view.sharepoint.med_account_mapping_media am on try_to_number(am.source_id)=ap.account_id
join edw_prod.data_model.dim_store ds on am.store_id = ds.store_id
where am.include_in_cpa_report = 1
    and am.source = 'Microsoft Ads'
    and ap.date::date >='2021-08-01'
    AND ap.date::date >= $low_watermark_ltz;


UPDATE _google_doubleclick_spend
SET channel = 'Programmatic-GDN'
WHERE lower(channel) = 'non branded search' AND subchannel = 'GDN';


CREATE OR REPLACE TEMPORARY TABLE _google_doubleclick_spend_final AS (
SELECT business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
	SUM(IFNULL(Impressions :: FLOAT,0)) AS Impressions_count,
	SUM(IFNULL(Clicks :: FLOAT,0)) AS Clicks_count,
	spend_currency,
	'MEDIACOST' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _google_doubleclick_spend
GROUP BY  business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
);

/************************************Completed google doubleclick Spend*************************/


/************************** Impact Radius Affiliate Accelerate ad insights by hour
3. With we are trying to capture the day level spend
select * from _Impact_Radius_Affiliate_Accelerate_spend_final;
****/

CREATE OR REPLACE TEMPORARY TABLE _Impact_Radius_Affiliate_Accelerate_spend_final AS(
SELECT  ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    afs.action_date AS spend_date,
	ds.store_id AS store_id,
	'Affiliate' AS channel,
	'CALCULATED' AS source,
	'Networks' AS subchannel,
	'AccelerationPartners' AS vendor,
    'Prospecting' AS targeting,
    am.MENS_ACCOUNT_FLAG as is_mens_flag,
    am.is_scrubs_flag,
    SUM(IFNULL(0.15 * payout :: FLOAT, 0 )) AS media_cost,
	0 AS Impressions_count,
	0 AS Clicks_count,
	am.currency AS spend_currency,
    'FEES' AS spend_type,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake.media.impactradius_affiliate_spend_legacy afs
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = afs.campaignid
	AND am.reference_column='campaign_id'
	AND am.source = 'ImpactRadius Affiliate'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = am.store_id
WHERE am.include_in_cpa_report = 1
	AND afs.action_date >= $low_watermark_ltz
GROUP BY ds.store_brand || ds.store_country,
        ds.store_country,
        afs.action_date,
		ds.store_id,
		am.currency,
        am.MENS_ACCOUNT_FLAG,
        am.is_scrubs_flag,
		IS_SPECIALTY_STORE,
		SPECIALTY_STORE
  );


/************************************Completed Impact Radius Affiliate Accelerate  Spend*************************/

/************************** Impact Radius Affiliate API ad insights by hour
4. With we are trying to capture the day level spend
select * from _Impact_Radius_Affiliate_API_spend
****/


CREATE OR REPLACE TEMPORARY TABLE _Impact_Radius_Affiliate_API_spend_final AS(
SELECT ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    afs.action_date AS spend_date,
	ds.store_id AS store_id,
	'Affiliate' AS channel,
	'API' AS source,
	'Networks' AS subchannel,
	'ImpactRadius' AS vendor,
    'Prospecting' AS targeting,
    am.MENS_ACCOUNT_FLAG as is_mens_flag,
    am.is_scrubs_flag,
    SUM(IFNULL(afs.payout :: FLOAT, 0)) AS media_cost,
	0 AS Impressions_count,
	0 AS Clicks_count,
	am.currency AS spend_currency,
    'MEDIACOST' AS spend_type,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake.media.impactradius_affiliate_spend_legacy afs
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = afs.campaignid
	AND am.reference_column='campaign_id'
	AND am.source = 'ImpactRadius Affiliate'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = am.store_id
WHERE am.include_in_cpa_report = 1
	AND afs.action_date >= $low_watermark_ltz
GROUP BY  ds.store_brand || ds.store_country,
	ds.store_country,
	afs.action_date,
	ds.store_id,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
  );


/************************************Completed Impact Radius Affiliate API  Spend*************************/


/************************** Snapchat API ad insights by hour
5. With we are trying to capture the day level spend

****/


CREATE OR REPLACE TEMPORARY TABLE _snapchat_api_spend AS(
SELECT IFF(STORE_REGION='EU',convert_timezone(tz.time_zone,pd.start_time),pd.start_time)::date as spend_date,
	ds.store_id AS store_id,
    ds.store_brand || ds.store_country AS business_unit,
	ds.store_region AS store_region,
	ds.store_country AS country,
	ds.store_country AS  store_country,
	'Snapchat' AS channel,
	'API' AS source,
	CASE
	    WHEN asmd.campaign_id in ('3edeac69-084f-4f7b-8314-49333994e973',
                                  '739ad502-96d6-49af-a9e0-87d2d84411fb') THEN 'Tubescience'
        WHEN (cmd.name ilike '%sapphire%' or asmd.name ilike '%sapphire%' ) and cmd.name not ilike '%CreativeExchange%' THEN 'Sapphire'
	    WHEN cmd.name ILIKE '%Gassed%' THEN 'Gassed'
        ELSE 'Snapchat'
    END AS subchannel,
	'Snapchat' AS vendor,
    asmd.name AS adset_name,
    CASE WHEN asmd.name ilike '%%Prospect%%' THEN 'Prospecting'
					  WHEN asmd.name ILIKE '%%LeadRet%%' THEN 'Lead Retargeting'
					  WHEN asmd.name ILIKE '%%SiteRet%%' THEN 'Site Retargeting'
					  ELSE 'Prospecting'
				 END AS targeting,
    am.MENS_ACCOUNT_FLAG as is_mens_flag,
    am.is_scrubs_flag,
    pd.spend :: float / 1000000 AS spend,
	am.currency AS spend_currency,
    pd.impressions AS impressions,
	pd.swipes AS clicks,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
am.SPECIALTY_STORE
FROM lake_view.snapchat.pixel_data_1day pd
JOIN lake_view.snapchat.ad_metadata amd
  ON pd.ad_id = amd.ad_id
JOIN lake_view.snapchat.adsquad_metadata asmd
  ON amd.ad_squad_id = asmd.adsquad_id
JOIN lake_view.snapchat.campaign_metadata cmd
  ON cmd.CAMPAIGN_ID=asmd.campaign_id
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = cmd.AD_ACCOUNT_ID
	AND am.reference_column='account_id'
	AND am.source = 'Snapchat'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
JOIN edw_prod.reference.store_timezone tz
on am.store_id = tz.store_id
WHERE am.include_in_cpa_report = 1
	AND (pd.start_time :: DATE )>= $low_watermark_ltz
  );

CREATE OR REPLACE TEMPORARY TABLE _snapchat_api_spend_final AS(
SELECT business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
	SUM(IFNULL(Impressions :: FLOAT,0)) AS impressions_count,
	SUM(IFNULL(Clicks :: FLOAT,0)) AS clicks_count,
	spend_currency,
	'MEDIACOST' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _snapchat_api_spend
GROUP BY  business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
);

/************************************Completed Snapchat API  Spend*************************/

/************************** Impact Radius Influencer API ad insights by hour
6. With we are trying to capture the day level spend
select * from _Impact_Radius_Influencer_API_spend
****/

CREATE OR REPLACE TEMPORARY TABLE _Impact_Radius_Influencer_API_spend AS(
SELECT   ifs.action_date AS spend_date,
	ds.store_id AS store_id,
	ds.store_brand || ds.store_country AS business_unit,
	ds.store_region AS store_region,
	ds.store_country AS country,
	ds.store_country AS store_country,
	'Influencers'  AS channel,
	'API' AS source,
	'Influencer' AS subchannel,
	'ImpactRadius' AS vendor,
    'Prospecting' AS targeting,
     am.MENS_ACCOUNT_FLAG as is_mens_flag,
     am.is_scrubs_flag,
	ifs.payout AS spend,
	 am.currency AS spend_currency,
	0 AS Impressions,
	0 AS Clicks,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
am.SPECIALTY_STORE
FROM lake.media.impactradius_influencers_spend_legacy ifs
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = ifs.campaignid
	AND am.reference_column='campaign_id'
	AND am.source = 'ImpactRadius Influencers'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
WHERE am.include_in_cpa_report = 1
	AND ifs.action_date >= $low_watermark_ltz
);



CREATE OR REPLACE TEMPORARY TABLE _Impact_Radius_Influencer_API_spend_final AS(
SELECT business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
	SUM(IFNULL(Impressions :: FLOAT,0)) AS Impressions_count,
	SUM(IFNULL(Clicks :: FLOAT,0)) AS Clicks_count,
	spend_currency,
	'MEDIACOST' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _Impact_Radius_Influencer_API_spend
GROUP BY  business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
  );


/************************************Completed Impact Radius Influencer API spend*************************/

/************************** Youtube API ad insights by hour
7. With we are trying to capture the day level spend

****/
-- Get labels for the corresponding label_ids and aggregate the label values to the grain
CREATE OR REPLACE TEMPORARY TABLE _youtube_api_spend AS(
SELECT  ys.date AS spend_date,
    ds.store_id AS store_id,
    ds.store_brand || ds.store_country AS business_unit,
    ds.store_region AS store_region,
    ds.store_country AS country,
    ds.store_country AS store_country,
    CASE WHEN ys.campaign_name ILIKE ANY('%%Discovery%%', '%%GDN%%', '%DemandGen%') OR ys.external_customer_id IN ('5403949764','2646630478','8543764201')
        THEN 'Programmatic-GDN'
        WHEN ys.campaign_name ilike '%$_NB$_%' escape '$' THEN 'Non Branded Search'
    ELSE 'Youtube'
    END AS channel,
    'API' AS source,
    CASE WHEN ys.external_customer_id IN ('5403949764', '2646630478','8543764201') OR ys.campaign_name ILIKE '%%GDN%%' THEN 'GDN'
        WHEN ys.campaign_name ILIKE '%%Discovery%%' THEN 'Discovery'
        WHEN ys.campaign_name ILIKE '%%Sapphire%%' OR ys.ad_group_name ILIKE '%%Sapphire%%' THEN 'Sapphire'
        WHEN ys.ad_group_name ILIKE '%%schema%%' THEN 'Schema'
        WHEN ys.campaign_name ILIKE '%Gassed%' and ys.external_customer_id in (9854334693, 6548940025) THEN 'Gassed10'
        WHEN ys.campaign_name ILIKE '%Gassed%' THEN 'Gassed'
        WHEN ys.campaign_name ILIKE '%DemandGen%' THEN 'DemandGen'
        WHEN ys.external_customer_id='8041772829' THEN 'Google'     --Update logic while pulling Search Ads data
        ELSE 'Youtube'
    END AS subchannel,
    IFF(ys.external_customer_id=1805095909,'GeistM',IFF(ys.campaign_name ILIKE '%%Discovery%%' OR ys.external_customer_id IN ('5403949764','2646630478','8543764201'),'Google','Adwords')) AS vendor,
    l.labels,
    CASE WHEN ys.ad_group_name like '%%Remarketing-STLeads%%' OR ys.ad_group_name like '%%Remarketing-Leads%%' THEN 'Lead Retargeting'
					 WHEN ys.ad_group_name like '%%Remarketing-SiteVisitor%%' OR ys.ad_group_name like '%%Remarketing-GASiteVisit%%' THEN 'Site Retargeting'
					 WHEN ys.ad_group_name like '%%Remarketing-Vip%%' THEN 'VIP Retargeting'
					 ELSE 'Prospecting'
				END AS targeting,
    am.MENS_ACCOUNT_FLAG as is_mens_flag,
    am.is_scrubs_flag,
    ys.cost :: float / 1000000 AS spend,
    ds.store_currency AS spend_currency,
    ys.impressions AS impressions,
    ys.clicks AS clicks,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake_view.google_ads.ad_spend ys
  JOIN lake_view.sharepoint.med_account_mapping_media am
  ON ys.external_customer_id = am.source_id
  AND reference_column = 'account_id'
  AND am.include_in_cpa_report = 1
  AND source IN ('Adwords Youtube', 'Adwords GDN')
JOIN edw_prod.data_model.dim_store ds
  ON am.store_id = ds.store_id
LEFT JOIN reporting_media_base_prod.google_ads.ad_metadata l ON l.ad_id = ys.ad_id and l.ad_group_id = ys.ad_group_id
WHERE ys.date >= $low_watermark_ltz
  );


CREATE OR REPLACE TEMPORARY TABLE _youtube_api_spend_final AS(
SELECT business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	SUM(IFNULL(spend :: FLOAT, 0))  AS media_cost,
	SUM(IFNULL(impressions :: FLOAT,0)) AS impressions_count,
	SUM(IFNULL(clicks :: FLOAT,0)) AS clicks_count,
	spend_currency,
	'MEDIACOST' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _youtube_api_spend
GROUP BY  business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
);


/************************************Completed youtube API spend Spend*************************/

/************************** Outbrain Flat File Import ad insights by hour
8. With we are trying to capture the day level spend

****/

CREATE OR REPLACE TEMPORARY TABLE _spend_outbrain AS(
SELECT date,
	USD_Spend,
	Impressions,
	Clicks,
	REPLACE(SUBSTR(Advertiser, 1, CHARINDEX('_',Advertiser)-1),'Just ','Just') AS store
FROM lake.media.outbrain_spend_legacy
WHERE (date :: DATE) >= $low_watermark_ltz
  );


CREATE OR REPLACE TEMPORARY TABLE _Outbrain_Flat_File_Import_spend AS(
SELECT   (sob.date :: DATE) AS spend_date,
	ds.store_id AS store_id,
	ds.store_brand || ds.store_country AS business_unit,
	ds.store_region AS store_region,
	ds.store_country AS country,
	ds.store_country AS store_country,
	'Programmatic' AS channel,
	'AutomatedEmail' AS source,
	'Content/Native' AS subchannel,
	'Outbrain' AS vendor,
    'Prospecting' AS targeting,
     am.MENS_ACCOUNT_FLAG as is_mens_flag,
    am.is_scrubs_flag,
	sob.USD_spend AS spend,
	am.currency AS spend_currency,
	sob.Impressions AS Impressions,
	sob.clicks AS Clicks,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
am.SPECIALTY_STORE
FROM _spend_outbrain sob
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = sob.store
	AND am.reference_column='store'
	AND am.source = 'Outbrain'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
WHERE am.include_in_cpa_report = 1
	AND (sob.date :: DATE) >= $low_watermark_ltz
  )

UNION ALL

SELECT
    ts.date AS spend_date,
    ds.store_id,
    ds.store_brand||ds.store_country AS business_unit,
    ds.store_region AS store_region,
    country,
    ds.store_country AS store_country,
    channel,
    'Manual' AS source,
    subchannel,
    vendor,
    'Prospecting' AS targeting,
    is_mens_flag::int ,
    null as is_scrubs_flag,
    ts.spend AS spend,
    ds.STORE_CURRENCY AS spend_currency,
    0 AS impressions,
    0 AS clicks,
	'FALSE' AS IS_SPECIALTY_STORE,
    NULL AS SPECIALTY_STORE
FROM LAKE_VIEW.SHAREPOINT.MED_OUTBRAIN_SPEND ts
JOIN edw_prod.data_model.dim_store ds
    ON ds.store_id = ts.store_id;



CREATE OR REPLACE TEMPORARY TABLE _Outbrain_Flat_File_Import_spend_final AS(
SELECT business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	SUM(IFNULL(spend :: FLOAT, 0))  AS media_cost,
	SUM(IFNULL(Impressions :: FLOAT,0)) AS Impressions_count,
	SUM(IFNULL(Clicks :: FLOAT,0)) AS Clicks_count,
	spend_currency,
	'MEDIACOST' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _Outbrain_Flat_File_Import_spend
GROUP BY  business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
  );


/************************************Completed Outbrain Flat File Import spend *************************/

/************************** Taboola Flat File Import ad insights by hour
9. With we are trying to capture the day level spend

****/


CREATE OR REPLACE TEMPORARY TABLE _Taboola_Flat_File_Import_spend AS(
SELECT  (sot.date :: DATE) AS spend_date,
	ds.store_id AS store_id,
	ds.store_brand||ds.store_country AS business_unit,
	ds.store_region AS store_region,
	ds.store_country AS country,
	ds.store_country AS store_country,
	'Programmatic' AS channel,
	'AUTOEMAIL' AS source,
	'Content/Native' AS subchannel,
	'Taboola' AS vendor,
    'Prospecting' AS targeting,
     am.MENS_ACCOUNT_FLAG as is_mens_flag,
    am.is_scrubs_flag,
	sot.spent AS spend,
	am.currency AS spend_currency,
	COALESCE(sot.impressions,0) AS impressions,
	COALESCE(sot.clicks,0) AS clicks,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
am.SPECIALTY_STORE
FROM lake.media.taboola_spend_legacy sot
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = sot.Content_Provider
	AND am.reference_column='content_provider'
	AND am.source = 'Taboola'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
WHERE am.include_in_cpa_report = 1
	AND sot.date :: DATE >= $low_watermark_ltz

UNION ALL

SELECT
    ts.date AS spend_date,
    ds.store_id,
    ds.store_brand||ds.store_country AS business_unit,
    ds.store_region AS store_region,
    country,
    ds.store_country AS store_country,
    channel,
    'Manual' AS source,
    subchannel,
    vendor,
    'Prospecting' AS targeting,
    is_mens_flag::int ,
    null as is_scrubs_flag,
    ts.spend AS spend,
    ds.STORE_CURRENCY AS spend_currency,
    0 AS impressions,
    0 AS clicks,
	'FALSE' AS IS_SPECIALTY_STORE,
    NULL AS SPECIALTY_STORE
FROM LAKE_VIEW.SHAREPOINT.MED_TABOOLA_SPEND ts
JOIN edw_prod.data_model.dim_store ds
    ON ds.store_id = ts.store_id
  );


CREATE OR REPLACE TEMPORARY TABLE _Taboola_Flat_File_Import_spend_final AS(
SELECT business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	SUM(IFNULL(spend :: FLOAT, 0))  AS media_cost,
	SUM(IFNULL(Impressions :: FLOAT,0)) AS Impressions_count,
	SUM(IFNULL(Clicks :: FLOAT,0)) AS Clicks_count,
	spend_currency,
	'MEDIACOST' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _Taboola_Flat_File_Import_spend
GROUP BY  business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
    );

/************************************Completed Taboola Flat File Import spend *************************/

/************************** Horizon Flat File Import ad insights by hour
10. With we are trying to capture the day level spend

****/

CREATE OR REPLACE TEMPORARY TABLE _Horizon_Flat_File_Import_spend_final AS(
SELECT hoz.Brand || hoz.Country AS business_unit,
    hoz.Country AS country,
    hoz.Date AS spend_date,
	hoz.store_id,
    'TV+Streaming' AS channel,
	'SFTP' AS source,
	'Streaming' AS subchannel,
	CASE WHEN hoz.Vendor = 'Hulu' THEN 'Horizon'
			   ELSE hoz.Vendor
		  END AS vendor,
    'Prospecting' AS targeting,
    0 as is_mens_flag,
    0 as is_scrubs_flag,
    SUM(IFNULL(spend:: float, 0)) AS media_cost,
	SUM(IFNULL(Impressions :: FLOAT,0)) AS Impressions_count,
	0 AS Clicks_count,
	hoz.currency AS spend_currency,
    'MEDIACOST' AS spend_type,
	IFF(hoz.SPECIALTY_STORE is null,'FAlSE','TRUE') as IS_SPECIALTY_STORE,
	hoz.SPECIALTY_STORE as SPECIALTY_STORE
FROM lake.media.horizon_spend_legacy hoz
group by all
-- WHERE hoz.Date >= '2018-07-01'
  );


/************************************Completed Horizon Flat File Import Spend*************************/

/************************** Tradedesk API ad insights by hour
11. With we are trying to capture the day level spend

****/

CREATE OR REPLACE TEMPORARY TABLE _Tradedesk_API_spend AS(
SELECT   aidu.date as spend_date,
	  CASE WHEN ds.store_brand = 'ShoeDazzle' and aidu.campaign_name ILIKE '%SDCA%' THEN 55
					 ELSE ds.store_id
				END as store_id,
      aidu.advertiser_id,
	  ds.store_brand || CASE WHEN ds.store_brand = 'ShoeDazzle' and aidu.campaign_name ILIKE '%SDCA%' THEN 'CA'
												ELSE ds.store_country
										   END AS business_unit,
	  ds.store_region AS store_region,
	  CASE WHEN ds.store_brand = 'ShoeDazzle' and aidu.campaign_name ILIKE '%SDCA%' THEN 'CA'
					Else ds.store_country
			   END AS country,
	 CASE WHEN ds.store_brand = 'ShoeDazzle' and aidu.campaign_name ILIKE '%SDCA%' THEN 'CA'
						  ELSE ds.store_country
					 END AS store_country,
     IFF(aidu.media_type = 'Video' AND (aidu.campaign_name ILIKE '%CTV%' OR aidu.campaign_name ILIKE '%FEP%'),
             'TV+Streaming', 'Programmatic') AS channel,
	  CASE WHEN aidu.media_type = 'Display' and aidu.campaign_name ILIKE '%Display%' THEN 'TTDDisplay'
						 WHEN aidu.media_type ILIKE '%Native%' THEN 'TTDNative'
                         WHEN aidu.campaign_name ILIKE '%spotify%' THEN 'TTDSpotify'
						 WHEN aidu.media_type = 'Audio' THEN 'TTDAudio'
						 WHEN aidu.media_type = 'Video' and aidu.campaign_name LIKE '%%CTV%%' THEN 'TTDCTVStreaming'
						 WHEN aidu.media_type = 'Video' and aidu.campaign_name LIKE '%%FEP%%' THEN 'TTDFEPStreaming'
                         WHEN aidu.campaign_name LIKE '%%DOOH%%' THEN 'TTDDOOH'
						 WHEN aidu.media_type = 'Video' THEN 'TTDOnlineVideo'
						 ELSE aidu.media_type
						END  AS subchannel,
	  'API' AS source,
	  'TradeDesk' AS vendor,
      CASE WHEN aidu.campaign_name ILIKE '%%Prospecting%%' then 'Prospecting'
				  WHEN aidu.campaign_name ILIKE '%%LeadRetargeting%%' OR aidu.campaign_name Ilike '%%LeadRT%%' THEN 'Lead Retargeting'
				  WHEN aidu.campaign_name ILIKE '%%SiteRetargeting%%' OR aidu.campaign_name Ilike '%%SiteRT%%' THEN 'Site Retargeting'
				  WHEN aidu.campaign_name ILIKE '%%VIPRetarget%%' THEN 'VIP Retargeting'
				  WHEN aidu.campaign_name ILIKE '%%CancelledRT%%' THEN 'Cancelled VIP'
				  ELSE 'Prospecting'
                END AS targeting,
      am.MENS_ACCOUNT_FLAG as is_mens_flag,
      am.is_scrubs_flag,
	  aidu.advertiser_cost AS spend,
	   am.currency AS spend_currency,
	  aidu.impressions AS Impressions,
	  aidu.clicks AS Clicks,
      CASE WHEN ds.store_brand = 'ShoeDazzle' and aidu.campaign_name ILIKE '%SDCA%' THEN TRUE
            WHEN am.specialty_store is not null THEN TRUE
            ELSE FALSE
      END as is_specialty_store,
      CASE WHEN ds.store_brand = 'ShoeDazzle' and aidu.campaign_name ILIKE '%SDCA%' THEN 'CA'
            ELSE am.specialty_store
      END as specialty_store
FROM lake.media.tradedesk_daily_site_optimization_legacy aidu
JOIN lake_view.sharepoint.med_account_mapping_media  am ON aidu.advertiser_id=am.source_id
	 AND am.source = 'TradeDesk'
	AND am.reference_column='advertiser_id'
 JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
 WHERE am.include_in_cpa_report = 1
	AND aidu.date >= $low_watermark_ltz
	AND aidu.media_type != 'UTC'
    AND aidu.timezone = 'UTC'
  );


CREATE OR REPLACE TEMPORARY TABLE _Tradedesk_API_spend_final AS(
SELECT business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	SUM(IFNULL(spend :: FLOAT, 0))  AS media_cost,
	 SUM(IFNULL(Impressions :: FLOAT,0)) AS Impressions_count,
	 SUM(IFNULL(Clicks :: FLOAT,0)) AS Clicks_count,
	spend_currency,
	 'MEDIACOST' AS spend_type,
	 IS_SPECIALTY_STORE,
	 SPECIALTY_STORE
FROM _Tradedesk_API_spend
GROUP BY  business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
  );
/****************************** Metric Theory FEE for Tradedesk ************************************************/

CREATE OR REPLACE TEMPORARY TABLE _Tradedesk_Metric_Theory_API_spend_final AS (
	SELECT business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,'Metric Theory' as vendor
	,targeting
	,is_mens_flag
    ,is_scrubs_flag
	,SUM(IFNULL(spend::FLOAT, 0)) * 0.08 AS media_cost
	,SUM(IFNULL(Impressions::FLOAT, 0)) AS Impressions_count
	,SUM(IFNULL(Clicks::FLOAT, 0)) AS Clicks_count
	,spend_currency
	,'FEES' AS spend_type
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE
  FROM _Tradedesk_API_spend
  WHERE (advertiser_id = 'le22f9z' AND spend_date < '2022-04-01' and spend_date >= '2021-10-18')
		OR (advertiser_id = '7hkttxc' AND spend_date >= '2021-10-18')
  GROUP BY business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,Vendor
	,targeting
	,is_mens_flag
    ,is_scrubs_flag
	,spend_currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE
	);
/************************************Completed Tradedesk API spend*************************/


/************************** Fixed CPL/CPA Partners ad insights by hour
12. With we are trying to capture the day level spend

****/


CREATE OR REPLACE TEMPORARY TABLE _Fixed_CPL_CPA_Partners_spend_final AS(
SELECT  ds.store_brand||ds.store_country AS business_unit,
    ds.store_country AS country,
    fps.date AS spend_date,
	fps.store_id AS store_id,
	mp.channel AS channel,
	 'Fixed' AS source,
	mp.subchannel AS subchannel,
	fps.vendor AS vendor,
    'Prospecting' AS targeting,
    0 as is_mens_flag,
    0 as is_scrubs_flag,
    SUM(IFNULL(fps.spend :: FLOAT, 0))  AS media_cost,
    0 AS Impressions_count,
	0 AS Clicks_count,
	fps.currency AS spend_currency,
    'MEDIACOST' AS spend_type,
	IFF(fps.SPECIALTY_STORE is null,'FALSE','TRUE') as IS_SPECIALTY_STORE,
	fps.SPECIALTY_STORE as SPECIALTY_STORE
FROM lake.media.fixed_partner_spend_legacy fps
join edw_prod.data_model.dim_store ds on fps.store_id=ds.store_id
JOIN LAKE_VIEW.SHAREPOINT.MED_MEDIA_PARTNER_MAPPING mp on mp.vendor = fps.vendor
WHERE fps.date >= $low_watermark_ltz
GROUP BY  business_unit,
	country,
	spend_date,
	fps.store_id,
	channel,
	source,
	subchannel,
	fps.vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
  );

/************************************Completed Fixed CPL/CPA Partners spend*************************/


/************************** googleanalytics.clicklab
13. With we are trying to capture the day level spend

****/

CREATE OR REPLACE TEMPORARY TABLE _googleanalytics_clicklab_spend_final AS(
SELECT  ds.store_brand||ds.store_country  AS business_unit,
    ds.store_country AS country,
    gcs.date AS spend_date,
	ds.store_id  AS store_id,
	'Affiliate' AS channel,
	'API' AS source,
	'Emailing' AS subchannel,
	'Clicklab/Darwin' AS vendor,
    'Prospecting' AS targeting,
     am.MENS_ACCOUNT_FLAG as is_mens_flag,
     am.is_scrubs_flag,
	 SUM(CASE WHEN am.store_id = 48 THEN gcs.sessions * 0.28
               WHEN am.store_id = 50 THEN gcs.sessions * 0.26
               WHEN am.store_id = 69 and gcs.date < '2018-02-01' THEN gcs.sessions * 0.28
               WHEN am.store_id = 69 and gcs.date >= '2018-02-01' THEN gcs.sessions *0.5
               ELSE 0
			  END) as media_cost,
    0 AS impressions_count,
	SUM(IFNULL(CAST(gcs.sessions AS FLOAT),0)) AS clicks_count,
    am.currency AS spend_currency,
    'MEDIACOST' AS spend_type,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake.media.clicklab_spend_legacy gcs
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id =  (gcs.view_id :: VARCHAR(100))
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
AND ds.store_id IN (48,50,69)
WHERE am.include_in_cpa_report = 1
	AND gcs.date >= $low_watermark_ltz
GROUP BY  business_unit,
	country,
	spend_date,
	ds.store_id,
	channel,
	subchannel,
	vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
  );

/************************************Completed googleanalytics clicklab spend*************************/
/************************** shopzilla
14. With we are trying to capture the day level spend

****/


CREATE OR REPLACE TEMPORARY TABLE _shopzilla_connexity_spend AS(
SELECT  sc.date_key AS spend_date,
	ds.store_id AS store_id,
	ds.store_brand||ds.store_country AS business_unit,
	'NA' AS store_region,
	'US' AS country,
	ds.store_country AS store_country,
	'Shopping' AS channel,
	'AUTOEMAIL' AS source,
	'CSE' AS subchannel,
	'Shopzilla/Connexity' AS vendor,
    'Prospecting' AS targeting,
     0 as is_mens_flag,
     0 as is_scrubs_flag,
	sc.cost AS spend,
	ds.store_currency AS spend_currency,
	0 AS Impressions,
	sc.clicks AS Clicks,
	'FAlSE' as IS_SPECIALTY_STORE,
	NULL as SPECIALTY_STORE
FROM lake.media.connexity_spend_legacy sc
JOIN edw_prod.data_model.dim_store ds ON ds.store_full_name=sc.Merchant_Name ||' '|| 'US'
WHERE sc.date_key >= $low_watermark_ltz
);



CREATE OR REPLACE TEMPORARY TABLE _shopzilla_connexity_spend_final AS(
SELECT business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	SUM(IFNULL(spend :: FLOAT, 0))  AS media_cost,
	SUM(IFNULL(Impressions :: FLOAT,0)) AS Impressions_count,
	SUM(IFNULL(Clicks :: FLOAT,0)) AS Clicks_count,
	spend_currency,
	'MEDIACOST' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _shopzilla_connexity_spend
GROUP BY  business_unit,
	country,
	spend_date,
	store_id,
	channel,
	source,
	subchannel,
	Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
);

/************************************Completed shopzilla spend*************************/


/************************************Pinterest API Ad1 Spend*************************/

CREATE OR REPLACE TEMPORARY TABLE _pinterest_api_spend_ad1_final AS
SELECT  CONCAT(ds.store_brand,ds.store_country) AS business_unit,
    ds.store_country AS country,
    ps1.date::DATE AS spend_date,
    ds.store_id,
    'Pinterest' AS channel,
    'API' AS source,
    CASE WHEN ps1.PIN_PROMOTION_CAMPAIGN_ID in ('626741273541','626741760799') THEN 'Tubescience'
	ELSE 'Pinterest' END AS subchannel,
    'Pinterest' AS vendor,
    CASE WHEN ps1.PIN_PROMOTION_CAMPAIGN_ID IN ('626740641426', '626740641401') THEN 'Free Alpha' ELSE 'Prospecting' END as targeting,
    am.MENS_ACCOUNT_FLAG as is_mens_flag,
    am.is_scrubs_flag,
    SUM(IFNULL((ps1.spend_in_micro_dollar/1000000)::FLOAT, 0)) AS media_cost,
    SUM(IFNULL(IFNULL(ps1.impression_1, 0) + IFNULL(ps1.impression_2, 0), 0)) AS impressions_count,
    SUM(IFNULL(IFNULL(ps1.clickthrough_1, 0) + IFNULL(ps1.clickthrough_2, 0), 0)) AS clicks_count,
    am.currency AS spend_currency,
    'MEDIACOST' AS spend_type,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake_view.pinterest.spend_ad1 ps1
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id =ps1.advertiser_id
	AND am.reference_column = 'account_id'
	AND am.source = 'Pinterest'
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
WHERE am.include_in_cpa_report = 1
	AND ps1.date::date >= $low_watermark_ltz
GROUP BY  business_unit,
    country ,
    spend_date,
    ds.store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;

---------------------------------------------------------------- Commission Junction Spend

CREATE OR REPLACE TEMPORARY TABLE _commissionjunction_spend AS
select
    ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    convert_timezone('America/Los_Angeles', s.POSTINGDATE)::DATE AS spend_date,
    ds.store_id,
    ds.store_region AS store_region,
    'Affiliate' AS channel,
    'API' AS source,
    'Networks' AS subchannel,
    'CommissionJunction' AS vendor,
    'MEDIACOST' AS spend_type,
    'Prospecting' AS targeting,
    IFF(ds.store_id=241 or s.actiontrackerid=432671,0,IFF(s.custSegment='M',1,0)) as is_mens_flag,
    am.is_scrubs_flag,
    s.PUBCOMMISSIONAMOUNTUSD AS spend,
    am.currency AS spend_currency,
    0 as impressions,
    0 as clicks,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake.cj.advertiser_spend s
JOIN lake_view.sharepoint.med_account_mapping_media am
    ON am.source_id = s.actiontrackerid
        AND am.source ilike 'CJ Spend'
JOIN edw_prod.data_model.dim_store ds
    ON ds.store_id = am.store_id
WHERE s.aid IS NOT NULL AND actiontype!='bonus'

UNION ALL

select
    ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    convert_timezone('America/Los_Angeles', s.POSTINGDATE)::DATE AS spend_date,
    ds.store_id,
    ds.store_region AS store_region,
    'Affiliate' AS channel,
    'API' AS source,
    'Networks' AS subchannel,
    'CommissionJunction' AS vendor,
    'FEES' AS spend_type,
    'Prospecting' AS targeting,
    IFF(ds.store_id=241 or s.actiontrackerid=432671,0,IFF(s.custSegment='M',1,0)) as is_mens_flag,
    am.is_scrubs_flag,
    s.CJFEEUSD AS spend,
    am.currency AS spend_currency,
    0 as impressions,
    0 as clicks,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake.cj.advertiser_spend s
JOIN lake_view.sharepoint.med_account_mapping_media am
    ON am.source_id = s.actiontrackerid
        AND am.source ilike 'CJ Spend'
JOIN edw_prod.data_model.dim_store ds
    ON ds.store_id = am.store_id
WHERE s.aid IS NOT NULL AND actiontype!='bonus';


CREATE OR REPLACE TEMPORARY TABLE _commissionjunction_spend_final AS
SELECT
    business_unit,
    country ,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(spend) AS media_cost,
    SUM(IFNULL(Impressions::decimal(18,4),0)) AS Impressions_count,
    SUM(IFNULL(Clicks::decimal(18,4),0)) AS Clicks_count,
    spend_currency,
    spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _commissionjunction_spend
GROUP BY  business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,Vendor
    ,targeting
    ,is_mens_flag
    ,is_scrubs_flag
	,spend_currency
	,spend_type
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;

---------------------------------------------------------------- TikTok Spend

CREATE OR REPLACE TEMPORARY TABLE _tiktok_spend AS
SELECT
	ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    ts.date AS spend_date,
    ds.store_id,
    ds.store_region AS store_region,
    'TikTok' AS channel,
    'API' AS source,
    campaign_name,
    CASE WHEN campaign_name ilike '%TubeScience%' THEN 'Tubescience'
        WHEN (campaign_name ilike '%Sapphire%' or adgroup_name ilike '%Sapphire%' or ad_name ilike '%Sapphire%') and CAMPAIGN_NAME not ilike '%CreativeExchange%' THEN 'Sapphire'
        WHEN ad_name ilike '%Schema%' OR campaign_name ilike '%schema%' THEN 'Schema'
        WHEN campaign_name ILIKE '%Gassed%' THEN 'Gassed'
        WHEN campaign_name ILIKE '%NRTV%' or campaign_name ilike '%Narrative%' then 'Narrative'
        ELSE 'TikTok'
    END AS subchannel,
    CASE WHEN campaign_name ilike '%TubeScience%' THEN 'Tubescience'
        ELSE 'TikTok'
    END AS vendor,
    'MEDIACOST' AS spend_type,
    'Prospecting' AS targeting,
    am.mens_account_flag as is_mens_flag,
    am.is_scrubs_flag,
    ts.media_cost AS spend,
    am.currency AS spend_currency,
    ts.impressions,
    ts.clicks,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
am.SPECIALTY_STORE
FROM LAKE_VIEW.TIKTOK.DAILY_SPEND ts
JOIN lake_view.sharepoint.med_account_mapping_media am
    ON am.source_id = ts.advertiser_id
        AND am.source ilike 'TikTok'
JOIN edw_prod.data_model.dim_store ds
    ON ds.store_id = am.store_id
WHERE ts.campaign_id <> '1692161851867170' --removed DPA Campaign as no longer applicable for CPA reports or Media
    AND not (((lower(ts.campaign_name) like '%afterpay%' and lower(ts.campaign_name)!='sxf_tiktok_afterpay_13') or (lower(ts.campaign_name)='sxf_tiktok_afterpay_13' and spend_date<'2022-05-11')) AND lower(ds.store_brand_abbr||ds.store_region) = 'sxna')
    AND ts.campaign_name not ilike '%TikTokShops_%'
    AND ts.campaign_name not ilike '%OrganicSocialBoosting%'
;


CREATE OR REPLACE TEMPORARY TABLE _tiktok_spend_final AS
SELECT
    business_unit,
    country ,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(spend) AS media_cost,
    SUM(IFNULL(Impressions::decimal(18,4),0)) AS Impressions_count,
    SUM(IFNULL(Clicks::decimal(18,4),0)) AS Clicks_count,
    spend_currency,
    spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _tiktok_spend
GROUP BY  business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,Vendor
    ,targeting
    ,is_mens_flag
    ,is_scrubs_flag
	,spend_currency
	,spend_type
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;
----------------------------------Schema FEES------------------------
CREATE OR REPLACE TEMPORARY TABLE _schema_spend AS
SELECT spend_date,
	store_id,
	business_unit,
	country,
	'TikTok' AS channel,
	'API' AS source,
	subchannel AS subchannel,
	subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	IFNULL(spend * 0.10, 0) AS spend,
	spend_currency,
	Impressions,
	Clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _tiktok_spend
WHERE subchannel IN ('Schema')
and spend_date>='2022-09-23'

UNION ALL

select spend_date,
    store_id,
    business_unit,
    country,
    'FB+IG' AS channel,
    'API' AS source,
    subchannel,
    subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    IFNULL(spend * 0.10,0) AS spend,
    spend_currency,
    impressions,
    clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _facebook_spend
WHERE subchannel IN ('Schema')
and spend_date>='2022-09-23'

UNION ALL

select spend_date,
    store_id,
    business_unit,
    country,
    channel,
    'API' AS source,
    subchannel,
    subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    IFNULL(spend * 0.10,0) AS spend,
    spend_currency,
    impressions,
    clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _youtube_api_spend
WHERE subchannel IN ('Schema')
and spend_date>='2022-09-23'
;

CREATE OR REPLACE TEMPORARY TABLE _schema_spend_final AS
SELECT
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
    SUM(IFNULL(impressions::FLOAT,0)) AS impressions_count,
    SUM(IFNULL(clicks::FLOAT,0)) AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _schema_spend
GROUP BY
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;
--------------------------------------------Peoplehype / 1180 / Volt / DonutDigital Fees---------------------
CREATE OR REPLACE TEMPORARY TABLE _Peoplehype_1180_spend AS
SELECT
    spend_date,
    store_id,
    business_unit,
    country,
    'FB+IG' AS channel,
    'API' AS source,
    subchannel,
    subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    CASE WHEN subchannel = '1180' THEN spend * 0.15
         WHEN subchannel = 'DonutDigital' THEN spend * 0.10
         WHEN subchannel = 'Volt' THEN spend * 0.10
         WHEN subchannel = 'Peoplehype' THEN spend * 0.15
    END AS spend,
    spend_currency,
    0 AS impressions,
    0 AS clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE,
    account_id
FROM _facebook_spend
WHERE subchannel in ('1180','DonutDigital','Volt')
    OR ((subchannel = 'Peoplehype' and account_id in ('2177180699166617','356365288557886') and spend_date<'2022-11-09')
        OR (subchannel = 'Peoplehype' and account_id not in ('2177180699166617','356365288557886')));

CREATE OR REPLACE TEMPORARY TABLE _Peoplehype_1180_spend_final AS
SELECT
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
    SUM(IFNULL(impressions::FLOAT,0)) AS impressions_count,
    SUM(IFNULL(clicks::FLOAT,0)) AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _Peoplehype_1180_spend
GROUP BY
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;

------------------------------------------------------------------------TubeScience

CREATE OR REPLACE TEMPORARY TABLE _tubescience_spend AS
SELECT
    spend_date,
    a.store_id,
    business_unit,
    country,
    'FB+IG' AS channel,
    'API' AS source,
    subchannel,
    subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    CASE WHEN subchannel = 'Tubescience' and spend_date < '2019-04-01' THEN media_cost * 0.15
          WHEN subchannel = 'Tubescience' and spend_date >= '2019-04-01' THEN media_cost * 0.10
          WHEN subchannel = 'Agencywithin' THEN media_cost * 0.10
          WHEN subchannel = 'geistm' THEN media_cost * 0.10
          WHEN subchannel = 'Narrative' and spend_date >= '2020-07-01' THEN media_cost * 0.10
          WHEN subchannel = 'ADMK' THEN media_cost * 0.10
          WHEN subchannel = 'MySubscriptionAddiction' AND store_brand_abbr='SX' and spend_date>= '2022-11-19' THEN media_cost * 0.10
          WHEN subchannel = 'MySubscriptionAddiction' and spend_date>= '2022-11-19' THEN media_cost * 0.05
          WHEN subchannel = 'Gassed' AND store_brand_abbr='SX' THEN IFNULL(media_cost * 0.10, 0)
          WHEN subchannel = 'Gassed' AND a.store_id = 67 AND spend_date < '2023-07-01' THEN IFNULL(media_cost * 0.15, 0)
          WHEN subchannel = 'Gassed'  THEN media_cost * 0.05
    END AS spend,
    spend_currency,
    0 AS impressions,
    0 AS clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _facebook_spend_final  a
join edw_prod.data_model.dim_store ds on a.store_id=ds.store_id
WHERE subchannel IN ('Tubescience','Agencywithin','Narrative', 'geistm', 'ADMK','MySubscriptionAddiction','Gassed')

UNION ALL

SELECT spend_date,
	store_id,
	business_unit,
	country,
	'Pinterest' AS channel,
	'API' AS source,
	subchannel AS subchannel,
	subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	IFNULL(media_cost * 0.10, 0) AS spend,
	spend_currency,
	0 AS Impressions,
	0 AS Clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _Pinterest_API_spend_ad1_final
WHERE subchannel IN ('Tubescience')
	and spend_date >= '2019-11-20'

UNION ALL

SELECT spend_date,
	store_id,
	business_unit,
	country,
	'Snapchat' AS channel,
	'API' AS source,
	subchannel AS subchannel,
	subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	CASE WHEN subchannel='Tubescience' THEN IFNULL(media_cost * 0.10, 0)
	    WHEN subchannel='Gassed' THEN IFNULL(media_cost * 0.05, 0)
	END AS spend,
	spend_currency,
	0 AS Impressions,
	0 AS Clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _snapchat_api_spend_final
WHERE subchannel IN ('Tubescience', 'Gassed')

UNION ALL

SELECT spend_date,
	store_id,
	business_unit,
	country,
	'TikTok' AS channel,
	'API' AS source,
	subchannel AS subchannel,
	subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    CASE WHEN subchannel = 'Gassed' AND store_id = 67 AND spend_date < '2023-07-01' THEN IFNULL(media_cost * 0.15, 0)
         ELSE IFNULL(media_cost * 0.10, 0)
    END AS spend,
	spend_currency,
	0 AS Impressions,
	0 AS Clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _tiktok_spend_final
WHERE subchannel IN ('Tubescience','Gassed')
;

CREATE OR REPLACE TEMPORARY TABLE _tubescience_spend_final AS
SELECT
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
    SUM(IFNULL(impressions::FLOAT,0)) AS impressions_count,
    SUM(IFNULL(clicks::FLOAT,0)) AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _tubescience_spend
GROUP BY
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;

------------------------------------------------------------------------------------sapphire

CREATE OR REPLACE TEMPORARY TABLE _sapphire_spend AS
SELECT
    spend_date,
    store_id,
    business_unit,
    country,
    'FB+IG' AS channel,
    'API' AS source,
    subchannel,
    subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    IFNULL(media_cost * 0.10, 0) AS spend,
    spend_currency,
    0 AS impressions,
    0 AS clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _facebook_spend_final
WHERE subchannel IN ('Sapphire') and SPEND_DATE>='2021-12-01'

UNION ALL

SELECT spend_date,
	store_id,
	business_unit,
	country,
	'Snapchat' AS channel,
	'API' AS source,
	subchannel AS subchannel,
	subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	IFNULL(media_cost * 0.10, 0) AS spend,
	spend_currency,
	0 AS Impressions,
	0 AS Clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _snapchat_api_spend_final
WHERE subchannel IN ('Sapphire') and SPEND_DATE>='2021-12-01'

UNION ALL

SELECT spend_date,
	store_id,
	business_unit,
	country,
	'TikTok' AS channel,
	'API' AS source,
	subchannel AS subchannel,
	subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	IFNULL(spend * 0.10, 0) AS spend,
	spend_currency,
	0 AS Impressions,
	0 AS Clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _tiktok_spend
WHERE subchannel IN ('Sapphire') and SPEND_DATE>='2021-12-01' and lower(campaign_name)!='sxf_tiktok_sapphire_ttcx_vipbidding_sap'

UNION ALL

SELECT
    spend_date,
    store_id,
    business_unit,
    country,
    channel,
    'API' AS source,
    subchannel,
    subchannel AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    IFNULL(media_cost * 0.10, 0) AS spend,
    spend_currency,
    0 AS impressions,
    0 AS clicks,
    is_specialty_store,
	specialty_store
FROM _youtube_api_spend_final
WHERE subchannel IN ('Sapphire') and SPEND_DATE>='2022-03-15';


CREATE OR REPLACE TEMPORARY TABLE _sapphire_spend_final AS
SELECT
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
    SUM(IFNULL(impressions::FLOAT,0)) AS impressions_count,
    SUM(IFNULL(clicks::FLOAT,0)) AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _sapphire_spend
GROUP BY
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;

/************************************Smartly Spend for Pinterest API*************************/



CREATE OR REPLACE TEMPORARY TABLE _smartly_spend_for_pinterest AS
SELECT
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    'Smartly' AS Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    media_cost*0.05 AS media_cost,
    0 AS impressions_count,
    0 AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _pinterest_api_spend_ad1_final
WHERE spend_date BETWEEN '2019-08-01' AND '2020-03-31';


/************************************ Gassed FEE For Google Ads ***************************/
CREATE OR REPLACE TEMPORARY TABLE _gassed_fee_for_google_ads AS
SELECT
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    subchannel AS Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    CASE WHEN subchannel = 'Gassed' THEN media_cost * 0.05
        ELSE media_cost * 0.10 END AS spend,
    0 AS impressions_count,
    0 AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
    is_specialty_store,
    specialty_store
FROM _youtube_api_spend_final
WHERE subchannel in ('Gassed', 'Gassed10')
;


/************************************ New Narrative and Within FEE ***************************/

CREATE OR REPLACE TEMPORARY TABLE _narrative_and_within_fees AS
(
    SELECT
        business_unit,
        country,
        spend_date,
        store_id,
        channel,
        source,
        subchannel,
        CASE WHEN adset_name ilike '%narrative%'
                THEN 'Narrative'
             WHEN adset_name ilike '%within%'
                THEN 'Agencywithin'
        END AS Vendor,
        targeting,
        is_mens_flag,
        is_scrubs_flag,
        SUM(IFNULL(spend :: FLOAT, 0))*0.10 AS media_cost,
        0 AS impressions_count,
        0 AS clicks_count,
        spend_currency,
        'FEES' AS spend_type,
		IS_SPECIALTY_STORE,
		SPECIALTY_STORE
    FROM _facebook_spend
    WHERE channel = 'FB+IG'
    and subchannel = 'PaidInfluencers'
    and adset_name ilike any ('%narrative%', '%within%')
    and spend_date >= '2020-12-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15 , 17 ,18

    UNION ALL

    SELECT
        business_unit,
        country,
        spend_date,
        store_id,
        channel,
        source,
        subchannel,
        CASE WHEN labels ilike '%narrative%'
                THEN 'Narrative'
             WHEN labels ilike '%within%'
                THEN 'Agencywithin'
        END AS Vendor,
        targeting,
        is_mens_flag,
        is_scrubs_flag,
        SUM(IFNULL(spend :: FLOAT, 0))*0.10  AS media_cost,
        0 AS impressions_count,
        0 AS clicks_count,
        spend_currency,
        'FEES' AS spend_type,
		IS_SPECIALTY_STORE,
		SPECIALTY_STORE
    FROM _youtube_api_spend
    WHERE channel = 'Youtube'
    and subchannel = 'Youtube'
    and labels ilike any ('%narrative%', '%within%')
    and spend_date >= '2020-12-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15 ,17 ,18

    UNION ALL

    SELECT
        business_unit,
        country,
        spend_date,
        store_id,
        channel,
        source,
        subchannel,
        CASE WHEN adset_name ilike '%narrative%'
                THEN 'Narrative'
             WHEN adset_name ilike '%within%'
                THEN 'Agencywithin'
        END AS Vendor,
        targeting,
        is_mens_flag,
        is_scrubs_flag,
        SUM(IFNULL(spend :: FLOAT, 0))*0.10 AS media_cost,
        0 AS impressions_count,
        0 AS clicks_count,
        spend_currency,
        'FEES' AS spend_type,
		IS_SPECIALTY_STORE,
		SPECIALTY_STORE
    FROM _snapchat_api_spend
    WHERE channel = 'Snapchat'
    and subchannel = 'Snapchat'
    and adset_name ilike any ('%narrative%', '%within%')
    and spend_date >= '2020-12-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15 ,17 ,18

    UNION ALL

    SELECT
        business_unit,
        country,
        spend_date,
        store_id,
        channel,
        source,
        subchannel,
        'Narrative' AS Vendor,
        targeting,
        is_mens_flag,
        is_scrubs_flag,
        SUM(IFNULL(spend :: FLOAT, 0))*0.10 AS media_cost,
        0 AS impressions_count,
        0 AS clicks_count,
        spend_currency,
        'FEES' AS spend_type,
		IS_SPECIALTY_STORE,
		SPECIALTY_STORE
    FROM _tiktok_spend
    WHERE campaign_name ilike '%narrative%' or campaign_name ILIKE '%NRTV%'
    and spend_date >= '2021-01-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15 ,17 ,18
)
;

/************************************Metric Theory FEE for Google API*************************/

CREATE OR REPLACE TEMPORARY TABLE _metrictheory_spend_for_google AS
SELECT
    business_unit,
    country,
    spend_date,
    dsp.store_id,
    channel,
    source,
    subchannel,
    'MetricTheory' AS Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    CASE WHEN ds.store_brand_abbr || ds.store_region IN ('JFNA','SDNA','SXNA')
            AND dsp.spend_date >= '2020-10-07'
            THEN dsp.media_cost*0.055
         WHEN ds.store_brand_abbr || ds.store_region = 'FLNA' AND is_mens_flag = 0
            AND lower(channel) = 'shopping'
            AND dsp.spend_date BETWEEN '2020-11-01' AND '2020-11-30'
            THEN dsp.media_cost*0.055
         WHEN ds.store_brand_abbr || ds.store_region = 'FLNA'
            AND dsp.spend_date >= '2020-12-01'
            THEN dsp.media_cost*0.055
         WHEN ds.store_brand_abbr || ds.store_country = 'FKUS'
            AND dsp.spend_date >= '2020-12-01'
            THEN dsp.media_cost*0.055
    ELSE 0
    END AS media_cost,
    0 AS impressions_count,
    0 AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _google_doubleclick_spend_final dsp
join edw_prod.data_model.dim_store ds
on ds.store_id = dsp.store_id
WHERE lower(channel) in ('branded search', 'non branded search', 'shopping', 'programmatic-gdn')
    AND dsp.spend_date >= '2020-10-07'
    AND (
     ds.store_brand_abbr || ds.store_region NOT IN ('JFNA','SDNA','FKNA','FLNA','YTYNA','SXNA') OR
     (dsp.spend_date <'2022-09-01' AND ds.store_brand_abbr || ds.store_region IN ('JFNA','SDNA')) OR
     (dsp.spend_date <'2022-10-01' AND ds.store_brand_abbr || ds.store_region IN ('FKNA','FLNA','YTYNA','SXNA'))
    )

UNION ALL

SELECT
    business_unit,
    country,
    spend_date,
    ysf.store_id,
    channel,
    source,
    subchannel,
    'MetricTheory' AS Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    CASE WHEN ds.store_brand_abbr || ds.store_region IN ('JFNA','SDNA','SXNA')
            AND ysf.spend_date >= '2020-10-07'
            THEN ysf.media_cost*0.055
         WHEN ds.store_brand_abbr || ds.store_region = 'FLNA'
            AND ysf.spend_date >= '2020-12-01'
            THEN ysf.media_cost*0.055
         WHEN ds.store_brand_abbr || ds.store_country = 'FKUS'
            AND ysf.spend_date >= '2020-12-01'
            THEN ysf.media_cost*0.055
    ELSE 0
    END AS media_cost,
    0 AS impressions_count,
    0 AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _youtube_api_spend_final ysf
join edw_prod.data_model.dim_store ds
on ds.store_id = ysf.store_id
WHERE lower(channel) in ('programmatic-gdn', 'youtube')
    AND ysf.spend_date >= '2020-10-07'
    AND (
     ds.store_brand_abbr || ds.store_region NOT IN ('JFNA','SDNA','FKNA','FLNA','YTYNA','SXNA') OR
     (ysf.spend_date <'2022-09-01' AND ds.store_brand_abbr || ds.store_region IN ('JFNA','SDNA')) OR
     (ysf.spend_date <'2022-10-01' AND ds.store_brand_abbr || ds.store_region IN ('FKNA','FLNA','YTYNA','SXNA'))
    )
;

----------------------------------------------------------------------------------------------Motive mertrics


CREATE OR REPLACE TEMPORARY TABLE _motivemetrics_spend  AS
SELECT
    dcs.spend_date::DATE AS spend_date,
    dcs.store_id,
    dcs.business_unit,
    dcs.store_region,
    dcs.country,
    dcs.store_country,
    dcs.channel,
    'API' AS source,
    dcs.subchannel,
    'MotiveMetrics' AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    CASE WHEN dcs.country IN ('UK','US','CA','AU') AND dcs.spend_date >= '2017-04-01' AND dcs.spend_date <= '2018-02-28' THEN dcs.spend*0.04
         WHEN dcs.country IN ('US','CA','AU') AND dcs.spend_date >= '2018-03-01' AND dcs.spend_date <= '2018-08-30' THEN dcs.spend*0.035
         WHEN dcs.country = 'UK' AND dcs.spend_date >= '2018-03-01' and dcs.spend_date < '2018-07-01' THEN dcs.spend*0.035
         WHEN dcs.country IN ('US','CA','AU') AND dcs.spend_date >= '2018-09-01' AND dcs.spend_date <= '2020-11-05' THEN dcs.spend*0.03
         WHEN ds.store_brand_abbr || ds.store_region = 'FLNA' AND is_mens_flag = 0
                AND dcs.spend_date between '2018-09-01' AND  '2020-11-30'
            THEN dcs.spend*0.03
         WHEN ds.store_brand_abbr || ds.store_region = 'FLNA' AND is_mens_flag = 1
                AND dcs.spend_date between '2018-09-01' AND  '2021-12-31'
            THEN dcs.spend*0.03
         WHEN ds.store_brand_abbr || ds.store_region = 'FKNA'
                AND dcs.spend_date between '2018-09-01' AND  '2021-12-31'
            THEN dcs.spend*0.03
         ELSE 0
    END AS spend,
    dcs.spend_currency,
    0 AS impressions,
    0 AS clicks,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _google_doubleclick_spend dcs
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=dcs.store_id
WHERE dcs.channel IN ('Branded Search', 'Non Branded Search')
	AND dcs.spend_date >= '2017-04-01';


CREATE OR REPLACE TEMPORARY TABLE _motivemetrics_spend_final  AS
SELECT business_unit,
    country ,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
    SUM(IFNULL(impressions::DECIMAL,0)) AS impressions_count,
    SUM(IFNULL(clicks::DECIMAL,0)) AS clicks_count,
    spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _motivemetrics_spend
GROUP BY
    business_unit,
	country ,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;


/************************************MSA FEE for Google API*************************/

CREATE OR REPLACE TEMPORARY TABLE _msa_spend  AS
SELECT
    dcs.business_unit,
    dcs.country,
    dcs.spend_date::DATE AS spend_date,
    dcs.store_id,
    dcs.channel,
    'API' AS source,
    dcs.subchannel,
    'MySubscriptionAddiction' AS vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    dcs.spend*0.05 AS spend,
    0 AS impressions,
    0 AS clicks,
    dcs.spend_currency,
    'FEES' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _google_doubleclick_spend dcs
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=dcs.store_id
WHERE dcs.channel = 'Non Branded Search'
    AND dcs.campaign like '%%MSA%%'
	AND dcs.spend_date >= '2023-02-01';


---------------------------------------------------------------------------------------------First_media
CREATE OR replace TEMPORARY TABLE _First_media AS
SELECT start_date
	,end_date
	,store_id
	,business_unit || country AS business_unit
	,region AS store_region
	,country AS country
	,channel
	,subchannel
	,vendor
	,is_mens_flag
    ,0 as is_scrubs_flag
	,IFNULL(spend::FLOAT, 0) AS spend
	,0 AS impressions
	,0 AS clicks
	,'MEDIACOST' AS spend_type
	,'USD' AS spend_currency
	,'FALSE' AS IS_SPECIALTY_STORE
	,NULL SPECIALTY_STORE
FROM LAKE_VIEW.SHAREPOINT.MED_FIRST_MEDIA_SPEND;

CREATE OR replace TEMPORARY TABLE _First_media_final AS
SELECT business_unit
	,country
	,dd.full_date AS spend_date
	,store_id
	,channel
	,'Manual' AS source
	,subchannel
	,Vendor
	,'Prospecting' AS targeting
	,is_mens_flag
     ,is_scrubs_flag
	,SUM(IFNULL((fm.spend / ((DATEDIFF(DAY, fm.start_date, fm.end_date)) + 1))::FLOAT, 0)) AS media_cost
	,SUM(IFNULL(impressions::DECIMAL(24, 4), 0)) AS impressions_count
	,SUM(IFNULL(clicks::DECIMAL(24, 4), 0)) AS clicks_count
	,spend_currency
	,spend_type
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE
FROM _First_media fm
JOIN edw_prod.data_model.dim_date dd ON dd.full_date BETWEEN fm.start_date
		AND fm.end_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,15,16,17,18;

------------------------------------------------------------------------------------------------Fixed Cost


CREATE OR REPLACE TEMPORARY TABLE _fixed_costs AS
SELECT
	c.spend_spread,
	c.effective_start_date AS start_date,
	c.effective_end_date AS end_date,
	ms.store_id,
	media_channel_name AS channel,
	media_subchannel_name AS subchannel,
	media_vendor_name AS vendor,
    'Prospecting' AS targeting,
    c.is_mens_flag as is_mens_flag,
    c.is_scrubs_flag,
	c.spend_amt AS spend,
	c.impressions_count AS impressions,
	c.clicks_count AS clicks,
	spend_type_code AS spend_type,
	spend_currency_code AS currency,
	'FALSE' as IS_SPECIALTY_STORE,
	NULL as SPECIALTY_STORE
FROM LAKE_VIEW.SHAREPOINT.MED_MANUAL_SPEND_ADJUSTMENTS c
JOIN edw_prod.data_model.dim_store ms ON lower(ms.store_full_name) =  lower(c.store_brand_name || ' ' || c.store_country_code)
WHERE media_channel_name NOT IN ('Influencers' ,'Ambassadors')
	OR (media_channel_name ILIKE 'Influencers'
		AND c.media_subchannel_name ILIKE 'Other'
		AND c.media_vendor_name ILIKE 'Gsheet');


CREATE OR REPLACE TEMPORARY TABLE _media_cost AS
SELECT
	fc.spend_spread,
	fc.start_date,
	dd.full_date,
	fc.store_id,
	fc.channel,
	fc.subchannel,
	fc.vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	CASE WHEN lower(fc.spend_spread) = 'even' THEN (fc.spend/((DATEDIFF(DAY,fc.start_date,fc.end_date))+1)) -- Spliting the spend evenly between start_date and end_date
         WHEN lower(fc.spend_spread) = 'print' THEN (fc.spend/(DATEDIFF(DAY,fc.start_date,DATEADD(DAY,14,fc.start_date)))) --Spliting the spend evenly for 14 days from the start_date
         WHEN lower(fc.spend_spread) = 'catalog' THEN CASE WHEN dd.day_of_month <= 7 THEN (fc.spend/(7*2)) --- Spliting the spend into 50 percent for week 1
                                                    WHEN dd.day_of_month > 7 AND dd.day_of_month < 22 THEN (fc.spend/(7*5)) --Spliting the spend into 20 percent for week 2 and week 3
                                                    WHEN dd.day_of_month >= 22 THEN (fc.spend/(7*10)) --Spliting the spend into 10 percent for week 4
                                                END
        ELSE spend
    END AS spend,
	CASE WHEN lower(fc.spend_spread) = 'even' THEN (1.00 * fc.impressions/((DATEDIFF(DAY,fc.start_date,fc.end_date))+1)) -- Spliting the impressions evenly between start_date and end_date
         WHEN lower(fc.spend_spread) = 'print' THEN (1.00 * fc.impressions/(DATEDIFF(DAY,fc.start_date,DATEADD(DAY,14,fc.start_date)))) --Spliting the impressions evenly for 14 days from the start_date
         WHEN lower(fc.spend_spread) = 'catalog' THEN CASE WHEN dd.day_of_month <= 7 THEN (1.00 * fc.impressions/(7*2)) --- Spliting the impressions into 50 percent for week 1
                                                    WHEN dd.day_of_month > 7 AND dd.day_of_month < 22 THEN (1.00 * fc.impressions/(7*5)) --Spliting the impressions into 20 percent for week 2 and week 3
                                                    WHEN dd.day_of_month >= 22 THEN (1.00 * fc.impressions/(7*10)) --Spliting the impressions into 10 percent for week 4
                                                END
        ELSE 1.00 * impressions
    END AS impressions,
	CASE WHEN lower(fc.spend_spread) = 'even' THEN (1.00 * fc.clicks/((DATEDIFF(DAY,fc.start_date,fc.end_date))+1)) -- Spliting the clicks evenly between start_date and end_date
         WHEN lower(fc.spend_spread) = 'print' THEN (1.00 * fc.clicks/(DATEDIFF(DAY,fc.start_date,DATEADD(DAY,14,fc.start_date)))) --Spliting the clicks evenly for 14 days from the start_date
         WHEN lower(fc.spend_spread) = 'catalog' THEN CASE WHEN dd.day_of_month <= 7 THEN (1.00 * fc.clicks/(7*2)) --- Spliting the clicks into 50 percent for week 1
                                                    WHEN dd.day_of_month > 7 AND dd.day_of_month < 22 THEN (1.00 * fc.clicks/(7*5)) --Spliting the clicks into 20 percent for week 2 and week 3
                                                    WHEN dd.day_of_month >= 22 THEN (1.00 * fc.clicks/(7*10)) --Spliting the clicks into 10 percent for week 4
                                                END
        ELSE	1.00 * clicks
    END AS clicks,
	fc.spend_type,
	fc.currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _fixed_costs fc
JOIN edw_prod.data_model.dim_date dd ON dd.full_date BETWEEN fc.start_date AND CASE WHEN lower(fc.spend_spread) IN ('even','catalog') THEN end_date
										       WHEN lower(fc.spend_spread) = 'fixed' THEN fc.start_date -- Keeping start_date as end_date for fixed spread
										       WHEN lower(fc.spend_spread) = 'print' THEN DATEADD(DAY,13,fc.start_date) -- Adding 14 days to the start_date as end_date
										  END
WHERE lower(fc.spend_spread) IN ('even','catalog','print','fixed');



CREATE OR REPLACE TEMPORARY TABLE _fixed_cost_final AS
SELECT
    full_date AS spend_date,
	s.store_id,
	s.store_brand||s.store_country AS business_unit,
	s.store_region AS store_region,
	s.store_country AS country,
	channel,
	subchannel,
	vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	IFNULL(mc.spend :: FLOAT, 0) AS spend,
	IFNULL(mc.impressions :: FLOAT, 0) AS impressions,
	IFNULL(mc.clicks :: FLOAT, 0) AS clicks,
	spend_type,
	currency AS spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _media_cost mc
JOIN edw_prod.data_model.dim_store s ON mc.store_id = s.store_id
;


CREATE OR REPLACE TEMPORARY TABLE _fixed_cost_spend AS
SELECT fmfc.spend_date,
	fmfc.store_id,
	fmfc.business_unit,
	fmfc.store_region ,
	fmfc.country ,
	fmfc.channel,
	'Fixed_Cost' AS source,
	fmfc.subchannel,
	fmfc.vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
	fmfc.spend,
	fmfc.spend_currency ,
	fmfc.impressions AS impressions_count,
	fmfc.clicks AS clicks_count,
	fmfc.spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _fixed_cost_final fmfc;


CREATE OR REPLACE TEMPORARY TABLE _fixed_cost_spend_final AS
SELECT
    business_unit,
    country ,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL(spend::FLOAT, 0)) AS media_cost,
    SUM(IFNULL(impressions_count::DECIMAL(24,4),0)) AS impressions_count,
    SUM(IFNULL(clicks_count::DECIMAL(24,4),0)) AS clicks_count,
    spend_currency,
    spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _fixed_cost_spend
GROUP BY
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
    spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;

--------------------------------------------------------------------------------------------------- Influencer Spend logic

CREATE OR REPLACE TEMPORARY TABLE _influencer_spend_final AS
SELECT
    ds.store_brand || ds.store_country AS business_unit
	,ds.store_country AS country
	,date AS spend_date
	,ds.store_id
	,'Influencers' AS channel
	,'Manual' AS source
	,IFF(lower(channel) IN ('microinfluencers','streamers'), 'MicroInfluencers', coalesce(channel,'Influencer')) AS subchannel
	,'Gsheet' AS vendor
	,'Prospecting' AS targeting
    ,mens_flag AS is_mens_flag
    ,is_scrubs_flag
	,SUM(COALESCE(UPFRONT_SPEND,0))::FLOAT AS media_cost
	,0 AS impressions_count
	,0 AS clicks_count
    ,'USD' AS spend_currency
	,'UPFRONT' AS spend_type
	,'FALSE' as IS_SPECIALTY_STORE
	,NULL as SPECIALTY_STORE
FROM reporting_media_base_prod.influencers.daily_spend inf
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = inf.store_id
WHERE INCLUDE_IN_CPA_REPORT_FLAG = 1
AND lower(POST_STATUS) = 'live'
AND (mens_flag <> 1 OR lower(store_region) <> 'na' OR (date < '2021-01-01' OR date >= '2024-01-01'))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,14,15,16,17

UNION ALL

SELECT
    ds.store_brand || ds.store_country AS business_unit
	,ds.store_country AS country
	,date AS spend_date
	,ds.store_id
	,'Influencers' AS channel
	,'Manual' AS source
	,IFF(lower(channel) IN ('microinfluencers','streamers'), 'MicroInfluencers', coalesce(channel,'Influencer')) AS subchannel
	,'Gsheet' AS vendor
	,'Prospecting' AS targeting
    ,mens_flag AS is_mens_flag
    ,is_scrubs_flag
	,SUM(COALESCE(COGS,0))::FLOAT AS media_cost
	,0 AS impressions_count
	,0 AS clicks_count
    ,'USD' AS spend_currency
	,'PRODUCTCOST' AS spend_type
	,'FALSE' as IS_SPECIALTY_STORE
	,NULL as SPECIALTY_STORE
FROM reporting_media_base_prod.influencers.daily_spend inf
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = inf.store_id
WHERE INCLUDE_IN_CPA_REPORT_FLAG = 1
AND lower(POST_STATUS) = 'live'
AND (mens_flag <> 1 OR lower(store_region) <> 'na' OR (date < '2021-01-01' OR date >= '2024-01-01'))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,14,15,16,17;

-----------------------------------------------------------------Bliss Point



CREATE OR REPLACE TEMPORARY TABLE _blissPointMedia_spend AS
SELECT
    bs.datetime::DATE AS spend_date,
    ds.store_id  ,
    ds.store_brand || ds.store_country AS business_unit,
    ds.store_region AS store_region,
    IFF(bs.country is null, ds.store_country, bs.country) AS country,
    ds.store_country AS store_country,
    IFF(bs.channel = 'Audio', 'Testing', 'TV+Streaming') AS channel,
    'SFTP' AS source,
    IFF(bs.channel = 'Video', 'Streaming', bs.channel) as subchannel,
    'BlissPointMedia' AS vendor,
    'Prospecting' AS targeting,
    IFF(bs.business_unit = 'fableticsmen', 1, 0) as is_mens_flag,
    0 AS is_scrubs_flag,
    mp.spend_type,
    CASE mp.spend_type WHEN 'MEDIACOST' THEN bs.media_cost
	                    WHEN 'FEES' THEN bs.agency_fee + bs.ad_serving END AS spend,
    'USD' AS spend_currency,
    CASE mp.spend_type WHEN 'MEDIACOST' THEN bs.impressions
					     WHEN 'FEES' THEN 0 END AS impressions,
    0 AS clicks,
	'FALSE' as IS_SPECIALTY_STORE,
	NULL as SPECIALTY_STORE
FROM lake.blisspoint.vw_hourly_spend bs
JOIN edw_prod.data_model.dim_store ds
    ON IFF(lower(ds.store_full_name) = 'savage x us', 'savage x', lower(ds.store_full_name)) = lower(bs.store_reporting_name)
        LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_MEDIA_PARTNER_MAPPING mp ON mp.channel = IFF(bs.channel='Audio', 'Testing', bs.channel)
		AND mp.subchannel = IFF(bs.channel='Audio', 'Audio', bs.subchannel)
		AND mp.vendor = 'BlissPointMedia'
WHERE bs.Datetime::DATE >= '2010-01-01';



CREATE OR REPLACE TEMPORARY TABLE _blissPointMedia_spend_final AS
SELECT
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL(spend :: FLOAT, 0)) AS media_cost,
    SUM(IFNULL(CAST(Impressions AS FLOAT),0)) AS impressions_count,
    SUM(IFNULL(CAST(Clicks AS FLOAT),0)) AS clicks_count,
    spend_currency,
    spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _blissPointMedia_spend
GROUP BY
    business_unit,
    country,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    spend_type,
    spend_currency,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE;

---------------------------------------------------------------- Podcast Spend


CREATE OR REPLACE TEMPORARY TABLE _podcast_media_spend_final AS
SELECT s.store_brand || s.store_country AS business_unit,
	s.store_country AS country,
	d.full_date AS spend_date,
	s.store_id,
	'Radio/Podcast' AS channel,
    'Manual' AS source,
	'Other' AS subchannel,
	ps.media_vendor_name AS vendor,
    'Prospecting' AS targeting,
     0 as is_mens_flag,
     0 as is_scrubs_flag,
     SUM((1.00 * IFNULL(ps.spend :: FLOAT, 0)) / 14) AS media_cost,
     SUM((1.00 * IFNULL(Impressions::FLOAT,0)) / 14) AS Impressions_count,
     0 AS Clicks_count,
	'USD' AS spend_currency,
	'MEDIACOST' AS spend_type,
	'FALSE' as IS_SPECIALTY_STORE,
	NULL as SPECIALTY_STORE
FROM lake_view.sharepoint.med_podcast_spend ps
JOIN edw_prod.data_model.dim_store s ON ps.store_id = s.store_id
JOIN edw_prod.data_model.dim_date d ON d.full_date BETWEEN ps.air_date AND DATEADD(D,13,ps.air_date)
WHERE ps.never_aired is null
GROUP BY
    s.store_brand || s.store_country,
    s.store_country,
    spend_date,
    s.store_id,
    vendor;

---------------------------------------------------------------- Criteo Spend

CREATE OR REPLACE TEMPORARY TABLE _criteo_spend_final AS
SELECT
	ds.store_brand||ds.store_country AS business_unit,
    ds.store_country AS country,
    cs.date AS spend_date,
    ds.store_id,
    'Programmatic' AS channel,
    'Manual' AS source,
    'Criteo' AS subchannel,
    'Criteo' AS vendor,
    'Prospecting' AS targeting,
    0 as is_mens_flag,
    0 as is_scrubs_flag,
    SUM(cs.spend) AS media_cost,
    SUM(IFNULL(cs.impressions::decimal(18,4),0)) AS Impressions_count,
    SUM(IFNULL(cs.clicks::decimal(18,4),0)) AS Clicks_count,
    cs.currency_code AS spend_currency,
    'MEDIACOST' AS spend_type,
	'FALSE' as IS_SPECIALTY_STORE,
	NULL as SPECIALTY_STORE
FROM LAKE_VIEW.SHAREPOINT.MED_CRITEO_SPEND cs
JOIN edw_prod.data_model.dim_store ds
    ON lower(ds.store_full_name) = lower(cs.business_unit || ' ' || cs.country)
GROUP BY ds.store_brand||ds.store_country
	,ds.store_country
	,spend_date
	,store_id
	,spend_currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;


---------------------------------------------------------------- LiveIntent spend

CREATE OR REPLACE TEMPORARY TABLE _liveintent_spend_final AS
SELECT ds.store_brand || ds.store_country AS business_unit,
       ds.store_country AS country,
       ls.date AS spend_date,
       ds.store_id,
       'Testing' AS channel,
       'Manual' AS source,
       'LiveIntent' AS subchannel,
       'LiveIntent' AS vendor,
       'Prospecting' AS targeting,
       am.mens_account_flag as is_mens_flag,
       am.is_scrubs_flag,
       SUM(ls.spend) AS media_cost,
       SUM(ifnull(ls.impressions::decimal(18,4), 0)) AS impressions_count,
       SUM(ifnull(ls.clicks::decimal(18,4), 0)) AS clicks_count,
       am.currency AS spend_currency,
       'MEDIACOST' AS spend_type,
	   IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
       am.SPECIALTY_STORE
FROM LAKE_VIEW.SHAREPOINT.MED_LIVEINTENT_SPEND ls
JOIN lake_view.sharepoint.med_account_mapping_media am
  ON am.source_id = ls.id
      AND am.source ilike 'LiveIntent'
JOIN edw_prod.data_model.dim_store ds
ON ls.store_id = ds.store_id
GROUP BY  ds.store_brand || ds.store_country
	,ds.store_country
	,ls.date
	,ds.store_id
    ,is_mens_flag
    ,is_scrubs_flag
	,spend_currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;
------------------------------------------------------ Rokt spend

CREATE OR REPLACE TEMPORARY TABLE _rokt_spend AS
SELECT ds.store_brand || ds.store_country AS business_unit,
       ds.store_country AS country,
       ad.datestart AS spend_date,
       ds.store_id,
       ds.store_region AS store_region,
       'Programmatic' AS channel,
       'API' AS source,
       creativename,
       'ROKT' AS subchannel,
       'ROKT' AS vendor,
       'MEDIACOST' AS spend_type,
       'Prospecting' AS targeting,
       iff (ad.creativename ilike '%men%' and ad.creativename not ilike '%women%', 1, 0) as is_mens_flag,
       iff (ad.creativename ilike '%scrubs%', 1, 0) as is_scrubs_flag,
       grosscost AS spend,
       am.currency AS spend_currency,
       impressions AS impressions,
       referrals AS clicks,
       IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
       am.SPECIALTY_STORE
FROM lake.rokt.ads_spend ad
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = ad.accountid
    AND am.source ilike 'Rokt'
JOIN edw_prod.data_model.dim_store ds ON am.store_id = ds.store_id
WHERE ad.datestart >= $low_watermark_ltz;

CREATE OR REPLACE TEMPORARY TABLE _rokt_spend_final AS
select business_unit,
	   country ,
	   spend_date,
       store_id,
	   channel,
	   source,
	   subchannel,
	   Vendor,
	   targeting,
	   is_mens_flag,
	   is_scrubs_flag,
	   SUM(spend) AS media_cost,
	   SUM(IFNULL(Impressions::decimal(18,4),0)) AS Impressions_count,
	   SUM(IFNULL(Clicks::decimal(18,4),0)) AS Clicks_count,
	   spend_currency,
	   spend_type,
	   IS_SPECIALTY_STORE,
	   SPECIALTY_STORE
from _rokt_spend
GROUP BY  business_unit
	,country
    ,spend_date
    ,store_id
    ,channel
    ,source
    ,subchannel
    ,Vendor
    ,targeting
    ,is_mens_flag
    ,is_scrubs_flag
    ,spend_currency
    ,spend_type
    ,IS_SPECIALTY_STORE
    ,SPECIALTY_STORE;
------------------------------------------------------ Roku spend

CREATE OR REPLACE TEMPORARY TABLE _roku_spend AS
SELECT ds.store_brand || ds.store_country AS business_unit
	,ds.store_country AS country
	,rs.DATE AS spend_date
	,ds.store_id
	,ds.store_region AS store_region
    ,advertiser_uid
	,'TV+Streaming' AS channel
	,'API' AS source
	,'rokuctvstreaming' AS subchannel
	,'Roku' AS vendor
	,'Prospecting' AS targeting
	,am.mens_account_flag AS is_mens_flag
    ,am.is_scrubs_flag
	,rs.TOTAL_SPEND as spend
	,am.currency AS spend_currency
	,rs.impressions
	,rs.campaign_clicks AS clicks
	,IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE
    ,am.SPECIALTY_STORE
FROM lake.roku.daily_spend rs
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = rs.advertiser_uid
	AND am.source ilike 'Roku'
JOIN edw_prod.data_model.dim_store ds ON am.store_id = ds.store_id;

CREATE OR REPLACE TEMPORARY TABLE _roku_spend_final AS
SELECT business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,vendor
	,targeting
	,is_mens_flag
    ,is_scrubs_flag
	,SUM(spend) AS media_cost
	,SUM(ifnull(impressions::DECIMAL(18, 4), 0)) AS impressions_count
	,SUM(ifnull(clicks::DECIMAL(18, 4), 0)) AS clicks_count
	,spend_currency
	,'MEDIACOST' AS spend_type
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE
FROM _roku_spend
GROUP BY business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,vendor
	,targeting
	,is_mens_flag
	,is_scrubs_flag
	,spend_currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;

/****************************** Metric Theory Fee for Roku ************************************************/

CREATE OR REPLACE TEMPORARY TABLE _Roku_Metric_Theory_API_spend_final AS (
	SELECT business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,'Metric Theory' as vendor
	,targeting
	,is_mens_flag
    ,is_scrubs_flag
	,SUM(IFNULL(spend::FLOAT, 0)) * 0.08 AS media_cost
	,SUM(IFNULL(Impressions::FLOAT, 0)) AS Impressions_count
	,SUM(IFNULL(Clicks::FLOAT, 0)) AS Clicks_count
	,spend_currency
	,'FEES' AS spend_type
    ,IS_SPECIALTY_STORE
    ,SPECIALTY_STORE
  FROM _roku_spend
  WHERE advertiser_uid IN (
		'0DpzhTL6Q0'
		)
	AND spend_date >= '2022-01-01'
    AND spend_date < '2022-03-01'
  GROUP BY business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,Vendor
	,targeting
	,is_mens_flag
    ,is_scrubs_flag
	,spend_currency
    ,IS_SPECIALTY_STORE
    ,SPECIALTY_STORE
	);

------------------------------------------------------ Tatari spend

CREATE OR REPLACE TEMPORARY TABLE _tatari_spend AS
SELECT ds.store_brand || ds.store_country AS business_unit
	,ds.store_country AS country
	,ts.SPOT_DATETIME::DATE AS spend_date
	,ds.store_id
	,ds.store_region AS store_region
	,'TV+Streaming' AS channel
	,'SFTP' AS source
	,'TV' AS subchannel
	,'Tatari' AS vendor
	,'Prospecting' AS targeting
	,IFF(CREATIVE_NAME ilike '%FLM%',1,0) AS is_mens_flag
    ,IFF(CREATIVE_NAME ilike 'Scrubs%',1,0) AS is_scrubs_flag
	,ts.spend
	,am.currency AS spend_currency
	,ts.impressions
	,0 AS clicks
	,IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
am.SPECIALTY_STORE
FROM lake.tatari.linear_spend_and_impressions ts
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = ts.ACCOUNT_NAME
	AND am.source ILIKE 'Tatari'
JOIN edw_prod.data_model.dim_store ds ON am.store_id = ds.store_id
WHERE ts.creative_name NOT ILIKE '%brand%'

UNION ALL

SELECT ds.store_brand || ds.store_country AS business_unit
	,ds.store_country AS country
	,ts.DATE::DATE AS spend_date
	,ds.store_id
	,ds.store_region AS store_region
	,'TV+Streaming' AS channel
	,'SFTP' AS source
	,'Streaming' AS subchannel
	,'Tatari' AS vendor
	,'Prospecting' AS targeting
	,IFF(CREATIVE_NAME ilike '%FLM%',1,0) AS is_mens_flag
    ,IFF(CREATIVE_NAME ilike '%Scrubs%',1,0) AS is_scrubs_flag
	,ts.EFFECTIVE_SPEND AS spend
	,am.currency AS spend_currency
	,ts.impressions
	,0 AS clicks
	,IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
am.SPECIALTY_STORE
FROM lake.tatari.streaming_spend_and_impression ts
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = ts.ACCOUNT_NAME
	AND am.source ILIKE 'Tatari'
JOIN edw_prod.data_model.dim_store ds ON am.store_id = ds.store_id
WHERE ts.creative_name NOT ILIKE '%brand%';


CREATE OR REPLACE TEMPORARY TABLE _tatari_spend_final AS
SELECT business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,vendor
	,targeting
	,is_mens_flag
     ,is_scrubs_flag
	,SUM(spend) AS media_cost
	,SUM(impressions) AS impressions_count
	,SUM(ifnull(clicks::DECIMAL(18, 4), 0)) AS clicks_count
	,spend_currency
	,'MEDIACOST' AS spend_type
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE
FROM _tatari_spend
GROUP BY business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,vendor
	,targeting
	,is_mens_flag
	,is_scrubs_flag
	,spend_currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE

UNION ALL

SELECT business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,vendor
	,targeting
	,is_mens_flag
     ,is_scrubs_flag
	,SUM(IFNULL(spend::FLOAT, 0)) * 0.095 AS media_cost
	,0 AS impressions_count
	,0 AS clicks_count
	,spend_currency
	,'FEES' AS spend_type
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE
FROM _tatari_spend
GROUP BY business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,vendor
	,targeting
	,is_mens_flag
	,is_scrubs_flag
	,spend_currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;

-- Logic to replace incomplete yesterday data with day-before-yesterday data as placeholder

DELETE
FROM _tatari_spend_final
WHERE spend_date = DATEADD(DAY, -1, CURRENT_DATE());


INSERT INTO _tatari_spend_final
SELECT business_unit,
       country,
       DATEADD(DAY, -1, CURRENT_DATE()) AS spend_date,
       store_id,
       channel,
       source,
       subchannel,
       vendor,
       targeting,
       is_mens_flag,
       is_scrubs_flag,
       media_cost,
       impressions_count,
       clicks_count,
       spend_currency,
       spend_type,
       is_specialty_store,
       specialty_store
FROM _tatari_spend_final
WHERE spend_date = DATEADD(DAY, -2, CURRENT_DATE());


---------------------------------------------------------------- CJ Placement Cost

CREATE OR REPLACE TEMPORARY TABLE _cj_placement_cost_final AS
SELECT ds.store_brand || ds.store_country AS business_unit
	,ds.store_country AS country
	,cj.DATE AS spend_date
	,ds.store_id
	,'Affiliate' AS channel
	,'Manual' AS source
	,'Networks' AS subchannel
	,'CommissionJunction' AS vendor
	,'Prospecting' AS targeting
	,cj.is_mens_flag
    ,am.is_scrubs_flag
    ,SUM(IFNULL(cj.placement_cost::FLOAT, 0)) AS media_cost
	,0 AS Impressions_count
	,0 AS Clicks_count
	,am.currency AS spend_currency
    ,'FEES' AS spend_type
	,IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE
    ,am.SPECIALTY_STORE
FROM LAKE_VIEW.SHAREPOINT.MED_CJ_PLACEMENT_COSTS cj
JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = cj.id
	AND cj.store_id=am.store_id
	AND am.source ilike 'CJ Bonus'
JOIN edw_prod.data_model.dim_store ds ON cj.store_id = ds.store_id
GROUP BY ds.store_brand || ds.store_country
	,ds.store_country
	,cj.DATE
	,ds.store_id
	,is_mens_flag
    ,is_scrubs_flag
	,am.currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;

---------------------------------------------------------------- Reddit Spend

CREATE OR REPLACE TEMPORARY TABLE _reddit_spend_final AS
SELECT
	ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    rs.date AS spend_date,
    ds.store_id,
    'Reddit' AS channel,
    'Manual' AS source,
    'Reddit' AS subchannel,
    'Reddit' AS vendor,
    'Prospecting' AS targeting,
    am.mens_account_flag as is_mens_flag,
    am.is_scrubs_flag,
    SUM(rs.spend) AS media_cost,
    SUM(IFNULL(rs.impressions::decimal(18,4),0)) AS Impressions_count,
    SUM(IFNULL(rs.clicks::decimal(18,4),0)) AS Clicks_count,
    am.currency AS spend_currency,
    'MEDIACOST' AS spend_type,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM LAKE_VIEW.SHAREPOINT.MED_REDDIT_DAILY_SPEND rs
JOIN lake_view.sharepoint.med_account_mapping_media am
    ON am.source_id = rs.account_id
        AND am.source ilike 'Reddit'
JOIN edw_prod.data_model.dim_store ds
    ON ds.store_id = rs.store_id
GROUP BY  ds.store_brand || ds.store_country
	,ds.store_country
	,rs.date
	,ds.store_id
    ,is_mens_flag
    ,is_scrubs_flag
	,spend_currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;

---------------------------------------------------------------- twitter Spend

CREATE OR REPLACE TEMPORARY TABLE _twitter_spend_final AS
SELECT
	ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    rs.date AS spend_date,
    ds.store_id,
    'twitter' AS channel,
    'API' AS source,
    'twitter' AS subchannel,
    'twitter' AS vendor,
    'Prospecting' AS targeting,
    am.mens_account_flag as is_mens_flag,
    am.is_scrubs_flag,
    SUM(IFNULL(rs.spend::decimal(18,4),0)) AS media_cost,
    SUM(IFNULL(rs.impressions::decimal(18,4),0)) AS Impressions_count,
    SUM(IFNULL(rs.link_clicks::decimal(18,4),0)) AS Clicks_count,
    am.currency AS spend_currency,
    'MEDIACOST' AS spend_type,
	IFF(am.SPECIALTY_STORE is not null,'TRUE','FALSE') as IS_SPECIALTY_STORE,
    am.SPECIALTY_STORE
FROM lake_view.twitter.twitter_spend_and_conversion_metrics_by_promoted_tweet_id rs
JOIN lake_view.sharepoint.med_account_mapping_media am
    ON am.source_id = rs.account_id
        AND am.source ilike 'twitter'
JOIN edw_prod.data_model.dim_store ds
    ON ds.store_id = am.store_id
WHERE ((rs.date<'2022-09-01' OR rs.date>'2022-12-31') AND am.store_id=121) OR am.store_id!=121
GROUP BY  ds.store_brand || ds.store_country
	,ds.store_country
	,rs.date
	,ds.store_id
    ,is_mens_flag
    ,is_scrubs_flag
	,spend_currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;


---------------------------------------------------------------- Nextdoor Spend

CREATE OR REPLACE TEMPORARY TABLE _nextdoor_spend_final AS
SELECT ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    rs.date AS spend_date,
    ds.store_id,
    'Testing' AS channel,
    'Manual' AS source,
    'Nextdoor' AS subchannel,
    'Nextdoor' AS vendor,
    'Prospecting' AS targeting,
    am.mens_account_flag AS is_mens_flag,
    am.is_scrubs_flag,
    SUM(IFNULL(rs.spend::DECIMAL(18, 4), 0)) AS media_cost,
    SUM(IFNULL(rs.impressions, 0)) AS impressions_count,
    SUM(IFNULL(rs.clicks, 0)) AS clicks_count,
    am.currency AS spend_currency,
    'MEDIACOST' AS spend_type,
    IFF(am.specialty_store IS NOT NULL, 'TRUE', 'FALSE') AS is_specialty_store,
    am.specialty_store
FROM lake_view.sharepoint.med_nextdoor_spend rs
JOIN lake_view.sharepoint.med_account_mapping_media am
  ON am.source_id = rs.advertiser_id::VARCHAR
JOIN edw_prod.data_model.dim_store ds
  ON ds.store_id = rs.store_id
GROUP BY ds.store_brand || ds.store_country,
     ds.store_country,
     rs.date,
     ds.store_id,
     is_mens_flag,
     is_scrubs_flag,
     spend_currency,
     is_specialty_store,
     specialty_store;


---------------------------------------------------------------- Applovin Spend


CREATE OR REPLACE TEMPORARY TABLE _applovin_spend AS
SELECT
	ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    a.date AS spend_date,
    ds.store_id,
    ds.store_region AS store_region,
    'Programmatic' AS channel,
    'API' AS source,
    'applovin' AS subchannel,
    'applovin' AS vendor,
    'Prospecting' AS targeting,
    am.mens_account_flag as is_mens_flag,
    am.is_scrubs_flag,
    a.cost,
    am.currency AS spend_currency,
    a.impressions,
    a.clicks,
	IFF(am.specialty_store is not null,'TRUE','FALSE') AS is_specialty_store,
    am.specialty_store
FROM lake.media.applovin_spend a
JOIN lake_view.sharepoint.med_account_mapping_media am
    ON am.source_id = a.advertiser_id
        AND lower(am.source) ilike 'applovin'
JOIN edw_prod.data_model.dim_store ds
    ON ds.store_id = am.store_id
WHERE a.date >= $low_watermark_ltz;


CREATE OR REPLACE TEMPORARY TABLE _applovin_spend_final AS
SELECT
    business_unit,
    country ,
    spend_date,
    store_id,
    channel,
    source,
    subchannel,
    Vendor,
    targeting,
    is_mens_flag,
    is_scrubs_flag,
    SUM(IFNULL(cost::decimal(18,4),0)) AS media_cost,
    SUM(IFNULL(Impressions::decimal(18,4),0)) AS impressions_count,
    SUM(IFNULL(Clicks::decimal(18,4),0)) AS clicks_count,
    spend_currency,
    'MEDIACOST' AS spend_type,
	IS_SPECIALTY_STORE,
	SPECIALTY_STORE
FROM _applovin_spend
GROUP BY  business_unit
	,country
	,spend_date
	,store_id
	,channel
	,source
	,subchannel
	,Vendor
    ,targeting
    ,is_mens_flag
    ,is_scrubs_flag
	,spend_currency
	,IS_SPECIALTY_STORE
	,SPECIALTY_STORE;


---------------------------------------------------------------- Nift Spend

CREATE OR REPLACE TEMPORARY TABLE _nift_spend_final AS
SELECT  ds.store_brand || ds.store_country AS business_unit,
        ds.store_country AS country,
        n.date AS spend_date,
        ds.store_id,
        'Programmatic' AS channel,
        'API' AS source,
        'Nift' AS subchannel,
        'Nift' AS vendor,
        'Prospecting' AS targeting,
        case when n.gift = 'MEN' then 1
            else 0 end as is_mens_flag,
        case when n.gift = 'SCRUBS' then 1
            else 0 end as is_scrubs_flag,
        SUM(IFNULL((REPLACE(REPLACE(n.spend, '$', ''), ',', ''))::DECIMAL(18, 4), 0)) AS media_cost,
        0 AS impressions_count,
        SUM(IFNULL(n.selections, 0)) AS clicks_count,
        ds.store_currency AS spend_currency,
        'MEDIACOST' AS spend_type,
        'FALSE' AS is_specialty_store,
        null AS specialty_store
FROM lake.nift.daily_spend n
JOIN edw_prod.data_model.dim_store ds
    ON ds.store_id = '52'
WHERE n.date >= $low_watermark_ltz
GROUP BY business_unit,
         country,
         spend_date,
         ds.store_id,
         is_mens_flag,
         is_scrubs_flag,
         spend_currency,
         is_specialty_store,
         specialty_store;

---------------------------------------------------------------- Claim Spend

CREATE OR REPLACE TEMPORARY TABLE _claim_spend_final AS
SELECT business_unit,
    country,
    cs.date AS spend_date,
    cs.store_id as store_id,
    'Testing' AS channel,
    'Manual' AS source,
    'Claim' AS subchannel,
    'Claim' AS vendor,
    'Prospecting' AS targeting,
    cs.mens_flag AS is_mens_flag,
    cs.scrubs_flag AS is_scrubs_flag,
    SUM(IFNULL(cs.spend::DECIMAL(18, 4), 0)) AS media_cost,
    SUM(IFNULL(cs.impressions, 0)) AS impressions_count,
    SUM(IFNULL(cs.clicks, 0)) AS clicks_count,
    ds.store_currency AS spend_currency,
    'MEDIACOST' AS spend_type,
    'FALSE' as is_specialty_store,
    NULL as specialty_store
FROM LAKE_VIEW.SHAREPOINT.MED_CLAIM_SPEND cs
JOIN edw_prod.data_model.dim_store ds
  ON ds.store_id = cs.store_id
GROUP BY business_unit,
    country,
    spend_date,
    cs.store_id,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
    is_specialty_store,
    specialty_store;

---------------------------------------------------------------- LinkedIn Spend

CREATE OR REPLACE TEMPORARY TABLE _linkedin_spend_final AS
SELECT business_unit,
    country,
    ls.date AS spend_date,
    ls.store_id as store_id,
    'Programmatic' AS channel,
    'Manual' AS source,
    'LinkedIn' AS subchannel,
    'LinkedIn' AS vendor,
    'Prospecting' AS targeting,
    ls.mens_flag AS is_mens_flag,
    ls.scrubs_flag AS is_scrubs_flag,
    SUM(IFNULL(ls.spend::DECIMAL(18, 4), 0)) AS media_cost,
    SUM(IFNULL(ls.impressions, 0)) AS impressions_count,
    SUM(IFNULL(ls.clicks, 0)) AS clicks_count,
    ds.store_currency AS spend_currency,
    'MEDIACOST' AS spend_type,
    'FALSE' as is_specialty_store,
    NULL as specialty_store
FROM LAKE_VIEW.SHAREPOINT.MED_LINKEDIN_SPEND ls
JOIN edw_prod.data_model.dim_store ds
  ON ds.store_id = ls.store_id
GROUP BY business_unit,
    country,
    spend_date,
    ls.store_id,
    is_mens_flag,
    is_scrubs_flag,
    spend_currency,
    is_specialty_store,
    specialty_store;

---------------------------------------------------------------- Amazon Subchannels Spend

CREATE OR REPLACE TEMPORARY TABLE _amazon_imdb_spend_final AS
SELECT  business_unit,
        country,
        ns.date AS spend_date,
        ns.store_id,
        channel,
        'Manual' AS source,
        subchannel,
        vendor,
        'Prospecting' AS targeting,
        ns.mens_flag AS is_mens_flag,
        ns.scrubs_flag AS is_scrubs_flag,
        ns.spend AS media_cost,
        ns.impressions AS impressions_count,
        ns.clicks AS clicks_count,
        ds.STORE_CURRENCY AS spend_currency,
        'MEDIACOST' AS spend_type,
        'FALSE' as is_specialty_store,
        NULL as specialty_store
FROM lake_view.sharepoint.amazon_imdb_spend ns
JOIN edw_prod.data_model.dim_store ds
  ON ds.store_id = ns.store_id;


CREATE OR REPLACE TEMPORARY TABLE _amazon_twitch_spend_final AS
SELECT  business_unit,
        country,
        ns.date AS spend_date,
        ns.store_id,
        channel,
        'Manual' AS source,
        subchannel,
        vendor,
        'Prospecting' AS targeting,
        ns.mens_flag AS is_mens_flag,
        ns.scrubs_flag AS is_scrubs_flag,
        ns.spend AS media_cost,
        ns.impressions AS impressions_count,
        ns.clicks AS clicks_count,
        ds.STORE_CURRENCY AS spend_currency,
        'MEDIACOST' AS spend_type,
        'FALSE' as is_specialty_store,
        NULL as specialty_store
FROM lake_view.sharepoint.amazon_twitch_spend ns
JOIN edw_prod.data_model.dim_store ds
  ON ds.store_id = ns.store_id;


CREATE OR REPLACE TEMPORARY TABLE _amazon_stv_spend_final AS
SELECT  business_unit,
        country,
        ns.date AS spend_date,
        ns.store_id,
        channel,
        'Manual' AS source,
        subchannel,
        vendor,
        'Prospecting' AS targeting,
        ns.mens_flag AS is_mens_flag,
        ns.scrubs_flag AS is_scrubs_flag,
        ns.spend AS media_cost,
        ns.impressions AS impressions_count,
        ns.clicks AS clicks_count,
        ds.STORE_CURRENCY AS spend_currency,
        'MEDIACOST' AS spend_type,
        'FALSE' as is_specialty_store,
        NULL as specialty_store
FROM lake_view.sharepoint.amazon_stv_spend ns
JOIN edw_prod.data_model.dim_store ds
  ON ds.store_id = ns.store_id;

-----------------------------------------------------------------Manual Spend logic


CREATE OR REPLACE TEMPORARY TABLE _umt_media_partner_costs AS
select
  spend_method_code,
  spend_spread,
  effective_start_date,
  effective_end_date,
  store_brand_name as store_brand,
  store_country_code,
  CASE
    WHEN lower(media_channel_name) = 'social' and lower(media_subchannel_name) like '%pinterest%' THEN 'Pinterest'
    WHEN lower(media_channel_name) = 'social' and lower(media_subchannel_name) = 'twitter' THEN 'FB+IG'
  END AS media_channel_name,
  CASE
    WHEN lower(media_channel_name) = 'social' and lower(media_subchannel_name) like '%pinterest%' THEN 'Pinterest'
    WHEN lower(media_channel_name) = 'social' and lower(media_subchannel_name) = 'twitter' THEN 'Other'
  END AS media_subchannel_name,
  media_vendor_name,
  AUDIENCE_TARGETING_TYPE_NAME AS targeting,
  0 as is_mens_flag,
  0 as is_scrubs_flag,
  spend_type_code,
  spend_currency_code,
  spend_amt,
  impressions_count,
  clicks_count,
  'FALSE' as IS_SPECIALTY_STORE,
  NULL as SPECIALTY_STORE
from lake.media.historical_spend

union all

select
  'Manual' AS spend_method_code,
  'Even' AS spend_spread,
  date AS effective_start_date,
  date AS effective_end_date,
  business_unit AS store_brand,
  country AS store_country_code,
  'Programmatic' AS media_channel_name,
  'Content/Native' AS media_subchannel_name,
  'Powerspace' AS media_vendor_name,
  'Prospecting' AS targeting,
   0 as is_mens_flag,
    0 as is_scrubs_flag,
  'MEDIACOST' AS spend_type_code,
  currency AS spend_currency_code,
  spend AS spend_amt,
  impressions AS impressions_count,
  clicks AS clicks_count,
  'FALSE' as IS_SPECIALTY_STORE,
  NULL as SPECIALTY_STORE
from lake_view.sharepoint.med_powerspace_spend

union all

select
  'Manual' AS spend_method_code,
  'Even' AS spend_spread,
  date AS effective_start_date,
  date AS effective_end_date,
  business_unit AS store_brand,
  country AS store_country_code,
  'TV+Streaming' AS media_channel_name,
  'TV' AS media_subchannel_name,
  vendor AS media_vendor_name,
  'Prospecting' AS targeting,
   is_mens_flag::int as is_mens_flag,
    0 as is_scrubs_flag,
  'MEDIACOST' AS spend_type_code,
  currency AS spend_currency_code,
  spend AS spend_amt,
  impressions AS impressions_count,
  0 AS clicks_count,
  'FALSE' as IS_SPECIALTY_STORE,
  NULL as SPECIALTY_STORE
from LAKE_VIEW.SHAREPOINT.MED_EU_TV_SPEND;


CREATE OR REPLACE TEMPORARY TABLE _manual_spend AS
SELECT temp.*
FROM
(
SELECT
  ms.store_brand||ms.store_country_code AS business_unit,
  ms.store_country_code AS country,
  ms.effective_start_date AS date,
  ds.store_id,
  ms.media_channel_name AS channel,
  CASE WHEN ms.spend_amt < 0 THEN 'CREDIT'
       ELSE 'Manual'
  END AS source,
  ms.media_subchannel_name AS subchannel,
  ms.media_vendor_name AS vendor,
  ms.targeting,
  ms.is_mens_flag,
  ms.is_scrubs_flag,
  ms.spend_currency_code AS currency,
  ms.spend_type_code AS spend_type,
  SUM(IFNULL(ms.spend_amt :: float, 0)) AS media_cost,
  SUM(IFNULL(ms.impressions_count :: float, 0)) AS impressions_count,
  SUM(IFNULL(ms.clicks_count :: float, 0)) AS clicks_count,
  ms.IS_SPECIALTY_STORE,
  ms.SPECIALTY_STORE
FROM _umt_media_partner_costs ms
JOIN edw_prod.data_model.dim_store ds ON lower(ds.store_full_name) = lower(TRIM(ms.store_brand)||' '||TRIM(ms.store_country_code))
WHERE spend_method_code = 'Manual'
	AND media_channel_name NOT IN ('Influencers' ,'Ambassadors')
GROUP BY
    ms.store_brand||ms.store_country_code,
    ms.store_country_code,
    ms.effective_start_date,
    ds.store_id,
    ms.media_channel_name,
    ms.media_subchannel_name,
    ms.media_vendor_name,
    ms.targeting,
    ms.is_mens_flag,
    ms.is_scrubs_flag,
    ms.spend_currency_code,
    ms.spend_type_code,
    CASE WHEN ms.spend_amt < 0 THEN 'CREDIT'
					   ELSE 'Manual'
				  END,
	ms.IS_SPECIALTY_STORE,
	ms.SPECIALTY_STORE
) temp
LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_MEDIA_PARTNER_MAPPING mp ON mp.channel = temp.channel
		AND mp.subchannel = temp.subchannel
		AND mp.vendor = temp.vendor
		AND mp.spend_type = REPLACE(temp.spend_type,' ','')
		--AND mp.meta_is_current = 1
//		AND temp.date BETWEEN mp.meta_effective_start_datetime
//		AND mp.meta_effective_end_datetime
WHERE temp.media_cost != 0;
-- 	AND temp.date >= DATEADD(DAY,-10,$low_watermark_ltz);

-----------------------------------------------------------------Final

CREATE OR REPLACE TEMPORARY TABLE _media_cost(
	business_unit varchar,
	country varchar,
	spend_date date,
	store_id NUMBER,
	channel varchar,
	source varchar,
	subchannel varchar,
	Vendor varchar,
    targeting varchar,
    is_mens_flag int,
    is_scrubs_flag int,
	media_cost decimal(38, 4),
	Impressions_count decimal(38, 4),
	Clicks_count decimal(38, 4),
	spend_currency varchar,
	spend_type varchar,
	IS_SPECIALTY_STORE boolean,
	SPECIALTY_STORE varchar
);


INSERT INTO _media_cost(
SELECT * FROM _facebook_spend_final
UNION ALL
SELECT * from _facebook_pixis_fees_Final
UNION ALL
SELECT * from _facebook_kargo_fees_final
UNION ALL
SELECT * FROM _tubescience_spend_final
UNION ALL
SELECT * FROM _google_doubleclick_spend_final
UNION ALL
SELECT * FROM _Impact_Radius_Affiliate_Accelerate_spend_final
UNION ALL
SELECT * FROM _Impact_Radius_Affiliate_API_spend_final
UNION ALL
SELECT * FROM _Snapchat_API_spend_final
UNION ALL
SELECT * FROM _snapchat_kargo_fees_final
UNION ALL
SELECT * FROM _Impact_Radius_Influencer_API_spend_final
UNION ALL
SELECT * FROM _youtube_api_spend_final
UNION ALL
SELECT * FROM _Outbrain_Flat_File_Import_spend_final
UNION ALL
SELECT * FROM _Taboola_Flat_File_Import_spend_final
UNION ALL
SELECT * FROM _googleanalytics_clicklab_spend_final
UNION ALL
SELECT * FROM _Horizon_Flat_File_Import_spend_final
UNION ALL
SELECT * FROM _Tradedesk_API_spend_final
UNION ALL
SELECT * FROM _Fixed_CPL_CPA_Partners_spend_final
UNION ALL
SELECT * FROM _shopzilla_connexity_spend_final
UNION ALL
SELECT * FROM _pinterest_api_spend_ad1_final
UNION ALL
SELECT * FROM _smartly_spend_for_pinterest
UNION ALL
SELECT * FROM _narrative_and_within_fees
UNION ALL
SELECT * FROM _motivemetrics_spend_final
UNION ALL
SELECT * FROM _metrictheory_spend_for_google
UNION ALL
SELECT * FROM _msa_spend
UNION ALL
SELECT * FROM _podcast_media_spend_final
UNION ALL
SELECT * FROM _criteo_spend_final
UNION ALL
SELECT * FROM _tiktok_spend_final
UNION ALL
SELECT * FROM _reddit_spend_final
UNION ALL
SELECT * FROM _twitter_spend_final
UNION ALL
SELECT * FROM _influencer_spend_final
UNION ALL
SELECT * FROM _blissPointMedia_spend_final
UNION ALL
SELECT * FROM _fixed_cost_spend_final
UNION ALL
SELECT * FROM _liveintent_spend_final
UNION ALL
SELECT * FROM _tatari_spend_final
UNION ALL
SELECT * FROM _commissionjunction_spend_final
UNION ALL
SELECT * FROM _cj_placement_cost_final
UNION ALL
SELECT * FROM _Tradedesk_Metric_Theory_API_spend_final
UNION ALL
SELECT * FROM _roku_spend_final
UNION ALL
SELECT * FROM _rokt_spend_final
UNION ALL
SELECT * FROM _sapphire_spend_final
UNION ALL
SELECT * FROM _Roku_Metric_Theory_API_spend_final
UNION ALL
SELECT * FROM _First_media_final
UNION ALL
SELECT * FROM _Peoplehype_1180_spend_final
UNION ALL
SELECT * FROM _schema_spend_final
UNION ALL
SELECT * FROM _gassed_fee_for_google_ads
UNION ALL
SELECT * FROM _nextdoor_spend_final
UNION ALL
SELECT * FROM _applovin_spend_final
UNION ALL
SELECT * FROM _nift_spend_final
UNION ALL
SELECT * FROM _claim_spend_final
UNION ALL
SELECT * FROM _linkedin_spend_final
UNION ALL
SELECT * FROM _amazon_imdb_spend_final
UNION ALL
SELECT * FROM _amazon_twitch_spend_final
UNION ALL
SELECT * FROM _amazon_stv_spend_final
);


---------------------------------------------- Reprocess Youtube Fees from the low_watermark i.e, for past 90 days

DELETE
    FROM reporting_media_prod.dbo.fact_media_cost
WHERE channel = 'Youtube'
    AND subchannel = 'Youtube'
    AND vendor in ('Narrative', 'Agencywithin')
    AND media_cost_date >= $low_watermark_ltz;


---------------------------------------------- Full Reprocess the Gsheets data

DELETE
FROM reporting_media_prod.dbo.fact_media_cost
WHERE source in ('Manual', 'Fixed_Cost');

-----------------------------------------------End of logic to Full reprocess Gsheet data


CREATE OR REPLACE TEMPORARY TABLE _media_cost_last_90 AS
SELECT
	ds.store_brand || ds.store_country AS business_unit,
    ds.store_country AS country,
    fmc.media_cost_date AS spend_date,
    ds.store_id,
    fmc.channel,
    fmc.source,
    fmc.subchannel,
    fmc.Vendor,
    fmc.targeting,
    fmc.is_mens_flag,
    fmc.is_scrubs_flag,
    0 AS media_cost,
    0 AS Impressions_count,
    0 AS Clicks_count,
    dc.iso_currency_code AS spend_currency,
    fmc.spend_type,
	tmc.IS_SPECIALTY_STORE,
	tmc.SPECIALTY_STORE
FROM reporting_media_prod.dbo.fact_media_cost fmc
JOIN edw_prod.data_model.dim_store ds ON ds.store_id=fmc.store_id
	AND ds.store_type <> 'Mobile App'
JOIN edw_prod.data_model.dim_currency dc
	ON dc.iso_currency_code = fmc.spend_iso_currency_code
LEFT JOIN _media_cost tmc
	ON fmc.media_cost_date::DATE = tmc.spend_date::DATE
	AND fmc.channel = tmc.channel
	AND fmc.vendor = tmc.vendor
	AND fmc.subchannel = tmc.subchannel
	AND fmc.spend_type = tmc.spend_type
	AND fmc.source = tmc.SOURCE
WHERE fmc.media_cost_date >= $low_watermark_ltz
	AND (tmc.channel IS NULL OR tmc.subchannel IS NULL OR tmc.vendor IS NULL OR tmc.source IS NULL);


------------------------------------------------------------------Temp Fact_media_cost

CREATE OR REPLACE TEMPORARY TABLE _fact_media_cost AS
SELECT tmc.business_unit,
    tmc.country ,
    tmc.spend_date,
    tmc.store_id,
    tmc.channel,
    CASE WHEN IFNULL(ms.media_cost,0) >=0 AND ((tmc.media_cost-ms.media_cost)/NULLIF(tmc.media_cost, 0))*100 > 3 THEN 'ADJUSTMENT' ELSE tmc.source END AS source,
    tmc.subchannel,
    tmc.Vendor,
    tmc.targeting,
    tmc.is_mens_flag,
    tmc.is_scrubs_flag,
    tmc.spend_currency,
    tmc.spend_type,
    CASE WHEN IFNULL(ms.media_cost,0) >=0 AND tmc.media_cost > 0 AND ((ms.media_cost-tmc.media_cost)/NULLIF(ms.media_cost,0))*100 > 3 THEN ms.media_cost ELSE tmc.media_cost END AS media_cost,
    CASE WHEN IFNULL(ms.media_cost,0) >=0 AND tmc.media_cost > 0 AND ((ms.media_cost-tmc.media_cost)/NULLIF(ms.media_cost,0))*100 > 3 THEN ms.Impressions_count ELSE tmc.Impressions_count END AS impressions_count,
    CASE WHEN IFNULL(ms.media_cost,0) >=0 AND tmc.media_cost > 0 AND ((ms.media_cost-tmc.media_cost)/NULLIF(ms.media_cost,0))*100 > 3 THEN ms.Clicks_count ELSE tmc.Clicks_count END AS clicks_count,
	tmc.IS_SPECIALTY_STORE,
	tmc.SPECIALTY_STORE
FROM _media_cost tmc
LEFT JOIN _manual_spend ms ON tmc.business_unit = ms.business_unit
	AND tmc.spend_date = ms.date
	AND tmc.channel = ms.channel
	AND tmc.subchannel = ms.subchannel
	AND tmc.vendor = ms.vendor
	AND tmc.spend_type = ms.spend_type

UNION

SELECT
    ms.business_unit,
    ms.country ,
    ms.date,
    ms.store_id,
    ms.channel,
    ms.source,
    ms.subchannel,
    ms.Vendor,
    ms.targeting,
    ms.is_mens_flag,
    ms.is_scrubs_flag,
    ms.currency,
    ms.spend_type,
    ms.media_cost,
    ms.Impressions_count,
    ms.Clicks_count,
	ms.IS_SPECIALTY_STORE,
	ms.SPECIALTY_STORE
FROM _manual_spend ms
LEFT JOIN _media_cost tmc ON tmc.business_unit = ms.business_unit
	AND tmc.spend_date = ms.date
	AND tmc.channel = ms.channel
	AND tmc.subchannel = ms.subchannel
	AND tmc.vendor = ms.vendor
	AND tmc.spend_type = ms.spend_type
WHERE tmc.spend_date IS NULL

UNION

SELECT
    mcl.business_unit,
    mcl.country ,
    mcl.spend_date,
    mcl.store_id,
    mcl.channel,
    mcl.source,
    mcl.subchannel,
    mcl.Vendor,
    mcl.targeting,
    mcl.is_mens_flag,
    mcl.is_scrubs_flag,
    mcl.spend_currency,
    mcl.spend_type,
    mcl.media_cost,
    mcl.Impressions_count,
    mcl.Clicks_count,
	mcl.IS_SPECIALTY_STORE,
	mcl.SPECIALTY_STORE
FROM _media_cost_last_90 mcl
LEFT JOIN _manual_spend ms ON mcl.spend_date = ms.date
	AND mcl.channel = ms.channel
	AND mcl.subchannel = ms.subchannel
	AND mcl.vendor = ms.vendor
	AND mcl.source = ms.source
WHERE ms.date IS NULL;



CREATE OR REPLACE TEMPORARY TABLE _stg_fact_media_cost AS
SELECT	fmc.spend_date,
    fmc.store_id,
    fmc.channel,
    fmc.subchannel,
    fmc.Vendor,
    fmc.targeting,
    fmc.is_mens_flag,
    fmc.is_scrubs_flag,
    fmc.source,
    fmc.spend_currency AS spend_iso_currency_code,
    ds.store_currency AS store_iso_currency_code,
    fmc.spend_type,
    fmc.media_cost,
    fmc.Impressions_count,
    fmc.Clicks_count,
    CASE WHEN fmc.source = 'CPL/CPA' THEN 'fixed'
          ELSE 'variable'
    END AS Type,
    COALESCE(order_er.exchange_rate,1) AS spend_date_USD_conv_rate,
    COALESCE(eur_order_er.exchange_rate,1) AS spend_date_EUR_conv_rate,
    CASE WHEN  fmc.spend_currency = ds.store_currency THEN 1
         ELSE loc_curr_con.exchange_rate
    END AS local_store_curr_conv_rate,
	fmc.IS_SPECIALTY_STORE,
    fmc.SPECIALTY_STORE
FROM _fact_media_cost  fmc
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = fmc.store_id
  AND ds.store_type <> 'Mobile App'
LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date order_er ON order_er.rate_date_pst = DATEADD(DAY,-1,fmc.spend_date)
  AND fmc.spend_currency = order_er.src_currency
  AND order_er.dest_currency = 'USD'
LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date eur_order_er ON eur_order_er.rate_date_pst = DATEADD(DAY,-1,fmc.spend_date)
  AND fmc.spend_currency = eur_order_er.src_currency
  AND eur_order_er.dest_currency = 'EUR'
LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date loc_curr_con ON loc_curr_con.rate_date_pst = DATEADD(DAY,-1,fmc.spend_date)
  AND fmc.spend_currency = loc_curr_con.src_currency
  AND loc_curr_con.dest_currency = ds.store_currency;


MERGE INTO reporting_media_prod.dbo.fact_media_cost fmc
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY spend_date, store_id, source, channel, subchannel, vendor, targeting, IS_SPECIALTY_STORE,SPECIALTY_STORE,
                is_mens_flag, is_scrubs_flag,spend_type, spend_iso_currency_code, store_iso_currency_code
                ORDER BY coalesce(media_cost, Impressions_count, Clicks_count, '0') DESC ) AS rn
        FROM _stg_fact_media_cost
     ) a
    WHERE a.rn = 1
) s ON      equal_null(fmc.media_cost_date, s.spend_date)
        AND equal_null(fmc.store_id, s.store_id)
        AND equal_null(fmc.IS_SPECIALTY_STORE, s.IS_SPECIALTY_STORE)
        AND equal_null(fmc.SPECIALTY_STORE, s.SPECIALTY_STORE)
        AND equal_null(fmc.source, s.source)
        AND equal_null(fmc.channel, s.channel)
        AND equal_null(fmc.subchannel, s.subchannel)
        AND equal_null(fmc.vendor, s.vendor)
        AND equal_null(fmc.targeting, s.targeting)
        AND equal_null(fmc.is_mens_flag, s.is_mens_flag)
        AND equal_null(fmc.is_scrubs_flag, s.is_scrubs_flag)
        AND equal_null(fmc.spend_type, s.spend_type)
        AND equal_null(fmc.spend_iso_currency_code, s.spend_iso_currency_code)
        AND equal_null(fmc.store_iso_currency_code, s.store_iso_currency_code)
WHEN NOT MATCHED THEN INSERT (
    media_cost_date, store_id, IS_SPECIALTY_STORE, SPECIALTY_STORE, channel, subchannel, vendor, targeting, is_mens_flag, is_scrubs_flag,
    source, spend_type, spend_iso_currency_code, store_iso_currency_code, cost,
    impressions, clicks, type, spend_date_usd_conv_rate,
    spend_date_eur_conv_rate, local_store_conv_rate, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    spend_date, store_id, IS_SPECIALTY_STORE, SPECIALTY_STORE, channel, subchannel, vendor, targeting, is_mens_flag, is_scrubs_flag,
    source, spend_type, spend_iso_currency_code, store_iso_currency_code, media_cost,
    Impressions_count, Clicks_count, type, spend_date_usd_conv_rate,
    spend_date_eur_conv_rate, local_store_curr_conv_rate, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND fmc.meta_row_hash != s.meta_row_hash THEN UPDATE
SET fmc.media_cost_date = s.spend_date,
    fmc.store_id = s.store_id,
    fmc.channel = s.channel,
    fmc.subchannel = s.subchannel,
    fmc.vendor = s.vendor,
    fmc.targeting = s.targeting,
    fmc.is_mens_flag = s.is_mens_flag,
    fmc.is_scrubs_flag = s.is_scrubs_flag,
    fmc.source = s.source,
    fmc.spend_type = s.spend_type,
    fmc.spend_iso_currency_code = s.spend_iso_currency_code,
    fmc.store_iso_currency_code = s.store_iso_currency_code,
    fmc.cost = s.media_cost,
    fmc.impressions = s.Impressions_count,
    fmc.clicks = s.Clicks_count,
    fmc.type = s.type,
    fmc.spend_date_usd_conv_rate = s.spend_date_usd_conv_rate,
    fmc.spend_date_eur_conv_rate = s.spend_date_eur_conv_rate,
    fmc.local_store_conv_rate = s.local_store_curr_conv_rate,
	fmc.IS_SPECIALTY_STORE = s.IS_SPECIALTY_STORE,
    fmc.SPECIALTY_STORE = s.SPECIALTY_STORE,
    fmc.META_ROW_HASH = s.META_ROW_HASH,
    fmc.meta_update_datetime = s.meta_update_datetime
;
