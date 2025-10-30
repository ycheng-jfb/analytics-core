truncate table EDW_PROD.NEW_STG.all_channel_optimization;
-- facebook
insert into EDW_PROD.NEW_STG.all_channel_optimization
select
    'fb+ig' AS channel,
    null as SUBCHANNEL ,
    null as VENDOR ,
    'fb+ig' as channel_optimization,
    'fb+ig' as channel_cpa_by_lead,
    null as MEMBER_SEGMENT,
    null as BIDDING_OBJECTIVE,

    cast(a.account_id as varchar) as account_id,
    a.account_name,
    cast(a.campaign_id as varchar) as campaign_id,
    a.campaign_name,
    null as ADGROUP_NAME,
    null as ADGROUP_ID,
    a.ad_name,
    cast(a.ad_id as varchar) as ad_id,
    null as AD_IS_ACTIVE_FLAG,
    null as ADGROUP_IS_ACTIVE_FLAG,
    null as OFFER,
    null as INFLUENCER_NAME,
    ds.store_brand as store_brand_name,
    ds.store_brand_abbr,
    ds.store_country as country,
    ds.store_region as region,
    cast(ds.store_id as integer) as store_id,
    null as CREATIVE_CODE,
    null as IMAGE_URL,
    null as LINK_TO_POST,
    a.date_day as date,
    sum(a.spend * coalesce(am.CURRENCY, 1)) as spend_usd,
    sum(a.spend * coalesce(am.CURRENCY, 1)) as spend_with_vendor_fees_usd,
    sum(a.spend) as spend_account_currency,
    sum(a.spend) as spend_with_vendor_fees_account_currency,
    sum(a.impressions) as impressions,
    sum(a.clicks) as clicks,
    null as VIDEO_VIEWS,
    null as PIXEL_LEAD_CLICK_1D,
    null as PIXEL_LEAD_VIEW_1D,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    null as PIXEL_LEAD_CLICK_7D,
    null as PIXEL_LEAD_VIEW_7D,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,
    case when ds.store_brand in ('Savage X','Fabletics Men') and ds.store_region = 'NA' and a.date_day <= '2020-09-01'
        then 0 else 0 end as pixel_vip_click_1d,
    case when ds.store_brand in ('Savage X','Fabletics Men') and ds.store_region = 'NA' and a.date_day <= '2020-09-01'
        then 0 else 0 end as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    case when ds.store_brand in ('Savage X','Fabletics Men') and ds.store_region = 'NA' and a.date_day <= '2020-09-01'
        then 0 else 0 end as pixel_vip_click_7d,
    case when ds.store_brand in ('Savage X','Fabletics Men') and ds.store_region = 'NA' and a.date_day <= '2020-09-01'
        then 0 else 0 end as pixel_vip_view_7d,
    0 as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1,
    null as UI_OPTIMIZATION_PIXEL_LEAD,
    null as UI_OPTIMIZATION_PIXEL_VIP,
    null as UI_OPTIMIZATION_WINDOW,
    null as RS_SPEND_USD,
    null as RS_SPEND_WITH_VENDOR_FEES_USD,
    null as RS_SPEND_ACCOUNT_CURRENCY,
    null as RS_SPEND_WITH_VENDOR_FEES_ACCOUNT_CURRENCY,
    null as UI_CAC_TARGET,
    null as DESIGNER_INITIALS,
    null as DATE_CREATED,
    null as VERSION,
    null as AD_TYPE,
    null as VIDEO_LENGTH,
    null as AUDIO,
    null as FIRST_3_SECONDS_TEXT,
    null as FIRST_3_SECONDS_VISUAL,
    null as MODEL_ACTION,
    null as MODEL_PLACEMENT,
    null as BODY_TYPE,
    null as GENDER,
    null as STILL_LIFE_ANGLE_SHOT,
    null as INFLUENCER,
    null as UGC_TALENT_NAME,
    null as EGC_TALENT_NAME,
    null as TRELLO_ID,
    null as ASSET_TYPE,
    null as ASSET_SIZE,
    null as CREATIVE_CONCEPT,
    null as TEST_NAME,
    null as PLATFORM,
    null as SKU1,
    null as SKU2,
    null as SKU3,
    null as SKU4,
    null as SKU5,
    null as MERCHANDISE_TYPE,
    null as NUMBER_OF_STYLES_SHOWN,
    null as LOGO,
    null as OFFER_FORMAT,
    null as OFFER_PLACEMENT,
    null as MESSAGING_THEME,
    null as PRODUCT_TYPE,
    null as DATA_LAST_UPDATED_DATETIME
from LAKE_MMOS.FACEBOOK_ADS_FACEBOOK_ADS.FACEBOOK_ADS__AD_REPORT a
left join lake_view.sharepoint.med_account_mapping_media am
    on a.account_id = am.source_id
    and am.source = 'Facebook'
    and am.reference_column = 'account_id'
left join edw_prod.data_model.dim_store ds
    on ds.store_id = am.store_id
where a.date_day>='2025-09-15'
group by
    a.account_id,
    a.account_name,
    a.campaign_id,
    a.campaign_name,
    a.ad_name,
    a.ad_id,
    ds.store_brand,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    ds.store_id,
    a.date_day



union 


-- insert into EDW_PROD.NEW_STG.all_channel_optimization_test
select
    CASE
        WHEN a.campaign_name ilike '%$_PLA$_%' ESCAPE '$' OR a.campaign_name ILIKE '%Shopping%' OR l.label_names ILIKE 'PLA' OR l.label_names ILIKE '% PLA%' OR l.label_names ILIKE '%| PLA%' THEN 'Shopping'
        WHEN l.label_names ILIKE '%NonBrand%' OR l.label_names ILIKE '%Non-Brand%' THEN 'Non Branded Search'
        WHEN l.label_names ILIKE ' Brand %' OR l.label_names ILIKE '| Brand%' OR a.campaign_name = 'Techstyle' THEN 'Branded Search'
        WHEN l.label_names ILIKE '%| GDN%' OR a.account_name ILIKE '%GDN%' THEN 'Programmatic-GDN'
        WHEN a.campaign_name ILIKE '%NonBrand%' OR a.campaign_name ILIKE '%_NB_%' THEN 'Non Branded Search'
        WHEN a.campaign_name ILIKE '%Brand%' OR a.campaign_name ILIKE '%_Brand_%' THEN 'Branded Search'
        ELSE 'Non Branded Search'
    END AS channel,
    CASE
        WHEN a.account_name ILIKE '%Bing%' OR a.account_name ILIKE '%BNG%' OR l.label_names ILIKE '%Bing%' THEN 'Bing'
        WHEN a.account_name ILIKE '%Gemini%' OR l.label_names ILIKE '%Gemini%' OR l.label_names ILIKE '%Yahoo%' THEN 'Yahoo'
        WHEN l.label_names ILIKE '%GDN%' OR a.account_name ILIKE '%GDN%' THEN 'GDN'
        WHEN a.account_name ILIKE '%Google%' OR a.account_name ILIKE '%GDN%' OR l.label_names ILIKE '%Google%' OR a.account_name ILIKE '%Search%' OR a.account_name ILIKE '%Shopping%' THEN 'Google'
        ELSE 'Google'
    END AS subchannel,
    'google' as vendor,
    'Google' as CHANNEL_OPTIMIZATION,
    'Google' as CHANNEL_CPA_BY_LEAD,
    'Prospecting' as member_segment,
    'VIP Bidding' as bidding_objective,
    a.account_id,
    a.account_name,
    a.campaign_id,
    a.campaign_name,
    a.ad_group_name as adgroup_name,
    a.ad_group_id as adgroup_id,
    a.ad_name,
    a.ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    null as offer,
    null as influencer_name,
    ds.store_brand as store_brand_name,
    ds.store_brand_abbr,
    ds.store_country as country,
    ds.store_region as region,
    ds.store_id,
    null as creative_code,
    null as image_url,
    null as link_to_post,
    a.date_day as date,
    sum(a.spend * coalesce(1, 1)) as spend_usd,
    sum(a.spend * coalesce(1, 1)) as spend_with_vendor_fees_usd,
    sum(a.spend) as spend_account_currency,
    sum(a.spend) as spend_with_vendor_fees_account_currency,
    sum(a.impressions) as impressions,
    sum(a.clicks) as clicks,
    0 as video_views,
    0 as pixel_lead_click_1d,
    0 as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    0 as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,
    0 as pixel_vip_click_1d,
    0 as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    0 as pixel_vip_view_7d,
    0 as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1,
    null as UI_OPTIMIZATION_PIXEL_LEAD,
    null as UI_OPTIMIZATION_PIXEL_VIP,
    null as UI_OPTIMIZATION_WINDOW,
    null as RS_SPEND_USD,
    null as RS_SPEND_WITH_VENDOR_FEES_USD,
    null as RS_SPEND_ACCOUNT_CURRENCY,
    null as RS_SPEND_WITH_VENDOR_FEES_ACCOUNT_CURRENCY,
    null as UI_CAC_TARGET,
    null as DESIGNER_INITIALS,
    null as DATE_CREATED,
    null as VERSION,
    null as AD_TYPE,
    null as VIDEO_LENGTH,
    null as AUDIO,
    null as FIRST_3_SECONDS_TEXT,
    null as FIRST_3_SECONDS_VISUAL,
    null as MODEL_ACTION,
    null as MODEL_PLACEMENT,
    null as BODY_TYPE,
    null as GENDER,
    null as STILL_LIFE_ANGLE_SHOT,
    null as INFLUENCER,
    null as UGC_TALENT_NAME,
    null as EGC_TALENT_NAME,
    null as TRELLO_ID,
    null as ASSET_TYPE,
    null as ASSET_SIZE,
    null as CREATIVE_CONCEPT,
    null as TEST_NAME,
    null as PLATFORM,
    null as SKU1,
    null as SKU2,
    null as SKU3,
    null as SKU4,
    null as SKU5,
    null as MERCHANDISE_TYPE,
    null as NUMBER_OF_STYLES_SHOWN,
    null as LOGO,
    null as OFFER_FORMAT,
    null as OFFER_PLACEMENT,
    null as MESSAGING_THEME,
    null as PRODUCT_TYPE,
    null as DATA_LAST_UPDATED_DATETIME
from LAKE_MMOS.GOOGLE_ADS_GOOGLE_ADS.GOOGLE_ADS__AD_REPORT a
left join lake_view.sharepoint.med_account_mapping_media am
    on try_to_number(am.source_id) = a.account_id
    and am.source = 'Doubleclick Account'
left join edw_prod.data_model.dim_store ds
    on am.store_id = ds.store_id
left join (
    SELECT
        clh.campaign_id,
        LISTAGG(l.name, ' | ') WITHIN GROUP (ORDER BY l.name) AS label_names
    FROM LAKE_MMOS.GOOGLE_ADS.CAMPAIGN_LABEL_HISTORY clh
    JOIN LAKE_MMOS.GOOGLE_ADS.LABEL l ON clh.label_id = l.id
    GROUP BY clh.campaign_id
) l on a.campaign_id = l.campaign_id
where a.date_day>='2025-09-15'

group by
    CASE
        WHEN a.campaign_name ilike '%$_PLA$_%' ESCAPE '$' OR a.campaign_name ILIKE '%Shopping%' OR l.label_names ILIKE 'PLA' OR l.label_names ILIKE '% PLA%' OR l.label_names ILIKE '%| PLA%' THEN 'Shopping'
        WHEN l.label_names ILIKE '%NonBrand%' OR l.label_names ILIKE '%Non-Brand%' THEN 'Non Branded Search'
        WHEN l.label_names ILIKE ' Brand %' OR l.label_names ILIKE '| Brand%' OR a.campaign_name = 'Techstyle' THEN 'Branded Search'
        WHEN l.label_names ILIKE '%| GDN%' OR a.account_name ILIKE '%GDN%' THEN 'Programmatic-GDN'
        WHEN a.campaign_name ILIKE '%NonBrand%' OR a.campaign_name ILIKE '%_NB_%' THEN 'Non Branded Search'
        WHEN a.campaign_name ILIKE '%Brand%' OR a.campaign_name ILIKE '%_Brand_%' THEN 'Branded Search'
        ELSE 'Non Branded Search'
    END,

    CASE
        WHEN a.account_name ILIKE '%Bing%' OR a.account_name ILIKE '%BNG%' OR l.label_names ILIKE '%Bing%' THEN 'Bing'
        WHEN a.account_name ILIKE '%Gemini%' OR l.label_names ILIKE '%Gemini%' OR l.label_names ILIKE '%Yahoo%' THEN 'Yahoo'
        WHEN l.label_names ILIKE '%GDN%' OR a.account_name ILIKE '%GDN%' THEN 'GDN'
        WHEN a.account_name ILIKE '%Google%' OR a.account_name ILIKE '%GDN%' OR l.label_names ILIKE '%Google%' OR a.account_name ILIKE '%Search%' OR a.account_name ILIKE '%Shopping%' THEN 'Google'
        ELSE 'Google'
    END,

    a.account_id,
    a.account_name,
    a.campaign_id,
    a.campaign_name,
    a.ad_group_name,
    a.ad_group_id,
    a.ad_name,
    a.ad_id,
    ds.store_brand,
    ds.store_brand_abbr,
    ds.store_country,
    ds.store_region,
    ds.store_id,
    a.date_day


union 





-- insert into EDW_PROD.NEW_STG.all_channel_optimization_test
SELECT
    'tiktok' AS channel,
    CASE
        WHEN d.campaign_name ILIKE '%TubeScience%' THEN 'Tubescience'
        WHEN (d.campaign_name ILIKE '%Sapphire%' OR c.adgroup_name ILIKE '%Sapphire%' OR b.ad_name ILIKE '%Sapphire%')
             AND d.campaign_name NOT ILIKE '%CreativeExchange%' THEN 'Sapphire'
        WHEN b.ad_name ILIKE '%Schema%' OR d.campaign_name ILIKE '%schema%' THEN 'Schema'
        WHEN d.campaign_name ILIKE '%Gassed%' THEN 'Gassed'
        WHEN d.campaign_name ILIKE '%NRTV%' OR d.campaign_name ILIKE '%Narrative%' THEN 'Narrative'
        ELSE 'TikTok'
    END AS subchannel,
    CASE
        WHEN d.campaign_name ILIKE '%TubeScience%' THEN 'Tubescience'
        WHEN (d.campaign_name ILIKE '%Sapphire%' OR c.adgroup_name ILIKE '%Sapphire%' OR b.ad_name ILIKE '%Sapphire%')
             AND d.campaign_name NOT ILIKE '%CreativeExchange%' THEN 'Sapphire'
        WHEN b.ad_name ILIKE '%Schema%' OR d.campaign_name ILIKE '%schema%' THEN 'Schema'
        WHEN d.campaign_name ILIKE '%Gassed%' THEN 'Gassed'
        WHEN d.campaign_name ILIKE '%NRTV%' OR d.campaign_name ILIKE '%Narrative%' THEN 'Narrative'
        ELSE 'TikTok'
    END AS vendor,
    'tiktok' AS channel_optimization,
    'tiktok' AS channel_cpa_by_lead,
    null as MEMBER_SEGMENT,
    null as BIDDING_OBJECTIVE,
    CAST(b.advertiser_id AS VARCHAR) AS account_id,
    e.name AS account_name,
    CAST(b.campaign_id AS VARCHAR) AS campaign_id,
    d.campaign_name AS campaign_name,
    c.adgroup_name AS adgroup_name,
    CAST(b.adgroup_id AS VARCHAR) AS adgroup_id,
    null as AD_NAME,
    CAST(a.ad_id AS VARCHAR) AS ad_id,
    b.operation_status AS ad_is_active_flag,
    c.operation_status AS adgroup_is_active_flag,
    null as OFFER,
    null as INFLUENCER_NAME,
    ds.store_brand AS store_brand_name,
    ds.store_brand_abbr,
    ds.store_country AS country,
    ds.store_region AS region,
    CAST(ds.store_id AS INTEGER) AS store_id,
    null as CREATIVE_CODE,
    null as IMAGE_URL,
    null as LINK_TO_POST,
    a.stat_time_day AS date,
    a.spend * COALESCE(1, 1) AS spend_usd,
    a.spend * COALESCE(1, 1) AS spend_with_vendor_fees_usd,
    a.spend AS spend_account_currency,
    a.spend AS spend_with_vendor_fees_account_currency,
    a.impressions AS impressions,
    a.clicks AS clicks,
    a.video_watched_2_s AS video_views,
    0 AS pixel_lead_click_1d,
    0 AS pixel_lead_view_1d,
    0 AS pixel_lead_click_3d,
    0 AS pixel_lead_view_3d,
    0 AS pixel_lead_click_7d,
    0 AS pixel_lead_view_7d,
    0 AS pixel_lead_click_30d,
    0 AS pixel_lead_view_30d,
    0 AS pixel_lead_30dc_click,
    0 AS pixel_lead_7x1,
    0 AS pixel_lead_30x7x1,
    0 AS pixel_vip_click_1d,
    0 AS pixel_vip_view_1d,
    0 AS pixel_vip_click_3d,
    0 AS pixel_vip_view_3d,
    0 AS pixel_vip_click_7d,
    0 AS pixel_vip_view_7d,
    0 AS pixel_vip_click_30d,
    0 AS pixel_vip_view_30d,
    0 AS pixel_vip_30dc_click,
    0 AS pixel_vip_7x1,
    0 AS pixel_vip_30x7x1,
    null as UI_OPTIMIZATION_PIXEL_LEAD,
    null as UI_OPTIMIZATION_PIXEL_VIP,
    '7x1' AS ui_optimization_window,
    null as RS_SPEND_USD,
    null as RS_SPEND_WITH_VENDOR_FEES_USD,
    null as RS_SPEND_ACCOUNT_CURRENCY,
    null as RS_SPEND_WITH_VENDOR_FEES_ACCOUNT_CURRENCY,
    null as UI_CAC_TARGET,
    null as DESIGNER_INITIALS,
    null as DATE_CREATED,
    null as VERSION,
    null as AD_TYPE,
    null as VIDEO_LENGTH,
    null as AUDIO,
    null as FIRST_3_SECONDS_TEXT,
    null as FIRST_3_SECONDS_VISUAL,
    null as MODEL_ACTION,
    null as MODEL_PLACEMENT,
    null as BODY_TYPE,
    null as GENDER,
    null as STILL_LIFE_ANGLE_SHOT,
    null as INFLUENCER,
    null as UGC_TALENT_NAME,
    null as EGC_TALENT_NAME,
    null as TRELLO_ID,
    null as ASSET_TYPE,
    null as ASSET_SIZE,
    null as CREATIVE_CONCEPT,
    null as TEST_NAME,
    null as PLATFORM,
    null as SKU1,
    null as SKU2,
    null as SKU3,
    null as SKU4,
    null as SKU5,
    null as MERCHANDISE_TYPE,
    null as NUMBER_OF_STYLES_SHOWN,
    null as LOGO,
    null as OFFER_FORMAT,
    null as OFFER_PLACEMENT,
    null as MESSAGING_THEME,
    null as PRODUCT_TYPE,
    null as DATA_LAST_UPDATED_DATETIME,

FROM LAKE_MMOS.TIKTOK_ADS.AD_REPORT_DAILY a
LEFT JOIN LAKE_MMOS.TIKTOK_ADS.AD_HISTORY b ON a.ad_id = b.ad_id
LEFT JOIN LAKE_MMOS.TIKTOK_ADS.ADGROUP_HISTORY c ON b.adgroup_id = c.adgroup_id
LEFT JOIN LAKE_MMOS.TIKTOK_ADS.CAMPAIGN_HISTORY d ON b.campaign_id = d.campaign_id
LEFT JOIN LAKE_MMOS.TIKTOK_ADS.ADVERTISER e ON b.advertiser_id = e.id
LEFT JOIN lake_view.sharepoint.med_account_mapping_media am
    ON am.source_id = CAST(b.advertiser_id AS VARCHAR)
    AND am.source ILIKE 'TikTok'
LEFT JOIN edw_prod.data_model.dim_store ds ON ds.store_id = am.store_id
WHERE b.campaign_id <> 1692161851867170 -- Removed DPA Campaign as no longer applicable for CPA reports or Media
    AND NOT (
        (
            (LOWER(d.campaign_name) LIKE '%afterpay%' AND LOWER(d.campaign_name) != 'sxf_tiktok_afterpay_13')
            OR (LOWER(d.campaign_name) = 'sxf_tiktok_afterpay_13' AND a.stat_time_day < '2022-05-11')
        )
        AND LOWER(ds.store_brand_abbr || ds.store_region) = 'sxna'
    )
    AND d.campaign_name NOT ILIKE '%TikTokShops_%'
    AND d.campaign_name NOT ILIKE '%OrganicSocialBoosting%'
    and a.stat_time_day>='2025-09-15'
;















