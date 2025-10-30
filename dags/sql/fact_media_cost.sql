-- facebook 广告查询 费用

truncate table EDW_PROD.NEW_STG.FACT_MEDIA_COST;

insert into EDW_PROD.NEW_STG.FACT_MEDIA_COST
SELECT
    aibh.date_day::DATE AS spend_date,
    CASE
        WHEN ds.store_brand || ' ' || ds.store_country = 'JustFab US'
             AND aibh.campaign_name ILIKE '%XAC%' THEN 41
        WHEN ds.store_brand || ' ' || ds.store_country = 'Fabletics US'
             AND aibh.campaign_name ILIKE '%XAC%' THEN 80
        WHEN aibh.campaign_name ILIKE ANY ('%flmus_canada%', '%flmca_%') THEN 79
        ELSE ds.store_id
    END AS store_id,
    IFF(am.specialty_store IS NOT NULL,'TRUE','FALSE') AS is_specialty_store,
    am.specialty_store,
    'FB+IG' AS channel,
    CASE
        WHEN aibh.account_name ILIKE '%1180%' THEN '1180'
        WHEN aibh.account_id='338056876952277'
             AND aibh.date_day::DATE >= '2023-02-01' THEN 'Volt'
        WHEN aibh.account_name ILIKE '%peoplehype%' THEN 'Peoplehype'
        WHEN aibh.account_name ILIKE '%agencywithin%' THEN 'Agencywithin'
        WHEN aibh.account_name ILIKE '%tubescience%'
             OR aibh.campaign_name ILIKE '%tubescience%' THEN 'Tubescience'
        WHEN aibh.campaign_name ILIKE '%Gassed%' THEN 'Gassed'
        WHEN aibh.campaign_name ILIKE '%DonutDigital%' THEN 'DonutDigital'
        WHEN aibh.account_name ILIKE '%narrative%'
             OR aibh.campaign_name ILIKE '%narrative%' THEN 'Narrative'
        WHEN aibh.account_name ILIKE '%geistm%' THEN 'geistm'
        WHEN aibh.account_name ILIKE '%Sandbox%' THEN 'Sandbox'
        WHEN aibh.account_name ILIKE ANY ('%influencers', '%collaborator%')
            OR (aibh.account_name ILIKE 'fabletics_us_promo'
                AND aibh.ad_set_name ILIKE ANY ('%infl_krowland%','%infl_mziegler%','%infl_dlovato%')) THEN 'PaidInfluencers'
        WHEN (aibh.campaign_name ILIKE '%sapphire%'
              OR aibh.ad_name ILIKE '%sapphire%'
              OR aibh.ad_set_name ILIKE '%sapphire%')
             AND aibh.campaign_name NOT ILIKE '%CreativeExchange%' THEN 'Sapphire'
        WHEN lower(aibh.account_name) ILIKE '%admk%' THEN 'ADMK'
        WHEN aibh.ad_set_name ILIKE '%schema%'
             OR aibh.campaign_name ILIKE '%schema%' THEN 'Schema'
        WHEN aibh.campaign_name ILIKE '%MySubscriptionAddiction%' THEN 'MySubscriptionAddiction'
        ELSE 'FB+IG'
    END AS subchannel,
    'facebook' AS vendor,
    'API' AS source,
    'MEDIACOST' AS spend_type,
    am.currency AS spend_iso_currency_code,
    ds.store_currency AS store_iso_currency_code,
    SUM(aibh.spend) AS cost,
    SUM(aibh.impressions) AS impressions,
    SUM(aibh.clicks) AS clicks,
    'MEDIACOST' AS type,
    CASE
        WHEN aibh.ad_set_name ILIKE '%%_PROSPECT%%' THEN 'Prospecting'
        WHEN aibh.ad_set_name ILIKE '%%_SITERET%%' THEN 'Site Retargeting'
        WHEN aibh.ad_set_name ILIKE '%%_LEADRET%%'
             OR aibh.ad_set_name ILIKE '%%AgedLead%%'
             OR aibh.account_name ILIKE '%Retargeting%'
             OR aibh.account_name ILIKE '%RTG%' THEN 'Lead Retargeting'
        WHEN aibh.ad_set_name ILIKE '%%_VIPRet%%' THEN 'VIP Retargeting'
        ELSE 'Prospecting'
    END AS targeting,
    am.mens_account_flag AS is_mens_flag,
    NULL AS spend_date_usd_conv_rate,
    NULL AS spend_date_eur_conv_rate,
    NULL AS local_store_conv_rate,
    HASH(
        aibh.date_day::DATE,
        CASE
            WHEN ds.store_brand || ' ' || ds.store_country = 'JustFab US'
                 AND aibh.campaign_name ILIKE '%XAC%' THEN 41
            WHEN ds.store_brand || ' ' || ds.store_country = 'Fabletics US'
                 AND aibh.campaign_name ILIKE '%XAC%' THEN 80
            WHEN aibh.campaign_name ILIKE ANY ('%flmus_canada%', '%flmca_%') THEN 79
            ELSE ds.store_id
        END,
        IFF(am.specialty_store IS NOT NULL,'TRUE','FALSE'),
        am.specialty_store,
        'FB+IG',
        CASE
            WHEN aibh.account_name ILIKE '%1180%' THEN '1180'
            WHEN aibh.account_id='338056876952277'
                 AND aibh.date_day::DATE >= '2023-02-01' THEN 'Volt'
            WHEN aibh.account_name ILIKE '%peoplehype%' THEN 'Peoplehype'
            WHEN aibh.account_name ILIKE '%agencywithin%' THEN 'Agencywithin'
            WHEN aibh.account_name ILIKE '%tubescience%'
                 OR aibh.campaign_name ILIKE '%tubescience%' THEN 'Tubescience'
            WHEN aibh.campaign_name ILIKE '%Gassed%' THEN 'Gassed'
            WHEN aibh.campaign_name ILIKE '%DonutDigital%' THEN 'DonutDigital'
            WHEN aibh.account_name ILIKE '%narrative%'
                 OR aibh.campaign_name ILIKE '%narrative%' THEN 'Narrative'
            WHEN aibh.account_name ILIKE '%geistm%' THEN 'geistm'
            WHEN aibh.account_name ILIKE '%Sandbox%' THEN 'Sandbox'
            WHEN aibh.account_name ILIKE ANY ('%influencers', '%collaborator%')
                OR (aibh.account_name ILIKE 'fabletics_us_promo'
                    AND aibh.ad_set_name ILIKE ANY ('%infl_krowland%','%infl_mziegler%','%infl_dlovato%')) THEN 'PaidInfluencers'
            WHEN (aibh.campaign_name ILIKE '%sapphire%'
                  OR aibh.ad_name ILIKE '%sapphire%'
                  OR aibh.ad_set_name ILIKE '%sapphire%')
                 AND aibh.campaign_name NOT ILIKE '%CreativeExchange%' THEN 'Sapphire'
            WHEN lower(aibh.account_name) ILIKE '%admk%' THEN 'ADMK'
            WHEN aibh.ad_set_name ILIKE '%schema%'
                 OR aibh.campaign_name ILIKE '%schema%' THEN 'Schema'
            WHEN aibh.campaign_name ILIKE '%MySubscriptionAddiction%' THEN 'MySubscriptionAddiction'
            ELSE 'FB+IG'
        END,
        'facebook',
        'API',
        'MEDIACOST',
        am.currency,
        ds.store_currency,
        CASE
            WHEN aibh.ad_set_name ILIKE '%%_PROSPECT%%' THEN 'Prospecting'
            WHEN aibh.ad_set_name ILIKE '%%_SITERET%%' THEN 'Site Retargeting'
            WHEN aibh.ad_set_name ILIKE '%%_LEADRET%%'
                 OR aibh.ad_set_name ILIKE '%%AgedLead%%'
                 OR aibh.account_name ILIKE '%Retargeting%'
                 OR aibh.account_name ILIKE '%RTG%' THEN 'Lead Retargeting'
            WHEN aibh.ad_set_name ILIKE '%%_VIPRet%%' THEN 'VIP Retargeting'
            ELSE 'Prospecting'
        END,
        am.mens_account_flag,
        am.is_scrubs_flag
    ) AS meta_row_hash,
    CURRENT_TIMESTAMP() AS meta_create_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime,
    am.is_scrubs_flag
FROM LAKE_MMOS.FACEBOOK_ADS_FACEBOOK_ADS.FACEBOOK_ADS__AD_REPORT aibh
left JOIN lake_view.sharepoint.med_account_mapping_media am ON aibh.account_id::VARCHAR=am.source_id::VARCHAR -- AND am.source='Facebook' 
AND am.reference_column='account_id'
left JOIN edw_prod.DATA_MODEL_JFB.dim_store ds ON ds.store_id=am.store_id
GROUP BY
    aibh.date_day::DATE,
    aibh.CAMPAIGN_NAME,
    ds.store_id,
    ds.store_brand || CASE
        WHEN (ds.store_brand || ' ' || ds.store_country) IN ('JustFab US', 'Fabletics US')
             AND aibh.campaign_name ILIKE '%XAC%' THEN 'CA'
        WHEN aibh.campaign_name ILIKE ANY ('%flmus_canada%', '%flmca_%') THEN 'CA'
        ELSE ds.store_country
    END,
    ds.store_region,
    CASE
        WHEN (ds.store_brand || ' ' || ds.store_country) IN ('JustFab US', 'Fabletics US')
             AND aibh.campaign_name ILIKE '%XAC%' THEN 'CA'
        WHEN aibh.campaign_name ILIKE ANY ('%flmus_canada%', '%flmca_%', 'sxca_%') THEN 'CA'
        ELSE ds.store_country
    END,
    ds.store_country,
    CASE
        WHEN aibh.account_name ILIKE '%1180%' THEN '1180'
        WHEN aibh.account_id='338056876952277'
             AND aibh.date_day::DATE >= '2023-02-01' THEN 'Volt'
        WHEN aibh.account_name ILIKE '%peoplehype%' THEN 'Peoplehype'
        WHEN aibh.account_name ILIKE '%agencywithin%' THEN 'Agencywithin'
        WHEN aibh.account_name ILIKE '%tubescience%'
             OR aibh.campaign_name ILIKE '%tubescience%' THEN 'Tubescience'
        WHEN aibh.campaign_name ILIKE '%Gassed%' THEN 'Gassed'
        WHEN aibh.campaign_name ILIKE '%DonutDigital%' THEN 'DonutDigital'
        WHEN aibh.account_name ILIKE '%narrative%'
             OR aibh.campaign_name ILIKE '%narrative%' THEN 'Narrative'
        WHEN aibh.account_name ILIKE '%geistm%' THEN 'geistm'
        WHEN aibh.account_name ILIKE '%Sandbox%' THEN 'Sandbox'
        WHEN aibh.account_name ILIKE ANY ('%influencers', '%collaborator%')
            OR (aibh.account_name ILIKE 'fabletics_us_promo'
                AND aibh.ad_set_name ILIKE ANY ('%infl_krowland%','%infl_mziegler%','%infl_dlovato%')) THEN 'PaidInfluencers'
        WHEN (aibh.campaign_name ILIKE '%sapphire%'
              OR aibh.ad_name ILIKE '%sapphire%'
              OR aibh.ad_set_name ILIKE '%sapphire%')
             AND aibh.campaign_name NOT ILIKE '%CreativeExchange%' THEN 'Sapphire'
        WHEN lower(aibh.account_name) ILIKE '%admk%' THEN 'ADMK'
        WHEN aibh.ad_set_name ILIKE '%schema%'
             OR aibh.campaign_name ILIKE '%schema%' THEN 'Schema'
        WHEN aibh.campaign_name ILIKE '%MySubscriptionAddiction%' THEN 'MySubscriptionAddiction'
        ELSE 'FB+IG'
    END,
    CASE
        WHEN aibh.ad_set_name ILIKE '%%_PROSPECT%%' THEN 'Prospecting'
        WHEN aibh.ad_set_name ILIKE '%%_SITERET%%' THEN 'Site Retargeting'
        WHEN aibh.ad_set_name ILIKE '%%_LEADRET%%'
             OR aibh.ad_set_name ILIKE '%%AgedLead%%'
             OR aibh.account_name ILIKE '%Retargeting%'
             OR aibh.account_name ILIKE '%RTG%' THEN 'Lead Retargeting'
        WHEN aibh.ad_set_name ILIKE '%%_VIPRet%%' THEN 'VIP Retargeting'
        ELSE 'Prospecting'
    END,
    am.mens_account_flag,
    am.is_scrubs_flag,
    am.currency,
    ds.store_currency,
    ds.STORE_BRAND,
    aibh.account_id,
    IFF(am.specialty_store IS NOT NULL,'TRUE','FALSE'),
    am.specialty_store














-- google
union all


SELECT
    pmax.date_day AS spend_date,
    CASE
        WHEN ds.store_brand = 'JustFab' AND l.label_names ILIKE '%CA |%' THEN 41
        WHEN ds.store_brand = 'Fabletics' AND l.label_names ILIKE '%CA |%' THEN 79
        WHEN ds.store_brand = 'ShoeDazzle' AND l.label_names ILIKE '%CA |%' THEN 55
        WHEN ds.store_brand = 'JustFab' AND ds.store_country = 'FR' AND l.label_names ILIKE '%BE |%' THEN 48
        WHEN ds.store_brand = 'Fabletics' AND ds.store_country = 'FR' AND l.label_names ILIKE '%BE |%' THEN 69
        WHEN ds.store_brand = 'JustFab' AND ds.store_country = 'DE' AND l.label_names ILIKE '%AT |%' THEN 36
        WHEN ds.store_brand = 'Fabletics' AND ds.store_country = 'DE' AND l.label_names ILIKE '%AT |%' THEN 65
        ELSE ds.store_id
    END AS store_id,
    CASE
        WHEN ds.store_brand IN ('ShoeDazzle') AND l.label_names ILIKE '%CA |%' THEN TRUE
        WHEN ds.store_brand IN ('JustFab','Fabletics') AND ds.store_country = 'FR' AND l.label_names ILIKE '%BE |%' THEN TRUE
        WHEN ds.store_brand IN ('JustFab','Fabletics') AND ds.store_country = 'DE' AND l.label_names ILIKE '%AT |%' THEN TRUE
        WHEN am.specialty_store IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_specialty_store,
    CASE
        WHEN ds.store_brand IN ('ShoeDazzle') AND l.label_names ILIKE '%CA |%' THEN 'CA'
        WHEN ds.store_brand IN ('JustFab','Fabletics') AND ds.store_country = 'FR' AND l.label_names ILIKE '%BE |%' THEN 'BE'
        WHEN ds.store_brand IN ('JustFab','Fabletics') AND ds.store_country = 'DE' AND l.label_names ILIKE '%AT |%' THEN 'AT'
        ELSE am.specialty_store
    END AS specialty_store,
    CASE
        WHEN pmax.campaign_name ILIKE '%$_PLA$_%' ESCAPE '$'
             OR pmax.campaign_name ILIKE '%Shopping%'
             OR l.label_names ILIKE 'PLA'
             OR l.label_names ILIKE '% PLA%'
             OR l.label_names ILIKE '%| PLA%' THEN 'Shopping'
        WHEN l.label_names ILIKE '%NonBrand%' OR l.label_names ILIKE '%Non-Brand%' THEN 'Non Branded Search'
        WHEN l.label_names ILIKE '%Brand %' OR l.label_names ILIKE '| Brand%' OR pmax.campaign_name = 'Techstyle' THEN 'Branded Search'
        WHEN l.label_names ILIKE '%| GDN%' OR pmax.account_name ILIKE '%GDN%' THEN 'Programmatic-GDN'
        WHEN pmax.campaign_name ILIKE '%NonBrand%' OR pmax.campaign_name ILIKE '%_NB_%' THEN 'Non Branded Search'
        WHEN pmax.campaign_name ILIKE '%Brand%' OR pmax.campaign_name ILIKE '%_Brand_%' THEN 'Branded Search'
        ELSE 'Non Branded Search'
    END AS channel,

    CASE WHEN pmax.account_name ILIKE '%%Bing%%' OR pmax.account_name ILIKE '%%BNG%%'OR l.label_names ILIKE '%%Bing%%' THEN 'Bing'
                       WHEN pmax.account_name ILIKE '%%Gemini%%' OR l.label_names ILIKE '%%Gemini%%' OR l.label_names ILIKE '%%Yahoo%%' THEN 'Yahoo'
                       WHEN l.label_names ILIKE '%%GDN%%' OR pmax.account_name ILIKE '%%GDN%%' THEN 'GDN'
                       WHEN pmax.account_name ILIKE '%%Google%%' OR pmax.account_name ILIKE '%%GDN%%' OR l.label_names ILIKE '%%Google%%' OR pmax.account_name ILIKE '%%Search%%' OR pmax.account_name ILIKE '%%Shopping%%' THEN 'Google'
                       ELSE 'Google'
                   END AS subchannel,
    'Doubleclick' AS vendor,
    'API' AS source,
    'MEDIACOST' AS spend_type,
    am.currency AS spend_iso_currency_code,
    ds.store_currency AS store_iso_currency_code,
    SUM(pmax.spend) AS cost,
    SUM(pmax.impressions) AS impressions,
    SUM(pmax.clicks) AS clicks,
    'MEDIACOST' AS type,
    CASE
        WHEN l.label_names ILIKE '%VIPRET%' OR pmax.campaign_name ILIKE '%VIPRET%' THEN 'VIP Retargeting'
        ELSE 'Prospecting'
    END AS targeting,
    am.mens_account_flag AS is_mens_flag,
    NULL AS spend_date_usd_conv_rate,
    NULL AS spend_date_eur_conv_rate,
    NULL AS local_store_conv_rate,
    null AS meta_row_hash,
    CURRENT_TIMESTAMP() AS meta_create_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime,
    am.is_scrubs_flag
FROM (select * from LAKE_MMOS.GOOGLE_ADS_GOOGLE_ADS.GOOGLE_ADS__CAMPAIGN_REPORT where account_name like '%FabKids US%' or account_name like '%JustFab US%'
or account_name like '%ShoeDazzle US%') pmax
LEFT JOIN (
    SELECT
        clh.campaign_id,
        LISTAGG(l.name, ' | ') WITHIN GROUP (ORDER BY l.name) AS label_names
    FROM LAKE_MMOS.GOOGLE_ADS.CAMPAIGN_LABEL_HISTORY clh
    JOIN LAKE_MMOS.GOOGLE_ADS.LABEL l ON clh.label_id = l.id
    GROUP BY
        clh.campaign_id
) l ON pmax.campaign_id = l.campaign_id
left JOIN lake_view.sharepoint.med_account_mapping_media am
    -- ON TRY_TO_NUMBER(am.source_id) = pmax.account_id::VARCHAR
    ON am.source_id::VARCHAR = pmax.account_id::VARCHAR
    -- AND am.source = 'Doubleclick Account'
left JOIN edw_prod.data_model.dim_store ds ON am.store_id = ds.store_id
GROUP BY
    pmax.date_day,
    CASE
        WHEN ds.store_brand = 'JustFab' AND l.label_names ILIKE '%CA |%' THEN 41
        WHEN ds.store_brand = 'Fabletics' AND l.label_names ILIKE '%CA |%' THEN 79
        WHEN ds.store_brand = 'ShoeDazzle' AND l.label_names ILIKE '%CA |%' THEN 55
        WHEN ds.store_brand = 'JustFab' AND ds.store_country = 'FR' AND l.label_names ILIKE '%BE |%' THEN 48
        WHEN ds.store_brand = 'Fabletics' AND ds.store_country = 'FR' AND l.label_names ILIKE '%BE |%' THEN 69
        WHEN ds.store_brand = 'JustFab' AND ds.store_country = 'DE' AND l.label_names ILIKE '%AT |%' THEN 36
        WHEN ds.store_brand = 'Fabletics' AND ds.store_country = 'DE' AND l.label_names ILIKE '%AT |%' THEN 65
        ELSE ds.store_id
    END,
    CASE
        WHEN ds.store_brand IN ('ShoeDazzle') AND l.label_names ILIKE '%CA |%' THEN TRUE
        WHEN ds.store_brand IN ('JustFab','Fabletics') AND ds.store_country = 'FR' AND l.label_names ILIKE '%BE |%' THEN TRUE
        WHEN ds.store_brand IN ('JustFab','Fabletics') AND ds.store_country = 'DE' AND l.label_names ILIKE '%AT |%' THEN TRUE
        WHEN am.specialty_store IS NOT NULL THEN TRUE
        ELSE FALSE
    END,
    CASE
        WHEN ds.store_brand IN ('ShoeDazzle') AND l.label_names ILIKE '%CA |%' THEN 'CA'
        WHEN ds.store_brand IN ('JustFab','Fabletics') AND ds.store_country = 'FR' AND l.label_names ILIKE '%BE |%' THEN 'BE'
        WHEN ds.store_brand IN ('JustFab','Fabletics') AND ds.store_country = 'DE' AND l.label_names ILIKE '%AT |%' THEN 'AT'
        ELSE am.specialty_store
    END,
    CASE
        WHEN pmax.campaign_name ILIKE '%$_PLA$_%' ESCAPE '$'
             OR pmax.campaign_name ILIKE '%Shopping%'
             OR l.label_names ILIKE 'PLA'
             OR l.label_names ILIKE '% PLA%'
             OR l.label_names ILIKE '%| PLA%' THEN 'Shopping'
        WHEN l.label_names ILIKE '%NonBrand%' OR l.label_names ILIKE '%Non-Brand%' THEN 'Non Branded Search'
        WHEN l.label_names ILIKE '%Brand %' OR l.label_names ILIKE '| Brand%' OR pmax.campaign_name = 'Techstyle' THEN 'Branded Search'
        WHEN l.label_names ILIKE '%| GDN%' OR pmax.account_name ILIKE '%GDN%' THEN 'Programmatic-GDN'
        WHEN pmax.campaign_name ILIKE '%NonBrand%' OR pmax.campaign_name ILIKE '%_NB_%' THEN 'Non Branded Search'
        WHEN pmax.campaign_name ILIKE '%Brand%' OR pmax.campaign_name ILIKE '%_Brand_%' THEN 'Branded Search'
        ELSE 'Non Branded Search'
    END,
    CASE WHEN pmax.account_name ILIKE '%%Bing%%' OR pmax.account_name ILIKE '%%BNG%%'OR l.label_names ILIKE '%%Bing%%' THEN 'Bing'
                       WHEN pmax.account_name ILIKE '%%Gemini%%' OR l.label_names ILIKE '%%Gemini%%' OR l.label_names ILIKE '%%Yahoo%%' THEN 'Yahoo'
                       WHEN l.label_names ILIKE '%%GDN%%' OR pmax.account_name ILIKE '%%GDN%%' THEN 'GDN'
                       WHEN pmax.account_name ILIKE '%%Google%%' OR pmax.account_name ILIKE '%%GDN%%' OR l.label_names ILIKE '%%Google%%' OR pmax.account_name ILIKE '%%Search%%' OR pmax.account_name ILIKE '%%Shopping%%' THEN 'Google'
                       ELSE 'Google'
                   END,
    CASE
        WHEN l.label_names ILIKE '%VIPRET%' OR pmax.campaign_name ILIKE '%VIPRET%' THEN 'VIP Retargeting'
        ELSE 'Prospecting'
    END,
    am.mens_account_flag,
    am.is_scrubs_flag,
    am.currency,
    ds.store_currency,
    pmax.account_id,
    pmax.campaign_id



union all

SELECT
    a.date_day AS spend_date,
    ds.store_id,
    IFF(am.specialty_store IS NOT NULL,'TRUE','FALSE') AS is_specialty_store,
    am.specialty_store,
    'TikTok' AS channel,
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
        ELSE 'TikTok'
    END AS vendor,
    'API' AS source,
    'MEDIACOST' AS spend_type,
    am.currency AS spend_iso_currency_code,
    ds.store_currency AS store_iso_currency_code,
    SUM(a.spend) AS cost,
    SUM(a.impressions) AS impressions,
    SUM(a.clicks) AS clicks,
    'MEDIACOST' AS type,
    'Prospecting' AS targeting,
    am.mens_account_flag AS is_mens_flag,
    NULL AS spend_date_usd_conv_rate,
    NULL AS spend_date_eur_conv_rate,
    NULL AS local_store_conv_rate,
    HASH(
        a.date_day,
        ds.store_id,
        IFF(am.specialty_store IS NOT NULL,'TRUE','FALSE'),
        am.specialty_store,
        'TikTok',
        CASE
            WHEN d.campaign_name ILIKE '%TubeScience%' THEN 'Tubescience'
            WHEN (d.campaign_name ILIKE '%Sapphire%' OR c.adgroup_name ILIKE '%Sapphire%' OR b.ad_name ILIKE '%Sapphire%')
                 AND d.campaign_name NOT ILIKE '%CreativeExchange%' THEN 'Sapphire'
            WHEN b.ad_name ILIKE '%Schema%' OR d.campaign_name ILIKE '%schema%' THEN 'Schema'
            WHEN d.campaign_name ILIKE '%Gassed%' THEN 'Gassed'
            WHEN d.campaign_name ILIKE '%NRTV%' OR d.campaign_name ILIKE '%Narrative%' THEN 'Narrative'
            ELSE 'TikTok'
        END,
        CASE
            WHEN d.campaign_name ILIKE '%TubeScience%' THEN 'Tubescience'
            ELSE 'TikTok'
        END,
        'API',
        'MEDIACOST',
        am.currency,
        ds.store_currency,
        'Prospecting',
        am.mens_account_flag,
        am.is_scrubs_flag
    ) AS meta_row_hash,
    CURRENT_TIMESTAMP() AS meta_create_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime,
    am.is_scrubs_flag
FROM LAKE_MMOS.TIKTOK_ADS_TIKTOK_ADS.TIKTOK_ADS__ADVERTISER_REPORT a
LEFT JOIN LAKE_MMOS.TIKTOK_ADS.AD_HISTORY b ON a.ADVERTISER_ID = b.ad_id
LEFT JOIN LAKE_MMOS.TIKTOK_ADS.ADGROUP_HISTORY c ON b.adgroup_id = c.adgroup_id
LEFT JOIN LAKE_MMOS.TIKTOK_ADS.CAMPAIGN_HISTORY d ON b.campaign_id = d.campaign_id
LEFT JOIN lake_view.sharepoint.med_account_mapping_media am
    ON am.source_id::VARCHAR = b.advertiser_id::VARCHAR
    -- AND am.source ILIKE 'TikTok'
LEFT JOIN edw_prod.data_model.dim_store ds ON ds.store_id = am.store_id
GROUP BY
    a.date_day,
    ds.store_id,
    IFF(am.specialty_store IS NOT NULL,'TRUE','FALSE'),
    am.specialty_store,
    CASE
        WHEN d.campaign_name ILIKE '%TubeScience%' THEN 'Tubescience'
        WHEN (d.campaign_name ILIKE '%Sapphire%' OR c.adgroup_name ILIKE '%Sapphire%' OR b.ad_name ILIKE '%Sapphire%')
             AND d.campaign_name NOT ILIKE '%CreativeExchange%' THEN 'Sapphire'
        WHEN b.ad_name ILIKE '%Schema%' OR d.campaign_name ILIKE '%schema%' THEN 'Schema'
        WHEN d.campaign_name ILIKE '%Gassed%' THEN 'Gassed'
        WHEN d.campaign_name ILIKE '%NRTV%' OR d.campaign_name ILIKE '%Narrative%' THEN 'Narrative'
        ELSE 'TikTok'
    END,
    CASE
        WHEN d.campaign_name ILIKE '%TubeScience%' THEN 'Tubescience'
        ELSE 'TikTok'
    END,
    am.currency,
    ds.store_currency,
    'Prospecting',
    am.mens_account_flag,
    am.is_scrubs_flag,
    b.campaign_id,
    b.advertiser_id;

