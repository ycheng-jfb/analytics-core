
-- SESSION CHANNEL MAPPING: ALL SESSIONS PRIOR TO 2021 --

create or replace temporary table _channels_to_map AS
SELECT DISTINCT
    sc.media_source_hash,
    '◊' || nvl(sc.gateway_type,'')
            || '◊' || nvl(sc.gateway_sub_type,'') || '◊' ||
                nvl(sc.utm_source, '') || '◊' ||
                nvl(sc.utm_medium, '') || '◊'
    as params_short,
    '◊' || nvl(sc.gateway_type,'')
            || '◊' || nvl(sc.gateway_sub_type,'') || '◊' ||
                nvl(sc.utm_source, '') || '◊' ||
                nvl(sc.utm_medium, '') || '◊' ||
                nvl(sc.utm_campaign,'') || '◊'
    as params_long,
    sc.utm_source,
    sc.utm_medium,
    sc.utm_campaign,
    sc.gateway_type,
    sc.gateway_sub_type,
    sc.seo_vendor,
    MAX(SESSION_ID) AS MAX_SESSION_ID,
    COUNT(*) as event_count
FROM REPORTING_BASE_PROD.SHARED.SESSION sc
where to_date(SESSION_LOCAL_DATETIME) < '2021-01-01'
group by sc.media_source_hash,
    '◊' || nvl(sc.gateway_type,'')
        || '◊' || nvl(sc.gateway_sub_type,'') || '◊' ||
            nvl(sc.utm_source, '') || '◊' ||
            nvl(sc.utm_medium, '') || '◊',
    '◊' || nvl(sc.gateway_type,'')
        || '◊' || nvl(sc.gateway_sub_type,'') || '◊' ||
            nvl(sc.utm_source, '') || '◊' ||
            nvl(sc.utm_medium, '') || '◊' ||
            nvl(sc.utm_campaign,'') || '◊',
    sc.utm_source,
    sc.utm_medium,
    sc.utm_campaign,
    sc.gateway_type,
    sc.gateway_sub_type,
    sc.seo_vendor
;

CREATE OR REPLACE TEMPORARY TABLE _savage_mapping AS
SELECT
    _utm_medium AS utm_medium,
    _utm_source AS utm_source,
    channel,
    subchannel,
    vendor,
    traffic_type
FROM (
    SELECT
        lake.public.udf_clean_attribute(lower(traffic_type)) AS traffic_type,
        lake.public.udf_clean_attribute(lower(utm_medium)) AS _utm_medium,
        lake.public.udf_clean_attribute(lower(utm_source)) AS _utm_source,
        lake.public.udf_clean_attribute(lower(channel)) AS channel,
        lake.public.udf_clean_attribute(lower(subchannel)) AS subchannel,
        lake.public.udf_clean_attribute(lower(vendor)) AS vendor,
        row_number() OVER(PARTITION BY _utm_source, _utm_medium ORDER BY meta_update_datetime DESC) AS rn
    FROM LAKE_VIEW.SHAREPOINT.MED_SAVAGEX_CHANNEL_MAPPING l
    ) l
WHERE l.rn = 1;

create or replace temporary table _mapped_channels as
SELECT DISTINCT
    media_source_hash,
    a.params_short,
    a.params_long,
    COALESCE(lower(a.channel),'unclassified') AS channel,
    COALESCE(lower(a.subchannel),'unclassified') AS subchannel,
    lower(a.vendor) AS vendor,
    lower(a.gateway_type) AS gateway_type,
    lower(a.gateway_sub_type) AS gateway_sub_type,
    lower(a.utm_source) AS utm_source,
    lower(a.utm_medium) AS utm_medium,
    lower(utm_campaign) AS utm_campaign,
    seo_vendor,
    a.event_count
FROM (
    SELECT
        media_source_hash,
        params_short,
        params_long,
        utm_campaign,
        seo_vendor,
        CASE

            -- testing
            WHEN params_short = '◊branded search◊◊irs◊affiliate◊' THEN 'Affiliate>Networks>ImpactRadius'
            WHEN l.utm_campaign = 'affiliates' and gateway_type = '' then 'Affiliate>Networks>ImpactRadius'
            WHEN params_short = '◊display◊◊linktrust◊display◊' THEN 'Affiliate>Networks>LinkTrust'
            WHEN l.utm_medium = 'testing' THEN
                CASE
                    WHEN l.utm_source = 'condenast' THEN 'Testing>CondeNast>CondeNast'
                    WHEN l.utm_source ilike 'google%%' THEN 'Programmatic-GDN>Discovery>Adwords'
                    WHEN l.utm_source = 'rokt' THEN 'Programmatic>ROKT>ROKT'
                    WHEN l.utm_source = 'ebg' THEN 'Affiliate>EBG>EBG'
                    WHEN l.utm_source = 'reddit' THEN 'Reddit>Reddit>Reddit'
                    WHEN l.utm_source = 'venga' THEN 'Affiliate>Venga>Venga'
                    ELSE 'Testing>Other>Other'
                END

            -- dealing with google/cpc
            WHEN (l.utm_medium ILIKE '%%cpc%%' OR l.utm_medium ILIKE '%%ppc%%')
                AND l.utm_source LIKE '%%google%%'
                AND (
                         l.utm_campaign IS NOT NULL
                         AND l.utm_campaign != '(not set)'
                         AND l.utm_campaign != ''
                         AND l.utm_campaign != 'undefined'
                     ) THEN
                CASE
                    WHEN utm_campaign ILIKE '%%nonbrand%%' OR utm_campaign ILIKE '%%^_nb^_%%' ESCAPE '^' OR gateway_type ilike 'non branded%%' THEN 'Non Branded Search>Google>Doubleclick'
                    WHEN utm_campaign LIKE ANY ('%%instream%%','%%ytube%%','action_cpa_similaraudience_liveramp') THEN 'Youtube>Youtube>Adwords'
                    WHEN l.utm_source = 'google_gdn' OR l.utm_campaign ILIKE '%%gdn%%' THEN 'Programmatic-GDN>GDN>Doubleclick'
                    WHEN utm_campaign ILIKE '%%^_pla%%' ESCAPE '^' THEN 'Shopping>Google>Doubleclick'
                    WHEN utm_campaign ILIKE '%%brand%%' THEN 'Branded Search>Google>Doubleclick'
                    WHEN l.gateway_type = 'display' AND l.gateway_sub_type = 'gdn' THEN 'Programmatic-GDN>GDN>Doubleclick'
                END

            -- google/cpc edge cases
            WHEN utm_campaign ILIKE 'ytube%%' THEN 'Youtube>Youtube>Adwords'
            WHEN l.utm_source = 'google,google' and l.utm_medium = 'ppc,ppc' THEN 'Branded Search>Google>Doubleclick'
            WHEN l.utm_source = 'google' AND utm_campaign ILIKE '%%^_nb^_%%' ESCAPE '^' THEN 'Non Branded Search>Google>Doubleclick'
            WHEN l.utm_source IN ('google_brand','google') AND l.utm_medium = 'cpc' THEN 'Non Branded Search>Google>Doubleclick'
            WHEN l.utm_source IN ('gas', 'gad') THEN 'Non Branded Search>Google>Doubleclick'

            WHEN params_short IN ('◊◊◊google◊ppc◊','◊◊◊google◊◊','◊◊◊◊google◊') THEN 'Branded Search>Google>Doubleclick'
            WHEN params_short LIKE ANY ('%%bing,bing◊ppc,ppc◊','◊◊◊bing_brand◊cpc◊','%%bing,bing◊sem,sem◊','◊◊◊bing◊sem◊',
                           '◊◊◊bing◊◊', '◊◊◊bing◊(not set)◊') THEN 'Branded Search>Bing>Doubleclick'
            WHEN params_short IN ('◊◊◊google◊cpc◊','◊brand site◊none◊google◊cpc◊') THEN 'Non Branded Search>Google>Doubleclick'

            -- email
            WHEN l.utm_medium ILIKE '%%email%%' OR l.gateway_type ILIKE 'email%%' THEN
                CASE
                    WHEN l.utm_source = 'crosspromo' THEN 'CRM>Crosspromo'
                    WHEN l.utm_source IN ('sailthru', 'bm23') THEN 'CRM>Sailthru'
                    WHEN l.utm_source ='exacttarget' THEN 'CRM>Other'
                    WHEN l.utm_source IN ('active', 'stx') THEN 'CRM>Sailthru'
                    WHEN l.utm_source = 'wknd' THEN 'CRM>Wunderkind'
                    WHEN l.utm_source ilike 'tiera%%' THEN 'Influencers>Ambassador'
                    WHEN l.utm_source ilike '%%liveintent%%' THEN 'Testing>LiveIntent>LiveIntent'
                    ELSE 'CRM>Other'
                END
            WHEN params_short ILIKE '%%sailthru%%' AND l.utm_medium like any ('%%meta%%','web','sms','messenger','%%em%%')THEN 'CRM>Sailthru'
            WHEN (gateway_type ilike 'email%%' AND gateway_sub_type = 'x-promo') OR params_short = '◊organic◊emailxpromo◊◊◊'
                THEN 'CRM>Crosspromo'
            WHEN l.utm_source = 'browser' AND l.utm_medium LIKE '%%notification' THEN 'CRM>Browser'
            WHEN l.utm_medium = 'bpush' THEN 'CRM>Browser'
            WHEN l.utm_source = 'attentive' THEN 'CRM>Attentive'
            WHEN l.utm_source IN ('sde','sde2') THEN 'CRM>Sailthru'

            -- Programmatic - Display
            WHEN l.utm_medium = 'gdn' and l.utm_source = 'google' THEN 'Programmatic-GDN>GDN>Doubleclick'
            WHEN l.utm_medium = 'google' and l.utm_source = 'discovery' THEN 'Programmatic-GDN>Discovery>Doubleclick'
            WHEN l.utm_medium = 'discovery' and l.utm_source = 'geistm' THEN 'Programmatic-GDN>Discovery>GeistM'
            WHEN l.utm_medium = 'programmatic' AND l.utm_source = 'ttddooh' THEN 'Programmatic>TTDDOOH>TradeDesk'
            WHEN l.utm_medium = 'programmatic' AND lower(l.utm_source) = 'milkmoneyooh' THEN 'Programmatic>Programmatic>MilkMoney'
            WHEN l.utm_medium = 'programmatic' AND lower(l.utm_source) = 'ttdspotify' THEN 'Programmatic>TTDSpotify>Tradedesk'
            WHEN l.utm_medium like any ('%%disc%%','%%disocvery%%') and (l.utm_source ilike 'googl%%' or l.utm_source is null) THEN 'Programmatic-GDN>Discovery>Doubleclick'
            WHEN l.utm_source ilike '%%gdn%%' OR l.utm_medium ilike '%%gdn%%' OR utm_campaign ilike '%%gdn%%' THEN 'Programmatic-GDN>GDN>Doubleclick'
            WHEN gateway_type = 'display' AND gateway_sub_type = 'gdn' AND (l.utm_medium is null OR l.utm_source is null OR l.utm_source like 'google%%') THEN
                CASE
                   WHEN utm_campaign ilike '%%discovery%%' OR l.utm_medium = 'discovery' THEN 'Programmatic-GDN>Discovery>Doubleclick'
                   ELSE 'Programmatic-GDN>GDN>Doubleclick'
                END
            WHEN l.utm_medium like any ('display','%%retargeting%%') and l.utm_source ilike 'criteo%%' THEN 'Programmatic>Criteo>Criteo'
            WHEN l.gateway_type = 'display' and l.gateway_sub_type = 'criteo' THEN 'Programmatic>Criteo>Criteo'
            WHEN params_short in ('◊display◊◊◊◊','◊display◊unknown◊◊programmatic◊')
                THEN 'Programmatic>Display>Other'
            WHEN params_short = '◊display◊◊yahoo◊display◊' THEN 'Programmatic>Yahoo>Yahoo'
            WHEN l.utm_medium IN ('display', 'programmatic','programmmatic') AND l.utm_source IN ('thetradedesk', 'ttddisplay')
                THEN 'Programmatic>TTDDisplay>Tradedesk'
            WHEN l.utm_source in ('ttdonlinevideo', 'ttdvideo') THEN 'Programmatic>TTDOnlineVideo>Tradedesk'
            WHEN l.utm_source = 'ttdfepstreaming' AND l.utm_medium = 'programmatic' THEN 'TV+Streaming>TTDFEPStreaming>Tradedesk'
            WHEN l.utm_source = 'ttdctvstreaming' AND l.utm_medium = 'programmatic' THEN 'TV+Streaming>TTDCTVStreaming>Tradedesk'
            WHEN l.utm_source = 'rokuctvstreaming' AND l.utm_medium = 'programmatic' THEN 'TV+Streaming>RokuCTVStreaming>Roku'
            WHEN l.utm_source = 'rokufepstreaming' AND l.utm_medium = 'programmatic' THEN 'TV+Streaming>RokuFEPStreaming>Roku'
            WHEN l.utm_source = 'rokufepstreaming' AND l.utm_medium ilike '%%programmatic%%' THEN 'TV+Streaming>RokuFEPStreaming>Roku'
            WHEN (l.utm_medium = 'display' OR l.utm_medium LIKE 'ad%%') AND l.utm_source LIKE 'lifescript%%' THEN 'Programmatic>Other>Lifescript'
            WHEN l.utm_medium = 'display' AND l.utm_source = 'yahoo' THEN 'Programmatic>Yahoo>Yahoo'
            WHEN params_short LIKE '%%spoutable%%' THEN 'Programmatic>Display>Spoutable'
            WHEN params_short LIKE '%%liveintent%%' THEN 'Programmatic>Display>LiveIntent'
            WHEN params_short LIKE '%%dstillery%%' THEN 'Programmatic>Display>Dstillery'
            WHEN params_short LIKE '%%linkstorm%%' THEN 'Programmatic>Display>Linstorm'
            WHEN params_short LIKE '%%conversant%%' THEN 'Programmatic>Display>Conversant'
            WHEN params_short LIKE '%%bradsdeals%%' THEN 'Programmatic>Display>BradsDeals'
            WHEN params_short LIKE '%%popsugar%%' THEN 'Programmatic>Content/Native>PopSugar'
            WHEN params_short LIKE '%%rubicon%%' THEN 'Programmatic>Display>RubiconProject'
            WHEN params_short LIKE '%%divisiond%%' THEN 'Programmatic>Display>Divisiond'
            WHEN params_short LIKE '%%rewardstyle%%' THEN 'Programmatic>Display>rewardStyle'
            WHEN params_short LIKE '%%aol%%' THEN 'Programmatic>Display>AOL'
            WHEN params_short LIKE '%%pandora%%' THEN 'Programmatic>Display>Pandora'
            WHEN params_short LIKE '%%adperio%%' THEN 'Programmatic>Display>Adperio'
            WHEN params_short ILIKE '%%engageim%%' THEN 'Programmatic>Content/Native>EngageIm'
            WHEN l.utm_medium = 'display' AND l.utm_source IS NULL THEN 'Programmatic>TTDDisplay>Tradedesk'
            WHEN params_short LIKE '%%thetradedesk%%' THEN 'Programmatic>TTDDisplay>Tradedesk'
            WHEN utm_campaign ilike '%%discovery%%' THEN 'Programmatic-GDN>Discovery>Doubleclick'
            WHEN l.utm_medium = 'programmatic' AND l.utm_source IS NULL THEN 'Programmatic>Other'

            -- influencers
            WHEN l.utm_medium = 'ambassadors' OR l.utm_source = 'ambassadors' THEN 'Influencers>Ambassador'
            WHEN l.gateway_type = 'ambassadors' AND (l.utm_source IS NULL OR l.utm_source != 'paidinfluencer') THEN 'Influencers>Ambassador'
            WHEN l.utm_source ilike 'tiera%%' THEN 'Influencers>Ambassador'

            WHEN l.gateway_type = 'influencers' AND l.gateway_sub_type = 'tier 2' THEN 'Influencers>Influencer'
            WHEN l.utm_source = 'tierb' THEN 'Influencers>Influencer'

            WHEN l.utm_medium = 'influencers' AND l.utm_source ilike 'microinfluencers%%' THEN 'Influencers>MicroInfluencers'
            WHEN l.utm_medium = 'influencers' AND l.utm_source ilike 'streamers%%' THEN 'Influencers>MicroInfluencers'

            WHEN params_short IN ('◊◊◊microinfluencers◊◊') THEN 'Influencers>MicroInfluencers'

            WHEN params_short LIKE ANY ('%%tier1%%','%%tier 1%%') THEN 'Influencers>Collaborator'
            WHEN params_short LIKE ANY ('%%tier2%%','%%tier 2%%') THEN 'Influencers>Influencer'
            WHEN params_short LIKE ANY ('%%tier3%%','%%tier 3%%') THEN 'Influencers>Influencer'
            WHEN l.utm_medium NOT ILIKE 'paid_social%%'
                AND params_short ILIKE '%%influencer%%'
                AND params_short NOT ILIKE '%%response%%' THEN 'Influencers>Influencer'
            WHEN params_short IN ('◊◊◊influencersm◊◊') THEN 'Influencers>Influencer'

            -- non branded search
            WHEN regexp_like(params_short, '.*non.{0,1}branded.{0,1}search.*', 'is') THEN
                CASE
                    WHEN params_short ILIKE '%%google%%' THEN 'Non Branded Search>Google>Doubleclick'
                    WHEN params_short ILIKE '%%bing%%' THEN 'Non Branded Search>Bing>Doubleclick'
                    WHEN params_short ILIKE '%%yahoo%%'
                        OR params_short ILIKE '%%gem%%' THEN 'Non Branded Search>Yahoo>Doubleclick'
                    WHEN l.utm_source ilike 'pla%%' THEN 'Shopping>Google>Doubleclick'
                    ELSE 'Non Branded Search>Google>Doubleclick'
                END
            WHEN l.utm_medium LIKE ANY ('search_non%%','nonbrand%%') AND l.utm_source LIKE 'google%%'
                THEN 'Non Branded Search>Google>Doubleclick'
            WHEN l.utm_medium LIKE ANY ('search_non%%','nonbrand%%') AND l.utm_source LIKE 'gem%%'
                THEN 'Non Branded Search>Yahoo>Doubleclick'
            WHEN l.utm_medium LIKE ANY ('search_non%%','nonbrand%%') AND l.utm_source LIKE 'bing%%'
                THEN 'Non Branded Search>Bing>Doubleclick'
            WHEN params_short IN ('◊◊◊bing◊search_non_branded◊','◊◊◊bing%%20◊search_non_branded◊') THEN 'Non Branded Search>Bing>Doubleclick'

            WHEN params_short IN ('◊◊◊◊◊', '◊brand site◊◊◊◊', '◊◊◊(direct)◊search_nonbranded◊',
                                  '◊not applicable◊not applicable◊◊◊', '◊brand site◊unknown◊◊◊') AND
                 utm_campaign IS NULL THEN 'Direct Traffic>Other'

            WHEN params_short LIKE ANY ('%%non_branded◊%%','%%nonbrand%%','%%non branded search%%') AND l.utm_medium not like '%%shopping%%' THEN 'Non Branded Search>Google>Doubleclick'
            WHEN utm_campaign ILIKE '%%nonbrand%%' OR utm_campaign ILIKE '%%^_nb^_%%' ESCAPE '^' THEN 'Non Branded Search>Google>Doubleclick'

            -- branded search
            WHEN regexp_like(params_short, '.*(◊branded.{0,1}search|◊search.{0,1}brand).*', 'is') THEN
                CASE
                    WHEN params_short ILIKE '%%google%%' THEN 'Branded Search>Google>Doubleclick'
                    WHEN params_short ILIKE '%%bing%%' THEN 'Branded Search>Bing>Doubleclick'
                    WHEN l.utm_source = 'pla' THEN 'Shopping>Google>Doubleclick'
                    ELSE 'Branded Search>Google>Doubleclick'
                END
            WHEN l.utm_medium LIKE ANY ('search_b%%') AND l.utm_source LIKE 'google%%'
                THEN 'Branded Search>Google>Doubleclick'
            WHEN l.utm_medium LIKE ANY ('search_b%%') AND l.utm_source LIKE 'bing%%'
                THEN 'Branded Search>Bing>Doubleclick'
            WHEN l.utm_medium = 'ppc' AND l.utm_source = 'gemini' THEN 'Branded Search>Yahoo>Doubleclick'
            WHEN l.utm_medium ilike 'sem%%' AND l.utm_source ilike 'adwords%%' THEN 'Branded Search>Google>Doubleclick'
            WHEN params_short ILIKE '◊◊◊google_brand◊◊%%' THEN 'Branded Search>Google>Doubleclick'
            WHEN params_short = '◊◊◊bing◊ppc◊' THEN 'Branded Search>Bing>Doubleclick'
            WHEN params_short IN ('◊brand site◊◊yahoo_gemini◊cpc◊','◊◊◊yahoo_gemini◊cpc◊') THEN 'Branded Search>Yahoo>Doubleclick'

            WHEN l.utm_source ilike 'google%%' AND l.utm_source != 'google_gdn' AND
                 (l.utm_medium ILIKE '%%cpc%%' OR l.utm_medium ILIKE '%%ppc%%') THEN 'Branded Search>Google>Doubleclick'
            WHEN gateway_type = 'brand site' AND l.utm_source like any ('%%adwords%%','%%google%%','%%cpc%%')
                     AND l.utm_medium like '%%search' THEN 'Branded Search>Google>Doubleclick'
            WHEN utm_campaign ILIKE '%%brand%%' THEN 'Branded Search>Google>Doubleclick'

            -- organic social
            WHEN params_short ilike '%%organic_social%%' or params_short ilike '%%internal_social%%' THEN
                CASE
                    WHEN params_short ILIKE '%%pinterest%%' THEN 'Organic Social>Pinterest>Other'
                    WHEN params_short ILIKE '%%linktree%%' THEN 'Organic Social>Instagram>Other'
                    WHEN params_short ILIKE '%%instagram%%' THEN 'Organic Social>Instagram>Other'
                    WHEN params_short ILIKE '%%facebook%%' THEN 'Organic Social>Facebook>Other'
                    WHEN params_short ILIKE '%%youtube%%' THEN 'Organic Social>Youtube>Other'
                    WHEN params_short ILIKE '%%like2buy%%' THEN 'Organic Social>Like2Buy>Other'
                    ELSE 'Organic Social>Other'
                END
            WHEN l.utm_medium = 'internal_social_media' AND l.utm_source = 'tiktok' THEN 'Organic Social>TikTok'
            WHEN (l.utm_source = 'instagram' AND utm_campaign ILIKE '%%linkinbio%%') OR
                 (l.utm_source = 'instagram' AND l.utm_medium = 'owned_social_media') THEN 'Organic Social>Instagram>Other'
            WHEN params_short LIKE '%%curalate%%' OR params_short LIKE '%%like2buy%%' THEN 'Organic Social>Instagram>Other'
            WHEN l.utm_source IN ('facebook', 'facebook_socialf_media') AND
                 l.utm_medium IN ('internalsocialmedia', 'owned_social_media', 'facebook')
                THEN 'Organic Social>Facebook'
            WHEN (l.utm_source = 'blog' OR l.utm_medium ILIKE '%%blog%%') THEN 'Organic Social>Other'
            WHEN params_short IN ('◊◊◊instagram◊insternal_social_media◊','◊◊◊instagram◊organic%%20social◊') THEN 'Organic Social>Instagram>Other'
            WHEN params_short IN ('◊◊◊sside◊organic◊', '◊brand site◊◊sside◊organic◊','◊organic◊◊◊◊','◊◊◊ask◊organic◊',
                            '◊◊◊sside◊naturalss◊','◊◊◊sside◊◊','◊brand site◊none◊sside◊organic◊','◊organic◊none◊◊◊',
                           '◊◊◊◊owned_social_media◊', '◊◊◊◊lookbook◊','◊◊◊◊lookbook/◊','◊brand site◊none◊◊lookbook◊',
                            '◊◊◊homepage◊owned_social_media◊', '◊◊◊reelio◊internal_media◊','◊brand site◊unknown◊sside◊organic◊')THEN 'Organic Social>Other'
            WHEN gateway_type = 'organic' AND gateway_sub_type = 'social' THEN 'Organic Social>Other'
            WHEN l.utm_source ilike '%%twittersumazi%%' THEN 'Organic Social>Other'

            -- social
            WHEN l.utm_source = 'snapchat' OR params_short = '◊social◊snapchat◊◊◊' THEN 'Snapchat>Snapchat>Snapchat'
            WHEN l.utm_source = 'peoplehype' THEN 'FB+IG>Peoplehype>Peoplehype'
            WHEN params_short ILIKE '%%pinterest%%' THEN 'Pinterest>Pinterest>Pinterest'
            WHEN params_short ILIKE '%%instagram_story%%' THEN 'FB+IG>FB+IG>Facebook'

            WHEN regexp_like(params_short, '.*facebook[%%20b+ ]*instagram.*', 'is') THEN 'FB+IG>FB+IG>Facebook'
            WHEN (l.utm_source LIKE 'instagram%%' OR l.utm_source IN ('fba','igshopping')) AND l.utm_medium IN
                                                    ('paid_social_media', 'social', 'paid%%20social%%20media',
                                                     'social,social', 'paid_social_media,paid_social_media',
                                                     'paid social media','paid_social', 'paidsocial') THEN 'FB+IG>FB+IG>Facebook'
            WHEN l.utm_medium ilike '%%paid_social%%' AND l.utm_source is not null THEN
                CASE WHEN l.utm_source in ('ig','fb','msg','an','jf','speedy','sd')
                        or l.utm_source = '{{site_source_name}}'
                        or l.utm_source = 'shopmessage' THEN 'FB+IG>FB+IG>Facebook'
                     WHEN l.utm_source like '%%instagram%%' THEN 'FB+IG>FB+IG>Facebook'
                     WHEN l.utm_source like '%%facebook%%' then 'FB+IG>FB+IG>Facebook'
                     WHEN l.utm_source = 'pinterest' THEN 'Pinterest>Pinterest>Pinterest'
                     WHEN l.utm_source in ('influencers','paidinfluencer') THEN 'FB+IG>PaidInfluencers>Facebook'
                     WHEN l.utm_source ilike 'tubescienc%%' THEN 'FB+IG>TubeScience>Facebook'
                     WHEN l.utm_source = 'agencywithin' THEN 'FB+IG>AgencyWithin>Facebook'
                     WHEN l.utm_source ilike '%%tiktok%%' THEN 'TikTok>TikTok>TikTok'
                     WHEN l.utm_source ilike '%%reddit%%' THEN 'Reddit>Reddit>Reddit'
                     WHEN l.utm_source ilike '%%narrative%%' THEN 'FB+IG>Narrative>Facebook'
                     WHEN l.utm_source ilike '%%twitter%%' THEN 'Twitter>Twitter>Twitter'
                     WHEN l.utm_source ilike '%%geistm%%' THEN 'FB+IG>geistm>Facebook'
                     ELSE 'Unclassified'
                END
            WHEN l.gateway_type = 'social' AND l.gateway_sub_type = 'instagram' THEN 'FB+IG>FB+IG>Facebook'
            WHEN params_short = '◊◊◊narrative◊◊' THEN 'FB+IG>Narrative>Facebook'
            WHEN params_short IN ('◊◊◊tiktok◊◊') THEN 'TikTok>TikTok>TikTok'

            WHEN params_short IN ('◊◊◊instagram,instagram◊paid social media,paid social media◊',
                           '◊◊◊instagram,instagram◊paid%%2520social%%2520media,paid%%2520social%%2520media◊',
                           '◊◊◊instagram,instagram◊paid%%20social%%20media,paid%%20social%%20media◊','◊◊◊instagram◊◊',
                           '◊◊◊instagram◊(not set)◊','◊◊◊copy_link◊paid_social_media◊','◊◊◊copy_link◊paid_social_media◊',
                           '◊◊◊instagram◊social+◊','◊brand site◊none◊fb◊◊','◊◊◊fb◊◊','◊◊◊copy_link◊paid_social_media◊',
                            '◊lead retargeting◊◊◊◊','◊shopping◊◊facebook◊paid_social_media◊','◊◊◊fbf◊◊',
                            '◊brand site◊none◊ig◊(none)◊','◊brand site◊none◊instagram◊◊','◊brand site◊none◊ig◊◊','◊lead retargeting◊none◊◊◊',
                            '◊◊◊copy_link◊paid_social_media◊','◊brand site◊none◊fb◊(none)◊')
                THEN 'FB+IG>FB+IG>Facebook'

            WHEN params_short ILIKE '%%paid_social_media%%' OR params_short ILIKE '%%◊fba%%' OR params_short = '◊social◊◊◊◊' THEN 'FB+IG>FB+IG>Facebook'
            WHEN params_short ILIKE '%%facebook%%' AND l.utm_source != 'pla' AND l.utm_source != 'google' THEN 'FB+IG>FB+IG>Facebook'

            -- content/native
            WHEN params_short ILIKE '%%content/native%%powerspace%%' THEN 'Programmatic>Content/Native>Powerspace'
            WHEN params_short ILIKE '%%content/native%%outbrain%%' THEN 'Programmatic>Content/Native>Outbrain'
            WHEN params_short ILIKE '%%content/native%%outbrain%%' THEN 'Programmatic>Content/Native>Outbrain'
            WHEN params_short ILIKE '%%content/native%%' AND l.utm_source = 'popsugar'
                THEN 'Programmatic>Content/Native>Popsugar'
            WHEN params_short ILIKE '%%content/native%%' AND l.utm_source = 'buzzfeed'
                THEN 'Programmatic>Content/Native>Buzzfeed'
            WHEN params_short ILIKE '%%content/native%%' AND l.utm_source IN ('plista', 'refinery29')
                THEN 'Programmatic>Content/Native>Other'
            WHEN l.utm_medium IN ('programmatic', 'native') AND l.utm_source IN ('ttdnative', 'thetradedesk')
                THEN 'Programmatic>TTDNative>Tradedesk'
            WHEN params_short LIKE '%%outbrain%%' THEN 'Programmatic>Content/Native>Outbrain'
            WHEN params_short LIKE '%%powerspace%%' AND l.utm_source = 'powerspace'
                THEN 'Programmatic>Content/Native>Powerspace'
            WHEN l.utm_medium IN ('native', 'content', 'content/native', 'display') AND l.utm_source = 'taboola'
                THEN 'Programmatic>Content/Native>Taboola'
            WHEN l.utm_medium IN ('native', 'content', 'content/native', 'display') AND l.utm_source = 'outbrain'
                THEN 'Programmatic>Content/Native>Outbrain'
            WHEN params_short LIKE '%%taboola%%' AND l.utm_source != 'ttdvideo' THEN 'Programmatic>Content/Native>Taboola'
            WHEN params_short = '◊content/native◊◊◊◊' THEN 'Programmatic>Content/Native>Other'

            -- youtube
            WHEN l.utm_medium = 'youtube' AND l.utm_source = 'warnermusic' THEN 'Youtube>Youtube>WarnerMusic'
            WHEN l.utm_source = 'adwords' AND l.utm_medium ILIKE 'search%%' THEN 'Youtube>Youtube>Adwords'
            WHEN params_short ILIKE '%%youtube%%' AND ((l.utm_source IS NULL AND l.utm_medium IS NULL) OR l.utm_source ilike '%%youtube%%' OR l.utm_medium ilike '%%youtube%%') THEN 'Youtube>Youtube>Adwords'
            WHEN l.utm_medium = 'video' AND l.gateway_type = 'youtube' THEN 'Youtube>Youtube>Adwords'
            WHEN utm_campaign LIKE ANY ('%%instream%%','%%ytube%%','action_cpa_similaraudience_liveramp') THEN 'Youtube>Youtube>Adwords'


            --radio/podcast
            WHEN l.utm_medium = 'podcast' AND l.utm_source = 'podcorn' THEN 'Radio/Podcast>Other>Podcorn'
            WHEN l.utm_medium = 'podcast' AND lower(l.utm_source) = 'lolnetwork' THEN 'Radio/Podcast>Other>LOLNetwork'
            WHEN params_short ILIKE '%%influencer_response%%' THEN 'Radio/Podcast>Other>InfluencerResponse'
            WHEN params_short ILIKE '%%radio/podcast%%' OR params_short LIKE '%%◊podcast◊%%' THEN 'Radio/Podcast>Other>Other'

            -- blisspoint video / audio
            WHEN l.utm_source = 'blisspoint_pdora' AND l.utm_medium = 'streaming_audio' THEN 'Testing>Audio>Blisspoint' -- need to update to final streaming audio channel
            WHEN l.utm_source = 'blisspoint_scloud' AND l.utm_medium = 'streaming_audio' THEN 'Testing>Audio>Blisspoint' -- need to update to final streaming audio channel
            WHEN l.utm_source ILIKE '%%blisspoint%%' THEN 'TV+Streaming>Streaming>Blisspoint'
            WHEN l.params_short ILIKE '%%video%%' AND l.utm_source is NULL AND l.utm_medium IS NULL THEN 'TV+Streaming>Streaming>Blisspoint'
            WHEN params_short IN ('◊◊◊(direct)◊streaming_audio?utm_source=blisspoint_audio◊',
                                  '◊brand site◊none◊(direct)◊streaming_audio?utm_source=blisspoint_audio◊')
                THEN 'Testing>Audio>Blisspoint'

            -- other streaming
            when l.utm_medium ilike '%%streaming%%' and l.utm_source ilike '%%tatari%%' then 'TV+Streaming>Streaming>Tatari'
            WHEN l.utm_medium = 'streaming' and l.utm_source = 'twitch' THEN 'TV+Streaming>Streaming>Twitch'

            -- metanav
            WHEN params_short ILIKE '%%meta_5fnav%%' THEN
                CASE
                    WHEN params_short ILIKE '%%◊sd◊%%' THEN 'MetaNav>ShoeDazzle'
                    ELSE 'MetaNav>Other>Other'
                END
            WHEN l.utm_medium IN ('meta_nav', 'metanav', 'mobile_meta_nav' ,'meta%%255fnav') THEN
                CASE
                    WHEN l.utm_source IN ('jf', 'jfutm_campaign=metanav') THEN 'MetaNav>JustFab'
                    WHEN l.utm_source = 'fk' THEN 'MetaNav>Fabkids'
                    WHEN l.utm_source = 'fl2' THEN 'MetaNav>Fabletics'
                    WHEN l.utm_source = 'sd' THEN 'MetaNav>ShoeDazzle'
                    WHEN l.utm_source IN ('fl', 'fabletics', 'fl_metanav') THEN 'MetaNav>Fabletics'
                    ELSE 'Unclassified'
                END
            WHEN l.utm_source = 'fl_metanav' then 'Metanav>Fabletics'
            WHEN params_short = '◊◊◊sd◊(not set)◊' or l.utm_campaign = 'sd_metanav' THEN 'MetaNav>ShoeDazzle'
            WHEN params_short IN ('◊brand site◊◊fl_metanav◊web◊', '◊◊◊fl◊mobile_meta_nav◊', '◊◊◊fl_metanav◊web◊')
                THEN 'MetaNav>Fabletics'
           WHEN params_short IN ('◊◊◊sd◊◊', '◊brand site◊◊sd◊◊') THEN 'MetaNav>ShoeDazzle'
            WHEN l.utm_source = 'jf_metanav' THEN 'MetaNav>JustFab'
            WHEN params_short = '◊◊◊fl2◊organic◊' THEN 'MetaNav>Fabletics'

            -- shopping
            WHEN params_short IN ('◊◊◊pla,pla◊shopping,shopping◊', '◊◊◊pla◊◊', '◊shopping◊◊◊◊') THEN 'Shopping>Google>Doubleclick'
            WHEN l.utm_medium ilike '%%shopping%%' OR gateway_type = 'shopping' THEN
                CASE
                    WHEN l.utm_source in ('google','pla') THEN 'Shopping>Google>Doubleclick'
                    WHEN l.utm_source = 'connexity-cse' THEN 'Shopping>CSE>Shopzilla/Connexity'
                    WHEN params_short = '◊shopping◊◊google◊cpc◊' THEN 'Shopping>Google>Doubleclick'
                    WHEN params_short ilike '%%bing◊shopping%%' THEN 'Shopping>Bing>Bing'
                    WHEN params_short ilike '%%connexity%%' THEN 'Shopping>CSE>Shopzilla/Connexity'
                    ELSE 'Shopping>Google>Doubleclick' END
            WHEN params_short ilike '%%connexity%%' then 'Shopping>CSE>Shopzilla/Connexity'
            WHEN utm_campaign ILIKE '%%^_pla%%' ESCAPE '^' THEN 'Shopping>Google>Doubleclick'

            -- savage
            WHEN sm.channel = 'cross promo' THEN 'CRM>Crosspromo'
            WHEN sm.channel IS NOT NULL THEN sm.channel || '>' || sm.subchannel || '>' || sm.vendor

            -- savage referral
            WHEN l.utm_medium = 'referral' THEN 'Referral>Other>Other'

            -- affiliate
            WHEN regexp_like(params_short, '.*crosspromo.*', 'is') THEN 'Affiliate>Emailing>Emailing'
            WHEN params_short ILIKE '%%affiliate%%emailing%%' THEN 'Affiliate>Emailing>Other'
            WHEN params_short ILIKE '%%clicklab%%' THEN 'Affiliate>Emailing>Clicklab/Darwin'
            WHEN params_short = '◊◊◊darwin◊paid_email◊' THEN 'Affiliate>Emailing>Clicklab/Darwin'
            WHEN l.utm_medium = 'affiliates' and l.utm_source in ('veepee','privalia') THEN 'Affiliate>Privalia/Veepee>Privalia/Veepee'
            WHEN l.utm_campaign IS NULL AND gateway_type = 'affiliate' AND gateway_sub_type = 'privalia/veepee' THEN 'Affiliate>Privalia/Veepee>Privalia/Veepee'
            WHEN l.utm_source = 'linktrust' THEN 'Affiliate>Networks>Linktrust'
            WHEN (l.utm_medium LIKE 'affiliate%%' OR gateway_type LIKE 'affiliate%%') AND
                 l.utm_source = 'clickwork7' THEN 'Affiliate>Networks>Clickwork7'
            WHEN (l.utm_medium LIKE 'affiliate%%' OR gateway_type LIKE 'affiliate%%') AND
                 l.utm_source IN ('lc', 'al', NULL)
                THEN 'Affiliate>Networks>Other'
            WHEN (l.utm_medium LIKE 'affiliate%%' OR gateway_type LIKE 'affiliate%%') AND
                 l.utm_source LIKE '%%irs%%' THEN 'Affiliate>Networks>ImpactRadius'
            WHEN (l.utm_medium LIKE 'affiliate%%' OR gateway_type LIKE 'affiliate%%') AND
                 l.utm_source LIKE '%%cj%%' THEN 'Affiliate>Networks>CommissionJunction'
            WHEN (l.utm_medium LIKE 'affiliate%%' OR gateway_type LIKE 'affiliate%%') AND
                 l.utm_source LIKE '%%sovendus%%' THEN 'Affiliate>Networks>Sovendus'
            WHEN (l.utm_medium LIKE 'affiliate%%' OR gateway_type LIKE 'affiliate%%') AND
                 l.utm_source LIKE '%%avantlink%%' THEN 'Affiliate>Networks>Avantlink'
            WHEN l.utm_source LIKE '%%dqna%%' THEN 'Affiliate>Networks>DQNA'
            WHEN params_short = '◊affiliate◊coupons/vouchers◊swagbucks◊affiliates◊'
                THEN 'Affiliate>Networks>Swagbucks'
            WHEN (l.utm_medium LIKE 'affiliat%%' OR gateway_type LIKE 'affiliate%%')
                OR l.gateway_sub_type = 'networks'
                THEN 'Affiliate>Networks>Networks'
            WHEN params_short IN ('◊affiliate◊◊◊◊', '◊◊◊irs◊◊', '◊◊◊ir◊◊') THEN 'Affiliate>Networks>ImpactRadius'
            WHEN params_short IN ('◊◊◊gaw◊web◊', '◊◊◊eag◊web◊') THEN 'Affiliate>Networks>Other'
            WHEN params_short LIKE '%%◊irs◊%%' THEN 'Affiliate>Networks>ImpactRadius'

            --varsity
            WHEN l.utm_medium ILIKE '%%varsity%%' and l.utm_source ILIKE '%%varsity%%' THEN 'Physical Partnerships>Varsity>Varsity'

            --organic press
            WHEN l.utm_medium ILIKE '%%organic%%' and l.utm_source ILIKE '%%press%%' THEN 'Organic Press>Organic Press>Vanity'

            -- direct mail
            WHEN l.utm_medium ilike '%%directmail%%' and l.utm_source ilike '%%instyle%%' then 'Print>Direct Mail>InStyle'
            WHEN l.utm_medium ilike '%%directmail%%' and l.utm_source ilike '%%slm%%' then 'Print>Direct Mail>SLM'
            WHEN l.utm_medium ilike '%%directmail%%' then 'Print>Direct Mail>Other'

            -- clean up product feed stuff (not ideal)
            WHEN params_short LIKE '%%productfeed%%' THEN 'FB+IG>FB+IG>Facebook'
            WHEN params_short ILIKE '%%sailthru%%' THEN 'CRM>Sailthru'
            WHEN params_short LIKE '%%display%%' THEN 'Programmatic>Display>Other'
            WHEN l.gateway_type = 'social' THEN 'FB+IG>FB+IG>Facebook'

            -- baylie cleaning 2021
            when params_short in ('◊◊◊influencers◊◊','◊influencers◊none◊◊◊','◊influencers◊unknown◊◊◊') then 'Influencers>Influencer'
            when params_short = '◊◊◊googleutm_medium=discovery◊◊' then 'Programmatic-GDN>Discovery>Doubleclick'
            when params_short = '◊◊◊geistm◊gn◊' then 'FB+IG>geistm>Facebook'
            when params_short = '◊◊◊tubescience◊◊' then 'FB+IG>TubeScience>Facebook'
            when params_short in ('◊◊◊narrativ◊◊','◊◊◊narrative◊pay_social_media◊') then 'FB+IG>Narrative>Facebook'
            when params_short in ('◊◊◊googleutm_medium=search_branded◊◊','◊◊◊google search◊search◊') then 'Branded Search>Google>Doubleclick'
            when params_short in ('◊content/native◊taboola◊◊◊') then 'Programmatic>Content/Native>Taboola'
            when params_short in ('◊testing◊none◊◊◊','◊testing◊unknown◊◊◊') then 'Testing>Other>Other'
            when params_short = '◊brand site◊unknown◊email◊sfmc◊' then 'CRM>Other'
            when params_short in ('◊◊◊pushly◊browser_notifications◊','◊brand site◊none◊pushly◊browser_notifications◊') then 'CRM>Browser'
            when params_short in ('◊brand site◊none◊linktree◊social◊','◊◊◊instagram◊post◊','◊◊◊linktree◊instagram◊',
                                 '◊brand site◊unknown◊linktree◊social◊') then 'Organic Social>Instagram>Other'
            when params_short in ('◊print◊magazines◊◊◊','◊print◊catalog◊◊◊') then 'Print>Magazine>Magazine'
            when params_short in ('◊◊◊online◊press◊') then 'Organic Press>Organic Press>Vanity'

            -- direct traffic
            when params_short in ('◊brand site◊none◊duckduckgo◊organic◊','◊◊◊narvar◊narvarshippingpage◊','◊brand site◊none◊ecosia.org◊organic◊',
                                 '◊brand site◊none◊◊◊','◊brand site◊none◊narvar◊narvarshippingpage◊','◊brand site◊none◊landingpage◊landingpage◊',
                                 '◊◊◊landingpage◊landingpage◊','◊◊◊narvar◊◊','◊theme (savage x only)◊◊◊◊', '◊◊◊duckduckgo◊organic◊',
                                 '◊◊◊ecosia.org◊organic◊','◊◊◊klarna◊app◊','◊brand site◊none◊klarna◊app◊','◊◊◊qwant.com◊organic◊',
                                 '◊brand site◊unknown◊narvar◊narvarshippingpage◊','◊brand site◊unknown◊duckduckgo◊organic◊','◊brand site◊unknown◊ecosia.org◊organic◊',
                                 '◊brand site◊unknown◊klarna◊app◊','◊brand site◊none◊qwant.com◊organic◊','◊◊◊applenews◊applenews◊','◊brand site◊none◊hsy◊web◊',
                                 '◊brand site◊unknown◊landingpage◊landingpage◊','◊brand site◊unknown◊qwant.com◊organic◊','◊brand site◊none◊trustpilot◊company_profile◊',
                                 '◊◊◊trustpilot◊company_profile◊', '◊◊◊jf◊fabkidsdeadcell◊', '◊◊◊tws◊web◊','◊◊◊◊◊','◊◊◊◊◊'
                                 ) then 'Direct Traffic>Other'
            WHEN params_short = '◊◊◊none◊none◊none◊' THEN 'Direct Traffic>Other'

            END AS classification,
        l.gateway_type,
        l.gateway_sub_type,
        l.utm_source,
        l.utm_medium,
        split_part(classification, '>', 1) AS channel,
        split_part(classification, '>', 2) AS subchannel,
        split_part(classification, '>', 3) AS vendor,
        0 as event_count
    FROM _channels_to_map l
    LEFT JOIN _savage_mapping sm
        ON sm.utm_medium = l.utm_medium
        AND sm.utm_source = l.utm_source
        AND sm.utm_source = l.utm_source
) a;

create or replace temporary table _channel_type as
select m.*, c.channel_type
from _mapped_channels m
left join reporting_media_base_prod.lkp.channel_display_name c
    on m.channel = c.channel_key;

UPDATE _channel_type
SET channel = 'organic search',
    subchannel = lower(seo_vendor)
WHERE seo_vendor IS NOT NULL
    and channel IN ('unclassified', 'direct traffic');

----------------------------------------------------------------------------

TRUNCATE TABLE REPORTING_BASE_PROD.shared.media_source_channel_mapping_historical_pre_2021;

INSERT INTO REPORTING_BASE_PROD.shared.media_source_channel_mapping_historical_pre_2021
    (MEDIA_SOURCE_HASH, EVENT_SOURCE, CHANNEL_TYPE, CHANNEL, SUBCHANNEL, VENDOR, UTM_SOURCE, UTM_MEDIUM, UTM_CAMPAIGN, GATEWAY_TYPE, GATEWAY_SUB_TYPE, HDYH_VALUE, SEO_VENDOR)
select distinct
    media_source_hash,
    'session' as event_source,
    channel_type,
    channel,
    subchannel,
    vendor,
    left(utm_source,255) as utm_source,
    left(utm_medium,255) as utm_medium,
    left(utm_campaign,255) as utm_campaign,
    gateway_type,
    gateway_sub_type,
    '' as hdyh_value,
    seo_vendor
from _channel_type m;
