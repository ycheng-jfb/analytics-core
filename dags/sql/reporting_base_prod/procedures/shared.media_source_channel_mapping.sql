ALTER SESSION SET TIMEZONE='America/Los_Angeles';

SET target_table = 'shared.media_source_channel_mapping';
SET execution_start_time = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3);

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (
        SELECT
            $target_table AS table_name,
            NULLIF(dependent_table_name,$target_table) AS dependent_table_name,
            high_watermark_datetime AS new_high_watermark_datetime
            FROM (
            SELECT
                'shared.session' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM shared.session
            ) AS h
        ) AS s
    ON t.table_name = s.table_name
        AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
            t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = $execution_start_time
    WHEN NOT MATCHED
    THEN INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
        )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime
    );

SET wm_reporting_base_prod_shared_session = public.udf_get_watermark($target_table,'shared.session');

-- SESSION CHANNEL MAPPING: TO BE USED 2021 - PRESENT --

CREATE OR REPLACE TEMPORARY TABLE _channels_to_map AS
SELECT DISTINCT
       sc.media_source_hash,
       '◊' || NVL(sc.utm_medium, '') || '◊' ||
       NVL(sc.utm_source, '') || '◊' ||
       NVL(sc.utm_campaign, '') || '◊' AS params,
       sc.utm_source,
       sc.utm_medium,
       sc.utm_campaign,
       sc.gateway_type,
       sc.gateway_sub_type,
       sc.seo_vendor
FROM shared.session sc
WHERE TO_DATE(session_local_datetime) >= '2021-01-01'
  AND meta_update_datetime > $wm_reporting_base_prod_shared_session;

CREATE OR REPLACE TEMPORARY TABLE _mapped_channels AS
SELECT DISTINCT
       media_source_hash,
       params,
       COALESCE(LOWER(a.channel), 'unclassified')    AS channel,
       COALESCE(LOWER(a.subchannel), 'unclassified') AS subchannel,
       COALESCE(LOWER(a.vendor), 'unclassified')     AS vendor,
       LOWER(a.gateway_type)                         AS gateway_type,
       LOWER(a.gateway_sub_type)                     AS gateway_sub_type,
       LOWER(a.utm_source)                           AS utm_source,
       LOWER(a.utm_medium)                           AS utm_medium,
       LOWER(utm_campaign)                           AS utm_campaign,
       seo_vendor
FROM (SELECT media_source_hash,
             params,
             utm_medium,
             utm_source,
             utm_campaign,
             seo_vendor,
             gateway_type,
             gateway_sub_type,

             ------------------------------------------
             ------------------------------------------
             -- session channel mapping --
             CASE
                 ------------------------------------------
                 ------------------------------------------
                 -- organic social media
                 when utm_medium ilike '%%internal_social%%' then (
                     case
                         when utm_source ilike '%%facebook%%' then 'Organic Social>Facebook>Other'
                         when utm_source ilike '%%instagram%%' then 'Organic Social>Instagram>Other'
                         when utm_source ilike '%%pinterest%%' then 'Organic Social>Pinterest>Other'
                         when utm_source ilike '%%linktree%%' then 'Organic Social>Instagram>Other'
                         when utm_source ilike '%%tiktok%%' then 'Organic Social>TikTok>Other'
                         when utm_source ilike '%%youtube%%' then 'Organic Social>Youtube>Other'
                         when utm_source ilike '%%like2buy%%' then 'Organic Social>Like2Buy>Other'
                         else 'Organic Social>Other' end)

                 when utm_medium = 'social' and utm_source in ('linktree', 'linkin.bio') then 'Organic Social>Other'
                 when utm_medium = 'social' and utm_source = 'igshopping' then 'Organic Social>Instagram>Other'
                 when utm_medium = 'social' and utm_source = 'tiktok' then 'Organic Social>TikTok>Other'
                 when utm_medium = 'social' and utm_source = 'instagram' then 'Organic Social>Instagram>Other'

                 ----- PAID -----

                 -- paid social
                 WHEN utm_medium ILIKE ANY
                      ('%%paid_social%%', '%%social network paid%%', '%%social+network+paid%%', '%%paid%%') THEN (
                     -- fb+ig
                     CASE
                         WHEN utm_source ILIKE ANY ('%%facebook%%', '%%insta%%', '%%fb%%', '%%ig%%') THEN 'FB+IG>FB+IG>Facebook'
                         WHEN utm_source ILIKE '%%tubescience%%' THEN 'FB+IG>TubeScience>Facebook'
                         WHEN utm_source ILIKE '%%narrative%%' THEN 'FB+IG>Narrative>Facebook'
                         WHEN utm_source ILIKE '%%influencer%%' THEN 'FB+IG>PaidInfluencers>Facebook'
                         WHEN utm_source ILIKE '%%agencywithin%%' THEN 'FB+IG>AgencyWithin>Facebook'
                         WHEN utm_source ILIKE '%%geistm%%' THEN 'FB+IG>geistm>Facebook'
                         WHEN utm_source ILIKE '%%peoplehype%%' THEN 'FB+IG>PeopleHype>Facebook'
                         WHEN utm_source = 'fbfirstmedia' THEN 'FB+IG>First Media>Facebook'
                         WHEN utm_source = 'oneoneeightzero' THEN 'FB+IG>1180>Facebook'
                         WHEN utm_campaign ILIKE '%fb_campaign%' THEN 'FB+IG>FB+IG>Facebook'
                         WHEN utm_campaign ILIKE '%%client_share%%' THEN 'Organic Social>Other'
                         -- snapchat
                         WHEN utm_source ILIKE '%%snapchat%%' THEN 'Snapchat>Snapchat>Snapchat'
                         WHEN params IN ('◊paid_social_media◊◊successful_campaign_id◊',
                                         '◊paid_social_media◊◊8a7dda6a-63f7-4a87-a0f4-c0c3be3efd12◊',
                                         '◊paid_social_media◊◊b096142e-d522-4a2a-a29e-e94ec43a60da◊',
                                         '◊paid_social_media◊◊29f53631-dd09-4a73-be54-3a17b48ea5d7◊',
                                         '◊paid_social_media◊◊cecb1986-dcc5-48ac-a013-a7e2578769ad◊',
                                         '◊paid_social_media◊(direct)◊b096142e-d522-4a2a-a29e-e94ec43a60da◊',
                                         '◊paid_social_media◊◊nb:snapchat:snapchat:successful_campaign_id:successful_adset_id:successful_ad_id◊',
                                         '◊paid_social_media◊◊9ae60021-fd97-422c-8696-7982c29701b1◊')
                             THEN 'Snapchat>Snapchat>Snapchat'
                         -- tiktok
                         WHEN utm_source ILIKE '%%tiktok%%' THEN 'TikTok>TikTok>TikTok'
                         WHEN utm_source = 'tiktokfirstmedia' THEN 'TikTok>First Media>TikTok'
                         WHEN utm_campaign ILIKE ANY ('%%tk_campaign%%', '%%tiktok_campaign%%')
                             THEN 'TikTok>TikTok>TikTok'
                         -- pinterest
                         WHEN utm_source ILIKE '%%pinterest%%' THEN 'Pinterest>Pinterest>Pinterest'
                         -- twitter
                         WHEN utm_source ILIKE '%%twitter%%' THEN 'Twitter>Twitter>Twitter'
                         -- reddit
                         WHEN utm_source ILIKE '%%reddit%%' THEN 'Reddit>Reddit>Reddit'
                         -- tinder
                         WHEN utm_source ILIKE '%%tinder%%' THEN 'Tinder>Tinder>Tinder'

                         -- bot traffic
                         WHEN params IN
                              ('◊paid_social_media◊◊◊', '◊paid_social_media◊◊ ◊', '◊paid_social_media◊(direct)◊ ◊')
                             THEN 'Direct Traffic>Other'

                         ELSE 'Unclassified>Unclassified>Unclassified' END)

                 -- facebook other
                 WHEN utm_medium ILIKE '%%social%%' AND
                      utm_source ILIKE ANY ('%facebook+instagram%', '%facebook%', '%%instagram%%')
                     THEN 'FB+IG>FB+IG>Facebook'
                 WHEN utm_source ILIKE '%%audience_network%%' THEN 'FB+IG>FB+IG>Facebook'
                 WHEN utm_campaign ILIKE '%%fb_ad_id%%' OR utm_campaign ILIKE '%%fb_campaign_id%%'
                     THEN 'FB+IG>FB+IG>Facebook'
                 WHEN utm_campaign ILIKE '%fb_campaign%' THEN (
                     CASE
                         WHEN utm_source ILIKE '%%tubescience%%' THEN 'FB+IG>TubeScience>Facebook'
                         WHEN utm_source ILIKE '%%narrative%%' THEN 'FB+IG>Narrative>Facebook'
                         WHEN utm_source ILIKE '%%influencer%%' THEN 'FB+IG>PaidInfluencers>Facebook'
                         WHEN utm_source ILIKE '%%agencywithin%%' THEN 'FB+IG>AgencyWithin>Facebook'
                         WHEN utm_source ILIKE '%%geistm%%' THEN 'FB+IG>geistm>Facebook'
                         WHEN utm_source ILIKE '%%peoplehype%%' THEN 'FB+IG>PeopleHype>Facebook'
                         ELSE 'FB+IG>FB+IG>Facebook' END)
                 WHEN utm_source = 'geistm' THEN 'FB+IG>geistm>Facebook'
                 WHEN utm_medium ILIKE '%%instagram%%' THEN 'FB+IG>FB+IG>Facebook'
                 WHEN params IN
                      ('◊◊facebook◊◊', '◊◊instagram◊◊', '◊◊facebook◊lookalike_reach◊', '◊◊facebook◊lookalike_leads◊',
                       '◊display◊facebook◊◊', '◊◊facebook instagram◊◊', '◊instagram◊dash hudson◊likeshopme◊')
                     THEN 'FB+IG>FB+IG>Facebook'
                 WHEN params IN ('◊link◊peoplehype◊◊', '◊paid◊peoplehype◊◊', '◊◊peoplehype◊◊')
                     THEN 'FB+IG>PeopleHype>Facebook'
                 WHEN params IN ('◊gn◊geistm◊gn_productlisticle_journiest_2for24◊', '◊gn◊geistm◊◊',
                                 '◊gn◊geistm◊gn_pantseditorial_journiest_2for24◊') THEN 'FB+IG>geistm>Facebook'

                 -- paid social misc.
                 WHEN utm_campaign ILIKE ANY ('%%tk_campaign%%', '%%tiktok_campaign%%') THEN 'TikTok>TikTok>TikTok'
                 WHEN utm_medium IS NULL AND utm_source = 'tiktok' THEN 'TikTok>TikTok>TikTok'
                 WHEN params IN ('◊◊snapchat◊◊') THEN 'Snapchat>Snapchat>Snapchat'
                 WHEN utm_campaign ILIKE '%%pin_campaign_id%%' THEN 'Pinterest>Pinterest>Pinterest'
                 WHEN params IN ('◊◊pinterest◊◊', '◊◊pinterest?utm_medium=paid_social_media')
                     THEN 'Pinterest>Pinterest>Pinterest'
                 WHEN params IN ('◊◊tiktok◊◊') THEN 'TikTok>TikTok>TikTok'
                 WHEN utm_medium ILIKE '%%testing%%' AND utm_source ILIKE '%%reddit%%' THEN 'Reddit>Reddit>Reddit'
                 WHEN params IN ('◊testing◊◊◊') THEN 'Reddit>Reddit>Reddit'


                 -- youtube
                 WHEN utm_medium = 'youtube' THEN (
                     CASE
                         WHEN utm_source ILIKE '%%google%%' THEN 'Youtube>Youtube>Adwords'
                         WHEN utm_source ILIKE '%%warnermusic%%' THEN 'Youtube>Youtube>WarnerMusic'
                         WHEN utm_source IS NULL THEN 'Youtube>Youtube>Adwords'
                         WHEN params IN ('◊youtube◊annaakana◊◊') THEN 'Influencers>Influencer'
                         WHEN params IN ('◊video◊youtube+carys◊influencer+campaign+carys◊',
                                         '◊video◊youtube carys◊influencer campaign carys◊')
                             THEN 'Influencers>Influencer'
                         ELSE 'Unclassified>Unclassified>Unclassified' END)
                 WHEN utm_source = 'youtube carys' OR utm_source = 'youtube+carys' THEN 'Influencers>Influencer'
                 WHEN utm_medium = 'google' AND utm_source = 'youtube' THEN 'Youtube>Youtube>Adwords'
                 WHEN utm_medium = 'youtube_precise' AND utm_source = 'google' THEN 'Youtube>Youtube>PreciseTV'
                 WHEN utm_medium ILIKE '%%youtube%%' THEN 'Youtube>Youtube>Adwords'
                 WHEN utm_campaign ILIKE '%%ytube%%' THEN 'Youtube>Youtube>Adwords'
                 WHEN params IN ('◊video◊youtube◊◊') THEN 'Youtube>Youtube>Adwords'

                 -- gdn
                 WHEN utm_medium = 'gdn' AND utm_source = 'google' THEN 'Programmatic-GDN>GDN>Doubleclick'
                 WHEN utm_medium = 'google' AND utm_source ILIKE ANY ('%%gdn%%', '%%display%%')
                     THEN 'Programmatic-GDN>GDN>Doubleclick'
                 WHEN utm_medium = 'display' AND utm_source ILIKE ANY ('%%gdn%%', '%%google%%')
                     THEN 'Programmatic-GDN>GDN>Doubleclick'
                 WHEN utm_medium ILIKE '%%display%%' AND utm_source = 'google' THEN 'Programmatic-GDN>GDN>Doubleclick'
                 WHEN params IN
                      ('◊gdn◊◊◊', '◊gdn,gdn◊google◊◊', '◊◊googleutm_medium=gdn◊◊', '◊gdn◊bing◊justfab fr native◊',
                       '◊gdn◊google{ignore}◊◊', '◊◊gdn◊◊', '◊gdn{ignore}◊google◊◊', '◊gdn◊(direct)◊◊')
                     THEN 'Programmatic-GDN>GDN>Doubleclick'

                 WHEN utm_campaign ILIKE '%|gdn|_%' ESCAPE '|' THEN 'Programmatic-GDN>GDN>Doubleclick'

                 -- discovery
                 WHEN utm_medium = 'discovery' AND utm_source = 'google' THEN 'Programmatic-GDN>Discovery>Doubleclick'
                 WHEN utm_medium = 'google' AND utm_source ILIKE '%discovery%'
                     THEN 'Programmatic-GDN>Discovery>Doubleclick'
                 WHEN utm_medium = 'discovery' THEN (
                     CASE
                         WHEN utm_source = 'geistm' THEN 'Programmatic-GDN>Discovery>GeistM'
                         ELSE 'Programmatic-GDN>Discovery>Doubleclick' END)

                 WHEN utm_campaign ILIKE '%|_discovery|_%' ESCAPE '|' THEN 'Programmatic-GDN>Discovery>Doubleclick'
                 WHEN params IN ('◊◊googleutm_medium=discovery◊◊', '◊google◊◊◊', '◊discovery,discovery◊google◊◊',
                                 '◊discovery?utm_source=google,discovery◊google◊◊')
                     THEN 'Programmatic-GDN>Discovery>Doubleclick'


                 -- shopping
                 WHEN utm_medium = 'shopping' THEN (
                     CASE
                         WHEN utm_source ILIKE '%%google%%' THEN 'Shopping>Google>Doubleclick'
                         WHEN utm_source ILIKE '%%connexity%%' THEN 'Shopping>CSE>Shopzilla/Connexity'
                         WHEN utm_source ILIKE '%%bing%%' THEN 'Shopping>Bing>Bing'
                         ELSE 'Shopping>Google>Doubleclick' END)
                 WHEN params IN ('◊◊pla◊◊') THEN 'Shopping>Google>Doubleclick'
                 WHEN utm_medium ILIKE '%%shopping%%' AND utm_source ILIKE '%%google%%'
                     THEN 'Shopping>Google>Doubleclick'
                 WHEN utm_source = 'shopping' THEN 'Shopping>Google>Doubleclick'
                 WHEN utm_medium = 'shopping,shopping' AND utm_source = 'pla,pla' THEN 'Shopping>Google>Doubleclick'

                 -- non branded search
                 WHEN utm_medium IN ('search_non_branded', 'nonbrandsearch') OR utm_medium ILIKE '%%search_nonbrand%%'
                     THEN (
                     CASE
                         WHEN utm_source ILIKE '%%google%%' THEN 'Non Branded Search>Google>Doubleclick'
                         WHEN utm_source ILIKE '%%yahoo%%' THEN 'Non Branded Search>Yahoo>Doubleclick'
                         WHEN utm_source ILIKE '%%bing%%' THEN 'Non Branded Search>Bing>Doubleclick'
                         ELSE 'Non Branded Search>Google>Doubleclick' END)

                 WHEN utm_source = 'search_nonbranded' THEN (
                     CASE
                         WHEN utm_medium ILIKE '%%google%%' THEN 'Non Branded Search>Google>Doubleclick'
                         WHEN utm_medium ILIKE '%%yahoo%%' THEN 'Non Branded Search>Yahoo>Doubleclick'
                         WHEN utm_medium ILIKE '%%bing%%' THEN 'Non Branded Search>Bing>Doubleclick'
                         ELSE 'Unclassified>Unclassified>Unclassified' END)

                 WHEN params = '◊◊◊{campaign}◊' THEN (
                     CASE
                         WHEN gateway_type = 'branded search' THEN 'Branded Search>Google>Doubleclick'
                         ELSE 'Non Branded Search>Google>Doubleclick' END)
                 WHEN utm_medium = 'search' AND utm_source = 'google' THEN 'Non Branded Search>Google>Doubleclick'


                 -- branded search
                 WHEN utm_medium = 'search_branded' OR utm_medium = 'search_brand' THEN (
                     CASE
                         WHEN utm_source ILIKE '%%google%%' THEN 'Branded Search>Google>Doubleclick'
                         WHEN utm_source ILIKE '%%yahoo%%' THEN 'Branded Search>Yahoo>Doubleclick'
                         WHEN utm_source ILIKE '%%bing%%' THEN 'Branded Search>Bing>Doubleclick'
                         ELSE 'Branded Search>Google>Doubleclick' END)

                 WHEN utm_source = 'search_branded' THEN (
                     CASE
                         WHEN utm_medium ILIKE '%%google%%' THEN 'Branded Search>Google>Doubleclick'
                         WHEN utm_medium ILIKE '%%yahoo%%' THEN 'Branded Search>Yahoo>Doubleclick'
                         WHEN utm_medium ILIKE '%%bing%%' THEN 'Branded Search>Bing>Doubleclick'
                         ELSE 'Unclassified>Unclassified>Unclassified' END)
                 WHEN params IN ('◊◊◊us_fl_brand_fabletics◊', '◊search_branded,search_branded◊google,google◊◊')
                     THEN 'Branded Search>Google>Doubleclick'

                 -- pmax
                 WHEN CONTAINS(utm_campaign, '_pmax') OR CONTAINS(utm_campaign, 'performancemax')
                     THEN 'Shopping>Google>Doubleclick'
                 WHEN CONTAINS(utm_medium, 'performance_max') THEN 'Shopping>Google>Doubleclick'


                 -- old search + shopping using utm_campaign
                 WHEN utm_medium ILIKE '%%cpc%%' AND utm_source ILIKE '%%google%%' THEN (
                     CASE
                         WHEN utm_campaign ILIKE '%%pla%%' THEN 'Shopping>Google>Doubleclick'
                         WHEN utm_campaign ILIKE ANY ('%%nb%%', '%%nonbrand%%')
                             THEN 'Non Branded Search>Google>Doubleclick'
                         WHEN utm_campaign ILIKE '%brand%' THEN 'Branded Search>Google>Doubleclick'
                         ELSE 'Branded Search>Google>Doubleclick' END)

                 WHEN utm_medium ILIKE '%%cpc%%' AND utm_source ILIKE '%%bing%%' THEN (
                     CASE
                         WHEN utm_campaign ILIKE '%%pla%%' THEN 'Shopping>Bing>Doubleclick'
                         WHEN utm_campaign ILIKE ANY ('%%nb%%', '%%nonbrand%%')
                             THEN 'Non Branded Search>Bing>Doubleclick'
                         WHEN utm_campaign ILIKE '%brand%' THEN 'Branded Search>Bing>Doubleclick'
                         ELSE 'Branded Search>Bing>Doubleclick' END)

                 WHEN utm_medium ILIKE '%%cpc%%' AND utm_source ILIKE '%%yahoo%%' THEN (
                     CASE
                         WHEN utm_campaign ILIKE '%%pla%%' THEN 'Shopping>Yahoo>Doubleclick'
                         WHEN utm_campaign ILIKE ANY ('%%nb%%', '%%nonbrand%%')
                             THEN 'Non Branded Search>Yahoo>Doubleclick'
                         WHEN utm_campaign ILIKE '%brand%' THEN 'Branded Search>Yahoo>Doubleclick'
                         ELSE 'Branded Search>Yahoo>Doubleclick' END)

                 WHEN utm_campaign ILIKE '%|_pla|_%' ESCAPE '|' THEN 'Shopping>Google>Doubleclick'
                 WHEN utm_campaign ILIKE '%|_nb|_%' ESCAPE '|' THEN 'Non Branded Search>Google>Doubleclick'
                 WHEN utm_campaign ILIKE '%|_brand%' ESCAPE '|' THEN 'Branded Search>Google>Doubleclick'

                 WHEN params IN ('◊ppc◊google◊◊', '◊◊google◊◊', '◊testing◊google◊◊', '◊search◊google search◊◊',
                                 '◊search_branded,search_branded◊google,google◊,◊',
                                 '◊◊googleutm_medium=search_branded◊◊',
                                 '◊search_brand◊google◊137774800◊') THEN 'Branded Search>Google>Doubleclick'

                 -- affiliate
                 WHEN utm_medium ILIKE '%%affiliate%%' THEN (
                     CASE
                         WHEN utm_source ILIKE '%%irs%%' THEN 'Affiliate>Networks>ImpactRadius'
                         WHEN utm_source ILIKE '%%cj%%' THEN 'Affiliate>Networks>CommissionJunction'
                         WHEN utm_source ILIKE ANY ('%%veepee%%', '%%privalia%%')
                             THEN 'Affiliate>Privalia/Veepee>Privalia/Veepee'
                         WHEN utm_source ILIKE '%%clicklab%%' THEN 'Affiliate>Emailing>Clicklab/Darwin'
                         WHEN utm_source ILIKE '%%linktrust%%' THEN 'Affiliate>Networks>Linktrust'
                         WHEN utm_source ILIKE '%%clickwork7%%' THEN 'Affiliate>Networks>Clickwork7'
                         WHEN utm_source ILIKE '%%sovendus%%' THEN 'Affiliate>Networks>Sovendus'
                         WHEN utm_source ILIKE '%%avantlink%%' THEN 'Affiliate>Networks>Avantlink'
                         WHEN utm_source ILIKE '%%dqna%%' THEN 'Affiliate>Networks>DQNA'
                         WHEN utm_source ILIKE '%%swagbucks%%' THEN 'Affiliate>Networks>Swagbucks'
                         WHEN utm_source ILIKE '%%gntm%%' OR utm_source IS NULL THEN 'Affiliate>Networks>Other'
                         WHEN utm_source ILIKE ANY ('%%lc%%', '%%al%%', '%%(direct)%%') THEN 'Affiliate>Networks>Other'
                         WHEN params IN ('◊affiliate◊famebit◊◊', '◊web◊gaw◊affiliates◊') THEN 'Affiliate>Networks>Other'
                         WHEN params IN ('◊affiliates◊influencersm◊bienetreetfitness◊') THEN 'Influencers>Influencer'
                         WHEN utm_source ILIKE '%%corporatebenefits%%' THEN 'Affiliate>Networks>Other'
                         ELSE 'Unclassified>Unclassified>Unclassified' END)
                 WHEN utm_medium = 'testing' AND utm_source ILIKE '%%venga%%' THEN 'Affiliate>Venga>Venga'
                 WHEN utm_medium = 'testing' AND utm_source ILIKE '%%ebg%%' THEN 'Affiliate>EBG>EBG'
                 WHEN utm_medium = 'dedicado' THEN 'Affiliate>Networks>Dedicado'
                 WHEN utm_source = 'irs' THEN 'Affiliate>Networks>ImpactRadius'
                 WHEN utm_source = 'cj' THEN 'Affiliate>Networks>CommissionJunction'
                 WHEN utm_source = 'linktrust' THEN 'Affiliate>Networks>Linktrust'

                 -- programmatic
                 WHEN utm_medium ILIKE ANY ('%%programmatic%%', '%%display%%') THEN (

                     -- ttd
                     CASE
                         WHEN utm_source ILIKE ANY ('%%ttddisplay%%', '%%tradedesk%%')
                             THEN 'Programmatic>TTDDisplay>Tradedesk'
                         WHEN utm_source ILIKE '%%ttdnative%%' THEN 'Programmatic>TTDNative>Tradedesk'
                         WHEN utm_source ILIKE '%%ttdaudio%%' THEN 'Programmatic>TTDAudio>Tradedesk'
                         WHEN utm_source ILIKE ANY ('%%ttdonlinevideo%%', '%%ttdvideo%%')
                             THEN 'Programmatic>TTDOnlineVideo>Tradedesk'
                         WHEN utm_source ILIKE '%%ttddooh%%' THEN 'Programmatic>TTDDOOH>TradeDesk'
                         WHEN utm_source ILIKE '%%ttdspotify%%' THEN 'Programmatic>TTDSpotify>Tradedesk'
                         -- display
                         WHEN utm_source ILIKE '%%spoutable%%' THEN 'Programmatic>Display>Spoutable'
                         WHEN utm_source ILIKE '%%liveintent%%' THEN 'Programmatic>Display>LiveIntent'
                         WHEN utm_source ILIKE '%%dstillery%%' THEN 'Programmatic>Display>Dstillery'
                         WHEN utm_source ILIKE '%%linkstorm%%' THEN 'Programmatic>Display>Linstorm'
                         WHEN utm_source ILIKE '%%conversant%%' THEN 'Programmatic>Display>Conversant'
                         WHEN utm_source ILIKE '%%bradsdeals%%' THEN 'Programmatic>Display>BradsDeals'
                         WHEN utm_source ILIKE '%%rubicon%%' THEN 'Programmatic>Display>RubiconProject'
                         WHEN utm_source ILIKE '%%divisiond%%' THEN 'Programmatic>Display>Divisiond'
                         WHEN utm_source ILIKE '%%rewardstyle%%' THEN 'Programmatic>Display>rewardStyle'
                         WHEN utm_source ILIKE '%%aol%%' THEN 'Programmatic>Display>AOL'
                         WHEN utm_source ILIKE '%%pandora%%' THEN 'Programmatic>Display>Pandora'
                         WHEN utm_source ILIKE '%%adperio%%' THEN 'Programmatic>Display>Adperio'
                         WHEN utm_source ILIKE '%%yumpu%%' THEN 'Programmatic>Display>Yumpu'
                         WHEN utm_source ILIKE '%%stackadapt%%' THEN 'Programmatic>Display>StackAdapt'
                         -- content / native
                         WHEN utm_source ILIKE '%%popsugar%%' THEN 'Programmatic>Content/Native>PopSugar'
                         WHEN utm_source ILIKE '%%engageim%%' THEN 'Programmatic>Content/Native>EngageIm'
                         WHEN utm_source ILIKE '%%outbrain%%' THEN 'Programmatic>Content/Native>Outbrain'
                         WHEN utm_source ILIKE '%%taboola%%' THEN 'Programmatic>Content/Native>Taboola'
                         WHEN utm_source ILIKE '%%powerspace%%' THEN 'Programmatic>Content/Native>Powerspace'
                         WHEN utm_source ILIKE '%%plista%%' THEN 'Programmatic>Content/Native>Plista'
                         WHEN utm_source ILIKE '%%refinery29%%' THEN 'Programmatic>Content/Native>Refinery29'
                         WHEN utm_source ILIKE '%%buzzfeed%%' THEN 'Programmatic>Content/Native>Buzzfeed'
                         -- other
                        when utm_source ilike '%linkedin%' then 'Programmatic>LinkedIn>LinkedIn'
                         WHEN utm_source ILIKE '%%yahoo%%' THEN 'Programmatic>Yahoo>Yahoo'
                         WHEN utm_source ILIKE '%%rokt%%' THEN 'Programmatic>ROKT>ROKT'
                         WHEN utm_source ILIKE '%%criteo%%' THEN 'Programmatic>Criteo>Criteo'
                         WHEN utm_source ILIKE '%%milkmoneyooh%%' THEN 'Programmatic>Programmatic>MilkMoney'
                         WHEN utm_source ILIKE '%%nift%%' THEN 'Programmatic>NIFT>NIFT'
                         WHEN utm_source ILIKE '%applovin%%' THEN 'Programmatic>AppLovin>AppLovin'
                         WHEN utm_source ILIKE '%%vinted%%' THEN 'Programmatic>Vinted>Vinted'
                         WHEN utm_source ILIKE '%%amzntwitch%%' THEN 'Programmatic>Twitch>Amazon'
                         WHEN utm_source ILIKE '%%amznimdb%%' THEN 'Programmatic>IMDB>Amazon'
                         WHEN utm_source IS NULL THEN 'Programmatic>Display>Other'
                         WHEN params IN ('◊programmatic◊◊lgvj6zo◊', '◊programmatic◊(direct)◊lgvj6zo◊')
                             THEN 'Programmatic>Display>Other'

                         -- streaming
                         WHEN utm_source ILIKE '%%ttdctvstreaming%%' THEN 'TV+Streaming>TTDCTVStreaming>Tradedesk'
                         WHEN utm_source ILIKE '%%ttdfepstreaming%%' THEN 'TV+Streaming>TTDFEPStreaming>Tradedesk'
                         WHEN utm_source ILIKE '%%rokuctvstreaming%%' THEN 'TV+Streaming>RokuCTVStreaming>Roku'
                         WHEN utm_source ILIKE '%%rokufepstreaming%%' THEN 'TV+Streaming>RokuFEPStreaming>Roku'
                         ELSE 'Unclassified>Unclassified>Unclassified' END)

                 WHEN utm_medium = 'retargeting' AND utm_source ILIKE '%%criteo%%' THEN 'Programmatic>Criteo>Criteo'
                 WHEN utm_medium = 'email' AND utm_source = 'liveintent' THEN 'Testing>LiveIntent>LiveIntent'
                 WHEN params IN ('◊◊ttddisplay◊◊', '◊◊thetradedesk◊◊', '◊programmmatic◊ttddisplay◊dalxsvz◊')
                     THEN 'Programmatic>TTDDisplay>Tradedesk'
                 WHEN utm_source = 'ttddisplay' AND utm_medium = 'programmmatic'
                     THEN 'Programmatic>TTDDisplay>Tradedesk'
                 WHEN params IN ('◊native◊thetradedesk◊6kzpkmb◊') THEN 'Programmatic>TTDNative>Tradedesk'
                 WHEN utm_source = 'thetradedesk' AND utm_medium = 'video' THEN 'Programmatic>TTDOnlineVideo>Tradedesk'


                 -- tv + streaming
                when utm_medium = 'tv' then (
                    case when utm_source = 'tatari' then 'TV+Streaming>TV>Tatari'
                        else 'TV+Streaming>TV>Other' end)

                WHEN utm_medium ILIKE ANY ('%%streaming%%', '%%video%%') THEN (
                     CASE
                         WHEN utm_source = 'blisspoint' THEN 'TV+Streaming>Streaming>Blisspoint'
                         WHEN utm_source = 'tatari' THEN 'TV+Streaming>Streaming>Tatari'
                         WHEN utm_source = 'twitch' THEN 'TV+Streaming>Streaming>Twitch'
                         WHEN utm_source = 'stackadapt' THEN 'TV+Streaming>Streaming>StackAdapt'
                         WHEN utm_source = 'mntn' THEN 'TV+Streaming>Streaming>Mntn'
                         WHEN utm_source IN ('blisspoint_pdora', 'blisspoint_scloud') THEN 'Testing>Audio>Blisspoint'
                         WHEN params IN ('◊streaming_audio?utm_source=blisspoint_audio◊(direct)◊◊',
                                         '◊streaming_audio?utm_source=blisspoint_audio◊◊◊')
                             THEN 'Testing>Audio>Blisspoint'
                         WHEN params IN
                              ('◊streaming◊◊faopliv130h◊', '◊streaming◊◊faemq2v130h◊', '◊streaming◊◊faoqqbv130h◊',
                               '◊streaming◊◊faopqbv130h◊') THEN 'TV+Streaming>Streaming>Other'
                         ELSE 'Unclassified>Unclassified>Unclassified' END)


                 -- influencers
                 WHEN utm_medium ILIKE '%%ambassador%%' OR utm_source ILIKE '%%ambassador%%'
                     THEN 'Influencers>Ambassador'
                 WHEN utm_medium ILIKE '%%influencer%%' THEN (
                     CASE
                         WHEN utm_source ILIKE '%%collaborator%%' THEN 'Influencers>Collaborator'
                         WHEN utm_source ILIKE '%%ambassador%%' THEN 'Influencers>Ambassador'
                         WHEN utm_source ILIKE '%%influencer%%' THEN 'Influencers>Influencer'
                         WHEN utm_source ILIKE '%%collegeconnector%%' THEN 'Influencers>CollegeConnector'
                         WHEN utm_source ILIKE ANY ('%%micro%%', '%%streamer%%') THEN 'Influencers>MicroInfluencers'
                         ELSE 'Influencers>Influencer' END)

                 WHEN params IN
                      ('◊◊influencers◊◊', '◊ig◊influencer◊◊', '◊◊influencers◊2524448◊', '◊ig◊influencer◊2960218◊',
                       '◊◊◊2795529◊', '◊instagram◊linktree◊shop+my+fitness+outfits◊',
                       '◊◊influencersm◊bienetreetfitness◊',
                       '◊ig◊influencer◊3013285◊', '◊◊◊137774800◊', '◊ig◊influencer◊2848297◊', '◊◊influencers◊2717263◊',
                       '◊◊influencers◊1962007◊', '◊◊influencers◊2283551◊', '◊◊influencers◊1330367◊',
                       '◊◊influencers◊2473743◊') THEN 'Influencers>Influencer'
                 WHEN params IN ('◊◊microinfluencers◊sdt32017◊', '◊◊influencers◊2350178◊')
                     THEN 'Influencers>MicroInfluencers'


                 -- radio / podcast
                 WHEN utm_medium ILIKE '%%podcast%%' THEN (
                     CASE
                         WHEN utm_source = 'podcorn' THEN 'Radio/Podcast>Other>Podcorn'
                         WHEN utm_source = 'lolnetwork' THEN 'Radio/Podcast>Other>LOLNetwork'
                         WHEN utm_source ILIKE '%%influencer_response%%' THEN 'Radio/Podcast>Other>InfluencerResponse'
                         WHEN utm_source ILIKE '%%bizzimumzi%%' THEN 'Radio/Podcast>Other>BizziMumzi'
                         ELSE 'Radio/Podcast>Other>Other' END)


                 -- print
                 WHEN utm_medium ILIKE '%%directmail%%' THEN (
                     CASE
                         WHEN utm_source ILIKE '%%instyle%%' THEN 'Print>Direct Mail>InStyle'
                         WHEN utm_source ILIKE '%%slm%%' THEN 'Print>Direct Mail>SLM'
                         WHEN utm_source ILIKE '%%pebblepost%%' THEN 'Print>Direct Mail>PebblePost'
                         WHEN utm_source ILIKE '%%epsilon%%' THEN 'Print>Direct Mail>Epsilon'
                         ELSE 'Print>Direct Mail>Other' END)
                 WHEN utm_medium ILIKE '%%print%%' THEN (
                     CASE
                         WHEN utm_source ILIKE '%%anccemail%%' THEN 'Print>ANCC>Email'
                         WHEN utm_source ILIKE '%%anccbanner%%' THEN 'Print>ANCC>Banner'
                         WHEN utm_source ILIKE '%%anccbaginsert%%' THEN 'Print>ANCC>Bag Insert'
                         WHEN utm_source ILIKE ANY ('%%vmxconference23%%') THEN 'Print>Other>Other'
                         WHEN utm_source ILIKE '%%insert%%' THEN (
                             CASE
                                 WHEN utm_campaign ILIKE '%%scrubsinsertretail%%' THEN 'Print>Insert>Scrubs Retail'
                                 WHEN utm_campaign ILIKE '%%scrubsinsertecomm%%' THEN 'Print>Insert>Scrubs Ecomm' END)
                         WHEN utm_campaign ILIKE '%%scrubsconfhandout%%' THEN 'Print>Other>Scrubs Conf Handouts'
                         ELSE 'Print>Other>Other' END)

                 WHEN params IN ('◊parcelinserts◊inparcelinserts◊◊') THEN 'Print>Direct Mail>Other'
                 WHEN params IN ('◊parcelinserts◊parcelinserts◊◊') THEN 'Print>Direct Mail>Other'
                 -- testing
                 WHEN utm_medium = 'testing' and utm_source = 'claim' THEN 'Testing>Claim>Claim'
                 WHEN utm_source = 'condenast' THEN 'Testing>CondeNast>CondeNast'
                 WHEN utm_source = 'adquickooh' OR utm_source = 'adquick' THEN 'Testing>AdQuick>AdQuick'
                 WHEN utm_source = 'amazon' THEN 'Testing>Amazon>Amazon'
                 WHEN utm_medium = 'cpm' AND utm_source = 'display' AND utm_campaign = 'womens_billboard_q4_2023'
                     THEN 'Testing>OOH>Maximum Media'
                 WHEN utm_medium = 'testing' AND utm_source = 'nextdoor' THEN 'Testing>Nextdoor>Nextdoor'
                 WHEN utm_medium = 'testing' AND utm_source = 'hospitalclinicooh' AND utm_campaign = 'scrubsoohh12024'
                     THEN 'Testing>OOH>ScrubsOOHH12024'
                 WHEN utm_medium = 'testing' AND utm_source = 'hospitalclinicooh' AND utm_campaign = 'scrubsoohh12024qr'
                     THEN 'Testing>OOH>ScrubsOOHH12024'
                 ------------------------------------------
                 ------------------------------------------
                 ----- ORGANIC -----

                 -- crm

                 WHEN CONTAINS(utm_medium, 'email') THEN (
                     CASE
                         WHEN CONTAINS(utm_source, 'iterable') THEN 'CRM>Email>Iterable'
                         WHEN CONTAINS(utm_source, 'sailthru') THEN 'CRM>Email>Sailthru'
                         WHEN CONTAINS(utm_source, 'emarsys') THEN 'CRM>Email>Emarsys'
                         ELSE 'CRM>Email>Other' END)
                 WHEN CONTAINS(utm_medium, 'sms') THEN (
                     CASE
                         WHEN CONTAINS(utm_source, 'iterable') THEN 'CRM>SMS>Iterable'
                         WHEN CONTAINS(utm_source, 'attentive') THEN 'CRM>SMS>Attentive'
                         WHEN CONTAINS(utm_source, 'sailthru') THEN 'CRM>SMS>Sailthru'
                         WHEN CONTAINS(utm_source, 'emarsys') THEN 'CRM>SMS>Emarsys'
                         ELSE 'CRM>SMS>Other' END)
                WHEN CONTAINS(utm_medium, 'push') THEN (
                        CASE
                            WHEN CONTAINS(utm_source, 'iterable') THEN 'CRM>Push>Iterable'
                            WHEN CONTAINS(utm_source, 'attentive') THEN 'CRM>Push>Attentive'
                            WHEN CONTAINS(utm_source, 'sailthru') THEN 'CRM>Push>Sailthru'
                            WHEN CONTAINS(utm_source, 'emarsys') THEN 'CRM>Push>Emarsys'
                            ELSE 'CRM>Push>Other' END)
                 WHEN CONTAINS(utm_medium, 'narvar') THEN 'CRM>Narvar>Other'

                 WHEN CONTAINS(utm_source, 'sailthru') OR CONTAINS(utm_source, 'sde') THEN 'CRM>Email>Sailthru'
                 WHEN CONTAINS(utm_medium, 'notification') OR CONTAINS(utm_medium, 'push') THEN 'CRM>Push>Other'
                 WHEN CONTAINS(utm_source, 'emarsys') THEN 'CRM>Email>Emarsys'
                 WHEN CONTAINS(utm_source, 'attentive') THEN 'CRM>SMS>Attentive'
                 WHEN CONTAINS(utm_source, 'iterable') THEN 'CRM>SMS>Iterable'

                 WHEN utm_medium = 'web-channel' AND
                      utm_source ILIKE ANY ('%%bounceback%%', '%%downgrade%%', '%%sale%%') THEN 'CRM>Email>Other'
                 WHEN params IN ('◊email◊afterpay◊newsletter◊', '◊web-channel◊failed-billers-april-fy22q2◊◊',
                                 '◊web-channel◊lapsed-factor-april-fy22q2◊◊') THEN 'CRM>Email>Other'

                 -- other organic social

                 WHEN utm_medium = 'rihanna' AND utm_source = 'igbio' THEN 'Rihanna>Instagram>Bio'

                 WHEN utm_medium ILIKE ('%%linkinbio%%') OR utm_medium ILIKE ('%%link_in_bio%%') OR utm_campaign = 'bio'
                     THEN (
                     CASE
                         WHEN utm_source ILIKE '%%pinterest%%' THEN 'Organic Social>Pinterest>Other'
                         WHEN utm_source ILIKE '%%instagram%%' THEN 'Organic Social>Instagram>Other'
                         WHEN utm_source ILIKE '%%facebook%%' THEN 'Organic Social>Facebook>Other'
                         WHEN utm_source ILIKE '%%youtube%%' THEN 'Organic Social>Youtube>Other'
                         WHEN utm_source ILIKE '%%tiktok%%' THEN 'Organic Social>TikTok>Other'
                         ELSE 'Organic Social>Other' END)

                 WHEN utm_medium = 'story' AND utm_source = 'instagram' THEN 'Organic Social>Instagram>Other'
                 WHEN utm_medium = 'story+' AND utm_source = 'instagram' THEN 'Organic Social>Instagram>Other'

                 WHEN utm_medium ILIKE ANY ('%%curalate%%', '%%like2buy%%')
                     OR utm_source ILIKE ANY ('%%curalate%%', '%%like2buy%%') THEN 'Organic Social>Like2Buy>Other'

                 WHEN utm_medium = 'blog' OR utm_source = 'blog' THEN 'Organic Social>Other'
                 WHEN utm_medium = 'post' AND utm_source = 'facebook' THEN 'Organic Social>Facebook>Other'
                 WHEN utm_campaign ILIKE '%%client_share%%' THEN 'Organic Social>Other'
                 WHEN utm_medium IN ('lookbook', 'lookbook/') THEN 'Organic Social>Other'

                 WHEN params IN ('◊◊facebook◊about◊', '◊social+◊facebook◊◊', '◊boost◊facebook◊reels◊',
                                 '◊facebook◊facebook_social_media◊facebook----082017◊',
                                 '◊facebook◊internal_social_media◊plus◊',
                                 '◊social◊linktree◊shop+fabletics+!!!+◊', '◊◊facebook◊newsfeed_pixel39◊',
                                 '◊◊facebook◊newsfeed_pixel50◊', '◊◊facebook_2x1◊◊',
                                 '◊◊facebook_50◊◊', '◊◊facebook◊pixel2x1◊', '◊◊facebook◊mobile2x1◊',
                                 '◊◊facebook_mobile2x1◊◊',
                                 '◊◊facebook◊newsfeed_39◊', '◊◊facebook◊newsfeed_mobile39◊', '◊◊facebook◊vip5◊',
                                 '◊◊facebook◊newsoldleads◊',
                                 '◊◊facebook◊newsfeed_mobilepixel19◊', '◊◊facebook◊benl◊', '◊nf_fitness_25e◊facebook◊◊',
                                 '◊◊facebook◊befr◊', '◊◊fbf◊◊',
                                 '◊mtt_nf◊facebook◊◊', '◊◊facebook-fr◊◊', '◊facebook◊_social_media◊facebook----082017◊',
                                 '◊facebook◊_social_media◊facebook----102017◊',
                                 '◊facebook◊internal_social_media◊maddiexfabletics◊',
                                 '◊facebook◊website◊tightcroptom20220302◊', '◊◊facebook◊swimrefocusg20220514◊',
                                 '◊internalsocialmedia◊facebook◊◊') THEN 'Organic Social>Facebook>Other'

                 WHEN params IN ('◊social+◊instagram◊◊', '◊story◊instagram◊christine_quinn◊', '◊post◊instagram◊sakiko◊',
                                 '◊feed◊instagram◊christinequinn◊', '◊instagram_stories◊internal_social_media◊stories◊',
                                 '◊instagram,instagram◊internal_social_media,internal_social_media◊instagram-bio-link-20170401,instagram-bio-link-20170401◊',
                                 '◊instagrambio◊internal_social_media◊plussizelinktree◊',
                                 '◊link_in_bio◊instagram◊link_in_bio◊') THEN 'Organic Social>Instagram>Other'

                 WHEN params IN ('◊◊◊{campaign_name}◊') OR params ILIKE '%%◊ad◊pinterest◊pin_fbk%%'
                     THEN 'Organic Social>Pinterest>Other'
                 WHEN params IN ('◊twitter◊website◊siatom20220301◊') THEN 'Organic Social>Twitter>Other'
                 WHEN params IN ('◊organic◊sside◊sside◊', '◊social◊◊◊', '◊naturalss◊sside◊sside◊')
                     THEN 'Organic Social>Other'


                 -- varsity
                 WHEN utm_medium ILIKE '%%varsity%%' AND utm_source ILIKE '%%varsity%%'
                     THEN 'Physical Partnerships>Varsity>Varsity'


                 -- organic press
                 WHEN utm_medium ILIKE '%%organic%%' AND utm_source ILIKE '%%press%%'
                     THEN 'Organic Press>Organic Press>Other'
                 WHEN params IN ('◊press◊online◊kellyrowland◊', '◊applenews◊applenews◊◊')
                     THEN 'Organic Press>Organic Press>Other'

                 -- metanav
                 WHEN utm_medium IN ('metanav', 'meta_nav') THEN (
                     CASE
                         WHEN utm_source ILIKE '%%fl%%' THEN 'MetaNav>Fabletics'
                         WHEN utm_source ILIKE '%%jf%%' THEN 'MetaNav>JustFab'
                         WHEN utm_source ILIKE '%%sd%%' THEN 'MetaNav>ShoeDazzle'
                         WHEN utm_source ILIKE '%%fk%%' THEN 'MetaNav>Fabkids'
                         ELSE 'Unclassified>Unclassified>Unclassified' END)

                 WHEN params IN ('◊web◊jf_metanav◊metanav◊') THEN 'MetaNav>JustFab'
                 WHEN params IN ('◊◊sd◊sd_metanav◊') THEN 'MetaNav>ShoeDazzle'

                 -- referral
                 WHEN utm_medium = 'referral' THEN 'Referral>Other>Other'

                 -- direct traffic
                 WHEN utm_source = 'narvar' THEN 'Direct Traffic>Other'
                 WHEN utm_medium = 'organic' AND utm_source IN ('google', 'bing', 'yahoo', 'ecosia.org', 'duckduckgo')
                     THEN 'Direct Traffic>Other'
                 WHEN utm_source = 'trustpilot' THEN 'Direct Traffic>Other'
                 WHEN utm_source ILIKE '%%direct%%' THEN 'Direct Traffic>Other'
                 WHEN utm_medium = 'paid_social_media' AND utm_source IS NULL THEN 'Direct Traffic>Other'
                 WHEN params IN
                      ('◊◊◊◊', '◊(none)◊(direct)◊◊', '◊narvarshippingpage◊narvar◊logo◊', '◊landingpage◊landingpage◊◊',
                       '◊◊amex◊◊', '◊organic◊qwant.com◊◊',
                       '◊fabkidsdeadcell◊jf◊fkonjf◊', '◊app◊klarna◊merchant_boost◊',
                       '◊organic◊baidu◊◊', '◊app◊klarna◊klarna-merchantboost◊', '◊◊◊b_sidebar_katepicks20190501◊',
                       '◊store-directory◊clearpay◊feature◊', '◊web◊fba◊◊', '◊web◊tws◊twitter◊', '◊paid◊◊◊',
                       '◊organic◊naver◊◊', '◊organic◊yandex◊◊', '◊(none)◊amex◊◊', '◊landingpage ◊landingpage◊◊',
                       '◊◊paid_social_media◊◊', '◊web◊◊◊', '◊◊productfeed◊◊', '◊app◊chrome◊chrome-cart◊',
                       '◊web◊gan◊◊', '◊paid_social_media◊◊ ◊') THEN 'Direct Traffic>Other'


                 -- product feeds
                 WHEN utm_medium ILIKE ANY ('%%productfeed%%') THEN (
                     CASE
                         WHEN utm_source ILIKE ANY ('%%facebook%%', '%%insta%%', '%%fb%%', '%%ig%%')
                             THEN 'FB+IG>FB+IG>Facebook'
                         WHEN utm_campaign ILIKE '%fb_campaign%' THEN 'FB+IG>FB+IG>Facebook'
                         WHEN utm_source ILIKE '%%snapchat%%' THEN 'Snapchat>Snapchat>Snapchat'
                         WHEN utm_source ILIKE '%%tiktok%%' THEN 'TikTok>TikTok>TikTok'
                         WHEN utm_source ILIKE '%%pinterest%%' THEN 'Pinterest>Pinterest>Pinterest'
                         WHEN utm_source ILIKE '%%twitter%%' THEN 'Twitter>Twitter>Twitter'
                         WHEN utm_source ILIKE '%%reddit%%' THEN 'Reddit>Reddit>Reddit'
                         WHEN utm_campaign ILIKE '%|_pla|_%' ESCAPE '|' THEN 'Shopping>Google>Doubleclick'
                         WHEN utm_campaign ILIKE '%|_nb|_%' ESCAPE '|' THEN 'Non Branded Search>Google>Doubleclick'
                         WHEN utm_campaign ILIKE '%|_brand%' ESCAPE '|' THEN 'Branded Search>Google>Doubleclick'
                         WHEN utm_campaign ILIKE '%%ytube%%' THEN 'Youtube>Youtube>Adwords'
                         WHEN utm_campaign ILIKE '%|gdn|_%' ESCAPE '|' THEN 'Programmatic-GDN>GDN>Doubleclick'
                         WHEN utm_campaign ILIKE '%|_discovery|_%' ESCAPE '|'
                             THEN 'Programmatic-GDN>Discovery>Doubleclick'
                         WHEN utm_medium = 'google' THEN 'Branded Search>Google>Doubleclick'
                         WHEN utm_source = 'emarsys' THEN 'CRM>Emarsys'
                         WHEN utm_source IN ('sailthru', 'sde', 'sde2') THEN 'CRM>Sailthru'
                         WHEN utm_source = 'browser' THEN 'CRM>Browser'
                         WHEN utm_campaign ILIKE ANY ('%%confirmation%%', '%%password%%', '%%boutique%%')
                             THEN 'CRM>Other'
                         WHEN utm_campaign ILIKE '%klarna%' THEN 'Direct Traffic>Other'

                         -- ONLY instance where we need to utilize gateway type
                         WHEN gateway_type = 'shopping' THEN 'Shopping>Google>Doubleclick'
                         WHEN gateway_type = 'social' AND gateway_sub_type = 'facebook' THEN 'FB+IG>FB+IG>Facebook'
                         WHEN gateway_type = 'social' AND gateway_sub_type = 'pinterest'
                             THEN 'Pinterest>Pinterest>Pinterest'

                         WHEN params IN ('◊productfeed◊productfeed◊◊', '◊productfeed◊◊◊', '◊productfeed◊productfeed◊◊',
                                         '◊productfeed◊◊◊',
                                         '◊productfeed◊(direct)◊◊') THEN 'Direct Traffic>Other'
                         ELSE 'Unclassified>Unclassified>Unclassified' END)
                 ------------------------------------------
                 ------------------------------------------
                 END                            AS classification,

             SPLIT_PART(classification, '>', 1) AS channel,
             SPLIT_PART(classification, '>', 2) AS subchannel,
             SPLIT_PART(classification, '>', 3) AS vendor
      FROM _channels_to_map) a;

-- COMMENT OUT WHEN TESTING --
CREATE OR REPLACE TEMPORARY TABLE _channel_type AS
SELECT m.media_source_hash,
       m.channel,
       m.subchannel,
       m.vendor,
       m.gateway_type,
       m.gateway_sub_type,
       m.utm_source,
       m.utm_medium,
       m.utm_campaign,
       m.seo_vendor,
       c.channel_type
FROM _mapped_channels m
LEFT JOIN reporting_media_base_prod.lkp.channel_display_name c
       ON m.channel = c.channel_key;

UPDATE _channel_type
SET channel    = 'organic search',
    subchannel = LOWER(seo_vendor)
WHERE seo_vendor IS NOT NULL
  AND channel IN ('unclassified', 'direct traffic');

CREATE OR REPLACE TEMPORARY TABLE _new_channel_mapping AS
SELECT DISTINCT
       media_source_hash,
       'session'               AS event_source,
       channel_type,
       channel,
       subchannel,
       vendor,
       LEFT(utm_source, 255)   AS utm_source,
       LEFT(utm_medium, 255)   AS utm_medium,
       LEFT(utm_campaign, 255) AS utm_campaign,
       gateway_type,
       gateway_sub_type,
       ''                      AS hdyh_value,
       seo_vendor
FROM _channel_type m;

-- when a media_source_hash exists prior to 2021 and after 2021, we use the channel, subchannel, vendor from the new mapping
CREATE OR REPLACE TEMPORARY TABLE _media_source_hash AS
SELECT n.media_source_hash,
       n.event_source,
       n.utm_medium,
       n.utm_source,
       n.utm_campaign,
       n.gateway_type,
       n.gateway_sub_type,
       n.hdyh_value,
       n.seo_vendor
FROM _new_channel_mapping n
UNION
SELECT o.media_source_hash,
       o.event_source,
       o.utm_medium,
       o.utm_source,
       o.utm_campaign,
       o.gateway_type,
       o.gateway_sub_type,
       o.hdyh_value,
       o.seo_vendor
FROM shared.media_source_channel_mapping_historical_pre_2021 o
JOIN _channels_to_map n ON o.media_source_hash = n.media_source_hash
WHERE $wm_reporting_base_prod_shared_session::DATE <> '1900-01-01'
UNION
SELECT o.media_source_hash,
       o.event_source,
       o.utm_medium,
       o.utm_source,
       o.utm_campaign,
       o.gateway_type,
       o.gateway_sub_type,
       o.hdyh_value,
       o.seo_vendor
FROM shared.media_source_channel_mapping_historical_pre_2021 o
WHERE $wm_reporting_base_prod_shared_session::DATE = '1900-01-01';

CREATE OR REPLACE TEMP TABLE _all_session_historical_new AS
SELECT h.media_source_hash,
       h.event_source,
       h.utm_medium,
       h.utm_source,
       h.utm_campaign,
       h.gateway_type,
       h.gateway_sub_type,
       h.hdyh_value,
       h.seo_vendor,
       COALESCE(n.channel_type, o.channel_type) AS channel_type,
       COALESCE(n.channel, o.channel)           AS channel,
       COALESCE(n.subchannel, o.subchannel)     AS subchannel,
       COALESCE(n.vendor, o.vendor)             AS vendor
FROM _media_source_hash h
LEFT JOIN _new_channel_mapping n ON n.media_source_hash = h.media_source_hash
LEFT JOIN shared.media_source_channel_mapping_historical_pre_2021 o
       ON o.media_source_hash = h.media_source_hash;

CREATE OR REPLACE TEMP TABLE _media_source_channel_mapping AS
SELECT media_source_hash,
       event_source,
       channel_type,
       channel,
       subchannel,
       vendor,
       utm_source,
       utm_medium,
       utm_campaign,
       gateway_type,
       gateway_sub_type,
       '' AS hdyh_value,
       seo_vendor
FROM _all_session_historical_new;

CREATE OR REPLACE TEMP TABLE _media_source AS
-- Handle 'friend' Event Source
SELECT HASH('friend') AS media_source_hash,
       'friend'       AS event_source,
       'Organic'      AS channel_type,
       'friend'       AS channel,
       'other'        AS subchannel,
       NULL           AS vendor,
       NULL           AS utm_source,
       NULL           AS utm_medium,
       NULL           AS utm_campaign,
       NULL           AS gateway_type,
       NULL           AS gateway_sub_type,
       NULL           AS hdyh_value,
       NULL           AS seo_vendor

UNION

-- Handle 'session-fake' Event Source
SELECT HASH('session-fake-lead') AS media_source_hash,
       'session-fake'            AS event_source,
       'Organic'                 AS channel_type,
       'direct traffic'          AS channel,
       'other'                   AS subchannel,
       NULL                      AS vendor,
       NULL                      AS utm_source,
       NULL                      AS utm_medium,
       NULL                      AS utm_campaign,
       NULL                      AS gateway_type,
       NULL                      AS gateway_sub_type,
       NULL                      AS hdyh_value,
       NULL                      AS seo_vendor
UNION
SELECT HASH('session-fake-vip') AS media_source_hash,
       'session-fake'           AS event_source,
       'Organic'                AS channel_type,
       'direct traffic'         AS channel,
       'other'                  AS subchannel,
       NULL                     AS vendor,
       NULL                     AS utm_source,
       NULL                     AS utm_medium,
       NULL                     AS utm_campaign,
       NULL                     AS gateway_type,
       NULL                     AS gateway_sub_type,
       NULL                     AS hdyh_value,
       NULL                     AS seo_vendor

UNION

-- Handle 'retail-order' Event Source
SELECT HASH('retail-order', 'retail') AS media_source_hash,
       'retail-order'                 AS event_source,
       'Paid'                         AS channel_type,
       'retail'                       AS channel,
       'retail'                       AS subchannel,
       NULL                           AS vendor,
       NULL                           AS utm_source,
       NULL                           AS utm_medium,
       NULL                           AS utm_campaign,
       NULL                           AS gateway_type,
       NULL                           AS gateway_sub_type,
       NULL                           AS hdyh_value,
       NULL                           AS seo_vendor
UNION
SELECT HASH('retail-order', 'varsity') AS media_source_hash,
       'retail-order'                  AS event_source,
       'Paid'                          AS channel_type,
       'physical partnerships'         AS channel,
       'varsity'                       AS subchannel,
       NULL                            AS vendor,
       NULL                            AS utm_source,
       NULL                            AS utm_medium,
       NULL                            AS utm_campaign,
       NULL                            AS gateway_type,
       NULL                            AS gateway_sub_type,
       NULL                            AS hdyh_value,
       NULL                            AS seo_vendor

UNION

-- Handle 'hdyh' Event Source
SELECT HASH('hdyh', LOWER(c.how_did_you_hear)) AS media_source_hash,
       'hdyh'                                  AS event_source,
       ct.channel_type                         AS channel_type,
       LOWER(h.channel)                        AS channel,
       LOWER(h.subchannel)                     AS subchannel,
       NULL                                    AS vendor,
       NULL                                    AS utm_source,
       NULL                                    AS utm_medium,
       NULL                                    AS utm_campaign,
       NULL                                    AS gateway_type,
       NULL                                    AS gateway_sub_type,
       LOWER(c.how_did_you_hear)               AS hdyh_value,
       NULL                                    AS seo_vendor
FROM edw_prod.data_model.fact_registration c
     JOIN reporting_media_base_prod.dbo.vw_hdyh_mapping h
          ON LOWER(h.hdyh_value) = LOWER(c.how_did_you_hear)
     LEFT JOIN reporting_media_base_prod.lkp.channel_display_name ct
               ON LOWER(ct.channel_key) = LOWER(h.channel)
WHERE LOWER(h.channel) != 'ignore'

UNION

-- Handle 'tradedesk' Event Source
SELECT HASH('tradedesk', a.campaign_name) AS media_source_hash,
       'tradedesk'                        AS event_source,
       'Paid'                             AS channel_type,
       tm.channel,
       tm.subchannel,
       tm.vendor,
       NULL                               AS utm_source,
       NULL                               AS utm_medium,
       a.campaign_name                    AS utm_campaign,
       NULL                               AS gateway_type,
       NULL                               AS gateway_sub_type,
       NULL                               AS hdyh_value,
       NULL                               AS seo_vendor
FROM (SELECT DISTINCT TRIM(LOWER(t.event_campaign_name)) AS campaign_name,
                      CASE
                          WHEN event_campaign_name ILIKE '%native%' THEN 'native'
                          WHEN event_campaign_name ILIKE '%audio%' THEN 'audio'
                          WHEN event_campaign_name ILIKE '%ctv%' THEN 'ctv'
                          WHEN event_campaign_name ILIKE '%video%' THEN 'video'
                          WHEN event_campaign_name ILIKE '%display%' THEN 'display'
                          ELSE NULL
                          END                            AS classification
      FROM lake.media.tradedesk_daily_path_to_conversion_legacy t) a
         LEFT JOIN (SELECT 'display'      AS classification,
                           'programmatic' AS channel,
                           'ttddisplay'   AS subchannel,
                           'tradedesk'    AS vendor
                    UNION
                    SELECT 'native'       AS classification,
                           'programmatic' AS channel,
                           'ttdnative'    AS subchannel,
                           'tradedesk'    AS vendor
                    UNION
                    SELECT 'audio'        AS classification,
                           'programmatic' AS channel,
                           'ttdaudio'     AS subchannel,
                           'tradedesk'    AS vendor
                    UNION
                    SELECT 'ctv'          AS classification,
                           'programmatic' AS channel,
                           'ttdvideo'     AS subchannel,
                           'tradedesk'    AS vendor
                    UNION
                    SELECT 'video'        AS classification,
                           'programmatic' AS channel,
                           'ttdvideo'     AS subchannel,
                           'tradedesk'    AS vendor) tm ON tm.classification = a.classification

UNION

-- Handle 'facebook' Event Source
SELECT media_source_hash,
       'facebook' AS event_source,
       'Paid'     AS channel_type,
       'fb+ig'    AS channel,
       subchannel,
       'facebook' AS vendor,
       NULL       AS utm_source,
       NULL       AS utm_medium,
       NULL       AS utm_campaign,
       NULL       AS gateway_type,
       NULL       AS gateway_sub_type,
       NULL       AS hdyh_value,
       NULL       AS seo_vendor
FROM (SELECT DISTINCT HASH('facebook', ad_id::STRING) AS media_source_hash,
                      subchannel
      FROM lake.media.facebook_order_id_attributions_legacy l
               JOIN (SELECT DISTINCT HASH('facebook', ad_id) AS media_source_hash,
                                     CASE
                                         WHEN LOWER(subchannel) = 'agencywithin' THEN 'agencywithin'
                                         WHEN LOWER(subchannel) = 'tubescience' THEN 'tubescience'
                                         WHEN LOWER(subchannel) = 'peoplehype' THEN 'peoplehype'
                                         WHEN LOWER(subchannel) = 'paidinfluencers' THEN 'paidinfluencers'
                                         WHEN LOWER(subchannel) = 'narrative' THEN 'narrative'
                                         ELSE 'fb+ig'
                                         END                 AS subchannel
                     FROM reporting_media_prod.facebook.facebook_optimization_dataset_hourly_by_ad) ad
                    ON ad.media_source_hash = HASH('facebook', l.ad_id::STRING)) a

UNION

-- Handle 'pinterest' Event Source
SELECT HASH('pinterest') AS media_source_hash,
       'pinterest'       AS event_source,
       'Paid'            AS channel_type,
       'pinterest'       AS channel,
       'pinterest'       AS subchannel,
       'pinterest'       AS vendor,
       NULL              AS utm_source,
       NULL              AS utm_medium,
       NULL              AS utm_campaign,
       NULL              AS gateway_type,
       NULL              AS gateway_sub_type,
       NULL              AS hdyh_value,
       NULL              AS seo_vendor

UNION

-- Handle 'blisspoint' Event Source
SELECT HASH('blisspoint') AS media_source_hash,
       'blisspoint'       AS event_source,
       'Paid'             AS channel_type,
       'tv+streaming'     AS channel,
       'streaming'        AS subchannel,
       'blisspointmedia'  AS vendor,
       NULL               AS utm_source,
       NULL               AS utm_medium,
       NULL               AS utm_campaign,
       NULL               AS gateway_type,
       NULL               AS gateway_sub_type,
       NULL               AS hdyh_value,
       NULL               AS seo_vendor

UNION

-- Handle 'youtube' Event Source
SELECT HASH('youtube') AS media_source_hash,
       'youtube'       AS event_source,
       'Paid'          AS channel_type,
       'youtube'       AS channel,
       'youtube'       AS subchannel,
       'adwords'       AS vendor,
       NULL            AS utm_source,
       NULL            AS utm_medium,
       NULL            AS utm_campaign,
       NULL            AS gateway_type,
       NULL            AS gateway_sub_type,
       NULL            AS hdyh_value,
       NULL            AS seo_vendor;

CREATE OR REPLACE TEMP TABLE _media_source_channel_mapping_stg_base AS
SELECT media_source_hash,
       event_source,
       channel_type,
       channel,
       subchannel,
       vendor,
       utm_source,
       utm_medium,
       utm_campaign,
       gateway_type,
       gateway_sub_type,
       hdyh_value,
       seo_vendor
FROM _media_source_channel_mapping
UNION
SELECT media_source_hash,
       event_source,
       channel_type,
       channel,
       subchannel,
       vendor,
       utm_source,
       utm_medium,
       utm_campaign,
       gateway_type,
       gateway_sub_type,
       hdyh_value,
       seo_vendor
FROM _media_source;

-- FINAL TABLE (includes all sessions: historical + new)
CREATE OR REPLACE TEMP TABLE _media_source_channel_mapping_stg AS
SELECT media_source_hash,
       event_source,
       channel_type,
       channel,
       subchannel,
       vendor,
       utm_source,
       utm_medium,
       utm_campaign,
       gateway_type,
       gateway_sub_type,
       hdyh_value,
       seo_vendor,
       HASH(media_source_hash,
            event_source,
            channel_type,
            channel,
            subchannel,
            vendor,
            utm_source,
            utm_medium,
            utm_campaign,
            gateway_type,
            gateway_sub_type,
            hdyh_value,
            seo_vendor) AS meta_row_hash,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM _media_source_channel_mapping_stg_base;

-- Delete orphan records
DELETE FROM shared.media_source_channel_mapping
WHERE media_source_hash NOT IN (
    SELECT media_source_hash FROM shared.session
    WHERE TO_DATE(session_local_datetime) >= '2021-01-01'
    UNION
    SELECT media_source_hash FROM shared.media_source_channel_mapping_historical_pre_2021
    UNION
    SELECT media_source_hash FROM _media_source);

MERGE INTO shared.media_source_channel_mapping t
USING _media_source_channel_mapping_stg s
    ON EQUAL_NULL(t.media_source_hash, s.media_source_hash)
WHEN NOT MATCHED THEN INSERT (
    media_source_hash, event_source, channel_type, channel, subchannel, vendor, utm_source, utm_medium, utm_campaign, gateway_type, gateway_sub_type, hdyh_value, seo_vendor, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    media_source_hash, event_source, channel_type, channel, subchannel, vendor, utm_source, utm_medium, utm_campaign, gateway_type, gateway_sub_type, hdyh_value, seo_vendor, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND NOT EQUAL_NULL(t.meta_row_hash, s.meta_row_hash)
THEN
UPDATE SET
    t.media_source_hash = s.media_source_hash,
    t.event_source = s.event_source,
    t.channel_type = s.channel_type,
    t.channel = s.channel,
    t.subchannel = s.subchannel,
    t.vendor = s.vendor,
    t.utm_source = s.utm_source,
    t.utm_medium = s.utm_medium,
    t.utm_campaign = s.utm_campaign,
    t.gateway_type = s.gateway_type,
    t.gateway_sub_type = s.gateway_sub_type,
    t.hdyh_value = s.hdyh_value,
    t.seo_vendor = s.seo_vendor,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;

UPDATE public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = $execution_start_time
WHERE table_name = $target_table;

-- -- USE FOR TESTING --
--
-- run after creating _mapped_channels to find missing combos
-- create or replace temporary table shared_session_channel_mapping_testing as
-- select distinct
--     m.media_source_hash,
--     'session' as event_source,
--     params,
--     m.channel,
--     m.subchannel,
--     m.vendor,
--     left(m.utm_source,255) as utm_source,
--     left(m.utm_medium,255) as utm_medium,
--     left(m.utm_campaign,255) as utm_campaign,
--     m.gateway_type,
--     m.gateway_sub_type,
--     '' as hdyh_value,
--     m.seo_vendor
-- from _mapped_channels m;
-- --
-- update shared_session_channel_mapping_testing
-- set
--     channel    = 'organic search',
--     subchannel = lower(seo_vendor)
-- where seo_vendor is not null
--     and channel in ('unclassified', 'direct traffic')
--     and event_source = 'session';
-- --
-- -- -- shows unclassified combos by count of sessions
-- select ab.params,
--        ab.utm_medium,
--        ab.utm_source,
--        ab.utm_campaign,
--        ab.gateway_type,
--        ab.gateway_sub_type,
--        count(*) as sessions
-- from reporting_base_prod.shared.session sc
-- join shared_session_channel_mapping_testing ab on sc.media_source_hash = ab.media_source_hash
-- where ab.channel = 'unclassified'
--     and sc.session_local_datetime >= '2022-01-01'
--     and ab.utm_medium not ilike '%%marketing%%'
-- group by 1,2,3,4,5,6
-- order by 7 desc;
