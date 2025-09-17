SET target_table = 'reporting_base_prod.staging.bot_confirmed';

SET execution_start_time = CURRENT_TIMESTAMP;


MERGE INTO
    public.meta_table_dependency_watermark AS t USING (SELECT $target_table           AS table_name,
                                                              NULLIF(
                                                                  dependent_table_name,
                                                                  'reporting_base_prod.staging.bot_confirmed'
                                                              )                       AS dependent_table_name,
                                                              high_watermark_datetime AS new_high_watermark_datetime
                                                       FROM (SELECT 'lake_consolidated_view.ultra_merchant.session_uri' AS dependent_table_name,
                                                                    MAX(meta_update_datetime)                           AS high_watermark_datetime
                                                             FROM lake_consolidated_view.ultra_merchant.session_uri) AS h) AS s ON
                t.table_name = s.table_name
            AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED AND
        NOT EQUAL_NULL(
            t.new_high_watermark_datetime,
            s.new_high_watermark_datetime
            )
        THEN UPDATE SET
        t.new_high_watermark_datetime = s.new_high_watermark_datetime,
        t.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3)
    WHEN NOT MATCHED THEN INSERT
        (
         table_name,
         dependent_table_name,
         high_watermark_datetime,
         new_high_watermark_datetime
            )
        VALUES (s.table_name,
                s.dependent_table_name,
                '1900-01-01'::TIMESTAMP_LTZ,
                s.new_high_watermark_datetime);


SET wm_lake_consolidated_view_ultra_merchant_session_uri = public.udf_get_watermark($target_table,
                                                                                    'lake_consolidated_view.ultra_merchant.session_uri');


CREATE OR REPLACE TEMP TABLE _ua_base AS
SELECT DISTINCT user_agent
FROM lake_consolidated_view.ultra_merchant.session_uri
WHERE meta_update_datetime > $wm_lake_consolidated_view_ultra_merchant_session_uri
  AND user_agent IS NOT NULL
ORDER BY user_agent ASC;


CREATE OR REPLACE TEMPORARY TABLE _ua_stg AS
SELECT ua.user_agent,
       CASE
           WHEN ua.user_agent ILIKE '%Chrome/Headless%'
               OR (
                            ua.user_agent ILIKE '%HeadlessChrome%'
                        AND ua.user_agent <>
                            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/98.0.4758.102 Safari/537.36'
                    ) /* HAS 840 ORDERS */ THEN 'headlesschrome'
           WHEN ua.user_agent ILIKE '%bot%' THEN 'bot'
           WHEN ua.user_agent ILIKE '%crawler%' THEN 'crawler'
           WHEN ua.user_agent ILIKE '%spider%' THEN 'spider'
           WHEN ua.user_agent ILIKE '%Node%' THEN 'node'
           WHEN ua.user_agent ILIKE '%facebookexternalhit%' THEN 'facebookexternalhit'
           WHEN ua.user_agent ILIKE '%facebookcatalog%' THEN 'facebookcatalog'
           WHEN ua.user_agent ILIKE '%AmazonProductDiscovery%' THEN 'amazonproductdiscovery'
           WHEN ua.user_agent ILIKE '%BingPreview%' THEN 'bingpreview'
           WHEN ua.user_agent ILIKE '%Chrome-Lighthouse%' THEN 'chrome-lighthouse'
           WHEN ua.user_agent ILIKE '%CloudFlare-AlwaysOnline%' THEN 'cloudflare-alwaysonline'
           WHEN ua.user_agent ILIKE '%GomezAgent%' THEN 'gomezagent'
           WHEN ua.user_agent ILIKE '%Google Page Speed Insights%' THEN 'google page speed insights'
           WHEN ua.user_agent ILIKE '%Google Web Preview%' THEN 'google web preview'
           WHEN ua.user_agent ILIKE '%LoadImpact%' THEN 'loadimpact'
           WHEN ua.user_agent ILIKE '%PhantomJS%' THEN 'phantomjs'
           WHEN ua.user_agent ILIKE '%Pingdom%' THEN 'pingdom'
           WHEN ua.user_agent ILIKE '%PTST/SpeedCurve%' THEN 'ptst/speedcurve'
           WHEN ua.user_agent ILIKE '%Webshot%' THEN 'webshot'
           WHEN ua.user_agent ILIKE '%Yahoo Ad monitoring%' THEN 'yahoo ad monitoring'
           WHEN ua.user_agent ILIKE '%browserStack%' THEN 'browserstack'
           WHEN ua.user_agent ILIKE '%Catchpoint%' THEN 'catchpoint'
           WHEN ua.user_agent ILIKE '%Screaming Frog%' THEN 'screaming frog'
           WHEN ua.user_agent ILIKE '%24/7%'
               OR ua.user_agent ILIKE '%site24x7%' THEN 'site24x7'
           WHEN ua.user_agent ILIKE '%python-request%' THEN 'python-request'
           WHEN ua.user_agent ILIKE '%Slurp%' THEN 'slurp'
           WHEN ua.user_agent ILIKE '%Sogou%' THEN 'sogou'
           WHEN ua.user_agent ILIKE '%externalhit_uatext%' THEN 'externalhit_uatext'
           WHEN ua.user_agent ILIKE
                '%Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko Firefox/11.0 (via ggpht.com GoogleImageProxy)%'
               THEN 'googleimageproxy'
           WHEN ua.user_agent ILIKE '%Mediapartners-Google%' THEN 'mediapartners-google'
           WHEN ua.user_agent ILIKE '%BUbiNG (+http://law.di.unimi.it/BUbiNG.html#wc)%' THEN 'undefined'
           WHEN ua.user_agent ILIKE '%APIs-Google%' THEN 'apis-google'
           WHEN ua.user_agent ILIKE '%FeedFetcher-Google%' THEN 'feedfetcher-google'
           WHEN ua.user_agent ILIKE '%Google-Read-Aloud%' THEN 'google-read-aloud'
           WHEN ua.user_agent ILIKE '%Google-Site-Verification%' THEN 'google-site-verification'
           WHEN ua.user_agent ILIKE '%Accoona-AI-Agent%' THEN 'accoona-ai-agent'
           WHEN ua.user_agent ILIKE '%Arachmo%' THEN 'arachmo'
           WHEN ua.user_agent ILIKE '%Cerberian Drtrs%' THEN 'cerberian drtrs'
           WHEN ua.user_agent ILIKE '%Charlotte/%' THEN 'charlotte'
           WHEN ua.user_agent ILIKE '%cosmos%' THEN 'cosmos'
           WHEN ua.user_agent ILIKE '%Covario%' THEN 'covario'
           WHEN ua.user_agent ILIKE '%DataparkSearch%' THEN 'datapartsearch'
           WHEN ua.user_agent ILIKE '%envolk%' THEN 'envolk'
           WHEN ua.user_agent ILIKE '%findlinks%' THEN 'findlinks'
           WHEN ua.user_agent ILIKE '%holmes%' THEN 'holmes'
           WHEN ua.user_agent ILIKE '%htdig%' THEN 'htdig'
           WHEN ua.user_agent ILIKE '%ia_archiver%' THEN 'ia_archiver'
           WHEN ua.user_agent ILIKE '%ichiro%' THEN 'ichiro'
           WHEN ua.user_agent ILIKE '%webis%' THEN 'webis'
           WHEN ua.user_agent ILIKE '%larbin%' THEN 'larbin'
           WHEN ua.user_agent ILIKE '%LinkWalker%' THEN 'linkwalker'
           WHEN ua.user_agent ILIKE '%lwp-trivial%' THEN 'lwp-trivial'
           WHEN ua.user_agent ILIKE '%masscan-ng%' THEN 'masscan-ng'
           WHEN ua.user_agent ILIKE '%Mnogosearch%' THEN 'mnogosearch'
           WHEN ua.user_agent ILIKE '%mogimogi%' THEN 'mogimogi'
           WHEN ua.user_agent ILIKE '%Morning Paper%' THEN 'morning paper'
           WHEN ua.user_agent ILIKE '%MVAClient%' THEN 'mvaclient'
           WHEN ua.user_agent ILIKE '%NetResearchServer%' THEN 'netresearchserver'
           WHEN ua.user_agent ILIKE '%NewsGator%' THEN 'newsgator'
           WHEN ua.user_agent ILIKE '%NG-Search/%' THEN 'ng-search'
           WHEN ua.user_agent ILIKE '%NutchCVS%' THEN 'nutchcvs'
           WHEN ua.user_agent ILIKE '%Nymesis%' THEN 'nymesis'
           WHEN ua.user_agent ILIKE '%oegp%' THEN 'oegp'
           WHEN ua.user_agent ILIKE '%Orbiter%' THEN 'orbiter'
           WHEN ua.user_agent ILIKE '%Peew%' THEN 'peew'
           WHEN ua.user_agent ILIKE '%Pompos%' THEN 'pompos'
           WHEN ua.user_agent ILIKE '%PostPost%' THEN 'postpost'
           WHEN ua.user_agent ILIKE '%PycURL%' THEN 'pycurl'
           WHEN ua.user_agent ILIKE '%Qseero%' THEN 'qseero'
           WHEN ua.user_agent ILIKE '%Radian6%' THEN 'radian6'
           WHEN ua.user_agent ILIKE '%SBIder%' THEN 'sbider'
           WHEN ua.user_agent ILIKE '%Scrubby%' THEN 'scrubby'
           WHEN ua.user_agent ILIKE '%ShopWiki%' THEN 'shopwiki'
           WHEN ua.user_agent ILIKE '%Snappy%' THEN 'snappy'
           WHEN ua.user_agent ILIKE '%Sqworm%' THEN 'sqworm'
           WHEN ua.user_agent ILIKE '%StackRambler%' THEN 'stackrambler'
           WHEN ua.user_agent ILIKE '%Teoma%' THEN 'teoma'
           WHEN ua.user_agent ILIKE '%TinEye%' THEN 'tineye'
           WHEN ua.user_agent ILIKE '%truwoGPS%' THEN 'truwogps'
           WHEN ua.user_agent ILIKE '%updated/%' THEN 'updated/'
           WHEN ua.user_agent ILIKE '%Vagabondo%' THEN 'vagabondo'
           WHEN ua.user_agent ILIKE '%Vortex%' THEN 'vortex'
           WHEN ua.user_agent ILIKE '%voyager%' THEN 'voyager'
           WHEN ua.user_agent ILIKE '%VYU2%' THEN 'vyu2'
           WHEN ua.user_agent ILIKE '%webcollage%' THEN 'webcollage'
           WHEN ua.user_agent ILIKE '%Websquash%' THEN 'websquash'
           WHEN ua.user_agent ILIKE '%wf84%' THEN 'wf84'
           WHEN ua.user_agent ILIKE '%WomlpeFactory%' THEN 'womlpefactory'
           WHEN ua.user_agent ILIKE '%yacy%' THEN 'yacy'
           WHEN ua.user_agent ILIKE '%YahooSeeker%' THEN 'yahooseeker'
           WHEN ua.user_agent ILIKE '%YandexImages%' THEN 'yandeximages'
           WHEN ua.user_agent ILIKE '%Yeti%' THEN 'yeti'
           WHEN ua.user_agent ILIKE '%yoogliFetchAgent%' THEN 'yooglifetchagent'
           WHEN ua.user_agent ILIKE '%Zao%' THEN 'zao'
           WHEN ua.user_agent ILIKE '%ZyBorg%' THEN 'zyborg'
           WHEN ua.user_agent ILIKE '%Sprinklr 2.0%' THEN 'sprinklr 2.0'
           WHEN ua.user_agent ILIKE '%ColdFusion%' THEN 'cold fusion'
           WHEN ua.user_agent ILIKE
                '%Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36%'
               THEN 'undefined'
           WHEN ua.user_agent ILIKE
                '%Mozilla/5.0 (Linux; Android 7.0) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/5.2 Chrome/51.0.2704.106 Mobile Safari/537.36%'
               THEN 'undefined'
           WHEN ua.user_agent ILIKE '%(smartly.io)%' THEN 'smartly.io'
           WHEN ua.user_agent ILIKE '%curl/7.57.0%' THEN 'curl'
           WHEN ua.user_agent ILIKE '%com.apple.WebKit.Networking/8614.2.9.0.10 CFNetwork/1399 Darwin/22.1.0%'
               THEN 'undefined'
           WHEN ua.user_agent ILIKE '%MobileSafari/8614.2.9.0.10 CFNetwork/1399 Darwin/22.1.0%' THEN 'undefined'
           WHEN ua.user_agent ILIKE
                '%Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36%'
               THEN 'undefined'
           WHEN ua.user_agent ILIKE '%ThousandEyes-Dragonfly-x1%' THEN 'thousandeyes-dragonfly'
           WHEN ua.user_agent ILIKE 'qa-automation' THEN 'qa-automation'
           WHEN ua.user_agent ILIKE 'emarsys url checker' THEN 'emarsys url checker'
           WHEN ua.user_agent ILIKE 'tfg-loadtest' THEN 'tfg-loadtest'
           WHEN ua.user_agent =
                'Mozilla / 5.0 (Windows NT 6.1; WOW64) AppleWebKit / 537.36 (KHTML, like Gecko) Chrome / 50.0.2661.102 Safari / 537.36'
               THEN 'tfg-loadtest'
           WHEN ua.user_agent ILIKE '<UAQ>' THEN 'undefined mobile app'
           WHEN ua.user_agent ILIKE
                'Mozilla/5.0 (iPad; CPU OS 15_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 FableticsApp/1.46.0-/0.0.1'
               THEN 'undefined mobile app'
           END AS bot_definition,
       IFF(
               bot_definition IN (
                                  'coldfusion',
                                  'smartly.io',
                                  'curl',
                                  'thousandeyes-dragonfly',
                                  'undefined',
                                  'qa-automation',
                                  'emarsys url checker',
                                  'tfg-loadtest'
               ),
               'internal',
               IFF(
                       bot_definition ILIKE 'undefined%',
                       'undefined',
                       IFF(
                           bot_definition IS NOT NULL,
                           'heap confirmed',
                           NULL
                       )
               )
       )       AS bot_type,
       HASH(
           ua.user_agent,
           bot_definition,
           bot_type
       )       AS meta_row_hash
FROM _ua_base AS ua
WHERE bot_definition IS NOT NULL;


MERGE INTO staging.bot_confirmed AS t
    USING _ua_stg AS src
    ON t.user_agent = src.user_agent
    WHEN NOT MATCHED THEN
        INSERT (
                user_agent,
                bot_definition,
                bot_type,
                meta_row_hash,
                meta_create_datetime,
                meta_update_datetime
            )
            VALUES (src.user_agent,
                    src.bot_definition,
                    src.bot_type,
                    src.meta_row_hash,
                    $execution_start_time,
                    $execution_start_time)
    WHEN MATCHED AND (
        t.meta_row_hash != src.meta_row_hash
        ) THEN
        UPDATE SET
            t.bot_definition = src.bot_definition,
            t.bot_type = src.bot_type,
            t.meta_row_hash = src.meta_row_hash,
            t.meta_update_datetime = $execution_start_time
;
