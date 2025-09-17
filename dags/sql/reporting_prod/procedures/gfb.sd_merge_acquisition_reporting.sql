CREATE OR REPLACE TEMPORARY TABLE _campaign_data AS
SELECT REPLACE(utmcampaign, 'fb_campaign_id_', '')::INT                                              AS campaign_id,
       svm.customer_id                                                                               AS customer_id,
       REPLACE((PARSE_URL(s.uri):parameters:utm_term), 'fb_ad_id_', '')                              AS ad_id,
       sessionlocaldate,
       COUNT(DISTINCT CASE WHEN membershipstate = 'Prospect' THEN svm.customer_id END)               AS prospects,
       COUNT(DISTINCT CASE WHEN membershipstate = 'Prospect' AND leads = 1 THEN svm.customer_id END) AS leads,
       COUNT(DISTINCT CASE WHEN activatingordersd7fromleads = 1 THEN svm.customer_id END)            AS vips
FROM reporting_base_prod.shared.session_single_view_media svm
     JOIN (SELECT session_id, uri FROM reporting_base_prod.shared.session) s
          ON svm.session_id = s.session_id
              AND
             svm.utmcampaign ILIKE ANY ('%fb_campaign_id_120200595258740728%', '%fb_campaign_id_23862392243530629%')
GROUP BY REPLACE(utmcampaign, 'fb_campaign_id_', '')::INT,
         svm.customer_id,
         REPLACE((PARSE_URL(s.uri):parameters:utm_term), 'fb_ad_id_', ''),
         sessionlocaldate
UNION
SELECT REPLACE(utmcampaign, 'fb_campaign_id_', '')::INT                                              AS campaign_id,
       svm.customer_id                                                                               AS customer_id,
       REPLACE((PARSE_URL(s.uri):parameters:utm_term), 'fb_ad_id_', '')                              AS ad_id,
       sessionlocaldate,
       COUNT(DISTINCT CASE WHEN membershipstate = 'Prospect' THEN svm.customer_id END)               AS prospects,
       COUNT(DISTINCT CASE WHEN membershipstate = 'Prospect' AND leads = 1 THEN svm.customer_id END) AS leads,
       COUNT(DISTINCT CASE WHEN activatingordersd7fromleads = 1 THEN svm.customer_id END)            AS vips
FROM reporting_base_prod.shared.session_single_view_media svm
     JOIN reporting_base_prod.shared.session s
          ON svm.session_id = s.session_id
              AND s.uri ILIKE '%utm_special=merge%'
GROUP BY REPLACE(utmcampaign, 'fb_campaign_id_', '')::INT,
         svm.customer_id,
         REPLACE((PARSE_URL(s.uri):parameters:utm_term), 'fb_ad_id_', ''),
         sessionlocaldate;

CREATE OR REPLACE TEMPORARY TABLE _campaign_customers AS
SELECT campaign_id,
       ad_id,
       customer_id,
       MIN(sessionlocaldate) AS campaign_start_date
FROM _campaign_data
GROUP BY campaign_id,
         ad_id,
         customer_id;

CREATE OR REPLACE TEMPORARY TABLE _campaign_spend AS
SELECT rtc.campaign_id,
       ad_id,
       UPPER(SUBSTR(rtc.ad_name, 3, LEN(rtc.ad_name))) AS ad_name,
       UPPER(LEFT(rtc.ad_name, 2))                     AS brand,
       SUM(spend_usd)                                  AS spend
FROM reporting_media_prod.dbo.real_time_click_performance rtc
     JOIN (SELECT DISTINCT campaign_id FROM _campaign_data) cd
          ON TRY_TO_NUMBER(rtc.campaign_id) = cd.campaign_id
GROUP BY rtc.campaign_id,
         ad_id,
         UPPER(SUBSTR(rtc.ad_name, 3, LEN(rtc.ad_name))),
         UPPER(LEFT(rtc.ad_name, 2));

CREATE OR REPLACE TEMPORARY TABLE _campaign_data_agg AS
SELECT campaign_id,
       ad_id,
       MIN(sessionlocaldate) AS campaign_start_date,
       SUM(prospects)        AS prospects,
       SUM(leads)            AS leads,
       SUM(vips)             AS vips
FROM _campaign_data
GROUP BY campaign_id,
         ad_id;

CREATE OR REPLACE TEMPORARY TABLE _cgm AS
SELECT cc.customer_id,
       cc.campaign_id,
       cc.ad_id,
       fa.activation_local_datetime::DATE                      AS activation_date,
       fa.cancellation_local_datetime::DATE                    AS cancellation_date,
       DATEDIFF(
               'day', activation_date, IFF(cancellation_date = '9999-12-31', CURRENT_DATE(),
                                           cancellation_date)) AS tenure,
       SUM(COALESCE(clvm.cash_gross_profit, 0))                AS cash_gross_profit
FROM edw_prod.analytics_base.customer_lifetime_value_monthly clvm
     JOIN _campaign_customers cc
          ON clvm.customer_id = cc.customer_id
     JOIN edw_prod.data_model_jfb.fact_activation fa
          ON clvm.activation_key = fa.activation_key
WHERE activation_date >= cc.campaign_start_date
  AND clvm.month_date >= DATE_TRUNC('month', cc.campaign_start_date)
GROUP BY cc.customer_id,
         cc.campaign_id,
         cc.ad_id,
         fa.activation_local_datetime::DATE,
         fa.cancellation_local_datetime::DATE,
         DATEDIFF('day', activation_date, IFF(cancellation_date = '9999-12-31', CURRENT_DATE(), cancellation_date));

CREATE OR REPLACE TEMPORARY TABLE _cgm_agg AS
SELECT cd.campaign_id,
       cd.ad_id,
       MIN(cd.sessionlocaldate)                                             AS campaign_start_date,
       SUM(CASE WHEN tenure <= 30 THEN COALESCE(cash_gross_profit, 0) END)  AS "30 Day Cash Gross Margin",
       COUNT(DISTINCT CASE WHEN tenure <= 30 THEN cd.customer_id END)       AS "30 Day VIPs",
       SUM(CASE WHEN tenure <= 60 THEN COALESCE(cash_gross_profit, 0) END)  AS "60 Day Cash Gross Margin",
       COUNT(DISTINCT CASE WHEN tenure <= 60 THEN cd.customer_id END)       AS "60 Day VIPs",
       SUM(CASE WHEN tenure <= 90 THEN COALESCE(cash_gross_profit, 0) END)  AS "90 Day Cash Gross Margin",
       COUNT(DISTINCT CASE WHEN tenure <= 90 THEN cd.customer_id END)       AS "90 Day VIPs",
       SUM(CASE WHEN tenure <= 120 THEN COALESCE(cash_gross_profit, 0) END) AS "120 Day Cash Gross Margin",
       COUNT(DISTINCT CASE WHEN tenure <= 120 THEN cd.customer_id END)      AS "120 Day VIPs",
       SUM(CASE WHEN tenure <= 180 THEN COALESCE(cash_gross_profit, 0) END) AS "180 Day Cash Gross Margin",
       COUNT(DISTINCT CASE WHEN tenure <= 180 THEN cd.customer_id END)      AS "180 Day VIPs",
       SUM(cash_gross_profit)                                               AS total_cash_gross_margin
FROM _campaign_data cd
     LEFT JOIN _cgm gm
               ON cd.customer_id = gm.customer_id
                   AND cd.campaign_id = gm.campaign_id
                   AND cd.ad_id = gm.ad_id
                   AND cd.sessionlocaldate >= gm.activation_date
                   AND cd.sessionlocaldate <= gm.cancellation_date
GROUP BY cd.campaign_id,
         cd.ad_id;

CREATE OR REPLACE TRANSIENT TABLE gfb.sd_merge_acquisition_reporting AS
SELECT 'fb_campaign_id_' || cda.campaign_id::STRING AS campaign_id,
       COALESCE(cs.ad_name, cs.ad_id)               AS ad_name,
       cs.brand,
       cda.campaign_start_date,
       cda.prospects,
       cda.leads,
       cda.vips,
       ca."30 Day Cash Gross Margin",
       ca."30 Day VIPs",
       ca."60 Day Cash Gross Margin",
       ca."60 Day VIPs",
       ca."90 Day Cash Gross Margin",
       ca."90 Day VIPs",
       ca."120 Day Cash Gross Margin",
       ca."120 Day VIPs",
       ca."180 Day Cash Gross Margin",
       ca."180 Day VIPs",
       ca.total_cash_gross_margin,
       cs.spend
FROM _campaign_data_agg cda
     LEFT JOIN _cgm_agg ca
               ON cda.campaign_id = ca.campaign_id
                   AND cda.ad_id = ca.ad_id
     LEFT JOIN _campaign_spend cs
               ON cda.campaign_id = cs.campaign_id
                   AND cda.ad_id = cs.ad_id;
