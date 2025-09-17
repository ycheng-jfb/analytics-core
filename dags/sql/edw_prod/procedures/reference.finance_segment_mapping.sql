CREATE OR REPLACE TEMPORARY TABLE _all_combos
(
    metric_type                          VARCHAR,
    event_store_id                       NUMBER(38),
    vip_store_id                         NUMBER(38),
    is_retail_vip                        BOOLEAN,
    customer_gender                      VARCHAR,
    is_cross_promo                       BOOLEAN,
    finance_specialty_store              VARCHAR,
    is_scrubs_customer                   BOOLEAN,
    mapping_start_date                   DATE,
    mapping_end_date                     DATE,
    is_ddd_consolidated                  BOOLEAN,
    ddd_consolidated_currency_type       VARCHAR,
    is_ddd_individual                    BOOLEAN,
    ddd_individual_currency_type         VARCHAR,
    is_ddd_hyperion                      BOOLEAN,
    ddd_hyperion_currency_type           VARCHAR,
    is_retail_attribution_ddd            BOOLEAN,
    retail_attribution_ddd_currency_type VARCHAR,
    is_daily_cash_usd                    BOOLEAN,
    daily_cash_usd_currency_type         VARCHAR,
    is_daily_cash_eur                    BOOLEAN,
    daily_cash_eur_currency_type         VARCHAR,
    is_vip_tenure                        BOOLEAN,
    vip_tenure_currency_type             VARCHAR,
    is_lead_to_vip_waterfall             BOOLEAN,
    lead_to_vip_waterfall_currency_type  VARCHAR,
    is_weekly_kpi                        BOOLEAN,
    weekly_kpi_currency_type             VARCHAR,
    is_daily_cash_file                   BOOLEAN,
    is_daily_cash_alt_budget             BOOLEAN
);

INSERT INTO _all_combos
SELECT CAST('Orders' AS VARCHAR)  AS metric_type,
       fso.store_id               AS event_store_id,
       COALESCE(vip_store_id, -1) AS vip_store_id,
       is_retail_vip,
       fso.gender                 AS customer_gender,
       fso.is_cross_promo,
       fso.finance_specialty_store,
       fso.is_scrubs_customer,
       NULL                       AS mapping_start_date,
       NULL                       AS mapping_end_date,
       FALSE                      AS is_ddd_consolidated,
       NULL                       AS ddd_consolidated_currency_type,
       FALSE                      AS is_ddd_individual,
       NULL                       AS ddd_individual_currency_type,
       FALSE                      AS is_ddd_hyperion,
       NULL                       AS ddd_hyperion_currency_type,
       FALSE                      AS is_retail_attribution_ddd,
       NULL                       AS retail_attribution_ddd_currency_type,
       FALSE                      AS is_daily_cash_usd,
       NULL                       AS daily_cash_usd_currency_type,
       FALSE                      AS is_daily_cash_eur,
       NULL                       AS daily_cash_eur_currency_type,
       FALSE                      AS is_vip_tenure,
       NULL                       AS vip_tenure_currency_type,
       FALSE                      AS is_lead_to_vip_waterfall,
       NULL                       AS lead_to_vip_waterfall_currency_type,
       FALSE                      AS is_weekly_kpi,
       NULL                       AS weekly_kpi_currency_type,
       FALSE                      AS is_daily_cash_file,
       FALSE                      AS is_daily_cash_alt_budget
FROM analytics_base.finance_sales_ops fso
         JOIN data_model_jfb.dim_customer dc ON dc.customer_id = fso.customer_id
         JOIN data_model_jfb.dim_store st ON st.store_id = dc.store_id

UNION

SELECT 'Acquisition' AS metric_type,
       sub_store_id  AS event_store_id,
       sub_store_id  AS vip_store_id,
       is_retail_vip,
       dc.gender     AS customer_gender,
       dc.is_cross_promo,
       dc.finance_specialty_store,
       IFF(st.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer,
       NULL          AS mapping_start_date,
       NULL          AS mapping_end_date,
       FALSE         AS is_ddd_consolidated,
       NULL          AS ddd_consolidated_currency_type,
       FALSE         AS is_ddd_individual,
       NULL          AS ddd_individual_currency_type,
       FALSE         AS is_ddd_hyperion,
       NULL          AS ddd_hyperion_currency_type,
       FALSE         AS is_retail_attribution_ddd,
       NULL          AS retail_attribution_ddd_currency_type,
       FALSE         AS is_daily_cash_usd,
       NULL          AS daily_cash_usd_currency_type,
       FALSE         AS is_daily_cash_eur,
       NULL          AS daily_cash_eur_currency_type,
       FALSE         AS is_vip_tenure,
       NULL          AS vip_tenure_currency_type,
       FALSE         AS is_lead_to_vip_waterfall,
       NULL          AS lead_to_vip_waterfall_currency_type,
       FALSE         AS is_weekly_kpi,
       NULL          AS weekly_kpi_currency_type,
       FALSE         AS is_daily_cash_file,
       FALSE         AS is_daily_cash_alt_budget
FROM data_model_jfb.fact_activation fa
         JOIN data_model_jfb.dim_store st ON st.store_id = fa.store_id
         JOIN data_model_jfb.dim_customer dc ON dc.customer_id = fa.customer_id

UNION

SELECT 'Acquisition' AS metric_type,
       sub_store_id  AS event_store_id,
       sub_store_id  AS vip_store_id,
       is_retail_vip,
       IFF(dc.gender = 'M' AND registration_local_datetime >= '2020-01-01' AND store_brand_abbr = 'FL', 'M',
           'F')      AS customer_gender,
       dc.is_cross_promo,
       dc.finance_specialty_store,
       IFF(st.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer,
       NULL          AS mapping_start_date,
       NULL          AS mapping_end_date,
       FALSE         AS is_ddd_consolidated,
       NULL          AS ddd_consolidated_currency_type,
       FALSE         AS is_ddd_individual,
       NULL          AS ddd_individual_currency_type,
       FALSE         AS is_ddd_hyperion,
       NULL          AS ddd_hyperion_currency_type,
       FALSE         AS is_retail_attribution_ddd,
       NULL          AS retail_attribution_ddd_currency_type,
       FALSE         AS is_daily_cash_usd,
       NULL          AS daily_cash_usd_currency_type,
       FALSE         AS is_daily_cash_eur,
       NULL          AS daily_cash_eur_currency_type,
       FALSE         AS is_vip_tenure,
       NULL          AS vip_tenure_currency_type,
       FALSE         AS is_lead_to_vip_waterfall,
       NULL          AS lead_to_vip_waterfall_currency_type,
       FALSE         AS is_weekly_kpi,
       NULL          AS weekly_kpi_currency_type,
       FALSE         AS is_daily_cash_file,
       FALSE         AS is_daily_cash_alt_budget
FROM data_model_jfb.fact_activation fa
         JOIN data_model_jfb.dim_store st ON st.store_id = fa.store_id
         JOIN data_model_jfb.dim_customer dc ON dc.customer_id = fa.customer_id

UNION

SELECT 'Acquisition'                                                         AS metric_type,
       st.store_id                                                           AS event_store_id,
       CASE WHEN st.store_id = 52 AND LOWER(fmc.channel) = 'physical partnerships' THEN 9999
            ELSE st.store_id END AS vip_store_id,
       IFF(LOWER(fmc.channel) = 'physical partnerships', 1, 0)               AS is_retail_vip,
       IFF(st.store_brand = 'Fabletics' AND fmc.is_mens_flag = 1, 'M', 'F')  AS customer_gender,
       0                                                                     AS is_cross_promo,
       CASE
           WHEN st.store_full_name = 'Fabletics AU' THEN 'AU'
           WHEN st.store_full_name = 'JustFab BE' THEN 'BE'
           WHEN st.store_full_name = 'Fabletics AT' THEN 'AT'
           WHEN st.store_full_name = 'JustFab AT' THEN 'AT'
           WHEN st.store_full_name = 'ShoeDazzle CA' THEN 'CA'
           WHEN st.store_full_name = 'FabKids CA' THEN 'CA'
           ELSE 'None'
           END                                                               AS finance_specialty_store,
       fmc.is_scrubs_flag                                                    AS is_scrubs_customer,
       NULL          AS mapping_start_date,
       NULL          AS mapping_end_date,
       FALSE         AS is_ddd_consolidated,
       NULL          AS ddd_consolidated_currency_type,
       FALSE         AS is_ddd_individual,
       NULL          AS ddd_individual_currency_type,
       FALSE         AS is_ddd_hyperion,
       NULL          AS ddd_hyperion_currency_type,
       FALSE         AS is_retail_attribution_ddd,
       NULL          AS retail_attribution_ddd_currency_type,
       FALSE         AS is_daily_cash_usd,
       NULL          AS daily_cash_usd_currency_type,
       FALSE         AS is_daily_cash_eur,
       NULL          AS daily_cash_eur_currency_type,
       FALSE         AS is_vip_tenure,
       NULL          AS vip_tenure_currency_type,
       FALSE         AS is_lead_to_vip_waterfall,
       NULL          AS lead_to_vip_waterfall_currency_type,
       FALSE         AS is_weekly_kpi,
       NULL          AS is_weekly_kpi_currency_type,
       FALSE         AS is_daily_cash_file,
       FALSE         AS is_daily_cash_alt_budget
FROM reporting_media_prod.dbo.vw_fact_media_cost fmc
         JOIN data_model_jfb.dim_store st ON st.store_id = fmc.store_id -- will ensure we get no specialty stores
WHERE lower(fmc.targeting) NOT IN ('vip retargeting', 'purchase retargeting', 'free alpha')
GROUP BY metric_type,
         event_store_id,
         vip_store_id,
         is_retail_vip,
         customer_gender,
         is_cross_promo,
         finance_specialty_store,
         fmc.is_scrubs_flag
;


DELETE
FROM _all_combos
WHERE event_store_id = -1
  AND vip_store_id = -1;

------------------------------------------------------------------------
------------------------------------------------------------------------
-- NOTE IN ALL THE FOLLOWING IF YOU DO NOT EXPLICITLY FILTER FOR METRIC TYPE, YOU WILL INCLUDE
-- ACQUISTION METRICS & ORDER METRICS (includes credit billing metrics)
-- FOR RREV EXPLICIT SEGMENTS WE FILTER OUT metric_type = 'Acquistion' to not show any acquistion metrics
-- we also exclude credits issued & cancelled, but we are already handling that in FSO directly since no credits issued/cancelled
-- will ever be associated with RREV segments

TRUNCATE TABLE reference.finance_segment_mapping;

------------------------------TREV Rollup Segments-----------------------------------
------------------------Include Orders & Acquistion Metrics--------------------------

----TREV:Brand by country & region
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-TREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
UNION ALL
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-TREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id;

-- all brands consolidated
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'ALL-TREV' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac;

-- all brands, excluding Savage X
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'ALLEXCLSXF-TREV' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand != 'Savage X';

-- JFBWW: JFNA + SDNA + FKNA + JFEU
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'JFB-TREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('JustFab', 'ShoeDazzle', 'FabKids');


-- JFBNA: JFNA + SDNA + FKNA
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'JFB-TREV-NA' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('JustFab', 'ShoeDazzle', 'FabKids')
AND event_store.store_region = 'NA';

-- JFNA + SDNA
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'JFSD-TREV-NA' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('JustFab', 'ShoeDazzle')
AND event_store.store_region = 'NA';

-- FLWW: includes FLNA + FLEU
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'FL+SC-TREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Fabletics');


-- FLWW: includes FLNA + FLEU +YT
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'FLC-TREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Fabletics','Yitty');

-- FLCWW OREV: includes FLNA + FLEU + YT
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'FLC-OREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Fabletics', 'Yitty')
    AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
            event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));


-- FLWW OREV: includes FLNA + FLEU
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'FL+SC-OREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Fabletics')
    AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
            event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));

-- FLWW RREV: includes FLNA + FLEU
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'FL+SC-RREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Fabletics')
    AND event_store.store_type = 'Retail';



-- FLWW: split by mens & womens
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'FLC-' || iff(ac.customer_gender='F','W',ac.customer_gender) || '-TREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Fabletics', 'Yitty');


-- YTWW: includes all Yitty
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'YT-TREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Yitty');


-- Include Scrub VIP Mappings
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , iff(ac.is_scrubs_customer = TRUE, 'SC','FL') || '-OREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Fabletics') and event_store.store_region = 'NA'
    AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
            event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));

INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , iff(ac.is_scrubs_customer = TRUE, 'SC','FL') || '-' || iff(ac.customer_gender='F','W',customer_gender) || '-OREV-' || event_store.store_region  AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Fabletics') and event_store.store_region = 'NA'
    AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
            event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));


-- SXWW: includes SXNA + SXEU
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'SX-TREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand = 'Savage X';

-- SXWW OREV: includes SXNA + SXEU
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'SX-OREV-Global' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand IN ('Savage X')
    AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
            event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));

--FL+YTYNA-TREV : includes both FL and YTY
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'FLC-TREV-NA' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand in ('Fabletics','Yitty')
and event_store.store_region = 'NA';


------------------------------RREV Segments------------------------------------------
------------------------Excluding Acquisition Metrics--------------------------------

-- Brand + Country - RREV
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-RREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE metric_type = 'Orders'            -- do not include any acquistion metrics
  AND event_store.store_type = 'Retail' -- making sure orders are in retail (will filter out billing issuances/cancels automatically)
UNION ALL
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
            || '-RREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE metric_type = 'Orders' -- do not include any acquistion metrics
  AND event_store.store_type = 'Retail';
-- making sure orders are in retail (will filter out billing issuances/cancels automatically)


-------------------OREV Segments (Incl. Online + Mobile App Orders)------------------
------------------------Including Orders & Acquisition Metrics-----------------------

-- Brand + Country - OREV
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
         || '-OREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
    AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
            event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'))
UNION ALL
-- Brand + Region - OREV
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
          || '-OREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
          event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));


----------------------GPREV Segments (Group Order Orders Only)---------------------
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-GPREV-' || event_store.store_country  AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_type = 'Group Order'
UNION ALL
-- Brand + Region - AppRev (not used in DDD, used in daily Cash)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
          || '-GPREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_type = 'Group Order';



----------------------APPREV Segments (Mobile App Orders Only)---------------------
------------------------Including Orders & Acquisition Metrics----------------------
-- for acquistion metrics there are some vips activating in mobile app, so we can consider them --

-- Brand + Country - APPREV (not used in DDD, used in daily Cash)
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-APPREV-' || event_store.store_country  AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_type = 'Mobile App'
UNION ALL
-- Brand + Region - AppRev (not used in DDD, used in daily Cash)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
          || '-APPREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_type = 'Mobile App';

----------------------Web Rev Segments (Online Excl. Mobile App Orders)-------------
------------------------Including Orders & Acquisition Metrics----------------------
-- for acquistion metrics there are some vips activating in mobile app, so we can consider them --

INSERT INTO reference.finance_segment_mapping
-- store brand + store country - WEBREV (not used in DDD, used in daily Cash)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
          || '-WEBREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_type = 'Online'
UNION ALL
-- store brand + store region - WEBREV (not used in DDD, used in daily Cash)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
            || '-WEBREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_type = 'Online';

---------------------Retail VIPs - Total Revenue------------------------------------
-----------------Including Orders & Acquisition Metrics for retail vips-------------

INSERT INTO reference.finance_segment_mapping
-- store brand + store country - Retail Activated VIPs (RVIP) - Total Revenue (TREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
            || '-R-TREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store
              ON event_store.store_id = ac.event_store_id -- getting brand+country from vip store
WHERE ac.is_retail_vip = TRUE -- ensuring customer is a retail vip
UNION ALL
-- store brand + store region - Retail Activated VIPs (RVIP) - Total Revenue (TREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
            || '-R-TREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_retail_vip = TRUE;

---------------------Retail VIPs - Online Revenue------------------------------------
-----------Including Online Orders & Acquisition Metrics for retail vips-------------

INSERT INTO reference.finance_segment_mapping
-- store brand + store country - RVIP-OREV
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-R-OREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_retail_vip = TRUE                                           -- ensuring customer is a retail vip
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
          event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order')) -- ensuring purchase is online (including mobile app)
UNION ALL
-- store brand + store region - RVIP-OREV
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-R-OREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_retail_vip = TRUE -- ensuring customer is a retail vip
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
          event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));
-- ensuring purchase is online (including mobile app)

---------------------Retail VIPs - Retail Revenue------------------------------------
-----------Order Metrics in Retail from Retail VIPs - NO ACQUISITION METRICS---------

INSERT INTO reference.finance_segment_mapping
-- store brand + store country - RVIP-RREV
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-R-RREV-' || event_store.store_country  AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_retail_vip = TRUE
  AND ac.metric_type = 'Orders'         -- do not include any acquisition metrics
  AND event_store.store_type = 'Retail' -- order is in a retail store
UNION ALL
-- store brand + store region - RVIP-RREV
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-R-RREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_retail_vip = TRUE
  AND ac.metric_type = 'Orders' -- do not include any acquisition metrics
  AND event_store.store_type = 'Retail';
-- order is in a retail store

---------------------Online VIPs - Online Revenue------------------------------------
----------------------Order & Acquisition Metrics------------------------------------

INSERT INTO reference.finance_segment_mapping
-- store brand + store country - OVIP-OREV
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-O-OREV-' || event_store.store_country  AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_retail_vip = FALSE                                          -- not a retail vip
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
          event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order')) -- online orders incl mobile app
UNION ALL
-- store brand + store region - OVIP-OREV
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-O-OREV-' || event_store.store_region  AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_retail_vip = FALSE -- not a retail vip
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
          event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));
-- online orders incl mobile app


-----------------Fabkids Cross Promo vs Not Cross Promo------------------------------
----------------------Order & Acquisition Metrics------------------------------------

INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , 'FK-CP-TREV-NA' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_cross_promo = TRUE
  AND event_store.store_brand = 'FabKids'
UNION ALL
SELECT ac.*
     , 'FK-NCP-TREV-NA' AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_cross_promo = FALSE
  AND event_store.store_brand = 'FabKids';

-----Online VIP (& non vip b/c filter is != Retail VIP - RETAIL REVENUE--------------
----------------------Order & Acquisition Metrics------------------------------------

INSERT INTO reference.finance_segment_mapping
-- store brand + store country - 0VIP-RREV
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-O-RREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_retail_vip = FALSE
  AND event_store.store_type = 'Retail'
  AND ac.metric_type = 'Orders'
UNION ALL
-- store brand + store region - 0VIP-RREV
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-O-RREV-' || event_store.store_region  AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.is_retail_vip = FALSE
  AND event_store.store_type = 'Retail'
  AND ac.metric_type = 'Orders';


------------Fabletics: Male vs. Female VIP - TREV, OREV & RREV-----------------------------
----------------------TREV: Order & Acquisition Metrics------------------------------------
----------------------OREV: Order & Acquisition Metrics------------------------------------
----------------------------RREV: Order Metrics--------------------------------------------

INSERT INTO reference.finance_segment_mapping
-- brand + country - female/male vip - total revenue (TREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender) || '-TREV-' || event_store.store_country  AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand = 'Fabletics'

UNION ALL
-- brand + region - female/male vip - total revenue (TREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender) || '-TREV-' || event_store.store_region  AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand = 'Fabletics';


INSERT INTO reference.finance_segment_mapping
-- brand + country - female/male vip - online revenue (OREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender) || '-OREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand = 'Fabletics'
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
          event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'))
UNION ALL
-- brand + region - female/male vip - online revenue (OREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender) || '-OREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand = 'Fabletics'
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
          event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));

INSERT INTO reference.finance_segment_mapping
-- brand + country - female/male vip - retail revenue (RREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
            || '-' || iff(ac.customer_gender='F','W',ac.customer_gender) || '-RREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE metric_type = 'Orders'           -- no acquisition metrics for RREV
  AND event_store.store_type = 'Retail'-- retail revenue
  AND event_store.store_brand = 'Fabletics'
UNION ALL
-- brand + region - female/male vip - retail revenue (RREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
            || '-' || iff(ac.customer_gender='F','W',ac.customer_gender) || '-RREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE metric_type = 'Orders'           -- no acquisition metrics for RREV
  AND event_store.store_type = 'Retail'-- retail revenue
  AND event_store.store_brand = 'Fabletics';


----Fabletics: Male vs. Female VIP Combine w/ Retail vs. Online Activated - TREV, OREV & RREV---------------
----------------------TREV: Order & Acquisition Metrics-----------------------------------------------------
----------------------OREV: Order & Acquisition Metrics-----------------------------------------------------
----------------------------RREV: Order Metrics-------------------------------------------------------------

INSERT INTO reference.finance_segment_mapping
-- brand + country - female/male vip - retail/online activated - total revenue (TREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender)
           || '-' || iff(ac.is_retail_vip = TRUE, 'R', 'O') || '-TREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand = 'Fabletics'
;

INSERT INTO reference.finance_segment_mapping
-- brand + country - female/male vip - retail/online activated - online revenue (OREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender)
           || '-' || iff(ac.is_retail_vip = TRUE, 'R', 'O') || '-OREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand = 'Fabletics'
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
          event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'))
UNION ALL
-- brand + region - female/male vip - retail/online activated - online revenue (OREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender)
           || '-' || iff(ac.is_retail_vip = TRUE, 'R', 'O') || '-OREV-' || event_store.store_region AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand = 'Fabletics'
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
          event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));

INSERT INTO reference.finance_segment_mapping
-- brand + country - female/male vip - retail/online activated - retail revenue (RREV)
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender)
           || '-' || iff(ac.is_retail_vip = TRUE, 'R', 'O')  || '-RREV-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE event_store.store_brand = 'Fabletics'
  AND event_store.store_type = 'Retail'
  AND ac.metric_type = 'Orders' -- no acquisition metrics
;

------------------------Finance Specialty Stores: TREV-----------------------------
----------------------TREV: Order & Acquisition Metrics----------------------------
-- create combos for FLAU, SXBF, etc.
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , case when st.store_brand='Yitty' then 'YT'
            when st.store_brand='Fabletics' and st.store_region = 'NA' then 'FL+SC' else st.store_brand_abbr end
            || '-TREV-' || ac.finance_specialty_store || '-' || st.store_country as report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store st ON st.store_id = ac.event_store_id
WHERE ac.finance_specialty_store != 'None'
  AND store_name != 'JustFab CA';

------------------------Finance Specialty Stores: gender &OREV-----------------------------

INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender)
           || '-OREV-' || ac.finance_specialty_store || '-' || event_store.store_country AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.finance_specialty_store != 'None'
  AND store_name != 'JustFab CA'
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
            event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));

----------------------TREV: Finance Specialty Store = None----------------------------
-- create combos for FLAU, SXBF, etc.
INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , case when st.store_brand='Yitty' then 'YT'
            when st.store_brand='Fabletics' and st.store_region = 'NA' then 'FL+SC' else st.store_brand_abbr end
           || '-TREV-' || st.store_country || 'only' || '-' || st.store_country AS report_mapping
     , null AS business_unit
     , null AS store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store st ON st.store_id = ac.event_store_id
WHERE ac.finance_specialty_store = 'None'
  AND store_name != 'JustFab CA';

------------------------Finance Specialty Stores = None: gender &OREV-----------------------------

INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , case when event_store.store_brand='Yitty' then 'YT'
            when event_store.store_brand='Fabletics' and event_store.store_region = 'NA' then 'FL+SC' else event_store.store_brand_abbr end
           || '-' || iff(ac.customer_gender='F','W',ac.customer_gender)
           || '-OREV-' || event_store.store_country || 'only' || '-' || event_store.store_country AS report_mapping
     , null AS business_unit
     , null AS store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store event_store ON event_store.store_id = ac.event_store_id
WHERE ac.finance_specialty_store = 'None'
  AND store_name != 'JustFab CA'
  AND IFF(metric_type = 'Orders', event_store.store_type IN ('Online', 'Mobile App'),
            event_store.store_type IN ('Retail', 'Online', 'Mobile App', 'Group Order'));

------------------------Hyperion Store Combos--------------------------------------

INSERT INTO reference.finance_segment_mapping
SELECT ac.*
     , CASE
           WHEN store_brand_abbr || store_country = 'JFDE' THEN 'JF-TREV-DE412'
           WHEN store_brand_abbr || store_country = 'JFNL' THEN 'JF-TREV-NL412'
           WHEN store_brand_abbr || store_country = 'JFES' THEN 'JF-TREV-ES432'
           WHEN store_brand_abbr || store_country = 'JFFR' THEN 'JF-TREV-FR432'
           WHEN store_brand_abbr || store_country = 'JFIT' THEN 'JF-TREV-IT432'
           WHEN store_brand_abbr || store_country = 'JFUK' THEN 'JF-TREV-UK422'
           WHEN store_brand_abbr || store_country = 'JFDK' THEN 'JF-TREV-DK422'
           WHEN store_brand_abbr || store_country = 'JFSE' THEN 'JF-TREV-SE422' END
    AS report_mapping
     , null as business_unit
     , null as store_brand
FROM _all_combos ac
         JOIN data_model_jfb.dim_store st ON st.store_id = ac.event_store_id
WHERE st.store_brand_abbr || st.store_country IN
      ('SXUK', 'JFDE', 'JFNL', 'JFES', 'JFFR', 'JFIT', 'JFUK', 'JFDK', 'JFSE');


----------------------TREV: Order & Acquisition Metrics----------------------------
-------------------------RETAIL ATTRIBUTION REPORT-----------------------

-- first: include the topline segments
UPDATE reference.finance_segment_mapping
SET is_retail_attribution_ddd            = TRUE,
    retail_attribution_ddd_currency_type = 'USD'
WHERE report_mapping IN ('FL+SC-W-R-OREV-US', 'FL+SC-M-R-OREV-US',
                         'FL+SC-W-R-RREV-US', 'FL+SC-M-R-RREV-US');


------INCORPORATE MAPPINGS FOR LEADS-------
CREATE OR REPLACE TEMPORARY TABLE _media_spend_only as
SELECT distinct
       CASE WHEN st.store_id = 52 AND LOWER(fmc.channel) = 'physical partnerships' THEN 9999
            ELSE st.store_id END AS vip_store_id
FROM reporting_media_prod.dbo.vw_fact_media_cost fmc
         JOIN data_model_jfb.dim_store st ON st.store_id = fmc.store_id -- will ensure we get no specialty stores
WHERE lower(fmc.targeting) NOT IN ('vip retargeting', 'purchase retargeting', 'free alpha')
MINUS SELECT DISTINCT sub_store_id AS vip_store_id FROM data_model_jfb.fact_activation
MINUS SELECT DISTINCT vip_store_id FROM analytics_base.finance_sales_ops;

INSERT INTO reference.finance_segment_mapping
SELECT DISTINCT metric_type,
                event_store_id,
                -1 AS vip_store_id,
                is_retail_vip,
                customer_gender,
                is_cross_promo,
                finance_specialty_store,
                is_scrubs_customer,
                mapping_start_date,
                mapping_end_date,
                is_ddd_consolidated,
                ddd_consolidated_currency_type,
                is_ddd_individual,
                ddd_individual_currency_type,
                is_ddd_hyperion,
                ddd_hyperion_currency_type,
                is_retail_attribution_ddd,
                retail_attribution_ddd_currency_type,
                is_daily_cash_usd,
                daily_cash_usd_currency_type,
                is_daily_cash_eur,
                daily_cash_eur_currency_type,
                is_vip_tenure,
                vip_tenure_currency_type,
                is_lead_to_vip_waterfall,
                lead_to_vip_waterfall_currency_type,
                is_weekly_kpi,
                weekly_kpi_currency_type,
                is_daily_cash_file,
                is_daily_cash_alt_budget,
                report_mapping,
                business_unit,
                store_brand
FROM reference.finance_segment_mapping
WHERE metric_type = 'Acquisition'
AND vip_store_id NOT IN (SELECT vip_store_id FROM _media_spend_only);



------------------------DDD CONSOLIDATED MAPPINGS------------------------

UPDATE reference.finance_segment_mapping
SET is_ddd_consolidated            = TRUE,
    ddd_consolidated_currency_type = 'USD' -- update currency to usd
WHERE report_mapping IN (
                        'FK-TREV-NA',
                        'FLC-TREV-NA',
                        'FL-W-TREV-EU',
                        'FL-M-TREV-EU',
                        'FL-TREV-EU',
                        'FL-OREV-NA',
                        'FL+SC-W-O-OREV-NA',
                        'FL+SC-W-R-OREV-NA',
                        'FL-W-OREV-NA',
                        'FL+SC-W-OREV-NA',
                        'SC-W-OREV-NA',
                        'FL+SC-M-O-OREV-NA',
                        'FL+SC-M-R-OREV-NA',
                        'FL-M-OREV-NA',
                        'FL+SC-M-OREV-NA',
                        'SC-M-OREV-NA',
                        'FL+SC-OREV-NA',
                        'FL+SC-O-OREV-NA',
                        'FL+SC-RREV-NA',
                        'FL+SC-R-OREV-NA',
                        'SC-OREV-NA',
                        'FL+SC-TREV-NA',
                        'FL+SC-GPREV-NA',
                        'JF-TREV-EU',
                        'JF-TREV-NA',
                        'JFSD-TREV-NA',
                        'SD-TREV-NA',
                        'SX-TREV-EU',
                        'SX-OREV-NA',
                        'SX-O-OREV-NA',
                        'SX-RREV-NA',
                        'SX-R-OREV-NA',
                        'SX-TREV-NA',
                        'YT-OREV-NA'
    );



------------------------DDD INDIVIDUAL MAPPINGS-----------------------------

UPDATE reference.finance_segment_mapping
SET is_ddd_individual            = TRUE,
    ddd_individual_currency_type = st.store_currency -- update currency to local store currency
FROM data_model_jfb.dim_store st
WHERE st.store_id = finance_segment_mapping.event_store_id
  AND finance_segment_mapping.report_mapping IN (
                                                    'FK-CP-TREV-NA',
                                                    'FK-NCP-TREV-NA',
                                                    'FK-TREV-US',
                                                    'FL+SC-TREV-CA',
                                                    'FL-W-OREV-AT-DE',
                                                    'FL-M-OREV-AT-DE',
                                                    'FL-W-OREV-DEonly-DE',
                                                    'FL-M-OREV-DEonly-DE',
                                                    'FL-W-O-OREV-DE',
                                                    'FL-W-R-OREV-DE',
                                                    'FL-W-OREV-DE',
                                                    'FL-W-TREV-DE',
                                                    'FL-M-O-OREV-DE',
                                                    'FL-M-R-OREV-DE',
                                                    'FL-M-OREV-DE',
                                                    'FL-M-TREV-DE',
                                                    'FL-TREV-DE',
                                                    'FL-W-OREV-DK',
                                                    'FL-W-TREV-DK',
                                                    'FL-M-OREV-DK',
                                                    'FL-TREV-DK',
                                                    'FL-W-OREV-ES',
                                                    'FL-W-TREV-ES',
                                                    'FL-M-OREV-ES',
                                                    'FL-TREV-ES',
                                                    'FL-W-OREV-BE-FR',
                                                    'FL-M-OREV-BE-FR',
                                                    'FL-W-OREV-FRonly-FR',
                                                    'FL-M-OREV-FRonly-FR',
                                                    'FL-W-OREV-FR',
                                                    'FL-W-TREV-FR',
                                                    'FL-M-OREV-FR',
                                                    'FL-M-TREV-FR',
                                                    'FL-TREV-FR',
                                                    'FL-W-OREV-BE-NL',
                                                    'FL-W-OREV-NL',
                                                    'FL-W-TREV-NL',
                                                    'FL-M-OREV-NL',
                                                    'FL-W-OREV-NLonly-NL',
                                                    'FL-M-OREV-NLonly-NL',
                                                    'FL-TREV-NL',
                                                    'FL-W-OREV-SE',
                                                    'FL-W-TREV-SE',
                                                    'FL-M-OREV-SE',
                                                    'FL-TREV-SE',
                                                    'FL-W-O-OREV-UK',
                                                    'FL-W-R-OREV-UK',
                                                    'FL-W-OREV-UK',
                                                    'FL-W-TREV-UK',
                                                    'FL-M-O-OREV-UK',
                                                    'FL-M-R-OREV-UK',
                                                    'FL-M-OREV-UK',
                                                    'FL-M-TREV-UK',
                                                    'FL-TREV-UK',
                                                    'FL+SC-W-O-OREV-US',
                                                    'FL+SC-W-R-OREV-US',
                                                    'FL+SC-W-OREV-US',
                                                    'FL+SC-M-O-OREV-US',
                                                    'FL+SC-M-R-OREV-US',
                                                    'FL+SC-M-OREV-US',
                                                    'FL+SC-OREV-US',
                                                    'FL+SC-O-OREV-US',
                                                    'FL+SC-RREV-US',
                                                    'FL+SC-R-OREV-US',
                                                    'FL+SC-TREV-US',
                                                    'JF-TREV-CA',
                                                    'JF-TREV-DE',
                                                    'JF-TREV-DK',
                                                    'JF-TREV-ES',
                                                    'JF-TREV-FR',
                                                    'JF-TREV-IT',
                                                    'JF-TREV-NL',
                                                    'JF-TREV-SE',
                                                    'JF-TREV-UK',
                                                    'JF-TREV-US',
                                                    'SD-TREV-US',
                                                    'SX-TREV-DE',
                                                    'SX-TREV-ES',
                                                    'SX-TREV-EUREM',
                                                    'SX-TREV-FR',
                                                    'SX-TREV-UK',
                                                    'SX-OREV-US',
                                                    'SX-O-OREV-US',
                                                    'SX-RREV-US',
                                                    'SX-R-OREV-US',
                                                    'SX-TREV-US',
                                                    'SX-TREV-CA-US'
                                                 );



------------------------DAILY CASH USD LOCAL MAPPINGS-----------------------------

UPDATE reference.finance_segment_mapping
SET is_daily_cash_usd            = TRUE,
    daily_cash_usd_currency_type = 'USD'
WHERE report_mapping IN (
                            'FK-CP-TREV-NA',
                            'FK-NCP-TREV-NA',
                            'FK-TREV-NA',
                            'FK-TREV-CA-US',
                            'FK-TREV-USonly-US',
                            'JF-TREV-USonly-US',
                            'SD-TREV-USonly-US',
                            'FL+SC-TREV-CA',
                            'FLC-TREV-NA',
                            'FLC-W-TREV-Global',
                            'FLC-M-TREV-Global',
                            'FLC-OREV-Global',
                            'FLC-TREV-Global',
                            'FL-APPREV-DE',
                            'FL-TREV-AT-DE',
                            'FL-W-OREV-DE',
                            'FL-M-OREV-DE',
                            'FL-OREV-DE',
                            'FL-RREV-DE',
                            'FL-TREV-DE',
                            'FL-WEBREV-DE',
                            'FL-TREV-DK',
                            'FL-APPREV-ES',
                            'FL-W-OREV-ES',
                            'FL-M-OREV-ES',
                            'FL-TREV-ES',
                            'FL-WEBREV-ES',
                            'FL-APPREV-ES',
                            'FL-W-OREV-EU',
                            'FL-W-TREV-EU',
                            'FL-M-OREV-EU',
                            'FL-M-TREV-EU',
                            'FL-OREV-EU',
                            'FL-O-OREV-EU',
                            'FL-RREV-EU',
                            'FL-R-OREV-EU',
                            'FL-TREV-EU',
                            'FL-WEBREV-EU',
                            'FL-APPREV-FR',
                            'FL-TREV-BE-FR',
                            'FL-W-OREV-FR',
                            'FL-M-OREV-FR',
                            'FL-TREV-FR',
                            'FL-WEBREV-FR',
                            'FL+SC-APPREV-NA',
                            'FL+SC-OREV-NA',
                            'FL+SC-GPREV-NA',
                            'FL-OREV-NA',
                            'FL-W-OREV-NA',
                            'FL+SC-W-OREV-NA',
                            'SC-W-OREV-NA',
                            'FL-M-OREV-NA',
                            'FL+SC-M-OREV-NA',
                            'SC-M-OREV-NA',
                            'FL+SC-O-OREV-NA',
                            'FL+SC-RREV-NA',
                            'FL+SC-W-RREV-NA',
                            'FL+SC-M-RREV-NA',
                            'FL+SC-R-OREV-NA',
                            'SC-OREV-NA',
                            'FL+SC-TREV-NA',
                            'FL-TREV-NL',
                            'FL-TREV-SE',
                            'FL-APPREV-UK',
                            'FL-W-OREV-UK',
                            'FL-M-OREV-UK',
                            'FL-OREV-UK',
                            'FL-RREV-UK',
                            'FL-TREV-UK',
                            'FL-WEBREV-UK',
                            'FL+SC-APPREV-US',
                            'FL+SC-TREV-AU-US',
                            'FL+SC-OREV-US',
                            'FL+SC-RREV-US',
                            'FL+SC-TREV-US',
                            'FL+SC-WEBREV-US',
                            'FL+SC-RREV-Global',
                            'FL+SC-TREV-Global',
                            'JFB-TREV-NA',
                            'JFB-TREV-Global',
                            'JF-TREV-CA',
                            'JF-APPREV-DE',
                            'JF-TREV-AT-DE',
                            'JF-TREV-DE',
                            'JF-WEBREV-DE',
                            'JF-TREV-DK',
                            'JF-APPREV-ES',
                            'JF-TREV-ES',
                            'JF-WEBREV-ES',
                            'JF-APPREV-EU',
                            'JF-TREV-EU',
                            'JF-WEBREV-EU',
                            'JF-APPREV-FR',
                            'JF-TREV-BE-FR',
                            'JF-TREV-FR',
                            'JF-WEBREV-FR',
                            'JF-APPREV-NA',
                            'JF-TREV-NA',
                            'JF-TREV-BE-NL',
                            'JF-TREV-NL',
                            'JFSD-TREV-NA',
                            'JF-TREV-SE',
                            'JF-APPREV-UK',
                            'JF-TREV-UK',
                            'JF-WEBREV-UK',
                            'JF-APPREV-US',
                            'JF-TREV-US',
                            'JF-WEBREV-US',
                            'SD-TREV-NA',
                            'SD-TREV-CA-NA',
                            'SX-TREV-DE',
                            'SX-TREV-ES',
                            'SX-TREV-EU',
                            'SX-TREV-EUREM',
                            'SX-TREV-FR',
                            'SX-OREV-NA',
                            'SX-TREV-NA',
                            'SX-TREV-UK',
                            'SX-BF-TREV-US',
                            'SX-OREV-US',
                            'SX-O-OREV-US',
                            'SX-RREV-US',
                            'SX-RREV-NA',
                            'SX-R-OREV-US',
                            'SX-OREV-Global',
                            'SX-TREV-Global',
                            'SX-TREV-USonly-US',
                            'SX-TREV-CA-US',
                            'YT-OREV-NA',
                            'YT-TREV-Global',
                            'SX-OREV-EU',
                            'SX-OREV-DE',
                            'SX-OREV-ES',
                            'SX-OREV-EUREM',
                            'SX-OREV-FR',
                            'SX-OREV-UK'
    );


------------------------DAILY CASH EU LOCAL MAPPINGS-----------------------------

UPDATE reference.finance_segment_mapping
SET is_daily_cash_eur            = TRUE,
    daily_cash_eur_currency_type = st.store_currency -- use local currency
FROM data_model_jfb.dim_store st
WHERE st.store_id = finance_segment_mapping.event_store_id
  AND finance_segment_mapping.report_mapping IN (
                                                    'FL-APPREV-DE',
                                                    'FL-TREV-AT-DE',
                                                    'FL-W-OREV-DE',
                                                    'FL-M-OREV-DE',
                                                    'FL-OREV-DE',
                                                    'FL-RREV-DE',
                                                    'FL-TREV-DE',
                                                    'FL-WEBREV-DE',
                                                    'FL-TREV-DK',
                                                    'FL-APPREV-ES',
                                                    'FL-W-OREV-ES',
                                                    'FL-M-OREV-ES',
                                                    'FL-TREV-ES',
                                                    'FL-WEBREV-ES',
                                                    'FL-APPREV-FR',
                                                    'FL-TREV-BE-FR',
                                                    'FL-W-OREV-FR',
                                                    'FL-M-OREV-FR',
                                                    'FL-TREV-FR',
                                                    'FL-WEBREV-FR',
                                                    'FL-TREV-NL',
                                                    'FL-TREV-SE',
                                                    'FL-APPREV-UK',
                                                    'FL-W-OREV-UK',
                                                    'FL-M-OREV-UK',
                                                    'FL-OREV-UK',
                                                    'FL-RREV-UK',
                                                    'FL-TREV-UK',
                                                    'FL-WEBREV-UK',
                                                    'JF-APPREV-DE',
                                                    'JF-TREV-AT-DE',
                                                    'JF-TREV-DE',
                                                    'JF-WEBREV-DE',
                                                    'JF-TREV-DK',
                                                    'JF-APPREV-ES',
                                                    'JF-TREV-ES',
                                                    'JF-WEBREV-ES',
                                                    'JF-APPREV-FR',
                                                    'JF-TREV-BE-FR',
                                                    'JF-TREV-FR',
                                                    'JF-WEBREV-FR',
                                                    'JF-TREV-IT',
                                                    'JF-TREV-BE-NL',
                                                    'JF-TREV-NL',
                                                    'JF-TREV-SE',
                                                    'JF-APPREV-UK',
                                                    'JF-TREV-UK',
                                                    'JF-WEBREV-UK',
                                                    'SX-TREV-DE',
                                                    'SX-TREV-ES',
                                                    'SX-TREV-EUREM',
                                                    'SX-TREV-FR',
                                                    'SX-TREV-UK',
                                                    'SX-OREV-DE',
                                                    'SX-OREV-ES',
                                                    'SX-OREV-EUREM',
                                                    'SX-OREV-FR',
                                                    'SX-OREV-UK'
                                                 );



------------------------DAILY CASH FILE ONLY MAPPINGS-----------------------------

UPDATE reference.finance_segment_mapping
SET is_daily_cash_file            = TRUE,
    daily_cash_usd_currency_type = 'USD'
WHERE report_mapping IN (
                           'FK-CP-TREV-NA',
                            'FK-NCP-TREV-NA',
                            'FK-TREV-CA-US',
                            'FK-TREV-NA',
                            'FL+SC-APPREV-NA',
                            'FL+SC-APPREV-US',
                            'FL+SC-GPREV-NA',
                            'FL+SC-M-OREV-NA',
                            'FL+SC-O-OREV-NA',
                            'FL+SC-OREV-NA',
                            'FL+SC-OREV-US',
                            'FL+SC-R-OREV-NA',
                            'FL+SC-RREV-Global',
                            'FL+SC-RREV-NA',
                            'FL+SC-RREV-US',
                            'FL+SC-TREV-AU-US',
                            'FL+SC-TREV-CA',
                            'FL+SC-TREV-Global',
                            'FL+SC-TREV-NA',
                            'FL+SC-TREV-US',
                            'FL+SC-W-OREV-NA',
                            'FL+SC-WEBREV-US',
                            'FL-APPREV-DE',
                            'FL-APPREV-ES',
                            'FL-APPREV-FR',
                            'FL-APPREV-UK',
                            'FL-M-OREV-DE',
                            'FL-M-OREV-ES',
                            'FL-M-OREV-EU',
                            'FL-M-OREV-FR',
                            'FL-M-OREV-NA',
                            'FL-M-OREV-UK',
                            'FL-M-TREV-EU',
                            'FL-O-OREV-EU',
                            'FL-OREV-DE',
                            'FL-OREV-EU',
                            'FL-OREV-NA',
                            'FL-OREV-UK',
                            'FL-R-OREV-EU',
                            'FL-RREV-DE',
                            'FL-RREV-EU',
                            'FL-RREV-UK',
                            'FL-TREV-AT-DE',
                            'FL-TREV-BE-FR',
                            'FL-TREV-DE',
                            'FL-TREV-DK',
                            'FL-TREV-ES',
                            'FL-TREV-EU',
                            'FL-TREV-FR',
                            'FL-TREV-NL',
                            'FL-TREV-SE',
                            'FL-TREV-UK',
                            'FL-W-OREV-DE',
                            'FL-W-OREV-ES',
                            'FL-W-OREV-EU',
                            'FL-W-OREV-FR',
                            'FL-W-OREV-NA',
                            'FL-W-OREV-UK',
                            'FL-W-TREV-EU',
                            'FL-WEBREV-DE',
                            'FL-WEBREV-ES',
                            'FL-WEBREV-EU',
                            'FL-WEBREV-FR',
                            'FL-WEBREV-UK',
                            'FLC-M-TREV-Global',
                            'FLC-OREV-Global',
                            'FLC-TREV-Global',
                            'FLC-TREV-NA',
                            'FLC-W-TREV-Global',
                            'JF-APPREV-DE',
                            'JF-APPREV-ES',
                            'JF-APPREV-EU',
                            'JF-APPREV-FR',
                            'JF-APPREV-NA',
                            'JF-APPREV-UK',
                            'JF-APPREV-US',
                            'JF-TREV-AT-DE',
                            'JF-TREV-BE-FR',
                            'JF-TREV-BE-NL',
                            'JF-TREV-CA',
                            'JF-TREV-DE',
                            'JF-TREV-DK',
                            'JF-TREV-ES',
                            'JF-TREV-EU',
                            'JF-TREV-FR',
                            'JF-TREV-IT',
                            'JF-TREV-NA',
                            'JF-TREV-NL',
                            'JF-TREV-SE',
                            'JF-TREV-UK',
                            'JF-TREV-US',
                            'JF-WEBREV-DE',
                            'JF-WEBREV-ES',
                            'JF-WEBREV-EU',
                            'JF-WEBREV-FR',
                            'JF-WEBREV-UK',
                            'JF-WEBREV-US',
                            'JFB-TREV-Global',
                            'JFB-TREV-NA',
                            'JFSD-TREV-NA',
                            'SC-M-OREV-NA',
                            'SC-OREV-NA',
                            'SC-W-OREV-NA',
                            'SD-TREV-NA',
                            'SX-O-OREV-US',
                            'SX-OREV-Global',
                            'SX-OREV-NA',
                            'SX-OREV-US',
                            'SX-R-OREV-US',
                            'SX-RREV-NA',
                            'SX-RREV-US',
                            'SX-TREV-CA-US',
                            'SX-TREV-DE',
                            'SX-TREV-ES',
                            'SX-TREV-EU',
                            'SX-TREV-EUREM',
                            'SX-TREV-FR',
                            'SX-TREV-Global',
                            'SX-TREV-NA',
                            'SX-TREV-UK',
                            'YT-OREV-NA',
                            'YT-TREV-Global',
                            'SX-OREV-EU',
                            'SX-OREV-DE',
                            'SX-OREV-ES',
                            'SX-OREV-EUREM',
                            'SX-OREV-FR',
                            'SX-OREV-UK'
    );

------------------------DAILY CASH ALTERNATE BUDGET MAPPINGS-----------------------------
UPDATE reference.finance_segment_mapping
SET is_daily_cash_alt_budget     = TRUE,
    daily_cash_usd_currency_type = 'USD'
WHERE report_mapping IN ('FL+SC-TREV-Global',
                         'FL+SC-TREV-NA',
                         'FL+SC-RREV-Global',
                         'FL+SC-W-OREV-NA',
                         'FL+SC-M-OREV-NA',
                         'FL+SC-RREV-NA',
                         'FL-TREV-EU',
                         'FL-OREV-EU',
                         'FL-RREV-EU',
                         'FL-W-OREV-EU',
                         'FL-M-OREV-EU'
    );
------------------------WEEKLY KPI MAPPINGS-----------------------------

UPDATE reference.finance_segment_mapping
SET is_weekly_kpi         = TRUE,
    weekly_kpi_currency_type = 'USD'
FROM data_model_jfb.dim_store st
WHERE st.store_id = finance_segment_mapping.event_store_id
  AND finance_segment_mapping.report_mapping IN ('FK-TREV-NA',
                                                    'FL-W-OREV-EU',
                                                    'FL-M-OREV-EU',
                                                    'FL-RREV-EU',
                                                    'FL-R-OREV-EU',
                                                    'FL-TREV-EU',
                                                    'FL+SC-W-OREV-NA',
                                                    'FL+SC-M-OREV-NA',
                                                    'FL+SC-OREV-NA',
                                                    'FL+SC-RREV-NA',
                                                    'FL+SC-R-OREV-NA',
                                                    'FL+SC-TREV-NA',
                                                    'FL-W-OREV-NA',
                                                    'FL-M-OREV-NA',
                                                    'SC-W-OREV-NA',
                                                    'SC-M-OREV-NA',
                                                    'JF-TREV-EU',
                                                    'JF-TREV-NA',
                                                    'SD-TREV-NA',
                                                    'SX-TREV-EU',
                                                    'SX-TREV-NA',
                                                    'SX-OREV-NA',
                                                    'SX-RREV-US',
                                                    'SX-R-OREV-US',
                                                    'SX-TREV-Global',
                                                    'SX-TREV-CA-US',
                                                    'YT-OREV-NA'
                                                 );

------------------------VIP TENURE-----------------------------

UPDATE reference.finance_segment_mapping
SET is_vip_tenure = TRUE
WHERE report_mapping IN ('FK-CP-TREV-NA',
                            'FK-NCP-TREV-NA',
                            'FK-TREV-NA',
                            'FK-TREV-US',
                            'FL+SC-W-O-OREV-CA',
                            'FL+SC-W-OREV-CA',
                            'FL+SC-M-O-OREV-CA',
                            'FL+SC-M-OREV-CA',
                            'FL+SC-O-OREV-CA',
                            'FL+SC-TREV-CA',
                            'FL-W-O-OREV-DE',
                            'FL-W-R-OREV-DE',
                            'FL-W-OREV-DE',
                            'FL-M-O-OREV-DE',
                            'FL-M-R-OREV-DE',
                            'FL-M-OREV-DE',
                            'FL-RREV-DE',
                            'FL-TREV-DE',
                            'FL-TREV-DK',
                            'FL-TREV-ES',
                            'FL-W-OREV-EU',
                            'FL-M-OREV-EU',
                            'FL-RREV-EU',
                            'FL-TREV-EU',
                            'FL-W-OREV-FR',
                            'FL-M-OREV-FR',
                            'FL-TREV-FR',
                            'FL-OREV-NA',
                            'FL+SC-W-O-OREV-NA',
                            'FL+SC-W-R-OREV-NA',
                            'FL-W-OREV-NA',
                            'FL+SC-W-OREV-NA',
                            'SC-W-OREV-NA',
                            'FL+SC-M-O-OREV-NA',
                            'FL+SC-M-R-OREV-NA',
                            'FL-M-OREV-NA',
                            'FL+SC-M-OREV-NA',
                            'SC-M-OREV-NA',
                            'FL+SC-O-OREV-NA',
                            'FL+SC-RREV-NA',
                            'FL+SC-R-OREV-NA',
                            'SC-OREV-NA',
                            'FL+SC-TREV-NA',
                            'FL-W-OREV-NL',
                            'FL-TREV-NL',
                            'FL-W-OREV-SE',
                            'FL-M-OREV-SE',
                            'FL-TREV-SE',
                            'FL-W-O-OREV-UK',
                            'FL-W-R-OREV-UK',
                            'FL-W-OREV-UK',
                            'FL-M-O-OREV-UK',
                            'FL-M-R-OREV-UK',
                            'FL-M-OREV-UK',
                            'FL-RREV-UK',
                            'FL-TREV-UK',
                            'FL+SC-W-O-OREV-US',
                            'FL+SC-W-R-OREV-US',
                            'FL+SC-W-OREV-US',
                            'FL+SC-M-O-OREV-US',
                            'FL+SC-M-R-OREV-US',
                            'FL+SC-M-OREV-US',
                            'FL+SC-O-OREV-US',
                            'FL+SC-RREV-US',
                            'FL+SC-R-OREV-US',
                            'FL+SC-TREV-US',
                            'JF-TREV-CA',
                            'JF-TREV-DE',
                            'JF-TREV-DK',
                            'JF-TREV-ES',
                            'JF-TREV-EU',
                            'JF-TREV-FR',
                            'JF-TREV-IT',
                            'JF-TREV-NA',
                            'JF-TREV-NL',
                            'JFSD-TREV-NA',
                            'JF-TREV-SE',
                            'JF-TREV-UK',
                            'JF-TREV-US',
                            'SD-TREV-NA',
                            'SD-TREV-US',
                            'SX-TREV-DE',
                            'SX-TREV-ES',
                            'SX-TREV-EU',
                            'SX-TREV-EUREM',
                            'SX-TREV-FR',
                            'SX-O-OREV-NA',
                            'SX-OREV-NA',
                            'SX-R-OREV-NA',
                            'SX-R-TREV-NA',
                            'SX-TREV-NA',
                            'SX-TREV-UK',
                            'SX-TREV-US',
                            'SX-TREV-CA-US',
                            'YT-OREV-NA'
                        )
;


------------------------LEAD TO VIP WATERFALL-----------------------------

UPDATE reference.finance_segment_mapping
SET is_lead_to_vip_waterfall = TRUE
WHERE report_mapping IN ('FK-CP-TREV-NA',
                            'FK-NCP-TREV-NA',
                            'FK-TREV-NA',
                            'FL+SC-W-TREV-CA',
                            'FL+SC-M-TREV-CA',
                            'FL+SC-TREV-CA',
                            'FL-W-O-OREV-DE',
                            'FL-W-R-OREV-DE',
                            'FL-W-TREV-DE',
                            'FL-M-O-OREV-DE',
                            'FL-M-R-OREV-DE',
                            'FL-M-TREV-DE',
                            'FL-TREV-DE',
                            'FL-W-TREV-DK',
                            'FL-M-TREV-DK',
                            'FL-TREV-DK',
                            'FL-W-TREV-ES',
                            'FL-M-TREV-ES',
                            'FL-TREV-ES',
                            'FL-W-TREV-EU',
                            'FL-M-TREV-EU',
                            'FL-TREV-EU',
                            'FL-W-OREV-FR',
                            'FL-M-TREV-FR',
                            'FL-TREV-FR',
                            'FL-OREV-NA',
                            'FL-W-OREV-NA',
                            'SC-W-OREV-NA',
                            'FL+SC-W-TREV-NA',
                            'FL-M-OREV-NA',
                            'SC-M-OREV-NA',
                            'FL+SC-M-TREV-NA',
                            'SC-OREV-NA',
                            'FL+SC-TREV-NA',
                            'FL-W-TREV-NL',
                            'FL-M-TREV-NL',
                            'FL-TREV-NL',
                            'FL-W-TREV-SE',
                            'FL-M-TREV-SE',
                            'FL-TREV-SE',
                            'FL-W-O-OREV-UK',
                            'FL-W-R-OREV-UK',
                            'FL-W-TREV-UK',
                            'FL-M-O-OREV-UK',
                            'FL-M-R-OREV-UK',
                            'FL-M-TREV-UK',
                            'FL-TREV-UK',
                            'FL+SC-W-O-TREV-US',
                            'FL+SC-W-TREV-US',
                            'FL+SC-M-TREV-US',
                            'FL+SC-TREV-US',
                            'JF-TREV-CA',
                            'JF-TREV-DE',
                            'JF-TREV-DK',
                            'JF-TREV-ES',
                            'JF-TREV-EU',
                            'JF-TREV-FR',
                            'JF-TREV-NA',
                            'JF-TREV-NL',
                            'JFSD-TREV-NA',
                            'JF-TREV-SE',
                            'JF-TREV-UK',
                            'JF-TREV-US',
                            'SD-TREV-NA',
                            'SD-TREV-US',
                            'SX-TREV-DE',
                            'SX-TREV-ES',
                            'SX-TREV-EU',
                            'SX-TREV-EUREM',
                            'SX-TREV-FR',
                            'SX-TREV-NA',
                            'SX-TREV-UK',
                            'SX-TREV-US',
                            'SX-TREV-CA-US',
                            'YT-OREV-NA'
                        )
;


------------------------DDD HYPERION MAPPINGS-----------------------------

UPDATE reference.finance_segment_mapping
SET is_ddd_hyperion            = TRUE,
    ddd_hyperion_currency_type = CASE WHEN st.store_country IN ('DK', 'SE', 'UK') THEN 'GBP' ELSE st.store_currency END
FROM data_model_jfb.dim_store st
WHERE st.store_id = finance_segment_mapping.event_store_id
  AND finance_segment_mapping.report_mapping IN (
                                                 'FK-TREV-US',
                                                    'FL+SC-W-TREV-CA',
                                                    'FL+SC-M-TREV-CA',
                                                    'FL-W-OREV-AT-DE',
                                                    'FL-M-OREV-AT-DE',
                                                    'FL-W-OREV-DEonly-DE',
                                                    'FL-M-OREV-DEonly-DE',
                                                    'FL-W-O-OREV-DE',
                                                    'FL-W-R-OREV-DE',
                                                    'FL-W-OREV-DE',
                                                    'FL-W-TREV-DE',
                                                    'FL-M-O-OREV-DE',
                                                    'FL-M-R-OREV-DE',
                                                    'FL-M-OREV-DE',
                                                    'FL-M-TREV-DE',
                                                    'FL-RREV-DE',
                                                    'FL-W-OREV-DK',
                                                    'FL-W-TREV-DK',
                                                    'FL-M-OREV-DK',
                                                    'FL-TREV-DK',
                                                    'FL-W-OREV-ES',
                                                    'FL-W-TREV-ES',
                                                    'FL-M-OREV-ES',
                                                    'FL-TREV-ES',
                                                    'FL-W-OREV-BE-FR',
                                                    'FL-M-OREV-BE-FR',
                                                    'FL-W-OREV-FRonly-FR',
                                                    'FL-M-OREV-FRonly-FR',
                                                    'FL-W-OREV-FR',
                                                    'FL-W-TREV-FR',
                                                    'FL-M-OREV-FR',
                                                    'FL-M-TREV-FR',
                                                    'FL-W-OREV-BE-NL',
                                                    'FL-W-OREV-NL',
                                                    'FL-W-TREV-NL',
                                                    'FL-M-OREV-NL',
                                                    'FL-W-OREV-NLonly-NL',
                                                    'FL-M-OREV-NLonly-NL',
                                                    'FL-TREV-NL',
                                                    'FL-W-OREV-SE',
                                                    'FL-W-TREV-SE',
                                                    'FL-M-OREV-SE',
                                                    'FL-TREV-SE',
                                                    'FL-W-O-OREV-UK',
                                                    'FL-W-R-OREV-UK',
                                                    'FL-W-OREV-UK',
                                                    'FL-W-TREV-UK',
                                                    'FL-M-O-OREV-UK',
                                                    'FL-M-R-OREV-UK',
                                                    'FL-M-OREV-UK',
                                                    'FL-M-TREV-UK',
                                                    'FL-RREV-UK',
                                                    'FL+SC-W-OREV-US',
                                                    'FL+SC-M-OREV-US',
                                                    'FL+SC-RREV-US',
                                                    'JF-TREV-CA',
                                                    'JF-TREV-DE412',
                                                    'JF-TREV-DE',
                                                    'JF-TREV-DK422',
                                                    'JF-TREV-DK',
                                                    'JF-TREV-ES432',
                                                    'JF-TREV-ES',
                                                    'JF-TREV-FR432',
                                                    'JF-TREV-FR',
                                                    'JF-TREV-IT432',
                                                    'JF-TREV-IT',
                                                    'JF-TREV-NL412',
                                                    'JF-TREV-NL',
                                                    'JFSD-TREV-NA',
                                                    'JF-TREV-SE422',
                                                    'JF-TREV-SE',
                                                    'JF-TREV-UK422',
                                                    'JF-TREV-UK',
                                                    'JF-TREV-US',
                                                    'SD-TREV-US',
                                                    'SX-TREV-DE',
                                                    'SX-TREV-ES',
                                                    'SX-TREV-EUREM',
                                                    'SX-TREV-FR',
                                                    'SX-TREV-UK',
                                                    'SX-OREV-US',
                                                    'SX-RREV-US',
                                                    'SX-TREV-US',
                                                    'YT-OREV-NA'
                                                );
-- Hyperion mappings (only one with Start & End Dates)
INSERT INTO reference.finance_segment_mapping
SELECT metric_type,
       event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       mapping_start_date,
       mapping_end_date,
       FALSE as is_ddd_consolidated,
       null as ddd_consolidated_currency_type,
       FALSE as is_ddd_individual,
       null as ddd_individual_currency_type,
       TRUE as is_ddd_hyperion,
       'EUR' AS ddd_hyperion_currency_type,
       FALSE as is_retail_attribution_ddd,
       null as retail_attribution_ddd_currency_type,
       FALSE as is_daily_cash_usd,
       null as daily_cash_usd_currency_type,
       FALSE as is_daily_cash_eur,
       null as daily_cash_eur_currency_type,
       FALSE as is_vip_tenure,
       null as vip_tenure_currency_type,
       FALSE as is_lead_to_vip_waterfall,
       null as lead_to_vip_waterfall_currency_type,
       FALSE as is_weekly_kpi,
       null as weekly_kpi_currency_type,
       FALSE as is_daily_cash_file,
       FALSE as is_daily_cash_alt_budget,
       report_mapping,
       business_unit,
       store_brand
FROM reference.finance_segment_mapping
WHERE report_mapping = 'SX-TREV-UK';


UPDATE reference.finance_segment_mapping
SET mapping_start_date = CASE
                             WHEN ddd_hyperion_currency_type = 'EUR' AND report_mapping = 'SX-TREV-UK' THEN '2010-01-01'
                             WHEN ddd_hyperion_currency_type = 'GBP' AND report_mapping = 'SX-TREV-UK' THEN '2019-11-01'
                             WHEN report_mapping IN
                                  ('JF-TREV-NL', 'JF-TREV-ES', 'JF-TREV-FR', 'JF-TREV-IT', 'JF-TREV-UK', 'JF-TREV-DK',
                                   'JF-TREV-SE', 'JF-TREV-DE') THEN '2010-01-01'
                             WHEN report_mapping IN
                                  ('JF-TREV-NL412', 'JF-TREV-ES432', 'JF-TREV-FR432', 'JF-TREV-IT432', 'JF-TREV-UK422',
                                   'JF-TREV-DK422', 'JF-TREV-SE422',
                                   'JF-TREV-DE412') THEN '2020-01-01'
    END,
    mapping_end_date   = CASE
                             WHEN ddd_hyperion_currency_type = 'EUR' AND report_mapping = 'SX-TREV-UK' THEN '2019-10-31'
                             WHEN ddd_hyperion_currency_type = 'GBP' AND report_mapping = 'SX-TREV-UK' THEN '2079-01-01'
                             WHEN report_mapping IN
                                  ('JF-TREV-NL', 'JF-TREV-ES', 'JF-TREV-FR', 'JF-TREV-IT', 'JF-TREV-UK', 'JF-TREV-DK',
                                   'JF-TREV-SE', 'JF-TREV-DE') THEN '2019-12-31'
                             WHEN report_mapping IN
                                  ('JF-TREV-NL412', 'JF-TREV-ES432', 'JF-TREV-FR432', 'JF-TREV-IT432', 'JF-TREV-UK422',
                                   'JF-TREV-DK422', 'JF-TREV-SE422',
                                   'JF-TREV-DE412') THEN '2079-01-01'
        END
WHERE is_ddd_hyperion = TRUE;

UPDATE reference.finance_segment_mapping
SET business_unit =
    CASE WHEN report_mapping ilike '%all%' then 'All Brands'
         WHEN report_mapping ilike '%jfb%global%' then 'GFB WW'
         WHEN report_mapping ilike '%fl%global%' then 'Fabletics WW'
         WHEN report_mapping ilike '%sx%global%' then 'Savage X WW'
         WHEN report_mapping ilike '%jfsd%' then 'JustFab & ShoeDazzle NA'
         WHEN report_mapping ilike '%jf%-na%'
                  OR report_mapping ilike '%jf%-us%'
                  OR report_mapping ilike '%jf%-ca%'
                  OR report_mapping ilike '%jfb%-na%'then 'JustFab NA'
         WHEN report_mapping ilike '%JF%' then 'JFEU'
         WHEN report_mapping ilike '%sx%-na%'
                  OR report_mapping ilike '%sx%-us%' then 'Savage X NA'
         WHEN report_mapping ilike '%sx%' then 'Savage X EU'
         WHEN report_mapping ilike '%fl%-na%'
                  OR report_mapping ilike '%fl%-us%'
                  OR report_mapping ilike '%fl%-ca%'
                  OR report_mapping ilike '%sc%-na%'
                  OR report_mapping ilike '%sc%-us%'
                  OR report_mapping ilike '%sc%-ca%'then 'Fabletics NA'
         WHEN report_mapping ilike 'yt%' then 'Yitty NA'
         WHEN report_mapping ilike '%flc%' then 'Fabletics + Yitty NA'
         WHEN report_mapping ilike '%fl%' then 'Fabletics EU'
         WHEN report_mapping ilike '%fk%' then 'FabKids NA'
         WHEN report_mapping ilike '%sd%' then 'ShoeDazzle NA'
    END
;

UPDATE reference.finance_segment_mapping
SET store_brand =
    CASE WHEN business_unit = 'All Brands' then 'All Brands'
         WHEN business_unit IN ('FabKids NA', 'GFB WW', 'JFEU', 'JustFab NA', 'ShoeDazzle NA', 'JustFab & ShoeDazzle NA') then 'JFB'
         WHEN business_unit IN ('Fabletics EU', 'Fabletics NA', 'Fabletics WW') then 'Fabletics'
         WHEN business_unit IN ('Savage X EU', 'Savage X NA', 'Savage X WW') then 'Savage X'
         WHEN business_unit IN ('Yitty NA') then 'Fabletics'
         WHEN business_unit IN ('Fabletics + Yitty NA') then 'Fabletics'
    END
;
