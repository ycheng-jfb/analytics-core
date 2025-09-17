SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _vips AS
SELECT 'VIPs'                                                                                                 AS segment,
       st.store_brand,
       st.store_brand_abbr,
       st.store_country,
       st.store_region,
       fa.sub_store_id                                                                                        AS store_id,
       is_retail_vip,
       dc.is_cross_promo,
       CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'M'
           ELSE 'F' END                                                                                       AS customer_gender,
       IFF(st.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)                        AS is_scrubs_customer,
       DATE_TRUNC(MONTH, fr.registration_local_datetime::DATE)                                                AS registration_cohort,
       DATE_TRUNC(MONTH, fa.activation_local_datetime::DATE)                                                  AS activation_cohort,
       'M' || CAST(DATEDIFF(MONTH, fr.registration_local_datetime, activation_local_datetime) + 1 AS VARCHAR) AS tenure,
       DATEDIFF(MONTH, fr.registration_local_datetime, activation_local_datetime) +
       1                                                                                                      AS tenure_number,
       CASE
           WHEN st.store_brand in ('JustFab','ShoeDazzle') THEN
               CASE
                   WHEN tenure_number >= 38 THEN 'M38+'
                   WHEN tenure_number >= 26 THEN 'M26-37'
                   WHEN tenure_number >= 14 THEN 'M14-25'
                   WHEN tenure_number >= 8 THEN 'M8-13'
                   ELSE tenure END
           WHEN tenure_number >= 14 THEN 'M14+'
           WHEN tenure_number >= 8 THEN 'M8-13'
           ELSE tenure END                                                                                    AS tenure_group,
       COUNT(DISTINCT fa.customer_id)                                                                         AS customers,
       MAX(fa.meta_create_datetime)                                                                           AS snapshot_datetime
FROM data_model.fact_registration fr
         JOIN data_model.dim_customer dc ON dc.customer_id = fr.customer_id
         JOIN data_model.fact_activation fa ON fr.customer_id = fa.customer_id
         JOIN data_model.dim_store st ON st.store_id = fr.store_id
WHERE IFNULL(fr.is_secondary_registration, FALSE) = FALSE
AND IFNULL(fr.is_fake_retail_registration, FALSE) = FALSE
GROUP BY segment,
         st.store_brand,
         st.store_brand_abbr,
         st.store_country,
         st.store_region,
         fa.sub_store_id,
         is_retail_vip,
         dc.is_cross_promo,
         CASE WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'M' ELSE 'F' END,
         IFF(st.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE),
         registration_cohort,
         activation_cohort,
         tenure,
         tenure_number,
         tenure_group;



CREATE OR REPLACE TEMPORARY TABLE _leads AS
SELECT 'LEADS'                                                 AS segment,
       st.store_brand,
       st.store_brand_abbr,
       st.store_country,
       st.store_region,
       fr.store_id,
       0                                                       AS is_retail_vip,
       dc.is_cross_promo,
       CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'M'
           ELSE 'F' END                                        AS customer_gender,
       IFF(st.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)  AS is_scrubs_customer,
       DATE_TRUNC(MONTH, fr.registration_local_datetime::DATE) AS registration_cohort,
       COUNT(DISTINCT fr.customer_id)                          AS customers,
       MAX(fr.meta_create_datetime)                            AS snapshot_datetime
FROM data_model.fact_registration fr
         JOIN data_model.dim_store st ON st.store_id = fr.store_id
         JOIN data_model.dim_customer dc ON dc.customer_id = fr.customer_id
WHERE IFNULL(fr.is_secondary_registration, FALSE) = FALSE
AND IFNULL(fr.is_fake_retail_registration, FALSE) = FALSE
GROUP BY segment,
         st.store_brand,
         st.store_brand_abbr,
         st.store_country,
         st.store_region,
         fr.store_id,
         is_retail_vip,
         dc.is_cross_promo,
         CASE WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'M' ELSE 'F' END,
         IFF(st.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE),
         registration_cohort
;

CREATE OR REPLACE TEMPORARY TABLE _month_date AS
SELECT DISTINCT registration_cohort AS month_date
FROM _leads;



CREATE OR REPLACE TEMPORARY TABLE _leads_grouped AS
SELECT 'Leads'                                                                          AS segment,
       store_brand,
       store_brand_abbr,
       store_country,
       store_region,
       store_id,
       is_retail_vip,
       is_cross_promo,
       customer_gender,
       is_scrubs_customer,
       registration_cohort,
       month_date                                                                       AS activation_cohort,
       'M' || CAST(DATEDIFF(MONTH, l.registration_cohort, m.month_date) + 1 AS VARCHAR) AS tenure,
       DATEDIFF(MONTH, l.registration_cohort, m.month_date) + 1                         AS tenure_number,
       CASE
           WHEN store_brand in ('JustFab','ShoeDazzle') THEN
               CASE
                   WHEN tenure_number >= 38 THEN 'M38+'
                   WHEN tenure_number >= 26 THEN 'M26-37'
                   WHEN tenure_number >= 14 THEN 'M14-25'
                   WHEN tenure_number >= 8 THEN 'M8-13'
                   ELSE tenure END
           WHEN tenure_number >= 14 THEN 'M14+'
           WHEN tenure_number >= 8 THEN 'M8-13'
           ELSE tenure END                                                              AS tenure_group,
       SUM(customers)                                                                   AS customers,
       MAX(snapshot_datetime)                                                           AS snapshot_datetime
FROM _leads l
         JOIN _month_date m ON m.month_date >= l.registration_cohort
GROUP BY segment,
         store_brand,
         store_brand_abbr,
         store_country,
         store_region,
         store_id,
         is_retail_vip,
         is_cross_promo,
         customer_gender,
         is_scrubs_customer,
         registration_cohort,
         activation_cohort,
         tenure,
         tenure_number
;


CREATE OR REPLACE TEMP TABLE _lead_to_vip_waterfall AS
SELECT *
FROM _vips v
UNION ALL
SELECT *
FROM _leads_grouped;



CREATE OR REPLACE TEMP TABLE _fsm AS
SELECT report_mapping,
       store_brand                                                             AS brand_mapping,
       event_store_id,
       is_retail_vip,
       CASE WHEN customer_gender = 'Unknown' THEN 'F' ELSE customer_gender END AS customer_gender,
       is_scrubs_customer,
       is_cross_promo
FROM reference.finance_segment_mapping fsm
WHERE metric_type = 'Acquisition'
  AND is_lead_to_vip_waterfall = 1

UNION

SELECT report_mapping,
       store_brand                                                             AS brand_mapping,
       event_store_id,
       is_retail_vip,
       CASE WHEN customer_gender = 'Unknown' THEN 'F' ELSE customer_gender END AS customer_gender,
       TRUE                                                                    AS is_scrubs_customer,
       is_cross_promo
FROM reference.finance_segment_mapping fsm
WHERE metric_type = 'Acquisition'
  AND is_lead_to_vip_waterfall = 1
  AND report_mapping = 'YT-OREV-NA'
;


CREATE OR REPLACE TABLE reporting.lead_to_vip_waterfall_final_output AS
SELECT CASE WHEN report_mapping = 'FK-CP-TREV-NA' THEN 'FK-CP-NA'
            WHEN report_mapping = 'FK-NCP-TREV-NA' THEN 'FK-NCP-NA'
            WHEN report_mapping = 'FK-TREV-NA' THEN 'FK-NA'
            WHEN report_mapping = 'FL-M-O-OREV-DE' THEN 'FL-M-O-DE'
            WHEN report_mapping = 'FL-M-O-OREV-UK' THEN 'FL-M-O-UK'
            WHEN report_mapping = 'FL-M-OREV-NA' THEN 'FL-M-NA'
            WHEN report_mapping = 'FL-M-R-OREV-DE' THEN 'FL-M-R-DE'
            WHEN report_mapping = 'FL-M-R-OREV-UK' THEN 'FL-M-R-UK'
            WHEN report_mapping = 'FL-M-TREV-DE' THEN 'FL-M-DE'
            WHEN report_mapping = 'FL-M-TREV-DK' THEN 'FL-M-DK'
            WHEN report_mapping = 'FL-M-TREV-ES' THEN 'FL-M-ES'
            WHEN report_mapping = 'FL-M-TREV-EU' THEN 'FL-M-EU'
            WHEN report_mapping = 'FL-M-TREV-FR' THEN 'FL-M-FR'
            WHEN report_mapping = 'FL-M-TREV-NL' THEN 'FL-M-NL'
            WHEN report_mapping = 'FL-M-TREV-SE' THEN 'FL-M-SE'
            WHEN report_mapping = 'FL-M-TREV-UK' THEN 'FL-M-UK'
            WHEN report_mapping = 'FL-OREV-NA' THEN 'FL-NA'
            WHEN report_mapping = 'FL-TREV-DE' THEN 'FL-DE'
            WHEN report_mapping = 'FL-TREV-DK' THEN 'FL-DK'
            WHEN report_mapping = 'FL-TREV-ES' THEN 'FL-ES'
            WHEN report_mapping = 'FL-TREV-EU' THEN 'FL-EU'
            WHEN report_mapping = 'FL-TREV-FR' THEN 'FL-FR'
            WHEN report_mapping = 'FL-TREV-NL' THEN 'FL-NL'
            WHEN report_mapping = 'FL-TREV-SE' THEN 'FL-SE'
            WHEN report_mapping = 'FL-TREV-UK' THEN 'FL-UK'
            WHEN report_mapping = 'FL-W-O-OREV-DE' THEN 'FL-W-O-DE'
            WHEN report_mapping = 'FL-W-O-OREV-UK' THEN 'FL-W-O-UK'
            WHEN report_mapping = 'FL-W-OREV-FR' THEN 'FL-W-FR'
            WHEN report_mapping = 'FL-W-OREV-NA' THEN 'FL-W-NA'
            WHEN report_mapping = 'FL-W-R-OREV-DE' THEN 'FL-W-R-DE'
            WHEN report_mapping = 'FL-W-R-OREV-UK' THEN 'FL-W-R-UK'
            WHEN report_mapping = 'FL-W-TREV-DE' THEN 'FL-W-DE'
            WHEN report_mapping = 'FL-W-TREV-DK' THEN 'FL-W-DK'
            WHEN report_mapping = 'FL-W-TREV-ES' THEN 'FL-W-ES'
            WHEN report_mapping = 'FL-W-TREV-EU' THEN 'FL-W-EU'
            WHEN report_mapping = 'FL-W-TREV-NL' THEN 'FL-W-NL'
            WHEN report_mapping = 'FL-W-TREV-SE' THEN 'FL-W-SE'
            WHEN report_mapping = 'FL-W-TREV-UK' THEN 'FL-W-UK'
            WHEN report_mapping = 'FL+SC-M-TREV-CA' THEN 'FL+SC-M-CA'
            WHEN report_mapping = 'FL+SC-M-TREV-NA' THEN 'FL+SC-M-NA'
            WHEN report_mapping = 'FL+SC-M-TREV-US' THEN 'FL+SC-M-US'
            WHEN report_mapping = 'FL+SC-TREV-CA' THEN 'FL+SC-CA'
            WHEN report_mapping = 'FL+SC-TREV-NA' THEN 'FL+SC-NA'
            WHEN report_mapping = 'FL+SC-TREV-US' THEN 'FL+SC-US'
            WHEN report_mapping = 'FL+SC-W-O-TREV-US' THEN 'FL+SC-W-O-US'
            WHEN report_mapping = 'FL+SC-W-TREV-CA' THEN 'FL+SC-W-CA'
            WHEN report_mapping = 'FL+SC-W-TREV-NA' THEN 'FL+SC-W-NA'
            WHEN report_mapping = 'FL+SC-W-TREV-US' THEN 'FL+SC-W-US'
            WHEN report_mapping = 'JF-TREV-CA' THEN 'JF-CA'
            WHEN report_mapping = 'JF-TREV-DE' THEN 'JF-DE'
            WHEN report_mapping = 'JF-TREV-DK' THEN 'JF-DK'
            WHEN report_mapping = 'JF-TREV-ES' THEN 'JF-ES'
            WHEN report_mapping = 'JF-TREV-EU' THEN 'JF-EU'
            WHEN report_mapping = 'JF-TREV-FR' THEN 'JF-FR'
            WHEN report_mapping = 'JF-TREV-NA' THEN 'JF-NA'
            WHEN report_mapping = 'JF-TREV-NL' THEN 'JF-NL'
            WHEN report_mapping = 'JF-TREV-SE' THEN 'JF-SE'
            WHEN report_mapping = 'JF-TREV-UK' THEN 'JF-UK'
            WHEN report_mapping = 'JF-TREV-US' THEN 'JF-US'
            WHEN report_mapping = 'JFSD-TREV-NA' THEN 'JFSD-NA'
            WHEN report_mapping = 'SC-M-OREV-NA' THEN 'SC-M-NA'
            WHEN report_mapping = 'SC-OREV-NA' THEN 'SC-NA'
            WHEN report_mapping = 'SC-W-OREV-NA' THEN 'SC-W-NA'
            WHEN report_mapping = 'SD-TREV-NA' THEN 'SD-NA'
            WHEN report_mapping = 'SD-TREV-US' THEN 'SD-US'
            WHEN report_mapping = 'SX-TREV-DE' THEN 'SX-DE'
            WHEN report_mapping = 'SX-TREV-ES' THEN 'SX-ES'
            WHEN report_mapping = 'SX-TREV-EU' THEN 'SX-EU'
            WHEN report_mapping = 'SX-TREV-EUREM' THEN 'SX-EUREM'
            WHEN report_mapping = 'SX-TREV-FR' THEN 'SX-FR'
            WHEN report_mapping = 'SX-TREV-NA' THEN 'SX-NA'
            WHEN report_mapping = 'SX-TREV-UK' THEN 'SX-UK'
            WHEN report_mapping = 'SX-TREV-US' THEN 'SX-US'
            WHEN report_mapping = 'YT-OREV-NA' THEN 'YT-NA' END AS report_mapping,
       fsm.brand_mapping,
       segment,
       store_brand,
       store_brand_abbr,
       store_country,
       store_region,
       store_id,
       w.is_retail_vip,
       w.is_cross_promo,
       w.customer_gender,
       w.is_scrubs_customer,
       registration_cohort,
       activation_cohort,
       tenure,
       tenure_number,
       tenure_group,
       customers,
       snapshot_datetime
FROM _lead_to_vip_waterfall w
         LEFT JOIN _fsm fsm ON fsm.event_store_id = w.store_id
    AND fsm.is_retail_vip = w.is_retail_vip
    AND fsm.is_cross_promo = w.is_cross_promo
    AND fsm.customer_gender = w.customer_gender
    AND fsm.is_scrubs_customer = w.is_scrubs_customer
WHERE report_mapping IS NOT NULL
  AND tenure <> 'M0'
;

/*Snapshot process. Delete records older than a year and a half*/
INSERT INTO snapshot.lead_to_vip_waterfall_final_output
(report_mapping,
 brand_mapping,
 segment,
 store_brand,
 store_brand_abbr,
 store_country,
 store_region,
 store_id,
 is_retail_vip,
 is_cross_promo,
 customer_gender,
 is_scrubs_customer,
 registration_cohort,
 activation_cohort,
 tenure,
 tenure_number,
 tenure_group,
 customers,
 snapshot_datetime,
 execution_snapshot_datetime)
SELECT report_mapping,
       brand_mapping,
       segment,
       store_brand,
       store_brand_abbr,
       store_country,
       store_region,
       store_id,
       is_retail_vip,
       is_cross_promo,
       customer_gender,
       is_scrubs_customer,
       registration_cohort,
       activation_cohort,
       tenure,
       tenure_number,
       tenure_group,
       customers,
       snapshot_datetime,
       $execution_start_time AS execution_snapshot_datetime

FROM reporting.lead_to_vip_waterfall_final_output;

DELETE
FROM snapshot.lead_to_vip_waterfall_final_output
WHERE snapshot_datetime < DATEADD(month, -18, GETDATE());
