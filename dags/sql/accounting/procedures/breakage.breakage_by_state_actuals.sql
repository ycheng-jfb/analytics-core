SET month_thru = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE));

CREATE OR REPLACE TEMPORARY TABLE _extended_key_original AS
SELECT dcf.original_credit_key
FROM lake_consolidated_view.ultra_merchant.membership_token mt
         JOIN reporting_base_prod.shared.dim_credit dcf
              ON mt.membership_token_id = dcf.credit_id AND dcf.source_credit_id_type = 'Token'
WHERE mt.extension_months > 0;

CREATE OR REPLACE TEMPORARY TABLE _original_credit_activity AS
SELECT CASE
           WHEN st2.store_brand = 'Fabletics' AND dcf.original_credit_type = 'Giftcard' AND dc2.gender = 'M' THEN 'Fabletics Mens'
           WHEN st2.store_brand = 'Fabletics' AND dcf.original_credit_type = 'Giftcard' THEN 'Fabletics Womens'
           WHEN st2.store_brand = 'Yitty' AND dcf.original_credit_type = 'Giftcard' THEN 'Yitty'
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                             AS brand,
       st.store_region                                                         AS region,
       st.store_country                                                        AS country,
       UPPER(IFF(LEN(REPLACE(dcf2.credit_state, ' ', '')) IN (1, 3) OR dcf2.credit_state = '--', 'UNKNOWN',
                 REGEXP_REPLACE(dcf2.credit_state, '\\.|\\,', '')))          AS state,
       CASE
           WHEN st.store_brand = 'Savage X'
               AND st.store_country = 'US' THEN dcf2.deferred_recognition_label_credit
           WHEN st.store_brand in ('Fabletics','Yitty')
               AND st.store_country = 'US'
               AND dcf.original_credit_type = 'Variable Credit'
               AND dcf.original_credit_reason = 'Refund' THEN dcf2.deferred_recognition_label_credit
           WHEN st2.store_brand in ('Fabletics')
               AND st2.store_country = 'US'
               AND dcf.original_credit_type = 'Giftcard'
               AND dcf.original_credit_reason = 'Gift Card - Paid' THEN dcf2.deferred_recognition_label_credit
           ELSE 'Recognize' END                                             AS deferred_recognition_label,
       IFF(ek.original_credit_key IS NOT NULL, TRUE, FALSE)                 AS is_extended,
       fce.credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN reporting_base_prod.shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN edw_prod.stg.dim_customer dc2 ON dc2.customer_id = dcf2.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         JOIN edw_prod.data_model.dim_store st2 ON st2.store_id = dcf2.store_id
         LEFT JOIN _extended_key_original ek
                   ON dcf.original_credit_key = ek.original_credit_key
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled')
  AND fce.original_credit_activity_type_action = 'Include'
  AND st.store_brand IN ('Fabletics', 'Yitty', 'Savage X', 'JustFab', 'FabKids', 'ShoeDazzle')
  AND st.store_country IN ('US', 'CA')
  AND iff(dcf.original_credit_type = 'Giftcard' AND dcf.original_credit_reason = 'Gift Card - Paid', 'None',dcf.giftco_transfer_status) <> 'Membership Credit to Giftco'
GROUP BY brand,
         region,
         country,
         UPPER(IFF(LEN(REPLACE(dcf2.credit_state, ' ', '')) IN (1, 3) OR dcf2.credit_state = '--', 'UNKNOWN',
                   REGEXP_REPLACE(dcf2.credit_state, '\\.|\\,', ''))),
         IFF(ek.original_credit_key IS NOT NULL, TRUE, FALSE),
         fce.credit_activity_type,
         dcf.original_credit_tender,
         dcf.original_credit_type,
         dcf.original_credit_reason,
         CASE
           WHEN st.store_brand = 'Savage X'
               AND st.store_country = 'US' THEN dcf2.deferred_recognition_label_credit
           WHEN st.store_brand in ('Fabletics','Yitty')
               AND st.store_country = 'US'
               AND dcf.original_credit_type = 'Variable Credit'
               AND dcf.original_credit_reason = 'Refund' THEN dcf2.deferred_recognition_label_credit
           WHEN st2.store_brand in ('Fabletics')
               AND st2.store_country = 'US'
               AND dcf.original_credit_type = 'Giftcard'
               AND dcf.original_credit_reason = 'Gift Card - Paid' THEN dcf2.deferred_recognition_label_credit
           ELSE 'Recognize' END ,
         DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE),
         DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE);

CREATE OR REPLACE TEMPORARY TABLE _pivot AS
WITH _issuance AS (SELECT *
                   FROM _original_credit_activity c
                            JOIN (SELECT month_date calendar_month
                                  FROM edw_prod.data_model.dim_date) dm
                                 ON dm.calendar_month >= c.original_issued_month
                                     AND dm.calendar_month <= CURRENT_DATE()
                   WHERE credit_activity_type = 'Issued')
SELECT DISTINCT i.brand,
                i.region,
                i.country                                          credit_country,
                i.state                                            credit_state,
                i.deferred_recognition_label,
                i.is_extended,
                i.original_credit_tender,
                i.original_credit_type,
                i.original_credit_reason,
                i.original_issued_month,
                i.activity_amount                                  original_issued_amount,
                COALESCE(p.activity_month::DATE, i.calendar_month) activity_month,
                NVL(p."'Issued'", 0)                               issued,
                NVL(p."'Redeemed'", 0)                             redeemed,
                NVL(p."'Cancelled'", 0)                            cancelled,
                NVL(p."'Expired'", 0)                              expired
FROM _issuance i
         LEFT JOIN _original_credit_activity
    PIVOT (SUM(activity_amount) FOR credit_activity_type IN ('Issued','Redeemed', 'Cancelled', 'Expired')) p
                   ON i.brand = p.brand
                       AND i.region = p.region
                       AND i.country = p.country
                       AND i.state = p.state
                       AND i.deferred_recognition_label = p.deferred_recognition_label
                       AND i.is_extended = p.is_extended
                       AND i.original_credit_tender = p.original_credit_tender
                       AND i.original_credit_type = p.original_credit_type
                       AND i.original_credit_reason = p.original_credit_reason
                       AND i.activity_month = p.original_issued_month
                       AND i.calendar_month = p.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _append AS
SELECT brand,
       month_end.udf_correct_state_country(p.credit_state, p.credit_country, 'country') AS country,
       month_end.udf_correct_state_country(p.credit_state, p.credit_country, 'state')   AS state,
       deferred_recognition_label,
       original_credit_type,
       original_issued_month,
       $month_thru                                                                                          activity_month,
       SUM(issued)                                                                                          issued_to_date,
       SUM(redeemed)                                                                                        redeemed_to_date,
       SUM(cancelled)                                                                                       cancelled_to_date,
       SUM(issued) - SUM(redeemed) - SUM(cancelled)                                                         unredeemed_to_date
FROM _pivot p
WHERE original_credit_tender = 'Cash'
  AND ((original_credit_type = 'Fixed Credit' AND original_credit_reason = 'Membership Credit')
    OR (original_credit_type = 'Token' AND original_credit_reason = 'Token Billing')
    OR (original_credit_type = 'Token' AND original_credit_reason = 'Refund' and brand in ('Fabletics Mens','Fabletics Womens', 'Yitty','Savage X'))
    OR (original_credit_type = 'Variable Credit' AND original_credit_reason = 'Refund' and brand in ('Fabletics Mens','Fabletics Womens', 'Yitty'))
    OR (original_credit_type = 'Giftcard' AND original_credit_reason = 'Gift Card - Paid' and brand in ('Fabletics Mens','Fabletics Womens')))
  AND original_issued_month <= $month_thru
  AND activity_month <= $month_thru
GROUP BY brand,
         country,
         state,
         deferred_recognition_label,
         original_credit_type,
         original_issued_month;

MERGE INTO breakage.breakage_by_state_actuals t
    USING _append a
    ON t.brand = a.brand AND
       t.country = a.country AND
       t.state = a.state AND
       t.deferred_recognition_label = a.deferred_recognition_label AND
       t.original_credit_type = a.original_credit_type AND
       t.original_issued_month = a.original_issued_month AND
       t.activity_month = a.activity_month
    WHEN NOT MATCHED THEN
        INSERT (brand, country, state, original_credit_type, original_issued_month, deferred_recognition_label, activity_month,
                issued_to_date, redeemed_to_date, cancelled_to_date, unredeemed_to_date)
            VALUES (brand, country, state, original_credit_type, original_issued_month, deferred_recognition_label, activity_month,
                    issued_to_date, redeemed_to_date, cancelled_to_date, unredeemed_to_date)
    WHEN MATCHED AND
        (NOT EQUAL_NULL(t.issued_to_date, a.issued_to_date) OR
         NOT EQUAL_NULL(t.redeemed_to_date, a.redeemed_to_date) OR
         NOT EQUAL_NULL(t.cancelled_to_date, a.cancelled_to_date) OR
         NOT EQUAL_NULL(t.unredeemed_to_date, a.unredeemed_to_date))
        THEN
        UPDATE
            SET t.issued_to_date = a.issued_to_date,
                t.redeemed_to_date = a.redeemed_to_date,
                t.cancelled_to_date = a.cancelled_to_date,
                t.unredeemed_to_date = a.unredeemed_to_date;

CREATE TRANSIENT TABLE IF NOT EXISTS breakage.breakage_by_state_actuals_snapshot (
	brand VARCHAR(155),
	country VARCHAR(155),
	state VARCHAR(155),
	original_issued_month DATE,
	original_credit_type VARCHAR(155),
	activity_month DATE,
	issued_to_date NUMBER(32,4),
	redeemed_to_date NUMBER(32,4),
	cancelled_to_date NUMBER(32,4),
	unredeemed_to_date NUMBER(32,4),
	snapshot_timestamp TIMESTAMP_LTZ(9),
	deferred_recognition_label varchar(155)
);

INSERT INTO breakage.breakage_by_state_actuals_snapshot
select brand,
       country,
       state,
       original_issued_month,
       original_credit_type,
       activity_month,
       issued_to_date,
       redeemed_to_date,
       cancelled_to_date,
       unredeemed_to_date,
       CURRENT_TIMESTAMP AS snapshot_timestamp,
       deferred_recognition_label
from breakage.breakage_by_state_actuals;

DELETE
FROM breakage.breakage_by_state_actuals_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
