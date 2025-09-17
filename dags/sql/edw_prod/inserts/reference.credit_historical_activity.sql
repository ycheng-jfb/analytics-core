-- Code by dragan for historical credit activity. The table is being used in fact_credit_event

CREATE OR REPLACE TABLE reference.credit_historical_activity AS
-- Historical Issued
SELECT sc.store_credit_id                                                            AS store_credit_id,
       sc.administrator_id,
       CAST('Historical Activity' AS TEXT(155))                                      AS activity_source,
       CAST('Historical Activity - Issued From store_credit' AS TEXT(155))           AS activity_source_reason,
       CAST('Issued' AS TEXT(155))                                                   AS activity_type,
       CAST(IFF(msc.order_id IS NOT NULL, 'Credit Billing', 'Unknown') AS TEXT(155)) AS activity_type_reason,
       sc.datetime_added                                                             AS activity_datetime,
       sc.amount                                                                     AS activity_amount,
       NULL                                                                          AS redemption_order_id,
       NULL                                                                          AS redemption_store_id,
       NULL                                                                          AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.store_credit sc
         LEFT JOIN lake_view.ultra_merchant.membership_store_credit msc ON msc.store_credit_id = sc.store_credit_id
WHERE sc.datetime_added < '2017-06-01'

UNION ALL
-- Transferred Pre 2017-06-01
-- we use store_credit_transaction post 2017-06-01
SELECT ctt.store_credit_id,
       NULL                                                                 AS adiminstrator_id,
       'Historical Activity'                                                AS activity_source,
       'Historical Activity - Transferred From credit_transfer_transaction' AS activity_source_reason,
       'Transferred'                                                        AS activity_type,
       'Transfer to Giftco'                                                 AS activity_type_reason,
       ctt.datetime_transacted                                              AS activity_datetime,
       ctt.amount                                                           AS activity_amount,
       NULL                                                                 AS redemption_order_id,
       NULL                                                                 AS redemption_store_id,
       NULL                                                                 AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.credit_transfer_transaction ctt
WHERE credit_transfer_transaction_type_id IN (100, 110, 120, 150)
  AND ctt.statuscode = 4461
  AND ctt.datetime_transacted < '2017-06-01'

UNION ALL
-- Cancelled Pre 2017-06-01, post 2013-07-01
-- we use store_credit_transaction post 2017-06-01
SELECT sct.store_credit_id
     , sct.administrator_id
     , 'Historical Activity'                                                          AS activity_source
     , 'Historical Activity - Cancelled Pre 2017-06-01 From store_credit_transaction' AS activity_source_reason
     , 'Cancelled'                                                                    AS activity_type
     , 'Unknown'                                                                      AS activity_type_reason
     , datetime_transaction                                                           AS activity_datetime
     , IFF(msc.store_credit_id IS NOT NULL, sc.amount, sct.amount)                    AS activity_amount
     ,-- if its fixed the whole amount was cancelled
    NULL                                                                              AS redemption_order_id
     , NULL                                                                           AS redemption_store_id
     , NULL                                                                           AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.store_credit_transaction sct
         LEFT JOIN lake_view.ultra_merchant.store_credit sc ON sc.store_credit_id = sct.store_credit_id
         LEFT JOIN lake_view.ultra_merchant.membership_store_credit msc ON msc.store_credit_id = sc.store_credit_id
WHERE store_credit_transaction_type_id = 30
  AND datetime_transaction < '2017-06-01'

-- Expired pre 2017-06-01
  -- we use store_credit_transaction post 2017-06-01
UNION ALL

SELECT sml.object_id                                                    AS store_credit_id,
       NULL                                                             AS administrator_id,
       'Historical Activity'                                            AS activity_source,
       'Historical Activity - Expired From statuscode_modification_log' AS activity_type_reason,
       'Expired'                                                        AS activity_type,
       'Credit Expiration'                                              AS activity_type_reason,
       sml.datetime_added                                               AS activity_datetime,
       sc.balance                                                       AS activity_amount,
       NULL                                                             AS redemption_order_id,
       NULL                                                             AS redemption_store_id,
       NULL                                                             AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.statuscode_modification_log sml
         JOIN lake_view.ultra_merchant.store_credit sc ON sc.store_credit_id = sml.object_id
WHERE sml.to_statuscode = 3246
  AND object = 'store_credit'
  AND sc.statuscode = 3246
  AND sml.datetime_added < '2017-06-01'

UNION ALL

SELECT scc.original_store_credit_id                                               AS store_credit_id,
       NULL                                                                       AS administrator_id,
       'Historical Activity'                                                      AS activity_source,
       'Historical Activity - Converted to Variable From store_credit_conversion' AS activity_type_reason,
       'Converted To Variable'                                                    AS activity_type,
       'Request to Break Credit'                                                  AS activity_type_reason,
       scc.datetime_added                                                         AS activity_datetime,
       sc.balance                                                                 AS activity_amount,
       NULL                                                                       AS redemption_order_id,
       NULL                                                                       AS redemption_store_id,
       NULL                                                                       AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.store_credit_conversion scc
         JOIN lake_view.ultra_merchant.store_credit sc ON sc.store_credit_id = scc.original_store_credit_id
WHERE scc.datetime_added < '2017-06-01'

UNION ALL

SELECT oc.store_credit_id,
       NULL                                                               AS administrator_id,
       'Historical Activity'                                              AS activity_source,
       'Historical Activity - Redeemed From order_credit'                 AS activity_type_reason,
       'Redeemed'                                                         AS activity_type,
       'Redeemed In Order'                                                AS activity_type_reason,
       COALESCE(o.datetime_transaction, o.date_shipped, o.datetime_added) AS activity_datetime,
       oc.amount                                                          AS activity_amount,
       oc.order_id                                                        AS redemption_order_id,
       o.store_id                                                         AS redemption_store_id,
       CASE
           WHEN st.store_country = 'CA' THEN 'CA'
           WHEN st.store_country = 'US' THEN 'US'
           WHEN a.country_code = 'GB' THEN 'UK'
           WHEN st.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
           WHEN st.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
           WHEN st.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
           WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
           WHEN st.store_country = 'EUREM' THEN 'NL'
           ELSE st.store_country END                                      AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.order_credit oc
         JOIN lake_view.ultra_merchant."ORDER" o ON o.order_id = oc.order_id
         LEFT JOIN lake_view.ultra_merchant.address a ON a.address_id = o.shipping_address_id
         JOIN data_model.dim_store st ON st.store_id = o.store_id
WHERE (o.date_shipped IS NOT NULL OR o.payment_statuscode >= 2600)
  AND oc.amount > 0
  AND oc.store_credit_id IS NOT NULL
  AND COALESCE(datetime_transaction, date_shipped) < '2017-06-01'
  AND NOT EXISTS(SELECT 1 FROM lake_view.ultra_merchant.box ps WHERE ps.order_id = o.order_id)

UNION ALL
-- Redeemed PS
SELECT oc.store_credit_id,
       NULL                                                          AS administrator_id,
       'Historical Activity'                                         AS activity_source,
       'Historical Activity - Redeemed PS From order_credit and box' AS activity_source_reason,
       'Redeemed'                                                    AS activity_type,
       'Redeemed In Order'                                           AS activity_type_reason,
       b.datetime_checkout                                           AS activity_datetime,
       oc.amount                                                     AS activity_amount,
       oc.order_id                                                   AS redemption_order_id,
       o.store_id                                                    AS redemption_store_id,
       CASE
           WHEN st.store_country = 'CA' THEN 'CA'
           WHEN st.store_country = 'US' THEN 'US'
           WHEN a.country_code = 'GB' THEN 'UK'
           WHEN st.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
           WHEN st.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
           WHEN st.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
           WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
           WHEN st.store_country = 'EUREM' THEN 'NL'
           ELSE st.store_country END                                 AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.order_credit oc
         JOIN lake_view.ultra_merchant."ORDER" o ON o.order_id = oc.order_id
         JOIN lake_view.ultra_merchant.box b ON b.order_id = o.order_id
         LEFT JOIN lake_view.ultra_merchant.address a ON a.address_id = o.shipping_address_id
         JOIN data_model.dim_store st ON st.store_id = o.store_id
WHERE oc.amount > 0
  AND b.datetime_checkout < '2017-06-01'
  AND oc.store_credit_id IS NOT NULL
  AND NOT EXISTS(SELECT 1
                 FROM lake_view.ultra_merchant.order_credit_delete_log dl
                 WHERE dl.order_credit_id = oc.order_credit_id);

------------------------------------------------------------------------
-- there were 4 credits with duplicate expirations, remove the second expiration

DELETE
FROM reference.credit_historical_activity
WHERE store_credit_id IN (34997397, 34997398)
  AND activity_type = 'Expired'
  AND activity_datetime::DATE = '2015-08-01';

DELETE
FROM reference.credit_historical_activity
WHERE store_credit_id = 9813521
  AND activity_type = 'Expired'
  AND activity_datetime::DATE = '2014-04-05';

DELETE
FROM reference.credit_historical_activity
WHERE store_credit_id = 12335751
  AND activity_type = 'Expired'
  AND activity_datetime::DATE = '2014-04-13';


------------------------------------------------------------------------
-- There were credits for FabKids and Shoedazzle that were migrated as already redeemed
-- In order for us to not show it as outstanding based on the activity we have for that credit, we enter in the redeemed events
-- These are all PRE Migration Credits
INSERT INTO reference.credit_historical_activity
SELECT sc.store_credit_id,
       NULL                                                         AS administrator_id,
       'Historical Activity'                                        AS activity_source,
       'Historical Activity - Redeemed Pre FK Migration 2013-06-01' AS activity_source_reason,
       'Redeemed'                                                   AS activity_type,
       'Redeemed Pre Migration'                                     AS activity_type_reason,
       '2013-06-01'                                                 AS activity_datetime,
       sc.amount                                                    AS activity_amount, -- this previously said balance,
       NULL                                                         AS redemption_order_id,
       46                                                           AS redemption_store_id,
       'US'                                                         AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.store_credit sc
         JOIN lake_view.ultra_merchant.membership m ON m.customer_id = sc.customer_id
         JOIN lake_view.ultra_merchant.statuscode scc ON scc.statuscode = sc.statuscode
WHERE m.store_id = 46
  AND sc.datetime_added < '2013-06-01'
  AND scc.statuscode = 3245
  AND NOT exists(
        SELECT 1
        FROM reference.credit_historical_activity ca
        WHERE ca.store_credit_id = sc.store_credit_id
          AND ca.activity_amount = sc.amount
          AND ca.activity_type = 'Redeemed') -- Redeemed
UNION ALL
-- SD Historical Redeemed
SELECT sc.store_credit_id,
       NULL                                                         AS administrator_id,
       'Historical Activity'                                        AS activity_source,
       'Historical Activity - Redeemed Pre SD Migration 2014-04-01' AS activity_source_reason,
       'Redeemed'                                                   AS activity_type,
       'Redeemed Pre Migration'                                     AS activity_type_reason,
       '2014-04-01'                                                 AS activity_datetime,
       sc.amount - IFNULL(redeemed_amount, 0)                       AS activity_amount, -- only add back in activity we dont have redemptions for
       NULL                                                         AS redemption_order_id,
       55                                                           AS redemption_store_id,
       'US'                                                         AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.store_credit sc
         JOIN lake_view.ultra_merchant.membership m ON m.customer_id = sc.customer_id
         JOIN lake_view.ultra_merchant.statuscode scc ON scc.statuscode = sc.statuscode
         LEFT JOIN (
    SELECT ca.store_credit_id, SUM(ca.activity_amount) AS redeemed_amount
    FROM reference.credit_historical_activity ca
    WHERE ca.activity_type = 'Redeemed'
    GROUP BY 1) hist ON hist.store_credit_id = sc.store_credit_id
WHERE m.store_id = 55
  AND sc.datetime_added < '2014-04-01'
  AND scc.statuscode = 3245
  AND NOT exists(
        SELECT 1
        FROM reference.credit_historical_activity ca
        WHERE ca.store_credit_id = sc.store_credit_id
          AND ca.activity_amount = sc.amount
          AND ca.activity_type = 'Redeemed')
  AND sc.amount - IFNULL(redeemed_amount, 0) > 0;

------------------------------------------------------------------------
-- Cancelled pre 2013-07-01 -- credits have cancelled statuscode, but no cancellation date,
-- we didnt start tracking cancellation dates until 2013-07-01
-- put cancellation date onto 2013-06-30
INSERT INTO reference.credit_historical_activity
SELECT sc.store_credit_id,
       NULL                                                           AS administrator_id,
       'Historical Activity'                                          AS activity_source,
       'Historical Activity - Cancelled Pre Date Tracking 2017-07-01' AS activity_source_reason,
       'Cancelled'                                                    AS activity_type,
       'Cancelled Pre Tracking'                                       AS activity_type_reason,
       '2013-06-30'                                                   AS activity_datetime,
       sc.balance                                                     AS activity_amount,
       NULL                                                           AS redemption_order_id,
       NULL                                                           AS redemption_store_id,
       NULL                                                           AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.store_credit sc
         JOIN lake_view.ultra_merchant.membership m ON m.customer_id = sc.customer_id
         JOIN lake_view.ultra_merchant.statuscode scc ON scc.statuscode = sc.statuscode
WHERE sc.datetime_added < '2013-07-01'
  AND scc.statuscode = 3247 -- Cancelled
  AND NOT exists(
        SELECT 1
        FROM reference.credit_historical_activity ca
        WHERE ca.store_credit_id = sc.store_credit_id
          AND ca.activity_amount = sc.amount
          AND ca.activity_type = 'Cancelled');

------------------------------------------------------------------------
INSERT INTO reference.credit_historical_activity
SELECT sc.store_credit_id,
       NULL                                                        AS administrator_id,
       'Historical Activity'                                       AS activity_source,
       'Historical Activity - Expired Pre SD Migration 2014-04-01' AS activity_source_reason,
       'Expired'                                                   AS activity_type,
       'Credit Expiration'                                         AS activity_type_reason,
       sc.datetime_added                                           AS activity_datetime,
       sc.amount - IFNULL(activity_amount, 0)                      AS activity_amount,
       NULL                                                        AS redemption_order_id,
       NULL                                                        AS redemption_store_id,
       NULL                                                        AS vat_rate_ship_to_country
FROM lake_view.ultra_merchant.store_credit sc
         JOIN lake_view.ultra_merchant.membership m ON m.customer_id = sc.customer_id
         LEFT JOIN (
    SELECT store_credit_id,
           SUM(activity_amount) AS activity_amount
    FROM reference.credit_historical_activity
    WHERE activity_type != 'Issued'
    GROUP BY store_credit_id
) act ON act.store_credit_id = sc.store_credit_id
WHERE 1 = 1
  AND sc.statuscode = 3246
  AND m.store_id = 55
  AND sc.datetime_added < '2014-04-01'-- currently expired
  AND sc.amount - IFNULL(activity_amount, 0) > 0;

------------------------------------------------------------------------
-- SD Credits Imported as Partially Redeemed

INSERT INTO reference.credit_historical_activity
SELECT dcf.credit_id,
       NULL                                                      AS administrator_id,
       'Historical Activity'                                     AS activity_source,
       'Historical Activity - SD Partial Redeemed Pre Migration' AS activity_source_reason,
       'Redeemed'                                                AS activity_type,
       'Redeemed Pre Migration'                                  AS activity_type_reason,
       sc.redemption_date                                        AS activity_datetime,
       redeemed_amount                                           AS activity_amount,
       NULL                                                      AS redemption_order_id,
       55                                                        AS redemption_store_id,
       'US'                                                      AS vat_rate_ship_to_country
FROM reporting_base.reference.sd_pre_migration_redemptions sc
         JOIN stg.dim_credit dcf ON dcf.credit_id = sc.store_credit_id
    AND dcf.credit_type ILIKE '%credit%';

------------------------------------------------------------------------
-- Credits stuck in redeemed from pre 2017 - discovered 1/1/2021
-- Dane found which ones we have reimbursed, so we will cancel those

INSERT INTO reference.credit_historical_activity
SELECT dcf.credit_id,
       NULL                             AS                                         administrator_id,
       'Historical Activity'            AS                                         activity_source,
       'Historical Activity - Stuck In Redeemed 2202-Reimbursed (Dane Work Table)' activity_source_reason,
       'Cancelled'                      AS                                         activity_type,
       'Stuck In Redeemed - Reimbursed' AS                                         activity_type_reason,
       o.datetime_added                 AS                                         activity_datetime,
       w.order_credit_amount            AS                                         activity_amount,
       NULL                                                                        redemption_order_id,
       NULL                             AS                                         redemption_store_id,
       st.store_country                 AS                                         vat_rate_ship_to_country
FROM reporting_base.reference.credits_stuck_redeemed_2202_reimbursement_lookup_020521 w
         JOIN lake_view.ultra_merchant."ORDER" o ON o.order_id = w.order_id
         JOIN stg.dim_credit dcf ON dcf.credit_id = w.store_credit_id
         JOIN data_model.dim_store st ON st.store_id = dcf.store_id
WHERE w.is_reimbursed = 1
  AND dcf.credit_type ILIKE '%credit%';

-- delete from reference.credit_historical_activity
-- where activity_type_reason = 'Stuck In Redeemed - Reimbursed'

CREATE OR REPLACE TEMPORARY TABLE _amounts AS
SELECT store_credit_id,
       SUM(IFF(activity_type = 'Issued', activity_amount, 0))     AS issued,
       SUM(IFF(activity_type = 'Redeemed', activity_amount, 0))   AS redeemed,
       SUM(IFF(activity_type = 'Cancelled', activity_amount, 0)) AS cancelled,
       SUM(IFF(activity_type IN ('Redeemed', 'Transferred', 'Converted to Variable', 'Cancelled', 'Expired'),
               activity_amount, 0))                               AS activity
FROM reference.credit_historical_activity
GROUP BY 1;


DELETE
FROM reference.credit_historical_activity
    USING _amounts
WHERE activity > issued
  AND issued = redeemed
  AND redeemed > 0
  AND cancelled > 0
  AND activity_type = 'Cancelled'
  AND reference.credit_historical_activity.store_credit_id = _amounts.store_credit_id;

UPDATE reference.credit_historical_activity
SET activity_amount = cancel_amount
FROM reporting_base.reference.credit_status_cancelled_no_amount c
WHERE c.store_credit_id = reference.credit_historical_activity.store_credit_id
  AND reference.credit_historical_activity.activity_type = 'Cancelled'
  AND reference.credit_historical_activity.activity_amount = 0;

-- below script should be run only once after the dbsplit


ALTER TABLE reference.credit_historical_activity
ADD meta_original_store_credit_id NUMERIC(38,0);

ALTER TABLE reference.credit_historical_activity
ADD meta_original_redemption_order_id NUMERIC(38,0);


UPDATE reference.credit_historical_activity
SET meta_original_store_credit_id = store_credit_id;

UPDATE reference.credit_historical_activity
SET store_credit_id = NULL;

UPDATE reference.credit_historical_activity h
SET h.store_credit_id = sc.store_credit_id
FROM lake_consolidated.ultra_merchant.store_credit sc
WHERE h.meta_original_store_credit_id = sc.meta_original_store_credit_id;

-- returns 0
SELECT COUNT(*)
FROM reference.credit_historical_activity
WHERE store_credit_id IS NULL;


UPDATE reference.credit_historical_activity
SET meta_original_redemption_order_id = redemption_order_id;

UPDATE reference.credit_historical_activity
SET redemption_order_id = NULL;

UPDATE reference.credit_historical_activity h
SET h.redemption_order_id = o.order_id
FROM lake_consolidated.ultra_merchant."ORDER" o
WHERE h.meta_original_redemption_order_id = o.meta_original_order_id;

-- returns 0
SELECT COUNT(*)
FROM reference.credit_historical_activity
WHERE meta_original_redemption_order_id IS NOT NULL
  AND redemption_order_id IS NULL;
