SET process_to_date = DATE_TRUNC(MONTH, CURRENT_DATE);

-- cash_noncash_credit_redemptions
CREATE OR REPLACE TEMPORARY TABLE _new_redemptions AS
SELECT oc.order_id
     , SUM(oc.amount) AS order_credit_amount
     , SUM(IFF(oc.amount > 0 AND ((scr.cash = 1) -- is a cash store credit
    OR (oc.gift_certificate_id IS NOT NULL AND gc.order_id IS NOT NULL)) -- is a cash giftcard
    , oc.amount, 0))  AS cash_credit_amount
     , SUM(IFF(oc.amount > 0 AND ((scr.cash = 0) -- is a noncash store credit
    OR (oc.gift_certificate_id IS NOT NULL AND gc.order_id IS NULL)) -- is a noncash giftcard
    , oc.amount, 0))  AS noncash_credit_amount
     , SUM(IFF(oc.amount > 0
                   AND oc.store_credit_id IS NULL AND oc.gift_certificate_id IS NULL, oc.amount,
               0))    AS missing_credit_amount
FROM lake_consolidated_view.ultra_merchant.order_credit oc
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = oc.order_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.gift_certificate gc
                   ON gc.gift_certificate_id = oc.gift_certificate_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.store_credit sc ON sc.store_credit_id = oc.store_credit_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.store_credit_reason scr
                   ON sc.store_credit_reason_id = scr.store_credit_reason_id
GROUP BY oc.order_id;


-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------
-- add in the negative amounts, we're getting these all against cash

CREATE OR REPLACE TEMPORARY TABLE _negative_amounts AS
SELECT oc.order_id
     , SUM(oc.amount) AS order_credit_amount
FROM lake_consolidated_view.ultra_merchant.order_credit oc
WHERE oc.amount < 0
  AND oc.store_credit_id IS NOT NULL
GROUP BY oc.order_id;

CREATE OR REPLACE TEMPORARY TABLE _new_calculation AS
SELECT nr.*, nr.cash_credit_amount + na.order_credit_amount AS cash_credit_amuont
FROM _new_redemptions nr
         JOIN _negative_amounts na ON nr.order_id = na.order_id;

-- 	SET
-- 		cash_credit_amount = nr.cash_credit_amount + na.order_credit_amount
-- 	FROM _new_redemptions nr
-- 	JOIN _negative_amounts na ON nr.order_id = na.order_id

-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------
-- Get all the Order records that have credit associated with them

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.cash_noncash_credit_redemptions AS

SELECT o.order_id
     , o.store_id
     , CAST(o.date_shipped AS DATE)         AS date_shipped
     , ZEROIFNULL(o.credit)                 AS credit
     , ZEROIFNULL(nr.order_credit_amount)   AS order_credit_amount
     , ZEROIFNULL(nr.cash_credit_amount)    AS cash_credit_amount
     , ZEROIFNULL(nr.noncash_credit_amount) AS noncash_credit_amount
     , ZEROIFNULL(nr.missing_credit_amount) AS missing_credit_amount
     , CURRENT_DATE()                       AS datetime_added
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         LEFT JOIN _new_calculation nr ON nr.order_id = o.order_id
WHERE credit > 0;

ALTER TABLE reporting_base_prod.shared.cash_noncash_credit_redemptions
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

--base orders
CREATE OR REPLACE TEMPORARY TABLE _order_base AS

SELECT o.order_id,
       o.store_id,
       o.subtotal,
       o.discount,
       fo.unit_count,
       CAST(o.date_shipped AS DATE) AS date_shipped
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         LEFT JOIN edw_prod.data_model.fact_order fo ON fo.order_id = o.order_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.box b ON b.order_id = o.order_id
         JOIN edw_prod.data_model.dim_store ds ON o.store_id = ds.store_id
WHERE o.date_shipped IS NOT NULL
  AND b.order_id IS NULL
  AND o.date_shipped < $process_to_date
  AND ds.store_full_name NOT LIKE '%dm%'
  AND ds.store_full_name NOT LIKE '%swag%'
  AND ds.store_full_name NOT LIKE '%sample%';

--getting order details
CREATE OR REPLACE TEMPORARY TABLE _with_order_classification AS

SELECT ob.*,
       IFF(oc.order_classification_id IS NOT NULL, 1, 0) AS reship_exch_flag,
       ZEROIFNULL(noncash_credit_amount)                 AS noncash_credit_amount
FROM _order_base ob
         LEFT JOIN lake_consolidated_view.ultra_merchant.order_classification oc ON oc.order_id = ob.order_id
    AND order_type_id IN (6, 11)
         LEFT JOIN reporting_base_prod.shared.cash_noncash_credit_redemptions cc
                   ON cc.order_id = ob.order_id;

--inserting into final table
CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.product_dollars_by_company_detail AS
SELECT IFF(st.store_id = 116, st.store_full_name || ' ' || st.store_region,
           st.store_brand || ' ' || st.store_region)                               AS business_unit,
       IFF(st.store_id = 116, st.store_full_name || ' ' || st.store_country,
           st.store_brand || ' ' || st.store_country)                              AS store,
       CASE
           WHEN st.store_id = 26 THEN '35'
           WHEN st.store_id = 41 THEN '32'
           WHEN st.store_id = 52 THEN '39'
           WHEN st.store_id = 79 THEN '52'
           WHEN st.store_id = 55 THEN '41'
           WHEN st.store_id = 46 THEN '36'

           WHEN st.store_id = 36 THEN '33'
           WHEN st.store_id = 59 THEN '44'
           WHEN st.store_id = 38 THEN '34'
           WHEN st.store_id = 61 THEN '45'
           WHEN st.store_id = 63 THEN '46'
           WHEN st.store_id = 50 THEN '37'
           WHEN st.store_id = 48 THEN '38'

           WHEN st.store_id = 65 THEN '47'
           WHEN st.store_id = 73 THEN '50'
           WHEN st.store_id = 67 THEN '48'
           WHEN st.store_id = 69 THEN '49'
           WHEN st.store_id = 71 THEN '51'
           ELSE ''
           END                                                                     AS company,
       o.date_shipped                                                              AS date_shipped,
       o.order_id,
       IFF(o.reship_exch_flag = 0, 1, 0)                                           AS orders_shipped,
       IFF(o.reship_exch_flag = 1, 1, 0)                                           AS orders_shipped_reship_exch,
       IFF(o.reship_exch_flag = 0, unit_count, 0)                                  AS units_shipped,
       IFF(o.reship_exch_flag = 1, unit_count, 0)                                  AS units_shipped_reship_exch,
       IFF(o.reship_exch_flag = 0, subtotal - discount - noncash_credit_amount, 0) AS product_dollars,
       IFF(o.reship_exch_flag = 1, subtotal - discount - noncash_credit_amount, 0) AS product_dollars_reship_exch
FROM _with_order_classification o
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id;

ALTER TABLE reporting_base_prod.shared.product_dollars_by_company_detail SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO reporting_base_prod.shared.product_dollars_by_company_detail_snapshot
SELECT business_unit
     , store
     , company
     , date_shipped
     , order_id
     , orders_shipped
     , orders_shipped_reship_exch
     , units_shipped
     , units_shipped_reship_exch
     , product_dollars
     , product_dollars_reship_exch
     , CURRENT_TIMESTAMP AS snapshot_timestamp
FROM reporting_base_prod.shared.product_dollars_by_company_detail;

DELETE
FROM reporting_base_prod.shared.product_dollars_by_company_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
