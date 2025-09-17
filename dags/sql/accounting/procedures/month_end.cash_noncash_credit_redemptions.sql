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

CREATE OR REPLACE TRANSIENT TABLE month_end.cash_noncash_credit_redemptions AS

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

ALTER TABLE month_end.cash_noncash_credit_redemptions
    SET DATA_RETENTION_TIME_IN_DAYS = 0;
