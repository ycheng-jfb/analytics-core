SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

--CASH AND TOKEN
CREATE OR REPLACE TABLE _oracle_credit AS
SELECT IFF(LEN(o1.operating_unit) = 0, NULL, o1.operating_unit::VARCHAR)                          operating_unit,
       IFF(LEN(o1.store_credit_id) = 0, NULL, o1.store_credit_id::VARCHAR)                        store_credit_id,
       IFF(LEN(o1.order_id) = 0, NULL, o1.order_id::NUMBER)                                       order_id,
       NULL::NUMBER                                                                               token_id,
       IFF(LEN(o1.store_credit_id) = 0, NULL, o1.store_credit_id::NUMBER)                         credit_id,
       IFF(LEN(o1.refund_id) = 0, NULL, o1.refund_id::NUMBER)                                     refund_id,
       IFF(LEN(o1.trx_date) = 0, NULL, TO_TIMESTAMP(o1.trx_date, 'DD-MON-YYYY HH24:MI:SS')::DATE) trx_date,
       IFF(LEN(o1.trx_number) = 0, NULL, o1.trx_number)                                           trx_number,
       IFF(LEN(o1.amount_original) = 0, 0, o1.amount_original::DECIMAL(20, 4))                    amount_original,
       IFF(LEN(o1.amount_remaining) = 0, 0, o1.amount_remaining::DECIMAL(20, 4))                  amount_remaining,
       IFF(LEN(o1.party_name) = 0, NULL, o1.party_name)                                           party_name,
       'Membership_Credit'                                                                        credit_type,
       IFF(LEN(o1.order_amount) = 0, 0, o1.order_amount::DECIMAL(20, 2))                          order_amount,
       IFF(LEN(o1.vat_amount) = 0, 0, o1.vat_amount::DECIMAL(20, 2))                              vat_amount,
       IFF(LEN(o1.vat_rate) = 0, 0, o1.vat_rate::DECIMAL(20, 2))                                  vat_rate,
       IFF(LEN(o1.currency_code) = 0, NULL, o1.currency_code)                                     currency_code,
       IFF(LEN(o1.calc_amount_remaining) = 0, 0, o1.calc_amount_remaining::DECIMAL(20, 2))        calc_amount_remaining,
       IFF(LEN(o1.vat_prepaid_flag) = 0, NULL, o1.vat_prepaid_flag)                               vat_prepaid_flag,
       IFF(LEN(o1.receivable_account) = 0, NULL, o1.receivable_account)                           receivable_account,
       IFF(LEN(o1.customer_trx_id) = 0, NULL, o1.customer_trx_id)                                 customer_trx_id,
       IFF(LEN(o1.exchange_rate) = 0, 0, o1.exchange_rate)                                        exchange_rate,
       IFF(LEN(o1.trx_type) = 0, NULL, o1.trx_type)                                               trx_type,
       TO_TIMESTAMP(o2.extract_datetime, 'DD-MON-YYYY HH24:MI:SS') ::DATE                         extract_date
FROM reporting_base_prod.shared.credit_recon o1
         LEFT JOIN reporting_base_prod.shared.credit_recon o2
                   ON o1.record_type = 'D'
                       AND o2.record_type = 'H'
                       AND o1.operating_unit = o2.operating_unit

UNION ALL

SELECT IFF(LEN(t1.operating_unit) = 0, NULL, t1.operating_unit::VARCHAR)                          operating_unit,
       NULL                                                                                       store_credit_id,
       IFF(LEN(t1.order_id) = 0, NULL, t1.order_id::NUMBER)                                       order_id,
       IFF(LEN(t1.token_id) = 0, NULL, t1.token_id::VARCHAR)                                      token_id,
       IFF(LEN(t1.token_id) = 0, NULL, t1.token_id::NUMBER)                                       credit_id,
       IFF(LEN(t1.refund_id) = 0, NULL, t1.refund_id::NUMBER)                                     refund_id,
       IFF(LEN(t1.trx_date) = 0, NULL, TO_TIMESTAMP(t1.trx_date, 'DD-MON-YYYY HH24:MI:SS')::DATE) trx_date,
       IFF(LEN(t1.trx_number) = 0, NULL, t1.trx_number)                                           trx_number,
       IFF(LEN(t1.amount_original) = 0, 0, t1.amount_original::DECIMAL(20, 2))                    amount_original,
       IFF(LEN(t1.amount_remaining) = 0, 0, t1.amount_remaining::DECIMAL(20, 2))                  amount_remaining,
       IFF(LEN(t1.party_name) = 0, NULL, t1.party_name)                                           party_name,
       'Token'                                                                                    credit_type,
       IFF(LEN(t1.order_amount) = 0, 0, t1.order_amount::DECIMAL(20, 4))                          order_amount,
       IFF(LEN(t1.vat_amount) = 0, 0, t1.vat_amount::DECIMAL(20, 4))                              vat_amount,
       IFF(LEN(t1.vat_rate) = 0, 0, t1.vat_rate::DECIMAL(20, 4))                                  vat_rate,
       IFF(LEN(t1.currency_code) = 0, NULL, t1.currency_code)                                     currency_code,
       IFF(LEN(t1.calc_amount_remaining) = 0, 0, t1.calc_amount_remaining::DECIMAL(20, 2))        calc_amount_remaining,
       IFF(LEN(t1.vat_prepaid_flag) = 0, NULL, t1.vat_prepaid_flag)                               vat_prepaid_flag,
       IFF(LEN(t1.receivable_account) = 0, NULL, t1.receivable_account)                           receivable_account,
       IFF(LEN(t1.customer_trx_id) = 0, NULL, t1.customer_trx_id)                                 customer_trx_id,
       IFF(LEN(t1.exchange_rate) = 0, 0, t1.exchange_rate)                                        exchange_rate,
       IFF(LEN(t1.trx_type) = 0, NULL, t1.trx_type)                                               trx_type,
       TO_TIMESTAMP(t2.extract_datetime, 'DD-MON-YYYY HH24:MI:SS') ::DATE                         extract_date
FROM reporting_base_prod.shared.token_recon t1
         LEFT JOIN reporting_base_prod.shared.token_recon t2
                   ON t1.record_type = 'D'
                       AND t2.record_type = 'H'
                       AND t1.operating_unit = t2.operating_unit;

CREATE OR REPLACE TEMPORARY TABLE _oracle_clean_up AS
SELECT oc.*,
       store_name AS store,
       dc.store_id
FROM _oracle_credit oc
         LEFT JOIN reporting_base_prod.shared.dim_credit dc
                   ON dc.meta_original_credit_id = oc.credit_id
                       AND IFF(dc.credit_type = 'Token', 'Token', 'Membership_Credit') = oc.credit_type
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dc.store_id
WHERE credit_tender = 'Cash';

SET last_run = (SELECT MAX(TO_TIMESTAMP(trx_date, 'DD-MON-YYYY HH24:MI:SS'))::DATE
                FROM reporting_base_prod.shared.credit_recon
                WHERE trx_date <> '');

CREATE OR REPLACE TEMPORARY TABLE _credit_activity AS
SELECT dc.store_id,
       store_name                         AS                       store,
       store_currency                     AS                       currency,
       dc.credit_id,
       dc.credit_reason,
       dc.credit_issued_hq_datetime::DATE AS                       date_issued,
       dc.credit_tender,
       IFF(dc.credit_type = 'Token', 'Token', 'Membership_Credit') credit_type,
       fce.credit_activity_type,
       fce.credit_activity_local_datetime::DATE                    activity_date,
       IFF(
                   fce.credit_activity_type = 'Issued', fce.credit_activity_gross_vat_local_amount,
                   -fce.credit_activity_gross_vat_local_amount)    rollforward_balance
FROM reporting_base_prod.shared.dim_credit dc
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON fce.credit_key = dc.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dc.store_id
WHERE credit_tender = 'Cash'
  AND fce.credit_activity_local_datetime::DATE <= $last_run;

CREATE OR REPLACE TEMPORARY TABLE _credit_rollforward AS
SELECT store_id,
       store,
       currency,
       credit_id,
       credit_reason,
       date_issued,
       credit_tender,
       credit_type,
       MAX(activity_date)                 last_activity_date,
       ROUND(SUM(rollforward_balance), 2) rollforward_balance
FROM _credit_activity
GROUP BY store_id,
       store,
       currency,
       credit_id,
       credit_reason,
       date_issued,
       credit_tender,
       credit_type;

-- default balance to 0 when < 0
UPDATE _credit_rollforward cr
SET cr.rollforward_balance = sc.balance
FROM lake_consolidated_view.ultra_merchant.store_credit sc
WHERE sc.store_credit_id = cr.credit_id
  AND cr.rollforward_balance < 0
  AND cr.rollforward_balance <> sc.balance;

-- bringing in ecom for futher validation
CREATE OR REPLACE TEMPORARY TABLE _ecom AS
SELECT s.store_id,
       store_name                AS store,
       store_currency            AS currency,
       scs.store_credit_id       AS credit_id,
       sc.datetime_added :: DATE AS date_issued,
       'Membership_Credit'       AS credit_type,
       scs.statuscode,
       scs.accounting_balance    AS ecom_balance
FROM reporting_prod.dbo.store_credit_snapshot scs
         JOIN lake_consolidated_view.ultra_merchant.store_credit sc
              ON scs.store_credit_id = edw_prod.stg.udf_unconcat_brand(sc.store_credit_id)
         JOIN reporting_base_prod.shared.dim_credit dc
              ON sc.store_credit_id = dc.credit_id
                  AND dc.credit_tender = 'Cash'
                  AND dc.credit_type <> 'Token'
         JOIN lake_consolidated_view.ultra_merchant.customer c
              ON sc.customer_id = c.customer_id
         JOIN edw_prod.data_model.dim_store s
              ON c.store_id = s.store_id

UNION ALL

SELECT s.store_id,
       store_name                AS store,
       store_currency            AS currency,
       mts.membership_token_id   AS credit_id,
       mt.datetime_added :: DATE AS date_issued,
       'Token'                   AS credit_type,
       mts.statuscode,
       mts.accounting_balance    AS ecom_balance
FROM reporting_prod.dbo.membership_token_snapshot mts
         JOIN lake_consolidated_view.ultra_merchant.membership_token mt
              ON mts.membership_token_id = edw_prod.stg.udf_unconcat_brand(mt.membership_token_id)
         JOIN lake_consolidated_view.ultra_merchant.membership m
              ON mt.membership_id = m.membership_id
         JOIN edw_prod.data_model.dim_store s
              ON m.store_id = s.store_id;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.ecom_oracle_balance_comp AS
SELECT cr.store,
       cr.store_id,
       cr.currency,
       edw_prod.stg.udf_unconcat_brand(cr.credit_id) AS                          credit_id,
       cr.credit_reason,
       cr.date_issued,
       cr.credit_type,
       oc.vat_prepaid_flag,
       oc.receivable_account,
       oc.customer_trx_id,
       oc.exchange_rate,
       oc.trx_type,
       ZEROIFNULL(cr.rollforward_balance)                                        rollforward_balance,
       ZEROIFNULL(oc.calc_amount_remaining)                                      calc_amount_remaining,
       ZEROIFNULL(cr.rollforward_balance) - ZEROIFNULL(oc.calc_amount_remaining) diff,
       CASE
           WHEN ZEROIFNULL(cr.rollforward_balance) = ZEROIFNULL(oc.calc_amount_remaining) THEN 'MATCH'
           WHEN ABS(ZEROIFNULL(cr.rollforward_balance) - ZEROIFNULL(oc.calc_amount_remaining)) <= 0.03 AND
                vat_prepaid_flag = 'N' THEN 'Rounding Issue'
           WHEN ABS(ZEROIFNULL(cr.rollforward_balance) - ZEROIFNULL(oc.calc_amount_remaining)) <= 0.03 AND
                vat_prepaid_flag = 'Y' THEN 'VAT Rounding Issue'
           WHEN ZEROIFNULL(cr.rollforward_balance) > 0 AND ZEROIFNULL(oc.calc_amount_remaining) = 0
               THEN 'Outstanding EDW Not Oracle'
           WHEN ZEROIFNULL(cr.rollforward_balance) = 0 AND ZEROIFNULL(oc.calc_amount_remaining) > 0
               THEN 'Outstanding Oracle Not EDW'
           WHEN ZEROIFNULL(cr.rollforward_balance) > ZEROIFNULL(oc.calc_amount_remaining)
               THEN 'Diff Outstanding EDW>Oracle'
           WHEN ZEROIFNULL(cr.rollforward_balance) < ZEROIFNULL(oc.calc_amount_remaining)
               THEN 'Diff Outstanding EDW<Oracle'
           END                                       AS                          match_status
FROM _credit_rollforward cr
         LEFT JOIN _oracle_clean_up oc
                   ON oc.credit_id = edw_prod.stg.udf_unconcat_brand(cr.credit_id)
                       AND oc.credit_type = cr.credit_type
                       AND oc.store = cr.store

UNION ALL

--should have no returns
SELECT oc.store,
       oc.store_id,
       oc.currency_code,
       oc.credit_id,
       cr.credit_reason,
       cr.date_issued,
       oc.credit_type,
       oc.vat_prepaid_flag,
       oc.receivable_account,
       oc.customer_trx_id,
       oc.exchange_rate,
       oc.trx_type,
       ZEROIFNULL(cr.rollforward_balance)                                        rollforward_balance,
       ZEROIFNULL(oc.calc_amount_remaining)                                      calc_amount_remaining,
       ZEROIFNULL(cr.rollforward_balance) - ZEROIFNULL(oc.calc_amount_remaining) diff,
       CASE
           WHEN ZEROIFNULL(cr.rollforward_balance) = ZEROIFNULL(oc.calc_amount_remaining) THEN 'MATCH'
           WHEN ABS(ZEROIFNULL(cr.rollforward_balance) - ZEROIFNULL(oc.calc_amount_remaining)) <= 0.03 AND
                vat_prepaid_flag = 'N' THEN 'Rounding Issue'
           WHEN ABS(ZEROIFNULL(cr.rollforward_balance) - ZEROIFNULL(oc.calc_amount_remaining)) <= 0.03 AND
                vat_prepaid_flag = 'Y' THEN 'VAT Rounding Issue'
           WHEN ZEROIFNULL(cr.rollforward_balance) > 0 AND ZEROIFNULL(oc.calc_amount_remaining) = 0
               THEN 'Outstanding EDW Not Oracle'
           WHEN ZEROIFNULL(cr.rollforward_balance) = 0 AND ZEROIFNULL(oc.calc_amount_remaining) > 0
               THEN 'Outstanding Oracle Not EDW'
           WHEN ZEROIFNULL(cr.rollforward_balance) > ZEROIFNULL(oc.calc_amount_remaining)
               THEN 'Diff Outstanding EDW>Oracle'
           WHEN ZEROIFNULL(cr.rollforward_balance) < ZEROIFNULL(oc.calc_amount_remaining)
               THEN 'Diff Outstanding EDW<Oracle'
           END AS                                                                match_status
FROM _oracle_clean_up oc
         LEFT JOIN _credit_rollforward cr
                   ON oc.credit_id = edw_prod.stg.udf_unconcat_brand(cr.credit_id)
                       AND oc.credit_type = cr.credit_type
                       AND oc.store = cr.store
WHERE cr.credit_id IS NULL;

UPDATE reporting_base_prod.shared.ecom_oracle_balance_comp
SET match_status = 'ShoeDazzle Classic Credit before oracle go live' -- only updated match status, and not adding that
WHERE match_status NOT IN
      ('MATCH', 'VAT Rounding Issue', 'Rounding Issue', 'ShoeDazzle Classic Credit before oracle go live')
  AND credit_reason = 'ShoeDazzle Classic Credit';

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.ecom_oracle_balance_comp AS
SELECT eobc.store_id,
       eobc.store,
       eobc.currency,
       eobc.credit_id,
       eobc.credit_type,
       eobc.credit_reason,
       eobc.date_issued,
       eobc.vat_prepaid_flag,
       eobc.receivable_account,
       eobc.customer_trx_id,
       eobc.exchange_rate,
       eobc.trx_type,
       eobc.rollforward_balance,
       eobc.calc_amount_remaining,
       eobc.diff,
       eobc.match_status,
       ZEROIFNULL(e.ecom_balance)                                 ecom_balance,
       ZEROIFNULL(eobc.rollforward_balance - e.ecom_balance)   AS edw_ecom_diff,
       ZEROIFNULL(eobc.calc_amount_remaining - e.ecom_balance) AS oracle_ecom_diff,
       CASE
           WHEN eobc.match_status IN
                ('MATCH', 'VAT Rounding Issue', 'Rounding Issue', 'ShoeDazzle Classic Credit before oracle go live')
               THEN 'IGNORE'
           WHEN e.statuscode IN (3245, 3545) THEN 'Redeeming'
           WHEN ZEROIFNULL(eobc.rollforward_balance) = ZEROIFNULL(e.ecom_balance) THEN 'MATCH'
           ELSE 'NOT MATCHED' END                                       AS edw_ecom_status,
       CASE
           WHEN eobc.match_status IN
                ('MATCH', 'VAT Rounding Issue', 'Rounding Issue', 'ShoeDazzle Classic Credit before oracle go live')
               THEN 'IGNORE'
           WHEN e.statuscode IN (3245, 3545) THEN 'Redeeming'
           WHEN ZEROIFNULL(eobc.calc_amount_remaining) = ZEROIFNULL(e.ecom_balance) THEN 'MATCH'
           ELSE 'NOT MATCHED' END                                       AS oracle_ecom_status
FROM reporting_base_prod.shared.ecom_oracle_balance_comp eobc
         LEFT JOIN _ecom e
                   ON eobc.credit_id = e.credit_id
                       AND eobc.credit_type = e.credit_type
                       AND eobc.store_id = e.store_id
                       AND eobc.match_status NOT IN ('MATCH', 'VAT Rounding Issue', 'Rounding Issue',
                                                     'ShoeDazzle Classic Credit before oracle go live');

/*Snapshot process. Delete records older than a year */
INSERT INTO reporting_base_prod.shared.ecom_oracle_balance_comp_snapshot
(store_id,
 store,
 currency,
 credit_id,
 credit_type,
 credit_reason,
 date_issued,
 vat_prepaid_flag,
 receivable_account,
 customer_trx_id,
 exchange_rate,
 trx_type,
 rollforward_balance,
 calc_amount_remaining,
 diff,
 match_status,
 ecom_balance,
 edw_ecom_diff,
 oracle_ecom_diff,
 edw_ecom_status,
 oracle_ecom_status,
 snapshot_datetime)
SELECT store_id,
       store,
       currency,
       credit_id,
       credit_type,
       credit_reason,
       date_issued,
       vat_prepaid_flag,
       receivable_account,
       customer_trx_id,
       exchange_rate,
       trx_type,
       rollforward_balance,
       calc_amount_remaining,
       diff,
       match_status,
       ecom_balance,
       edw_ecom_diff,
       oracle_ecom_diff,
       edw_ecom_status,
       oracle_ecom_status,
       $execution_start_time AS snapshot_datetime
FROM reporting_base_prod.shared.ecom_oracle_balance_comp;

DELETE
FROM reporting_base_prod.shared.ecom_oracle_balance_comp_snapshot
WHERE snapshot_datetime < DATEADD(month, -12, GETDATE());

--NONCASH
--get the last run from oracle file
SET target_date = (SELECT MAX(TO_TIMESTAMP(extract_datetime, 'DD-MON-YYYY HH24:MI:SS')):: DATE
                   FROM reporting_base_prod.shared.noncashbalance);

-- edw data
CREATE OR REPLACE TEMPORARY TABLE _temp_non_cash AS
SELECT dc.store_id,
       st.store_name                      AS                             store,
       st.store_currency                  AS                             currency,
       dc.credit_id,
       dc.credit_reason,
       dc.credit_issued_hq_datetime::DATE AS                             date_issued,
       dc.credit_tender,
       IFF(dc.credit_type = 'Token', 'Token', 'Membership_Credit')       credit_type,
       fce.credit_activity_type,
       fce.credit_activity_local_datetime::DATE                          activity_date,
       IFF(fce.credit_activity_type = 'Issued',
           fce.credit_activity_gross_vat_local_amount,
           -fce.credit_activity_gross_vat_local_amount) ::DECIMAL(20, 2) credit_balance
FROM reporting_base_prod.shared.dim_credit dc
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON fce.credit_key = dc.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dc.store_id
WHERE dc.credit_tender = 'NonCash'
  AND fce.credit_activity_local_datetime::DATE <= $target_date;

CREATE OR REPLACE TEMPORARY TABLE _credit_rollforward_non_cash AS
SELECT store_id,
       store,
       currency,
       credit_id,
       credit_reason,
       date_issued,
       credit_tender,
       credit_type,
       MAX(activity_date)            last_activity_date,
       ROUND(SUM(credit_balance), 2) rollforward_balance
FROM _temp_non_cash
GROUP BY store_id,
       store,
       currency,
       credit_id,
       credit_reason,
       date_issued,
       credit_tender,
       credit_type;

-- default balance to 0 when < 0
UPDATE _credit_rollforward_non_cash cr
SET cr.rollforward_balance = sc.balance
FROM lake_consolidated_view.ultra_merchant.store_credit sc
WHERE sc.store_credit_id = cr.credit_id
  AND cr.rollforward_balance < 0;


CREATE OR REPLACE TEMPORARY TABLE _ecom_non_cash AS
SELECT s.store_id,
       IFF(s.label IN ('Savage X', 'FabKids', 'Fabletics', 'JustFab', 'ShoeDazzle'), s.label || 'US', s.label) AS store,
       CASE
           WHEN s.label IN
                ('Fabletics', 'JustFab', 'Savage X', 'FabKids', 'ShoeDazzle', 'JustFab CA', 'Fabletics CA', 'Yitty')
               THEN 'USD'
           WHEN s.label IN ('Fabletics DK', 'JustFab DK', 'Savage X DK') THEN 'DKK'
           WHEN s.label IN ('Fabletics SE', 'JustFab SE', 'Savage X SE') THEN 'SEK'
           WHEN s.label IN ('Fabletics UK', 'JustFab UK', 'Savage X UK') THEN 'GBP'
           ELSE 'EUR' END                                                                                      AS currency,
       scs.store_credit_id                                                                                     AS credit_id,
       sc.datetime_added :: DATE                                                                               AS date_issued,
       'Membership_Credit'                                                                                     AS credit_type,
       scs.statuscode,
       scs.accounting_balance                                                                                  AS ecom_balance
FROM reporting_prod.dbo.store_credit_snapshot scs
         JOIN lake_consolidated_view.ultra_merchant.store_credit sc
              ON scs.store_credit_id = edw_prod.stg.udf_unconcat_brand(sc.store_credit_id)
         JOIN reporting_base_prod.shared.dim_credit dc
              ON sc.store_credit_id = dc.credit_id
                  AND dc.credit_tender = 'NonCash'
                  AND dc.credit_type <> 'Token'
         JOIN lake_consolidated_view.ultra_merchant.customer c
              ON sc.customer_id = c.customer_id
         JOIN lake_consolidated_view.ultra_merchant.store s
              ON c.store_id = s.store_id

UNION ALL

SELECT s.store_id,
       IFF(s.label IN ('Savage X', 'FabKids', 'Fabletics', 'JustFab', 'ShoeDazzle'), s.label || 'US', s.label) AS store,
       CASE
           WHEN s.label IN
                ('Fabletics', 'JustFab', 'Savage X', 'FabKids', 'ShoeDazzle', 'JustFab CA', 'Fabletics CA', 'Yitty')
               THEN 'USD'
           WHEN s.label IN ('Fabletics DK', 'JustFab DK', 'Savage X DK') THEN 'DKK'
           WHEN s.label IN ('Fabletics SE', 'JustFab SE', 'Savage X SE') THEN 'SEK'
           WHEN s.label IN ('Fabletics UK', 'JustFab UK', 'Savage X UK') THEN 'GBP'
           ELSE 'EUR' END                                                                                      AS currency,
       mts.membership_token_id                                                                                 AS credit_id,
       mt.datetime_added :: DATE                                                                               AS date_issued,
       'Token'                                                                                                 AS credit_type,
       mts.statuscode,
       mts.accounting_balance                                                                                  AS ecom_balance
FROM reporting_prod.dbo.membership_token_snapshot mts
         JOIN lake_consolidated_view.ultra_merchant.membership_token mt
              ON mts.membership_token_id = edw_prod.stg.udf_unconcat_brand(mt.membership_token_id)
         JOIN lake_consolidated_view.ultra_merchant.membership m
              ON mt.membership_id = m.membership_id
         JOIN lake_consolidated_view.ultra_merchant.store s
              ON m.store_id = s.store_id;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.ecom_oracle_noncash_balance_comp_initial AS
SELECT cr.store,
       cr.store_id,
       cr.currency,
       IFNULL(edw_prod.stg.udf_unconcat_brand(cr.credit_id),
              oc.noncash_credit_id)::NUMBER                               credit_id,
       cr.credit_reason,
       cr.date_issued,
       cr.credit_type,
       ZEROIFNULL(cr.rollforward_balance)                                 rollforward_balance,
       ZEROIFNULL(oc.oracle_balance)                                      calc_amount_remaining,
       ZEROIFNULL(cr.rollforward_balance) - ZEROIFNULL(oc.oracle_balance) diff,
       CASE
           WHEN ZEROIFNULL(cr.rollforward_balance) = ZEROIFNULL(oc.oracle_balance) THEN 'MATCH'
           WHEN ABS(ZEROIFNULL(cr.rollforward_balance) - ZEROIFNULL(oc.oracle_balance)) <= 0.03 THEN 'Rounding Issue'
           WHEN ZEROIFNULL(cr.rollforward_balance) > 0 AND ZEROIFNULL(oc.oracle_balance) = 0
               THEN 'Outstanding EDW Not Oracle'
           WHEN ZEROIFNULL(cr.rollforward_balance) = 0 AND ZEROIFNULL(oc.oracle_balance) > 0
               THEN 'Outstanding Oracle Not EDW'
           WHEN ZEROIFNULL(cr.rollforward_balance) > ZEROIFNULL(oc.oracle_balance) THEN 'Diff Outstanding EDW>Oracle'
           WHEN ZEROIFNULL(cr.rollforward_balance) < ZEROIFNULL(oc.oracle_balance) THEN 'Diff Outstanding EDW<Oracle'
           END AS                                                         match_status
FROM _credit_rollforward_non_cash cr
         FULL JOIN reporting_base_prod.shared.noncashbalance oc
                   ON oc.noncash_credit_id::VARCHAR = edw_prod.stg.udf_unconcat_brand(cr.credit_id)
                       AND oc.credit_type = cr.credit_type;

UPDATE reporting_base_prod.shared.ecom_oracle_noncash_balance_comp_initial
SET match_status = 'ShoeDazzle Classic Credit before oracle go live' -- only updated match status, and not adding that
WHERE match_status NOT IN
      ('MATCH', 'VAT Rounding Issue', 'Rounding Issue', 'ShoeDazzle Classic Credit before oracle go live')
  AND credit_reason = 'ShoeDazzle Classic Credit';

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.ecom_oracle_noncash_balance_comp AS
SELECT eobc.store_id,
       eobc.store,
       eobc.currency,
       eobc.credit_id                                          AS credit_id,
       eobc.credit_type,
       eobc.credit_reason,
       eobc.date_issued,
       eobc.rollforward_balance,
       eobc.calc_amount_remaining,
       eobc.diff,
       eobc.match_status,
       ZEROIFNULL(e.ecom_balance)                                 ecom_balance,
       ZEROIFNULL(eobc.rollforward_balance - e.ecom_balance)   AS edw_ecom_diff,
       ZEROIFNULL(eobc.calc_amount_remaining - e.ecom_balance) AS oracle_ecom_diff,
       CASE
           WHEN eobc.match_status IN
                ('MATCH', 'VAT Rounding Issue', 'Rounding Issue', 'ShoeDazzle Classic Credit before oracle go live')
               THEN 'IGNORE'
           WHEN e.statuscode IN (3245, 3545) THEN 'Redeeming'
           WHEN ZEROIFNULL(eobc.rollforward_balance) = ZEROIFNULL(e.ecom_balance) THEN 'MATCH'
           ELSE 'NOT MATCHED' END                                       AS edw_ecom_status,
       CASE
           WHEN eobc.match_status IN
                ('MATCH', 'VAT Rounding Issue', 'Rounding Issue', 'ShoeDazzle Classic Credit before oracle go live')
               THEN 'IGNORE'
           WHEN e.statuscode IN (3245, 3545) THEN 'Redeeming'
           WHEN ZEROIFNULL(eobc.calc_amount_remaining) = ZEROIFNULL(e.ecom_balance) THEN 'MATCH'
           ELSE 'NOT MATCHED' END                                       AS oracle_ecom_status
FROM reporting_base_prod.shared.ecom_oracle_noncash_balance_comp_initial eobc
         LEFT JOIN _ecom_non_cash e
                   ON eobc.credit_id = e.credit_id
                       AND eobc.credit_type = e.credit_type
                       AND eobc.store_id = e.store_id
                       AND eobc.match_status NOT IN ('MATCH', 'VAT Rounding Issue', 'Rounding Issue',
                                                     'ShoeDazzle Classic Credit before oracle go live');

/*Snapshot process. Delete records older than a year */
INSERT INTO reporting_base_prod.shared.ecom_oracle_noncash_balance_comp_snapshot
(store_id,
 store,
 currency,
 credit_id,
 credit_type,
 credit_reason,
 date_issued,
 rollforward_balance,
 calc_amount_remaining,
 diff,
 match_status,
 ecom_balance,
 edw_ecom_diff,
 oracle_ecom_diff,
 edw_ecom_status,
 oracle_ecom_status,
 snapshot_datetime)
SELECT store_id,
       store,
       currency,
       credit_id,
       credit_type,
       credit_reason,
       date_issued,
       rollforward_balance,
       calc_amount_remaining,
       diff,
       match_status,
       ecom_balance,
       edw_ecom_diff,
       oracle_ecom_diff,
       edw_ecom_status,
       oracle_ecom_status,
       $execution_start_time AS snapshot_datetime
FROM reporting_base_prod.shared.ecom_oracle_noncash_balance_comp;

DELETE
FROM reporting_base_prod.shared.ecom_oracle_noncash_balance_comp_snapshot
WHERE snapshot_datetime < DATEADD(month, -12, GETDATE());
