SET target_table = 'stg.fact_order';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

-- Switch warehouse if performing a full refresh
SET initial_warehouse = CURRENT_WAREHOUSE();
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

-- SELECT CURRENT_WAREHOUSE();
-- USE WAREHOUSE IDENTIFIER ('da_wh_adhoc_large'); -- Large Warehouse
-- USE WAREHOUSE IDENTIFIER ('da_wh_edw'); -- Normal Warehouse used by System (Low Concurrent Usage)
-- USE WAREHOUSE IDENTIFIER ('da_wh_etl'); -- Normal Warehouse used by System (High Concurrent Usage)
-- USE WAREHOUSE IDENTIFIER ('da_wh_analytics'); -- Normal Warehouse used by Individuals

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/


-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_lake_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_dim_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer'));
SET wm_fact_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_membership_event'));
SET wm_fact_activation = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_activation'));
SET wm_lake_ultra_merchant_order_line = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line'));
SET wm_lake_ultra_merchant_order_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_classification'));
SET wm_lake_ultra_merchant_order_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_credit'));
SET wm_lake_ultra_merchant_credit_transfer_transaction = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.credit_transfer_transaction'));
SET wm_lake_ultra_merchant_gift_certificate = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_certificate'));
SET wm_lake_ultra_merchant_gift_certificate_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_certificate_store_credit'));
SET wm_lake_ultra_merchant_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit'));
SET wm_lake_ultra_merchant_store_credit_reason = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit_reason'));
SET wm_lake_ultra_merchant_membership_plan_membership_brand = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_plan_membership_brand'));
SET wm_lake_ultra_merchant_membership_token = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_token'));
SET wm_lake_ultra_merchant_membership_token_reason = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_token_reason'));
SET wm_lake_ultra_merchant_order_tracking_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_tracking_detail'));
SET wm_lake_ultra_merchant_order_discount = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_discount'));
SET wm_lake_ultra_merchant_order_surcharge = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_surcharge'));
SET wm_lake_ultra_merchant_payment_transaction_creditcard = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.payment_transaction_creditcard'));
SET wm_lake_ultra_merchant_payment_transaction_psp = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.payment_transaction_psp'));
SET wm_lake_ultra_merchant_payment_transaction_cash = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.payment_transaction_cash'));
SET wm_lake_ultra_merchant_billing_retry_schedule_queue = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.billing_retry_schedule_queue'));
SET wm_lake_ultra_merchant_box = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.box'));
SET wm_lake_ultra_merchant_box_completion = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.box_completion'));
SET wm_lake_ultra_merchant_order_shipment = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_shipment'));
SET wm_lake_ultra_merchant_carrier_uw = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.carrier_uw'));
SET wm_lake_ultra_merchant_exchange = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.exchange'));
SET wm_lake_ultra_merchant_order_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_detail'));
SET wm_lake_ultra_merchant_order_cancel = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_cancel'));
SET wm_lake_ultra_merchant_order_shipping = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_shipping'));
SET wm_lake_ultra_merchant_order_tax = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_tax'));
SET wm_lake_ultra_merchant_address = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.address'));
SET wm_lake_ultra_merchant_gift_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_order'));
SET wm_lake_ultra_merchant_reship = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.reship'));
SET wm_lake_ultra_merchant_bounceback_endowment = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.bounceback_endowment'));
SET wm_lkp_order_classification = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.lkp_order_classification'));
SET wm_edw_reference_test_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.test_customer'));
SET wm_edw_reference_finance_assumption = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.finance_assumption'));


CREATE OR REPLACE TEMP TABLE _fact_order__order_base (order_id INT, store_id INT, master_order_id INT);

-- Full Refresh
INSERT INTO _fact_order__order_base (order_id, master_order_id)
SELECT o.order_id, o.master_order_id
FROM lake_consolidated.ultra_merchant."ORDER" AS o /* WHERE o.customer_id = 316416712 */
WHERE $is_full_refresh /* SET is_full_refresh = TRUE; */
ORDER BY o.order_id;

-- Incremental Refresh
INSERT INTO _fact_order__order_base (order_id)
SELECT DISTINCT incr.order_id
FROM (
    SELECT o.order_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        LEFT JOIN stg.dim_customer AS dc
            ON dc.customer_id = o.customer_id
        LEFT JOIN stg.fact_membership_event AS fme
            ON fme.customer_id = o.customer_id
        LEFT JOIN stg.fact_activation AS fa
            ON fa.customer_id = o.customer_id
        LEFT JOIN reference.test_customer AS tc
            ON tc.customer_id = o.customer_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmb
            ON mpmb.membership_brand_id = o.membership_brand_id
        LEFT JOIN lake_consolidated.ultra_merchant.address AS a
            ON a.address_id = o.shipping_address_id
    WHERE (o.meta_update_datetime > $wm_lake_ultra_merchant_order
        OR dc.meta_update_datetime > $wm_dim_customer
        OR fme.meta_update_datetime > $wm_fact_membership_event
        OR fa.meta_update_datetime > $wm_fact_activation
        OR tc.meta_update_datetime > $wm_edw_reference_test_customer
        OR mpmb.meta_update_datetime > $wm_lake_ultra_merchant_membership_plan_membership_brand
        OR a.meta_update_datetime > $wm_lake_ultra_merchant_address)
    UNION ALL
    SELECT o.order_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
            ON gfto.order_id = o.order_id
        LEFT JOIN stg.dim_customer AS dc
            ON dc.customer_id = gfto.sender_customer_id
        LEFT JOIN stg.fact_membership_event AS fme
            ON fme.customer_id = gfto.sender_customer_id
        LEFT JOIN stg.fact_activation AS fa
            ON fa.customer_id = gfto.sender_customer_id
        LEFT JOIN reference.test_customer AS tc
            ON tc.customer_id = gfto.sender_customer_id
    WHERE (gfto.meta_update_datetime > $wm_lake_ultra_merchant_gift_order
        OR dc.meta_update_datetime > $wm_dim_customer
        OR fme.meta_update_datetime > $wm_fact_membership_event
        OR fa.meta_update_datetime > $wm_fact_activation
        OR tc.meta_update_datetime > $wm_edw_reference_test_customer)
    UNION ALL
    SELECT ol.order_id
    FROM lake_consolidated.ultra_merchant.order_line as ol
    WHERE ol.meta_update_datetime > $wm_lake_ultra_merchant_order_line
    UNION ALL
    SELECT oc.order_id
    FROM lake_consolidated.ultra_merchant.order_classification AS oc
    WHERE oc.meta_update_datetime > $wm_lake_ultra_merchant_order_classification
    UNION ALL
    SELECT oc.order_id
    FROM lake_consolidated.ultra_merchant.order_credit AS oc
        LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction AS ctt
	    	ON ctt.store_credit_id = oc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
            ON gc.gift_certificate_id = oc.gift_certificate_id
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit AS gcs
            ON gcs.gift_certificate_id = gc.gift_certificate_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit AS sc
            ON COALESCE(oc.store_credit_id, gcs.store_credit_id) = sc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason AS scr
            ON scr.store_credit_reason_id = sc.store_credit_reason_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership_token AS mt
            ON mt.membership_token_id = oc.membership_token_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership_token_reason AS mtr
            ON mtr.membership_token_reason_id = mt.membership_token_reason_id
        LEFT JOIN lake_consolidated.ultra_merchant.bounceback_endowment be
            ON be.object_id = gc.gift_certificate_id
    WHERE (oc.meta_update_datetime > $wm_lake_ultra_merchant_order_credit
        OR ctt.meta_update_datetime > $wm_lake_ultra_merchant_credit_transfer_transaction
        OR gc.meta_update_datetime > $wm_lake_ultra_merchant_gift_certificate
        OR gcs.meta_update_datetime > $wm_lake_ultra_merchant_gift_certificate_store_credit
        OR sc.meta_update_datetime > $wm_lake_ultra_merchant_store_credit
        OR scr.meta_update_datetime > $wm_lake_ultra_merchant_store_credit_reason
        OR mt.meta_update_datetime > $wm_lake_ultra_merchant_membership_token
        OR mtr.meta_update_datetime > $wm_lake_ultra_merchant_membership_token_reason
        OR be.meta_update_datetime > $wm_lake_ultra_merchant_bounceback_endowment)
    UNION ALL
    SELECT o.order_id
    FROM  lake_consolidated.ultra_merchant."ORDER" o
        JOIN lake_consolidated.ultra_merchant.order_tracking_detail otd
            ON o.order_tracking_id = otd.order_tracking_id AND otd.object ='administrator'
    WHERE otd.meta_update_datetime > $wm_lake_ultra_merchant_order_tracking_detail
    UNION ALL
    SELECT od.order_id
    FROM lake_consolidated.ultra_merchant.order_discount AS od
    WHERE od.meta_update_datetime > $wm_lake_ultra_merchant_order_discount
    UNION ALL
    SELECT os.order_id
    FROM lake_consolidated.ultra_merchant.order_surcharge AS os
    WHERE os.meta_update_datetime > $wm_lake_ultra_merchant_order_surcharge
    UNION ALL
    SELECT ptc.order_id
    FROM lake_consolidated.ultra_merchant.payment_transaction_creditcard AS ptc
    WHERE ptc.meta_update_datetime > $wm_lake_ultra_merchant_payment_transaction_creditcard
        AND ptc.transaction_type IN ('PRIOR_AUTH_CAPTURE', 'SALE_REDIRECT')
        AND ptc.statuscode = 4001
    UNION ALL
    SELECT psp.order_id
    FROM lake_consolidated.ultra_merchant.payment_transaction_psp AS psp
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.order_id = psp.order_id
        JOIN stg.dim_store AS st
            ON st.store_id = o.store_id
    WHERE psp.meta_update_datetime > $wm_lake_ultra_merchant_payment_transaction_psp
        AND psp.transaction_type IN ('PRIOR_AUTH_CAPTURE', 'SALE_REDIRECT', IFF(st.store_region = 'EU' AND st.store_type = 'Retail', 'AUTH_REDIRECT', 'SALE_REDIRECT'))
        AND psp.statuscode IN (4001, 4040)
    UNION ALL
    SELECT ptc.order_id
    FROM lake_consolidated.ultra_merchant.payment_transaction_cash AS ptc
    WHERE ptc.meta_update_datetime > $wm_lake_ultra_merchant_payment_transaction_cash
        AND ptc.transaction_type = 'SALE'
        AND ptc.statuscode = 4001
    UNION ALL
    SELECT brsq.order_id
    FROM lake_consolidated.ultra_merchant.billing_retry_schedule_queue AS brsq
    WHERE brsq.meta_update_datetime > $wm_lake_ultra_merchant_billing_retry_schedule_queue
    UNION ALL
    SELECT b.order_id
    FROM lake_consolidated.ultra_merchant.box AS b
    WHERE b.meta_update_datetime > $wm_lake_ultra_merchant_box
    UNION ALL
    SELECT b.order_id
    FROM lake_consolidated.ultra_merchant.box_completion as bc
        JOIN lake_consolidated.ultra_merchant.box as b
            on b.box_id = bc.box_id
    WHERE bc.meta_update_datetime > $wm_lake_ultra_merchant_box_completion
    UNION ALL
    SELECT os.order_id
    FROM lake_consolidated.ultra_merchant.order_shipment AS os
        LEFT JOIN lake_consolidated.ultra_merchant.carrier_uw AS cu
            ON cu.carrier_id = os.carrier_id
    WHERE (os.meta_update_datetime > $wm_lake_ultra_merchant_order_shipment
            OR cu.meta_update_datetime > $wm_lake_ultra_merchant_carrier_uw)
        AND os.statuscode = 4510
    UNION ALL
    SELECT loc.order_id
    FROM stg.lkp_order_classification as loc
    WHERE loc.meta_update_datetime > $wm_lkp_order_classification
    UNION ALL
    SELECT e.exchange_order_id AS order_id
    FROM lake_consolidated.ultra_merchant.exchange AS e
    WHERE e.meta_update_datetime > $wm_lake_ultra_merchant_exchange
    UNION ALL
    SELECT od.order_id
    FROM lake_consolidated.ultra_merchant.order_detail AS od
    WHERE od.name IN ('retail_store_id', 'original_store_id', 'masterpass_checkout_payment_id', 'visa_checkout_call_id')
    AND od.meta_update_datetime > $wm_lake_ultra_merchant_order_detail
    UNION ALL
    SELECT oc.order_id
    FROM lake_consolidated.ultra_merchant.order_cancel AS oc
    WHERE oc.meta_update_datetime > $wm_lake_ultra_merchant_order_cancel
    UNION ALL
    SELECT osc.order_id
    FROM lake_consolidated.ultra_merchant.order_shipping AS osc
    WHERE osc.meta_update_datetime > $wm_lake_ultra_merchant_order_shipping
    UNION ALL
    SELECT ot.order_id
    FROM lake_consolidated.ultra_merchant.order_tax AS ot
    WHERE ot.meta_update_datetime > $wm_lake_ultra_merchant_order_tax
    AND type = 'delivery'
    UNION ALL
    SELECT reship_order_id
    FROM lake_consolidated.ultra_merchant.reship
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_reship
    UNION ALL
    SELECT order_id
    FROM stg.fact_order
    WHERE DATE_TRUNC(MONTH, COALESCE(shipped_local_datetime, order_local_datetime)::DATE)
              IN (SELECT DISTINCT financial_date
                  FROM reference.finance_assumption
                  WHERE meta_update_datetime > $wm_edw_reference_finance_assumption)
    UNION ALL /* previously errored rows */
    SELECT order_id
    FROM excp.fact_order
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.order_id;

/* for dropship splits, we want to make sure we are capturing the master and all the child orders tied to it.
   If a child order got picked up, we want to include the master order
   If a master order got picked up, want to include all the child orders
 */

INSERT INTO _fact_order__order_base (order_id)
SELECT DISTINCT o.master_order_id AS order_id
FROM _fact_order__order_base AS base
    JOIN stg.lkp_order_classification AS loc
        ON loc.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = loc.order_id
WHERE loc.order_status = 'DropShip Split'
    AND o.master_order_id IS NOT NULL
    AND $is_full_refresh = FALSE
    AND NOT EXISTS (
        SELECT b.order_id
        FROM _fact_order__order_base AS b
        WHERE b.order_id = o.master_order_id
    );

INSERT INTO _fact_order__order_base (order_id)
SELECT DISTINCT o.order_id
FROM _fact_order__order_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.master_order_id = base.order_id
    JOIN stg.lkp_order_classification AS oc
        ON oc.order_id = o.order_id
WHERE oc.order_status = 'DropShip Split'
    AND $is_full_refresh = FALSE
    AND NOT EXISTS (
            SELECT b.order_id
            FROM _fact_order__order_base AS b
            WHERE b.order_id = o.order_id
    );

-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', CURRENT_WAREHOUSE()) FROM _fact_order__order_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

-- capture valid stores
UPDATE _fact_order__order_base AS base
SET base.store_id = ds.store_id
FROM lake_consolidated.ultra_merchant."ORDER" AS o
    JOIN stg.dim_store AS ds
        ON ds.store_id = o.store_id
WHERE o.order_id = base.order_id
    AND o.store_id > 1
    AND ds.store_brand <> 'Legacy';

-- Prevent legacy stores from being processed
DELETE FROM _fact_order__order_base WHERE store_id IS NULL;

UPDATE _fact_order__order_base AS t
SET t.master_order_id = s.master_order_id
FROM lake_consolidated.ultra_merchant."ORDER" as s
WHERE t.order_id = s.order_id
    AND NOT $is_full_refresh
    AND s.master_order_id IS NOT NULL;

CREATE OR REPLACE TEMP TABLE _fact_order__dropship_split AS
SELECT
    base.order_id, 'child' AS master_child
FROM _fact_order__order_base AS base
    JOIN stg.lkp_order_classification AS oc
        ON oc.order_id = base.order_id
WHERE oc.order_status = 'DropShip Split'
UNION ALL
SELECT DISTINCT
    base.master_order_id, 'master' AS master_child
FROM _fact_order__order_base AS base
    JOIN stg.lkp_order_classification AS oc
        ON oc.order_id = base.order_id
WHERE oc.order_status = 'DropShip Split';

-- Capture ship dates
CREATE OR REPLACE TEMP TABLE _fact_order__ship_dates (
    order_id INT,
    ship_date TIMESTAMP_LTZ(0)
    );
INSERT INTO _fact_order__ship_dates (order_id, ship_date)
SELECT o.order_id, COALESCE(o.datetime_shipped, o.date_shipped) AS ship_date
FROM _fact_order__order_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id;
/*
fl eu retail orders are supposed to have a SALE_REDIRECT transaction type in payment_transaction_psp,
but after changing POS systems on 3/7/22,it's now Auth redirect.
We have to adjust for this until they fix this upstream.
Storing FLEU retail orders from 3/7/22 we can use when querying against payment_transaction_psp
*/
CREATE OR REPLACE TEMP TABLE _fact_order__fleu_retail AS
SELECT ob.order_id
FROM _fact_order__order_base AS ob
JOIN lake_consolidated.ultra_merchant."ORDER" AS o
    ON o.order_id = ob.order_id
JOIN stg.dim_store AS st
    ON st.store_id = o.store_id
WHERE
    st.store_type = 'Retail'
    AND st.store_region = 'EU'
    AND o.date_added >= '2021-03-07';

-- Capture new transaction dates
CREATE OR REPLACE TEMP TABLE _fact_order__transaction_dates (
    order_id INT,
    transaction_date TIMESTAMP_LTZ(3)
    );
INSERT INTO _fact_order__transaction_dates (order_id, transaction_date)
SELECT order_id, MIN(transaction_date) AS transaction_date
FROM (
    SELECT t.order_id, t.datetime_added AS transaction_date
    FROM _fact_order__order_base AS base
        JOIN lake_consolidated.ultra_merchant.payment_transaction_creditcard AS t
            ON t.order_id = base.order_id
    WHERE LOWER(t.transaction_type) IN ('prior_auth_capture', 'sale_redirect')
        AND t.statuscode = 4001
    UNION ALL
    SELECT t.order_id, t.datetime_added AS transaction_date
    FROM _fact_order__order_base AS base
        JOIN lake_consolidated.ultra_merchant.payment_transaction_psp AS t
            ON t.order_id = base.order_id
        LEFT JOIN _fact_order__fleu_retail AS fleu
            ON fleu.order_id = t.order_id
    WHERE LOWER(t.transaction_type) IN ('prior_auth_capture', 'sale_redirect',IFF(fleu.order_id IS NOT NULL, 'auth_redirect', 'sale_redirect'))
        AND t.statuscode IN (4001, 4040)
    UNION ALL
    SELECT t.order_id, t.datetime_added AS transaction_date
    FROM _fact_order__order_base AS base
        JOIN lake_consolidated.ultra_merchant.payment_transaction_cash AS t
            ON t.order_id = base.order_id
    WHERE LOWER(t.transaction_type) = 'sale'
        AND t.statuscode = 4001
    ) AS trans
GROUP BY order_id;

-- capture new ps transaction dates
CREATE OR REPLACE TEMP TABLE _fact_order__ps_transaction_dates (
    order_id INT,
    ps_completion_date TIMESTAMP_LTZ(3)
    );
INSERT INTO _fact_order__ps_transaction_dates (order_id, ps_completion_date)
SELECT order_id, MIN(ps_completion_date) AS ps_completion_date
FROM (
    SELECT b.order_id, bc.datetime_added AS ps_completion_date
    FROM _fact_order__order_base AS base
        JOIN lake_consolidated.ultra_merchant.box AS b
            ON b.order_id = base.order_id
        JOIN lake_consolidated.ultra_merchant.box_completion AS bc
            ON bc.box_id = b.box_id
    WHERE b.order_id IS NOT NULL
    ) AS box
GROUP BY order_id;

-- merge ship/transaction/checkout dates
CREATE OR REPLACE TEMP TABLE _fact_order__order_base_dates (
    order_id INT,
    ship_date TIMESTAMP_LTZ(3),
    transaction_date TIMESTAMP_LTZ(3),
    ps_completion_date TIMESTAMP_LTZ(3)
    );
INSERT INTO _fact_order__order_base_dates (
    order_id,
    ship_date,
    transaction_date,
    ps_completion_date
    )
-- ships only
SELECT
    s.order_id,
    s.ship_date,
    NULL AS transaction_date,
    NULL AS ps_completion_date
FROM _fact_order__ship_dates AS s
    LEFT JOIN _fact_order__transaction_dates AS t
        ON t.order_id = s.order_id
WHERE t.order_id IS NULL
UNION ALL
-- transactions only
SELECT
    t.order_id,
    NULL AS ship_date,
    t.transaction_date,
    NULL AS ps_completion_date
FROM _fact_order__transaction_dates AS t
    LEFT JOIN _fact_order__ship_dates AS s
        ON s.order_id = t.order_id
WHERE s.order_id IS NULL
UNION ALL
-- ships and transactions
SELECT
    s.order_id,
    s.ship_date,
    t.transaction_date,
    NULL AS ps_completion_date
FROM _fact_order__ship_dates AS s
    JOIN _fact_order__transaction_dates AS t
        ON t.order_id = s.order_id;

-- add ps transactions
UPDATE _fact_order__order_base_dates AS bd
SET bd.ps_completion_date = ps.ps_completion_date
FROM _fact_order__ps_transaction_dates AS ps
WHERE ps.order_id = bd.order_id;

-- migrated (historical) orders
CREATE OR REPLACE TEMP TABLE _fact_order__migrated_orders (order_id INT);
INSERT INTO _fact_order__migrated_orders (order_id)
SELECT oc.order_id
FROM _fact_order__order_base AS base
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = base.order_id
WHERE oc.order_type_id = 4; /* 'Historical' */

-- add in cash/non-cash
CREATE OR REPLACE TEMP TABLE _fact_order__cash_non_cash (
	order_credit_id INT,
	order_id INT,
	cash_credit_gross_of_vat_local_amount NUMBER(19, 4),
	non_cash_credit_gross_of_vat_local_amount NUMBER(19, 4),
	cash_credit_count INT,
	non_cash_credit_count INT,
	cash_membership_credit_local_amount NUMBER(19, 4),
	cash_membership_credit_count INT,
	cash_refund_credit_local_amount NUMBER(19, 4),
	cash_refund_credit_count INT
	);
INSERT INTO _fact_order__cash_non_cash (
	order_credit_id,
	order_id,
	cash_credit_gross_of_vat_local_amount,
	non_cash_credit_gross_of_vat_local_amount,
    cash_credit_count,
    non_cash_credit_count,
	cash_membership_credit_local_amount,
    cash_membership_credit_count,
	cash_refund_credit_local_amount,
    cash_refund_credit_count
	)
SELECT
	oc.order_credit_id,
	oc.order_id,
    IFF(scr.cash = 1 OR mtr.cash = 1 OR gc.order_id IS NOT NULL OR NVL(gc.gift_certificate_type_id,0) = 8, oc.amount, 0) AS cash_credit_gross_of_vat_local_amount,
    IFF(COALESCE(scr.cash, mtr.cash, 0) = 0 AND gc.order_id IS NULL AND  NVL(gc.gift_certificate_type_id,0) != 8, oc.amount, 0) AS non_cash_credit_gross_of_vat_local_amount,
    IFF(scr.cash = 1 OR mtr.cash = 1 OR gc.order_id IS NOT NULL OR  NVL(gc.gift_certificate_type_id,0) = 8,
        IFF(st.label = 'ShoeDazzle' AND LOWER(scr.label) = 'membership credit' AND sc.amount = 9.95, 0, 1), 0)
        AS cash_credit_count,
    IFF(COALESCE(scr.cash, mtr.cash, 0) = 0 AND gc.order_id IS NULL AND  NVL(gc.gift_certificate_type_id,0) != 8, 1, 0) AS non_cash_credit_count,
    IFF(LOWER(scr.label) in ('membership credit', 'converted membership token'), oc.amount, 0) AS cash_membership_credit_local_amount,
    IFF(LOWER(scr.label) in ('membership credit', 'converted membership token'),
        IFF(st.label = 'ShoeDazzle' AND LOWER(scr.label) = 'membership credit' AND sc.amount = 9.95, 0, 1), 0)
        AS cash_membership_credit_count,
    IFF(LOWER(scr.label) IN ('refund', 'refund - partial shipment')
       OR LOWER(mtr.label) IN ('refund', 'refund - converted credit'), oc.amount, 0) AS cash_refund_credit_local_amount,
    IFF(LOWER(scr.label) IN ('refund', 'refund - partial shipment')
       OR LOWER(mtr.label) IN ('refund', 'refund - converted credit'), 1, 0) AS cash_refund_credit_count
FROM _fact_order__order_base AS base
	JOIN lake_consolidated.ultra_merchant.order_credit AS oc
		ON oc.order_id = base.order_id
	JOIN lake_consolidated.ultra_merchant.store AS st
		ON st.store_id = base.store_id
    LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
		ON gc.gift_certificate_id = oc.gift_certificate_id
	LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit AS gcs
		ON gcs.gift_certificate_id = gc.gift_certificate_id
	LEFT JOIN lake_consolidated.ultra_merchant.store_credit AS sc
		ON COALESCE(oc.store_credit_id, gcs.store_credit_id) = sc.store_credit_id
	LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason AS scr
		ON scr.store_credit_reason_id = sc.store_credit_reason_id
	LEFT JOIN lake_consolidated.ultra_merchant.membership_token AS mt
	    ON mt.membership_token_id = oc.membership_token_id
	LEFT JOIN lake_consolidated.ultra_merchant.membership_token_reason AS mtr
	    ON mtr.membership_token_reason_id = mt.membership_token_reason_id
WHERE COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id) IS NOT NULL
AND oc.hvr_is_deleted = 0;

-- calculate cash and non-cash amounts
CREATE OR REPLACE TEMP TABLE _fact_order__cash_non_cash_totals (
	order_id INT,
	cash_credit_gross_of_vat_local_amount NUMBER(19, 4),
	non_cash_credit_gross_of_vat_local_amount NUMBER(19, 4),
	cash_credit_count INT,
	non_cash_credit_count INT,
	cash_membership_credit_local_amount NUMBER(19, 4),
	cash_membership_credit_count INT,
	cash_refund_credit_local_amount NUMBER(19, 4),
	cash_refund_credit_count INT
	);
INSERT INTO _fact_order__cash_non_cash_totals (
	order_id,
	cash_credit_gross_of_vat_local_amount,
	non_cash_credit_gross_of_vat_local_amount,
	cash_credit_count,
    non_cash_credit_count,
	cash_membership_credit_local_amount,
    cash_membership_credit_count,
	cash_refund_credit_local_amount,
    cash_refund_credit_count
	)
SELECT
	c.order_id,
	COALESCE(SUM(cash_credit_gross_of_vat_local_amount), 0) AS cash_credit_gross_of_vat_local_amount,
	COALESCE(SUM(non_cash_credit_gross_of_vat_local_amount), 0) AS non_cash_credit_gross_of_vat_local_amount,
    COALESCE(SUM(cash_credit_count), 0) AS cash_credit_count,
    COALESCE(SUM(non_cash_credit_count), 0) AS non_cash_credit_count,
	COALESCE(SUM(cash_membership_credit_local_amount), 0) AS cash_membership_credit_local_amount,
    COALESCE(SUM(cash_membership_credit_count), 0) AS cash_membership_credit_count,
	COALESCE(SUM(cash_refund_credit_local_amount), 0) AS cash_refund_credit_local_amount,
    COALESCE(SUM(cash_refund_credit_count), 0) AS cash_refund_credit_count
FROM _fact_order__cash_non_cash AS c
GROUP BY c.order_id;

-- add in payment transaction local amount
CREATE OR REPLACE TEMP TABLE _fact_order__payment_transaction_local_amount (
	order_id INT,
	has_successful_payment BOOLEAN,
	payment_transaction_local_amount NUMBER(19, 4),
	payment_method VARCHAR(50),
	creditcard_type VARCHAR(50),
	is_prepaid_creditcard BOOLEAN,
	is_applepay BOOLEAN,
	is_mastercard_checkout BOOLEAN,
	is_visa_checkout BOOLEAN,
    funding_type VARCHAR(25),
    prepaid_type VARCHAR(25),
    card_product_type VARCHAR(25)
	);
INSERT INTO _fact_order__payment_transaction_local_amount (
	order_id,
	has_successful_payment,
	payment_transaction_local_amount,
	payment_method,
	creditcard_type,
	is_prepaid_creditcard,
    is_applepay,
	is_mastercard_checkout,
	is_visa_checkout,
    funding_type,
    prepaid_type,
    card_product_type
	)
SELECT
	base.order_id,
	MAX(IFF(COALESCE(cc.order_id, psp.order_id, cash.order_id) IS NOT NULL, TRUE, FALSE)) AS has_successful_payment,
	SUM(COALESCE(cc.amount, psp.amount, cash.amount)) AS payment_transaction_local_amount,
	MAX(CASE
		WHEN cc.order_id IS NOT NULL THEN 'Credit Card'
		WHEN psp.order_id IS NOT NULL THEN 'PSP'
		ELSE 'Unknown' END) AS payment_method,
    MAX(CASE
		WHEN cc.order_id IS NOT NULL THEN c.card_type
		WHEN psp.order_id IS NOT NULL THEN ps.type
		ELSE 'Unknown' END) AS creditcard_type,
	MAX(IFF(cc.order_id IS NOT NULL, COALESCE(TO_BOOLEAN(ptcd.is_prepaid), FALSE), FALSE)) AS is_prepaid_creditcard,
    MAX(IFF(c.card_type ILIKE 'ap_%%', TRUE, FALSE)) AS is_applepay,
    MAX(IFF(LOWER(od.name) = 'masterpass_checkout_payment_id', TRUE, FALSE)) AS is_mastercard_checkout,
    MAX(IFF(LOWER(od.name) = 'visa_checkout_call_id', TRUE, FALSE)) AS is_visa_checkout,
    MAX(COALESCE(IFF(ASCII(ptcd.funding_type) = 0,NULL,ptcd.funding_type),'Unknown')) AS funding_type,
    MAX(COALESCE(IFF(ASCII(ptcd.prepaid_type) = 0,NULL,ptcd.prepaid_type),'Unknown')) AS prepaid_type,
    MAX(COALESCE(IFF(ASCII(ptcd.card_product_type) = 0,NULL,ptcd.card_product_type),'Unknown')) AS card_product_type
FROM _fact_order__order_base AS base
	LEFT JOIN lake_consolidated.ultra_merchant.payment_transaction_creditcard AS cc
		ON cc.order_id = base.order_id
		AND LOWER(cc.transaction_type) IN ('prior_auth_capture', 'sale_redirect')
		AND cc.statuscode = 4001 /* Success */
	LEFT JOIN lake_consolidated.ultra_merchant.creditcard AS c
		ON c.creditcard_id = cc.creditcard_id
	LEFT JOIN lake_consolidated.ultra_merchant.payment_transaction_creditcard_data AS ptcd
		ON ptcd.payment_transaction_id = cc.original_payment_transaction_id
	LEFT JOIN _fact_order__fleu_retail AS fleu
	    ON fleu.order_id = base.order_id
	LEFT JOIN lake_consolidated.ultra_merchant.payment_transaction_psp AS psp
		ON psp.order_id = base.order_id
		AND LOWER(psp.transaction_type) IN ('prior_auth_capture', 'sale_redirect', IFF(fleu.order_id IS NOT NULL, 'auth_redirect', 'sale_redirect'))
		AND psp.statuscode IN (4001, 4040) /* Success and 4040 chargeback */
	LEFT JOIN lake_consolidated.ultra_merchant.psp AS ps
		ON ps.psp_id = psp.psp_id
	LEFT JOIN lake_consolidated.ultra_merchant.payment_transaction_cash AS cash
		ON cash.order_id = base.order_id
		AND LOWER(cash.transaction_type) = 'sale'
		AND cash.statuscode = 4001 /* Success */
    LEFT JOIN (
        SELECT order_id, name
        FROM lake_consolidated.ultra_merchant.order_detail
        WHERE LOWER(name) IN ('masterpass_checkout_payment_id', 'visa_checkout_call_id')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY datetime_added DESC) = 1
        ) AS od
        ON od.order_id = base.order_id
	GROUP BY base.order_id;

-- get the creditcard type and is_prepaid for failed orders
-- for those who don't have successful payment, check for failed payment and get details
UPDATE _fact_order__payment_transaction_local_amount AS ptla
SET
    ptla.payment_method = fc.payment_method,
	ptla.creditcard_type = fc.creditcard_type,
	ptla.is_prepaid_creditcard = fc.is_prepaid_creditcard,
    ptla.is_applepay = fc.is_applepay,
	ptla.is_mastercard_checkout = fc.is_mastercard_checkout,
	ptla.is_visa_checkout = fc.is_visa_checkout,
	ptla.funding_type = fc.funding_type,
    ptla.prepaid_type = fc.prepaid_type,
    ptla.card_product_type = fc.card_product_type
FROM (
    SELECT
        la.order_id,
        CASE
            WHEN c.card_type IS NOT NULL THEN 'Credit Card'
            WHEN ps.type IS NOT NULL THEN 'PSP'
            ELSE la.payment_method END AS payment_method,
        COALESCE(c.card_type, ps.type, la.creditcard_type) AS creditcard_type,
        IFF(cc.order_id IS NOT NULL, COALESCE(TO_BOOLEAN(ptcd.is_prepaid), FALSE), FALSE) AS is_prepaid_creditcard,
        IFF(c.card_type ILIKE 'ap_%%', TRUE, FALSE) AS is_applepay,
        IFF(LOWER(od.name) = 'masterpass_checkout_payment_id', TRUE, FALSE) AS is_mastercard_checkout,
        IFF(LOWER(od.name) = 'visa_checkout_call_id', TRUE, FALSE) AS is_visa_checkout,
        COALESCE(IFF(ASCII(ptcd.funding_type) = 0,NULL,ptcd.funding_type),'Unknown') AS funding_type,
        COALESCE(IFF(ASCII(ptcd.prepaid_type) = 0,NULL,ptcd.prepaid_type),'Unknown') AS prepaid_type,
        COALESCE(IFF(ASCII(ptcd.card_product_type) = 0,NULL,ptcd.card_product_type),'Unknown') AS card_product_type
    FROM _fact_order__payment_transaction_local_amount AS la
        LEFT JOIN lake_consolidated.ultra_merchant.payment_transaction_creditcard AS cc
            ON cc.order_id = la.order_id
            AND LOWER(cc.transaction_type) = 'auth_only'
        LEFT JOIN lake_consolidated.ultra_merchant.creditcard AS c
            ON c.creditcard_id = cc.creditcard_id
        LEFT JOIN lake_consolidated.ultra_merchant.payment_transaction_creditcard_data AS ptcd
            ON ptcd.payment_transaction_id = cc.payment_transaction_id
        LEFT JOIN lake_consolidated.ultra_merchant.payment_transaction_psp AS psp
            ON psp.order_id = la.order_id
            AND LOWER(psp.transaction_type) = 'auth_only'
        LEFT JOIN lake_consolidated.ultra_merchant.psp AS ps
            ON ps.psp_id = psp.psp_id
        LEFT JOIN (
            SELECT order_id, name
            FROM lake_consolidated.ultra_merchant.order_detail
            WHERE LOWER(name) IN ('masterpass_checkout_payment_id', 'visa_checkout_call_id')
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY datetime_added DESC) = 1
            ) AS od
            ON od.order_id = la.order_id
    WHERE NOT la.has_successful_payment
    QUALIFY ROW_NUMBER() OVER (PARTITION BY la.order_id ORDER BY COALESCE(cc.datetime_modified, psp.datetime_modified) DESC) = 1
    ) AS fc
WHERE fc.order_id = ptla.order_id;

-- add in shipping cost local amount
CREATE OR REPLACE TEMP TABLE _fact_order__shipping_cost_local_amount (
	order_id INT,
	shipment_carrier VARCHAR(255),
	shipping_cost_local_amount NUMBER(19, 4)
	);
INSERT INTO _fact_order__shipping_cost_local_amount (
	order_id,
	shipment_carrier,
	shipping_cost_local_amount
	)
SELECT
	os.order_id,
	MAX(cu.label) AS shipment_carrier,
	SUM(os.rate) AS shipping_cost_local_amount
FROM _fact_order__order_base AS base
	JOIN lake_consolidated.ultra_merchant.order_shipment AS os
		ON os.order_id = base.order_id
	JOIN lake_consolidated.ultra_merchant.carrier_uw AS cu
		ON cu.carrier_id = os.carrier_id
WHERE os.statuscode = 4510
GROUP BY os.order_id;

-- get credit billing retry order
CREATE OR REPLACE TEMP TABLE _fact_order__credit_billing_retry_order (
	order_id INT,
	is_credit_billing_on_retry BOOLEAN
	);
INSERT INTO _fact_order__credit_billing_retry_order (
	order_id,
	is_credit_billing_on_retry
	)
SELECT DISTINCT
	base.order_id,
    IFF(brsq.order_id IS NOT NULL, TRUE, FALSE) AS is_credit_billing_on_retry
FROM _fact_order__order_base AS base
	LEFT JOIN lake_consolidated.ultra_merchant.billing_retry_schedule_queue AS brsq
		ON brsq.order_id = base.order_id AND statuscode NOT IN (4209);

-- get units quantity
CREATE OR REPLACE TEMP TABLE _fact_order_units_quantity (
    order_id INT,
    master_order_id INT,
    unit_count INT,
    loyalty_unit_count INT,
    third_party_unit_count INT
    );
INSERT INTO _fact_order_units_quantity (
    order_id,
    master_order_id,
    unit_count,
    loyalty_unit_count,
    third_party_unit_count
    )
SELECT
    ol.order_id,
    base.master_order_id,
    SUM(ol.quantity) AS unit_count,
    SUM(CASE WHEN pt.label = 'Membership Reward Points Item' THEN 1 ELSE 0 END) AS loyalty_unit_count,
    SUM(IFF(oc.order_id IS NOT NULL, ol.quantity, 0)) AS third_party_unit_count
FROM _fact_order__order_base AS base
    JOIN lake_consolidated.ultra_merchant.order_line AS ol
        ON ol.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.statuscode AS sc
        ON sc.statuscode = ol.statuscode
    JOIN lake_consolidated.ultra_merchant.statuscode_category AS scc
        ON sc.statuscode BETWEEN scc.range_start AND scc.range_end
    JOIN lake_consolidated.ultra_merchant.product_type AS pt
        ON pt.product_type_id = ol.product_type_id
    LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = base.order_id
        AND oc.order_type_id = 51
WHERE LOWER(scc.label) = 'orderline codes'
    AND LOWER(pt.label) NOT IN ('bundle', 'box fee', 'customization')
    AND (NOT COALESCE(TO_BOOLEAN(pt.is_free), FALSE) OR LOWER(pt.label) = 'membership reward points item')
    AND ol.statuscode <> 2830
GROUP BY ol.order_id, base.master_order_id;

/* assigning third_party_unit count to the dropship split master order */
UPDATE _fact_order_units_quantity AS t
SET t.third_party_unit_count = s.third_party_unit_count
FROM (
        SELECT fouq.master_order_id AS order_id,
               SUM(fouq.third_party_unit_count) AS third_party_unit_count
        FROM _fact_order__dropship_split AS ds
            JOIN _fact_order_units_quantity AS fouq
                ON fouq.order_id = ds.order_id
        WHERE ds.master_child = 'child'
            AND fouq.master_order_id IS NOT NULL
        GROUP BY fouq.master_order_id
     ) AS s
WHERE s.order_id = t.order_id;

-- get cash/credit amounts
CREATE OR REPLACE TEMP TABLE _fact_order__cash_credit_amounts (
    order_id INT,
    subtotal_excl_tariff_amount NUMBER(19, 4),
    discount_amount NUMBER(19, 4),
    product_discount_amount NUMBER(19, 4),
    shipping_discount_amount NUMBER(19, 4),
    tax_amount NUMBER(19, 4),
    tax_cash_amount NUMBER(19, 4),
    tax_credit_amount NUMBER(19, 4),
    shipping_revenue NUMBER(19, 4),
    shipping_revenue_cash_amount NUMBER(19, 4),
    shipping_revenue_credit_amount NUMBER(19, 4),
    tariff_amount NUMBER(19, 4)
    );
INSERT INTO _fact_order__cash_credit_amounts (
    order_id,
    subtotal_excl_tariff_amount,
    discount_amount,
    product_discount_amount,
    shipping_discount_amount,
    tax_amount,
    tax_cash_amount,
    tax_credit_amount,
    shipping_revenue,
    shipping_revenue_cash_amount,
    shipping_revenue_credit_amount,
    tariff_amount
    )
WITH _order_credit AS (
        SELECT
            oc.order_id,
            SUM(IFNULL(oc.tax,0)) AS tax_credit,
            SUM(IFNULL(oc.shipping,0)) AS shipping_credit
        FROM _fact_order__order_base AS base
            JOIN lake_consolidated.ultra_merchant.order_credit AS oc
                ON oc.order_id = base.order_id
        WHERE oc.hvr_is_deleted = 0
            AND base.order_id NOT IN (SELECT order_id FROM _fact_order__dropship_split WHERE master_child = 'master')
        GROUP BY oc.order_id
        UNION ALL
        SELECT
            base.master_order_id AS order_id,
            SUM(IFNULL(oc.tax,0)) AS tax_credit,
            SUM(IFNULL(oc.shipping,0)) AS shipping_credit
        FROM _fact_order__order_base AS base
            JOIN lake_consolidated.ultra_merchant.order_credit AS oc
                ON oc.order_id = base.order_id
        WHERE oc.hvr_is_deleted = 0
            AND base.master_order_id IS NOT NULL
            AND base.order_id IN (SELECT order_id FROM _fact_order__dropship_split WHERE master_child = 'child')
        GROUP BY base.master_order_id
)
SELECT
	o.order_id,
	IFF(o.order_source_id = 9, COALESCE(ol.subtotal, 0), COALESCE(o.subtotal, 0)) - COALESCE(os.tariff_amount, 0) AS subtotal_excl_tariff_amount,
    COALESCE(o.discount, 0) - COALESCE(od.shipping_discount, 0) AS discount_amount,
    IFF(o.order_source_id = 9, COALESCE(ol.discount, 0), COALESCE(od.product_discount, 0)) AS product_discount_amount,
    COALESCE(od.shipping_discount, 0) AS shipping_discount_amount,
    COALESCE(o.tax, 0) AS tax_amount,
	COALESCE(o.tax, 0) - COALESCE(oc.tax_credit, 0) AS tax_cash_amount,
	COALESCE(oc.tax_credit, 0) AS tax_credit_amount,
	COALESCE(o.shipping, 0) AS shipping_revenue,
	COALESCE(o.shipping, 0) - COALESCE(od.shipping_discount, 0) - COALESCE(oc.shipping_credit, 0)
	    AS shipping_revenue_cash_amount,
	COALESCE(oc.shipping_credit, 0) AS shipping_revenue_credit_amount,
    COALESCE(os.tariff_amount, 0) AS tariff_amount
FROM _fact_order__order_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    LEFT JOIN _order_credit AS oc
        ON oc.order_id = o.order_id
    LEFT JOIN (
        SELECT
            od.order_id,
            SUM(IFF(LOWER(od.applied_to) = 'subtotal', od.amount, 0)) AS product_discount,
            SUM(IFF(LOWER(od.applied_to) = 'shipping', od.amount, 0)) AS shipping_discount
        FROM _fact_order__order_base AS base
            JOIN lake_consolidated.ultra_merchant.order_discount AS od
                ON od.order_id = base.order_id
        WHERE LOWER(od.applied_to) IN ('subtotal', 'shipping')
        GROUP BY od.order_id
        ) AS od
        ON od.order_id = o.order_id
    LEFT JOIN (
        SELECT
            os.order_id,
            SUM(os.surcharge_amount_rounded) AS tariff_amount
        FROM _fact_order__order_base AS base
            JOIN lake_consolidated.ultra_merchant.order_surcharge AS os
                ON os.order_id = base.order_id
        WHERE LOWER(os.type) = 'tariff'
        GROUP BY os.order_id
        ) AS os
        ON os.order_id = o.order_id
    LEFT JOIN (
        SELECT
            oline.order_id,
            SUM(oline.normal_unit_price) AS subtotal,
            SUM(oline.markdown_discount) AS discount
        FROM _fact_order__order_base AS base
            JOIN lake_consolidated.ultra_merchant."ORDER" AS o
                ON o.order_id = base.order_id
                AND o.order_source_id = 9
            JOIN lake_consolidated.ultra_merchant.order_line AS oline
                ON oline.order_id = base.order_id
                AND oline.statuscode != 2830
        GROUP BY oline.order_id
        ) AS ol
        ON ol.order_id = o.order_id;

CREATE OR REPLACE TEMP TABLE _fact_order__sales_channel AS
SELECT
    base.order_id,
    COALESCE(dosc.order_sales_channel_key, -1) AS order_sales_channel_key,
    oc.order_sales_channel,
    oc.order_classification_name,
    oc.is_border_free_order,
    oc.is_ps_order,
    oc.is_retail_ship_only_order,
    oc.is_test_order,
    oc.is_preorder,
    oc.is_product_seeding_order,
    oc.is_bops_order,
    oc.is_amazon_order,
    oc.is_bill_me_now_online,
    oc.is_bill_me_now_gms,
    oc.is_membership_gift,
    oc.is_dropship,
    oc.is_warehouse_outlet_order,
    oc.is_third_party,
    oc.order_local_datetime
FROM _fact_order__order_base AS base
    JOIN stg.lkp_order_classification AS oc
        ON oc.order_id = base.order_id
    LEFT JOIN stg.dim_order_sales_channel AS dosc
        ON dosc.is_current = TRUE
        AND dosc.order_sales_channel_l2 = oc.order_sales_channel
        AND dosc.order_classification_l2 = oc.order_classification_name
        AND dosc.is_border_free_order = oc.is_border_free_order
        AND dosc.is_ps_order = oc.is_ps_order
        AND dosc.is_retail_ship_only_order = oc.is_retail_ship_only_order
        AND dosc.is_test_order = oc.is_test_order
        AND dosc.is_preorder = oc.is_preorder
        AND dosc.is_product_seeding_order = oc.is_product_seeding_order
        AND dosc.is_bops_order = oc.is_bops_order
        AND dosc.is_custom_order = oc.is_custom_order
        AND dosc.is_discreet_packaging = oc.is_discreet_packaging
        AND dosc.is_bill_me_now_online = oc.is_bill_me_now_online
        AND dosc.is_bill_me_now_gms = oc.is_bill_me_now_gms
        AND dosc.is_membership_gift = oc.is_membership_gift
        AND dosc.is_dropship = oc.is_dropship
        AND dosc.is_warehouse_outlet_order = oc.is_warehouse_outlet_order
        AND dosc.is_third_party = oc.is_third_party
        AND oc.order_local_datetime
		    BETWEEN dosc.effective_start_datetime and dosc.effective_end_datetime;

-- get giftco data
CREATE OR REPLACE TEMP TABLE _fact_order__giftco (
    order_id INT,
    giftco_redemption_amount NUMBER(19, 4),
    giftco_redemption_count NUMBER(38, 0)
    );
INSERT INTO _fact_order__giftco (
    order_id,
    giftco_redemption_amount,
    giftco_redemption_count
    )
SELECT
	gco.order_id,
    SUM(gco.amount) AS giftco_redemption_amount,
	COUNT(1) AS giftco_redemption_count
FROM (
    SELECT oc.order_id, oc.amount
    FROM _fact_order__order_base AS base
        JOIN lake_consolidated.ultra_merchant.order_credit AS oc
            ON oc.order_id = base.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
            ON gc.gift_certificate_id = oc.gift_certificate_id
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit AS gcs
            ON gcs.gift_certificate_id = gc.gift_certificate_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit AS sc
            ON sc.store_credit_id = COALESCE(oc.store_credit_id, gcs.store_credit_id)
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason AS scr
            ON scr.store_credit_reason_id = sc.store_credit_reason_id
    WHERE scr.label = 'Gift Card Redemption (Expired Credit)'
    ) AS gco
GROUP BY gco.order_id;

-- get giftcard data
CREATE OR REPLACE TEMP TABLE _fact_order__giftcard_nonagg (
    order_id INT,
    order_credit_id INT,
    amount NUMBER(19, 4)
    );
INSERT INTO _fact_order__giftcard_nonagg (order_id, order_credit_id, amount)
SELECT DISTINCT gc.order_id, gc.order_credit_id, gc.amount
FROM (
    SELECT oc.order_id, oc.order_credit_id, oc.amount
	FROM _fact_order__order_base AS base
        JOIN lake_consolidated.ultra_merchant.order_credit AS oc
            ON oc.order_id = base.order_id
	    LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit AS gcs
            ON gcs.gift_certificate_id = oc.gift_certificate_id
	    LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
	        ON gc.gift_certificate_id = oc.gift_certificate_id
	        AND gc.gift_certificate_type_id = 9 --single use compensation
	WHERE oc.gift_certificate_id IS NOT NULL
	    AND gcs.gift_certificate_id IS NULL
	    AND gc.gift_certificate_id IS NULL
	UNION ALL
	SELECT oc.order_id, oc.order_credit_id, oc.amount
	FROM _fact_order__order_base AS base
        JOIN lake_consolidated.ultra_merchant.order_credit AS oc
            ON oc.order_id = base.order_id
        JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit AS gcs
			ON gcs.store_credit_id = oc.store_credit_id
	    JOIN lake_consolidated.ultra_merchant.store_credit AS sc
            ON sc.store_credit_id = oc.store_credit_id
	    JOIN lake_consolidated.ultra_merchant.store_credit_reason AS scr
	        ON scr.store_credit_reason_id = sc.store_credit_reason_id
	WHERE scr.cash = 1
	UNION ALL
	SELECT oc.order_id, oc.order_credit_id, oc.amount
	FROM _fact_order__order_base AS base
        JOIN lake_consolidated.ultra_merchant.order_credit AS oc
            ON oc.order_id = base.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
            ON gc.gift_certificate_id = oc.gift_certificate_id
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit AS gcs
            ON gcs.gift_certificate_id = gc.gift_certificate_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit AS sc
            ON sc.store_credit_id = COALESCE(oc.store_credit_id, gcs.store_credit_id)
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason AS scr
            ON scr.store_credit_reason_id = sc.store_credit_reason_id
    WHERE scr.label = 'Gift Card Redemption (Gift Certificate)'
    ) AS gc;

CREATE OR REPLACE TEMP TABLE _fact_order__giftcard AS
SELECT
    order_id,
	SUM(amount) AS giftcard_redemption_amount,
	COUNT(1) AS giftcard_redemption_count
FROM _fact_order__giftcard_nonagg
GROUP BY order_id;

-- get token data
CREATE OR REPLACE TEMP TABLE _fact_order__token (
    order_id INT,
    token_count NUMBER(38, 0),
    token_amount NUMBER(19, 4),
    cash_token_count NUMBER(38, 0),
    cash_token_amount NUMBER(19, 4),
    non_cash_token_count NUMBER(38, 0),
    non_cash_token_amount NUMBER(19, 4)
    );
INSERT INTO _fact_order__token
(
    order_id,
    token_count,
    token_amount,
    cash_token_count,
    cash_token_amount,
    non_cash_token_count,
    non_cash_token_amount
)
SELECT
    oc.order_id,
    COUNT(oc.membership_token_id) AS token_count,
    SUM(NVL(oc.amount, 0)) AS token_amount,
    COUNT(IFF(mtr.cash = 1, oc.membership_token_id, NULL)) AS cash_token_count,
    SUM(IFF(mtr.cash = 1, NVL(oc.amount, 0), 0)) AS cash_token_amount,
    COUNT(IFF(mtr.cash = 0, oc.membership_token_id, NULL)) AS noncash_token_count,
    SUM(IFF(mtr.cash = 0, NVL(oc.amount, 0), 0)) AS noncash_token_amount
FROM _fact_order__order_base base
JOIN lake_consolidated.ultra_merchant.order_credit oc
    ON base.order_id = oc.order_id
JOIN lake_consolidated.ultra_merchant.membership_token mt
    ON mt.membership_token_id = oc.membership_token_id
JOIN lake_consolidated.ultra_merchant.membership_token_reason mtr
    ON mtr.membership_token_reason_id = mt.membership_token_reason_id
WHERE oc.hvr_is_deleted = 0
GROUP BY oc.order_id;

-- get retail store id
CREATE OR REPLACE TEMP TABLE _fact_order__retail_store AS
SELECT
	base.order_id,
    TRY_TO_NUMBER(od.value) AS retail_store_id
FROM _fact_order__order_base AS base
	JOIN lake_consolidated.ultra_merchant.order_detail AS od
        ON od.order_id = base.order_id
        AND od.name = 'retail_store_id'
WHERE TRY_TO_NUMBER(od.value) IS NOT NULL AND TRY_TO_NUMBER(od.value) <> 0;

/* get BOPS original store_id. We don't want it to tie to retail and want to give online the credit.
For bops_store_id we want to populate split childs shipped to customer */
CREATE OR REPLACE TEMP TABLE _fact_order__bops_store AS
WITH cte AS (
    SELECT sc.order_id, o.store_id
    FROM _fact_order__sales_channel AS sc
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = sc.order_id
    WHERE sc.is_bops_order = TRUE
    UNION /* This UNION captures child order_ids in split BOPS that were shipped to customer */
    SELECT base.order_id, o.store_id
    FROM _fact_order__order_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = o.master_order_id
    WHERE oc.order_type_id = 40
)
SELECT
    cte.order_id,
    TRY_TO_NUMBER(od.value) AS bops_original_store_id,
    cte.store_id AS bops_store_id
FROM cte
LEFT JOIN lake_consolidated.ultra_merchant.order_detail AS od
    ON od.order_id = cte.order_id
    AND od.name = 'original_store_id'
    AND TRY_TO_NUMBER(od.value) IS NOT NULL;

--DA-30463 Tying FL group orders with store_id 52 to 315
CREATE OR REPLACE TEMP TABLE _fact_order__group_order_store AS
SELECT base.order_id, grp_store.store_id
FROM _fact_order__order_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    JOIN stg.dim_customer AS dc
        ON dc.customer_id = o.customer_id
    JOIN stg.dim_store AS st
        ON st.store_id = o.store_id
    JOIN stg.dim_store AS grp_store
        ON grp_store.store_brand = st.store_brand
        AND grp_store.store_country = st.store_country
        AND grp_store.store_type = 'Group Order'
WHERE
    st.store_type = 'Online'
    AND dc.email ilike 'grouporders%'
    AND (o.subtotal - o.discount) > 100;

 /*
 mapping the correct store_id with the Yitty launch
 Online VIP orders goes to the brand of VIP
 Retail orders goes to Retail
 Guest orders goes to the site where customer checked out
 Yitty VIPS shopping on FL mobile, will go to YTY mobile
  */

CREATE OR REPLACE TEMP TABLE _fact_order__brand_store_id_mapping AS
SELECT
    ob.order_id,
    CASE
        WHEN o.membership_brand_id IS NULL THEN o.store_id
        WHEN st.store_type = 'Retail' THEN o.store_id
        WHEN loc.is_guest = TRUE THEN o.store_id
        WHEN st.is_core_store = FALSE THEN o.store_id
        WHEN lower(st.store_brand) = lower(mbst.store_brand) THEN o.store_id
        WHEN o.membership_brand_id = 2 AND st.store_type = 'Mobile App' THEN 24101
        WHEN o.membership_brand_id IS NOT NULL THEN mpmb.store_id
    ELSE o.store_id
    END AS store_id
FROM _fact_order__order_base AS ob
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = ob.order_id
    JOIN stg.dim_store AS st
        ON st.store_id = o.store_id
    JOIN stg.lkp_order_classification AS loc
        ON loc.order_id = ob.order_id
    LEFT JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmb
        ON mpmb.membership_brand_id = o.membership_brand_id
    LEFT JOIN stg.dim_store AS mbst
        ON mbst.store_id = mpmb.store_id;

CREATE OR REPLACE TEMP TABLE _fact_order__delivery_fee AS
SELECT
    ob.order_id,
    SUM(ot.amount) AS delivery_fee_local_amount
FROM _fact_order__order_base AS ob
    JOIN lake_consolidated.ultra_merchant.order_tax AS ot
        ON ot.order_id = ob.order_id
WHERE ot.type = 'delivery'
GROUP BY ob.order_id;

CREATE OR REPLACE TEMP TABLE _fact_order__order_tracking_detail AS
SELECT
       o.order_tracking_id,
       otd.object_id
FROM _fact_order__order_base AS base
JOIN lake_consolidated.ultra_merchant."ORDER" AS o
    ON o.order_id = base.order_id
JOIN lake_consolidated.ultra_merchant.order_tracking_detail otd
    ON otd.order_tracking_id = o.order_tracking_id
    AND otd.object ='administrator'
QUALIFY ROW_NUMBER() OVER (PARTITION BY otd.order_tracking_id ORDER BY otd.datetime_modified DESC) = 1;

---------------------------------
-- load initial staging tables --
---------------------------------

CREATE OR REPLACE TEMP TABLE _fact_order__stg_order_base (
	order_id INT,
    meta_original_order_id INT,
	session_id INT,
	customer_id INT,
	store_id INT,
	cart_store_id INT,
	master_order_id INT,
	order_hq_datetime_ltz TIMESTAMP_LTZ(3),
	payment_transaction_hq_datetime_ltz TIMESTAMP_LTZ(3),
	shipped_hq_datetime_ltz TIMESTAMP_LTZ(3),
	ps_completion_hq_datetime_ltz TIMESTAMP_LTZ(3),
	unit_count INT,
	loyalty_unit_count INT,
    third_party_unit_count INT,
	order_sales_channel_key NUMBER(30, 0),
    order_payment_status_code INT,
	order_processing_status_code INT,
	iso_currency_code VARCHAR(10),
	order_classification_name VARCHAR(50),
	order_sales_channel VARCHAR(50),
	is_cash BOOLEAN,
	ship_to_country VARCHAR(20),
	shipping_address_id INT,
	billing_address_id INT,
	order_payment_method VARCHAR(50),
	is_credit_billing_on_retry BOOLEAN,
	is_deleted BOOLEAN,
	administrator_id INT,
	bops_store_id INT,
	bops_pickup_datetime_ltz TIMESTAMP_LTZ(3),
	membership_brand_id INT,
    placed_datetime_ltz TIMESTAMP_LTZ(3)
	);
INSERT INTO _fact_order__stg_order_base (
    order_id,
    meta_original_order_id,
	session_id,
	customer_id,
    store_id,
    cart_store_id,
	master_order_id,
	order_hq_datetime_ltz,
	payment_transaction_hq_datetime_ltz,
	shipped_hq_datetime_ltz,
	ps_completion_hq_datetime_ltz,
	unit_count,
	loyalty_unit_count,
    third_party_unit_count,
	order_sales_channel_key,
    order_payment_status_code,
	order_processing_status_code,
	iso_currency_code,
	order_classification_name,
    order_sales_channel,
    is_cash,
	ship_to_country,
	shipping_address_id,
	billing_address_id,
	order_payment_method,
	is_credit_billing_on_retry,
    is_deleted,
    administrator_id,
    bops_store_id,
    bops_pickup_datetime_ltz,
    membership_brand_id,
    placed_datetime_ltz
)
SELECT
	o.order_id,
	o.meta_original_order_id,
	o.session_id,
	o.customer_id,
    COALESCE(gos.store_id, bops.bops_original_store_id, rs.retail_store_id, rso.store_id, bsm.store_id, o.store_id) as store_id,
    COALESCE(gos.store_id, bops.bops_original_store_id, rs.retail_store_id, rso.store_id, o.store_id) as cart_store_id,
	o.master_order_id,
	IFF(mo.order_id IS NOT NULL, o.date_placed, o.datetime_added) AS order_hq_datetime_ltz,
	bd.transaction_date AS payment_transaction_hq_datetime_ltz,
	bd.ship_date AS shipped_hq_datetime_ltz,
	bd.ps_completion_date AS ps_completion_hq_datetime_ltz,
	COALESCE(uq.unit_count, 0) AS unit_count,
	COALESCE(uq.loyalty_unit_count, 0) AS loyalty_unit_count,
	COALESCE(uq.third_party_unit_count, 0) AS third_party_unit_count,
	COALESCE(sc.order_sales_channel_key, -1) AS order_sales_channel_key,
    o.payment_statuscode AS order_payment_status_code,
	o.processing_statuscode AS order_processing_status_code,
	COALESCE(rsc.currency_code, 'Unknown') AS iso_currency_code,
	sc.order_classification_name,
    sc.order_sales_channel,
    IFF((o.subtotal - o.discount - o.credit) <= 0, FALSE, TRUE) AS is_cash,
	COALESCE(REPLACE(UPPER(a.country_code), 'UK', 'GB'), 'Unknown') AS ship_to_country,
	COALESCE(o.shipping_address_id, -1) AS shipping_address_id,
	COALESCE(o.billing_address_id, -1) AS billing_address_id,
	o.payment_method AS order_payment_method,
	brsq.is_credit_billing_on_retry,
    IFF(o.hvr_is_deleted = 1 OR sc.is_amazon_order = TRUE OR sc.is_dropship = TRUE, TRUE, FALSE) AS is_deleted,
    COALESCE(otd.object_id, -1) as administrator_id,
    COALESCE(bops.bops_store_id, -1) AS bops_store_id,
    IFF(sc.is_bops_order = TRUE AND o.processing_statuscode = 2100 AND sc.order_classification_name <> 'Exchange', o.datetime_processing_modified, NULL) AS bops_pickup_datetime_ltz,
    o.membership_brand_id,
    o.date_placed AS placed_datetime_ltz
FROM _fact_order__order_base AS base
	JOIN _fact_order__credit_billing_retry_order AS brsq
		ON brsq.order_id = base.order_id
	LEFT JOIN _fact_order__sales_channel AS sc
        ON sc.order_id = base.order_id
	LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
		ON o.order_id = base.order_id
    LEFT JOIN _fact_order__order_tracking_detail otd
        ON otd.order_tracking_id = o.order_tracking_id
    LEFT JOIN _fact_order__retail_store AS rs
        ON rs.order_id = base.order_id
    LEFT JOIN _fact_order__bops_store AS bops
        ON bops.order_id = base.order_id
    LEFT JOIN reference.historical_retail_ship_only_vip_store_id AS rso
        ON rso.order_id = base.order_id
	LEFT JOIN _fact_order__migrated_orders AS mo
		ON mo.order_id = o.order_id
	LEFT JOIN reference.store_currency AS rsc
        ON rsc.store_id = o.store_id
		AND IFF(mo.order_id IS NOT NULL, o.date_placed, o.datetime_added)
		    BETWEEN rsc.effective_start_date AND rsc.effective_end_date
    LEFT JOIN lake_consolidated.ultra_merchant.address AS a
		ON a.address_id = o.shipping_address_id
	LEFT JOIN _fact_order_units_quantity AS uq
        ON uq.order_id = base.order_id
    LEFT JOIN _fact_order__order_base_dates AS bd
        ON bd.order_id = base.order_id
    LEFT JOIN _fact_order__brand_store_id_mapping AS bsm
        ON bsm.order_id = base.order_id
    LEFT JOIN _fact_order__group_order_store AS gos
        ON gos.order_id = base.order_id;
-- SELECT * FROM _fact_order__stg_order_base;


/* For Dropship Split Orders - setting the first shipped and payment transaction datetimes amongst the child orders to the master order */
UPDATE _fact_order__stg_order_base AS t
SET t.shipped_hq_datetime_ltz = s.shipped_hq_datetime_ltz,
    t.payment_transaction_hq_datetime_ltz = s.payment_transaction_hq_datetime_ltz
FROM (SELECT base.master_order_id                     AS order_id,
             MIN(shipped_hq_datetime_ltz)             AS shipped_hq_datetime_ltz,
             MIN(payment_transaction_hq_datetime_ltz) AS payment_transaction_hq_datetime_ltz
      FROM _fact_order__stg_order_base AS base
               JOIN _fact_order__dropship_split AS ds
                    ON ds.order_id = base.order_id
      WHERE ds.master_child = 'child'
            AND base.master_order_id IS NOT NULL
      GROUP BY base.master_order_id
      ) AS s
WHERE t.order_id = s.order_id;

/* assigning order processing statuscode to the master of dropship split orders based on ranking */
UPDATE _fact_order__stg_order_base AS t
SET t.order_processing_status_code = s.order_processing_status_code
FROM (SELECT base.master_order_id AS order_id,
             base.order_processing_status_code
      FROM _fact_order__stg_order_base AS base
               JOIN _fact_order__dropship_split AS ds
                    ON ds.order_id = base.order_id
               JOIN reference.order_processing_statuscode_rank AS opsr
                    ON opsr.processing_statuscode = base.order_processing_status_code
      WHERE ds.master_child = 'child'
            AND base.master_order_id IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY base.master_order_id ORDER BY opsr.statuscode_rank) = 1
      ) AS s
WHERE t.order_id = s.order_id;

/* assigning order payment statuscode to the master of dropship split orders based on ranking */
UPDATE _fact_order__stg_order_base AS t
SET t.order_payment_status_code = s.order_payment_status_code
FROM (SELECT base.master_order_id AS order_id,
             base.order_payment_status_code
      FROM _fact_order__stg_order_base AS base
               JOIN _fact_order__dropship_split AS ds
                    ON ds.order_id = base.order_id
               JOIN reference.order_payment_statuscode_rank AS opsr
                    ON opsr.payment_statuscode = base.order_payment_status_code
      WHERE ds.master_child = 'child'
            AND base.master_order_id IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY base.master_order_id ORDER BY opsr.statuscode_rank) = 1
      ) AS s
WHERE t.order_id = s.order_id;

/* For gift orders, we want the customer_id to remain as the sender and not be overwritten with the recipient */
UPDATE _fact_order__stg_order_base AS ob
SET ob.customer_id = gft.sender_customer_id
FROM (
        SELECT ob.order_id, gfto.sender_customer_id
        FROM _fact_order__stg_order_base AS ob
            JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
                ON gfto.order_id = ob.order_id
    ) AS gft
WHERE gft.order_id = ob.order_id
    AND NOT EQUAL_NULL(gft.sender_customer_id, ob.customer_id);

/* setting gift order exchange and reship orders to be sender customer_id */
UPDATE _fact_order__stg_order_base as ob
SET ob.customer_id = er.sender_customer_id
FROM (
        SELECT e.exchange_order_id AS order_id, gfto.sender_customer_id
        FROM _fact_order__stg_order_base AS ob
            JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
                ON gfto.order_id = ob.order_id
            JOIN lake_consolidated.ultra_merchant.exchange AS e
                ON e.original_order_id = gfto.order_id
        UNION
        SELECT r.reship_order_id AS order_id, gfto.sender_customer_id
        FROM _fact_order__stg_order_base AS ob
            JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
                ON gfto.order_id = ob.order_id
            JOIN lake_consolidated.ultra_merchant.reship AS r
                ON r.original_order_id = gfto.order_id
    ) AS er
WHERE er.order_id = ob.order_id
    AND NOT EQUAL_NULL(ob.customer_id, er.sender_customer_id);

CREATE OR REPLACE TEMP TABLE _fact_order__endowment_amounts AS
SELECT fo.order_id,
       SUM(IFF(be.bounceback_type_id = 1, oc.amount, 0)) AS bounceback_endowment_amount,
       SUM(IFF(be.bounceback_type_id = 2, oc.amount, 0)) AS vip_endowment_amount
FROM _fact_order__order_base fo
    JOIN lake_consolidated.ultra_merchant.order_credit oc
        ON oc.order_id = fo.order_id
    JOIN lake_consolidated.ultra_merchant.gift_certificate gc
        ON gc.gift_certificate_id = oc.gift_certificate_id
    JOIN lake_consolidated.ultra_merchant.bounceback_endowment be
        ON be.object_id = gc.gift_certificate_id
WHERE gc.gift_certificate_type_id = 9
    AND oc.hvr_is_deleted = 0
GROUP BY fo.order_id;

CREATE OR REPLACE TEMP TABLE _fact_order__stg_order_amounts (
	order_id INT,
    master_order_id INT,
	effective_vat_rate NUMBER(18, 6),
	payment_transaction_local_amount NUMBER(19, 4),
	subtotal_excl_tariff_local_amount NUMBER(19, 4),
	product_discount_local_amount NUMBER(19, 4),
    shipping_discount_local_amount NUMBER(19, 4),
    cash_credit_local_amount NUMBER(19, 4),
	non_cash_credit_local_amount NUMBER(19, 4),
	cash_credit_count INT,
	non_cash_credit_count INT,
	cash_membership_credit_local_amount NUMBER(19, 4),
    cash_membership_credit_count INT,
	cash_refund_credit_local_amount NUMBER(19, 4),
    cash_refund_credit_count INT,
	tax_local_amount NUMBER(19, 4),
	tax_cash_local_amount NUMBER(19, 4),
	tax_credit_local_amount NUMBER(19, 4),
	cash_giftco_credit_local_amount NUMBER(19, 4),
	cash_giftco_credit_count NUMBER(38, 0),
	cash_giftcard_credit_local_amount NUMBER(19, 4),
	cash_giftcard_credit_count NUMBER(38, 0),
	token_count NUMBER(38, 0),
    token_local_amount NUMBER(19, 4),
    cash_token_count NUMBER(38, 0),
    cash_token_local_amount NUMBER(19, 4),
    non_cash_token_count NUMBER(38, 0),
    non_cash_token_local_amount NUMBER(19, 4),
	shipping_revenue_before_discount_local_amount NUMBER(19, 4),
	shipping_revenue_cash_local_amount NUMBER(19, 4),
    shipping_revenue_credit_local_amount NUMBER(19, 4),
    tariff_revenue_local_amount NUMBER(19, 4),
	shipping_cost_local_amount NUMBER(19, 4),
    delivery_fee_local_amount NUMBER(19, 4),
    bounceback_endowment_local_amount NUMBER(19, 4),
    vip_endowment_local_amount NUMBER(19, 4)
	);
INSERT INTO _fact_order__stg_order_amounts (
	order_id,
    master_order_id,
	effective_vat_rate,
	payment_transaction_local_amount,
	subtotal_excl_tariff_local_amount,
	product_discount_local_amount,
    shipping_discount_local_amount,
    cash_credit_local_amount,
	non_cash_credit_local_amount,
	cash_credit_count,
    non_cash_credit_count,
	cash_membership_credit_local_amount,
    cash_membership_credit_count,
	cash_refund_credit_local_amount,
    cash_refund_credit_count,
    tax_local_amount,
    tax_cash_local_amount,
    tax_credit_local_amount,
	cash_giftco_credit_local_amount,
	cash_giftco_credit_count,
	cash_giftcard_credit_local_amount,
	cash_giftcard_credit_count,
    token_count,
    token_local_amount,
    cash_token_count,
    cash_token_local_amount,
    non_cash_token_count,
    non_cash_token_local_amount,
	shipping_revenue_before_discount_local_amount,
	shipping_revenue_cash_local_amount,
    shipping_revenue_credit_local_amount,
    tariff_revenue_local_amount,
	shipping_cost_local_amount,
    delivery_fee_local_amount,
    bounceback_endowment_local_amount,
    vip_endowment_local_amount
	)
SELECT
	base.order_id,
	base.master_order_id,
	COALESCE(vrh.rate, 0) AS effective_vat_rate,
	COALESCE(ptla.payment_transaction_local_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS payment_transaction_local_amount,
	COALESCE(cca.subtotal_excl_tariff_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS subtotal_excl_tariff_local_amount,
    COALESCE(cca.product_discount_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS product_discount_local_amount,
	COALESCE(cca.shipping_discount_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS shipping_discount_local_amount,
    COALESCE(cnct.cash_credit_gross_of_vat_local_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS cash_credit_local_amount,
	COALESCE(cnct.non_cash_credit_gross_of_vat_local_amount, 0) / (1 + COALESCE(vrh.rate, 0))
	    AS non_cash_credit_local_amount,
	COALESCE(cnct.cash_credit_count, 0) AS cash_credit_count,
    COALESCE(cnct.non_cash_credit_count, 0) AS non_cash_credit_count,
	COALESCE(cnct.cash_membership_credit_local_amount, 0) / (1 + COALESCE(vrh.rate, 0))
	    AS cash_membership_credit_local_amount,
    COALESCE(cnct.cash_membership_credit_count, 0) AS cash_membership_credit_count,
	COALESCE(cnct.cash_refund_credit_local_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS cash_refund_credit_local_amount,
    COALESCE(cnct.cash_refund_credit_count, 0) AS cash_refund_credit_count,
    COALESCE(cca.tax_amount, 0) AS tax_local_amount,
	COALESCE(cca.tax_cash_amount, 0) AS tax_cash_local_amount,
    COALESCE(cca.tax_credit_amount, 0) AS tax_credit_local_amount,
	COALESCE(gco.giftco_redemption_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS cash_giftco_credit_local_amount,
	COALESCE(gco.giftco_redemption_count, 0) AS cash_giftco_credit_count,
	COALESCE(gc.giftcard_redemption_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS cash_giftcard_credit_local_amount,
	COALESCE(gc.giftcard_redemption_count, 0) AS cash_giftcard_credit_count,
    COALESCE(tk.token_count, 0) AS token_count,
    COALESCE(tk.token_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS token_local_amount,
    COALESCE(tk.cash_token_count, 0) AS cash_token_count,
    COALESCE(tk.cash_token_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS cash_token_local_amount,
    COALESCE(tk.non_cash_token_count, 0) AS non_cash_token_count,
    COALESCE(tk.non_cash_token_amount, 0)  / (1 + COALESCE(vrh.rate, 0))AS non_cash_token_local_amount,
	COALESCE(cca.shipping_revenue, 0) / (1 + COALESCE(vrh.rate, 0)) AS shipping_revenue_before_discount_local_amount,
	COALESCE(cca.shipping_revenue_cash_amount, 0) / (1 + COALESCE(vrh.rate, 0))
	    AS shipping_revenue_cash_local_amount,
	COALESCE(cca.shipping_revenue_credit_amount, 0) / (1 + COALESCE(vrh.rate, 0))
	    AS shipping_revenue_credit_local_amount,
    COALESCE(cca.tariff_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS tariff_revenue_local_amount,
	COALESCE(scla.shipping_cost_local_amount, 0) AS shipping_cost_local_amount,
	COALESCE(df.delivery_fee_local_amount, 0) AS delivery_fee_local_amount,
	COALESCE(ea.bounceback_endowment_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS bounceback_endowment_local_amount,
	COALESCE(ea.vip_endowment_amount, 0) / (1 + COALESCE(vrh.rate, 0)) AS vip_endowment_local_amount
FROM _fact_order__stg_order_base AS base
    LEFT JOIN reference.vat_rate_history AS vrh
        ON vrh.country_code = base.ship_to_country
        AND base.order_hq_datetime_ltz::DATE BETWEEN vrh.start_date AND vrh.expires_date
    LEFT JOIN _fact_order__payment_transaction_local_amount AS ptla
        ON ptla.order_id = base.order_id
    LEFT JOIN _fact_order__cash_non_cash_totals AS cnct
        ON cnct.order_id = base.order_id
    LEFT JOIN _fact_order__shipping_cost_local_amount AS scla
        ON scla.order_id = base.order_id
    LEFT JOIN _fact_order__cash_credit_amounts AS cca
        ON cca.order_id = base.order_id
    LEFT JOIN _fact_order__giftco AS gco
        ON gco.order_id = base.order_id
    LEFT JOIN _fact_order__giftcard AS gc
        ON gc.order_id = base.order_id
    LEFT JOIN _fact_order__token AS tk
        ON base.order_id = tk.order_id
    LEFT JOIN _fact_order__delivery_fee AS df
        ON base.order_id = df.order_id
    LEFT JOIN _fact_order__endowment_amounts AS ea
        ON base.order_id = ea.order_id;
-- SELECT * FROM _fact_order__stg_order_amounts;

UPDATE _fact_order__stg_order_amounts AS t
SET t.payment_transaction_local_amount = s.payment_transaction_local_amount,
    t.cash_credit_local_amount = s.cash_credit_local_amount,
    t.non_cash_credit_local_amount = s.non_cash_credit_local_amount,
    t.cash_credit_count = s.cash_credit_count,
    t.non_cash_credit_count = s.non_cash_credit_count,
    t.cash_membership_credit_local_amount = s.cash_membership_credit_local_amount,
    t.cash_membership_credit_count = s.cash_membership_credit_count,
    t.cash_refund_credit_local_amount = s.cash_refund_credit_local_amount,
    t.cash_refund_credit_count = s.cash_refund_credit_count,
    t.cash_giftco_credit_local_amount = s.cash_giftco_credit_local_amount,
    t.cash_giftco_credit_count = s.cash_giftco_credit_count,
    t.token_count = s.token_count,
    t.token_local_amount = s.token_local_amount,
    t.cash_token_count = s.cash_token_count,
    t.cash_token_local_amount = s.cash_token_local_amount,
    t.non_cash_token_count = s.non_cash_token_count,
    t.non_cash_token_local_amount = s.non_cash_token_local_amount,
    t.shipping_cost_local_amount = s.shipping_cost_local_amount
FROM (SELECT oa.master_order_id                          AS order_id,
             SUM(oa.payment_transaction_local_amount)    AS payment_transaction_local_amount,
             SUM(oa.cash_credit_local_amount)            AS cash_credit_local_amount,
             SUM(oa.non_cash_credit_local_amount)        AS non_cash_credit_local_amount,
             SUM(oa.cash_credit_count)                   AS cash_credit_count,
             SUM(oa.non_cash_credit_count)               AS non_cash_credit_count,
             SUM(oa.cash_membership_credit_local_amount) AS cash_membership_credit_local_amount,
             SUM(oa.cash_membership_credit_count)        AS cash_membership_credit_count,
             SUM(oa.cash_refund_credit_local_amount)     AS cash_refund_credit_local_amount,
             SUM(oa.cash_refund_credit_count)            AS cash_refund_credit_count,
             SUM(oa.cash_giftco_credit_local_amount)     AS cash_giftco_credit_local_amount,
             SUM(oa.cash_giftco_credit_count)            AS cash_giftco_credit_count,
             SUM(oa.token_count)                         AS token_count,
             SUM(oa.token_local_amount)                  AS token_local_amount,
             SUM(oa.cash_token_count)                    AS cash_token_count,
             SUM(oa.cash_token_local_amount)             AS cash_token_local_amount,
             SUM(oa.non_cash_token_count)                AS non_cash_token_count,
             SUM(oa.non_cash_token_local_amount)         AS non_cash_token_local_amount,
             SUM(oa.shipping_cost_local_amount)          AS shipping_cost_local_amount
      FROM _fact_order__dropship_split AS ds
               JOIN _fact_order__stg_order_amounts AS oa
                    ON oa.order_id = ds.order_id
      WHERE ds.master_child = 'child'
            AND oa.master_order_id IS NOT NULL
      GROUP BY oa.master_order_id
      ) AS s
WHERE s.order_id = t.order_id;

CREATE OR REPLACE TEMP TABLE _fact_order__stg_order_payment (
	order_id INT,
	order_status VARCHAR(20),
    order_type VARCHAR(20),
	is_activating BOOLEAN,
	is_guest BOOLEAN,
    is_vip_membership_trial BOOLEAN,
	shipment_carrier VARCHAR(255),
	payment_method VARCHAR(50),
	raw_creditcard_type VARCHAR(50),
	is_prepaid_creditcard BOOLEAN,
	is_applepay BOOLEAN,
	is_mastercard_checkout BOOLEAN,
	is_visa_checkout BOOLEAN,
    funding_type VARCHAR(25),
    prepaid_type VARCHAR(25),
    card_product_type VARCHAR(25)
	);
INSERT INTO _fact_order__stg_order_payment (
	order_id,
	order_status,
    order_type,
	is_activating,
	is_guest,
    is_vip_membership_trial,
    shipment_carrier,
	payment_method,
	raw_creditcard_type,
	is_prepaid_creditcard,
	is_applepay,
	is_mastercard_checkout,
	is_visa_checkout,
    funding_type,
    prepaid_type,
    card_product_type
	)
SELECT
	base.order_id,
	oc.order_status,
    IFF(oc.is_billing_order, 'Credit Billing', 'Product Order') AS order_type,
	oc.is_activating,
    oc.is_guest,
    oc.is_vip_membership_trial,
    CASE
		WHEN oc.is_billing_order OR oc.is_membership_fee THEN 'Not Applicable'
		ELSE scla.shipment_carrier END AS shipment_carrier,
	CASE
		WHEN LOWER(base.order_payment_method) = 'creditcard' THEN 'Credit Card'
		WHEN LOWER(base.order_payment_method) = 'moneyorder' THEN 'Money Order'
		WHEN base.order_payment_method IS NULL THEN 'Unknown'
		ELSE INITCAP(base.order_payment_method) END AS payment_method,
    CASE
        WHEN LOWER(ptla.creditcard_type) IN ('mastercard', 'mc') THEN 'Master Card'
        WHEN LOWER(ptla.creditcard_type) IN ('vi', 'visadankort', 'visasaraivacard') THEN 'Visa'
        WHEN LOWER(ptla.creditcard_type) = 'paypallitle' THEN 'Paypal'
        WHEN LOWER(ptla.creditcard_type) IN ('bijcard', 'directdebit_nl', 'ideal', 'cartebancaire', 'elv', 'sepadirectdebit', 'diners') THEN 'Other'
        WHEN LOWER(base.order_payment_method) IN ('none', 'cash') AND nvl(ptla.creditcard_type, 'Unknown') = 'Unknown' THEN 'Not Applicable'
        ELSE INITCAP(ptla.creditcard_type) END AS raw_creditcard_type,
    COALESCE(ptla.is_prepaid_creditcard, FALSE) AS is_prepaid_creditcard,
    COALESCE(ptla.is_applepay, FALSE) AS is_applepay,
    IFF(ptla.is_applepay OR raw_creditcard_type = 'Not Applicable', FALSE, ptla.is_mastercard_checkout) AS is_mastercard_checkout,
    IFF(ptla.is_applepay OR raw_creditcard_type ILIKE '%%paypal%%' OR raw_creditcard_type = 'Not Applicable', FALSE, ptla.is_visa_checkout) AS is_visa_checkout,
    ptla.funding_type,
    ptla.prepaid_type,
    ptla.card_product_type
FROM _fact_order__stg_order_base AS base
	LEFT JOIN _fact_order__payment_transaction_local_amount AS ptla
		ON ptla.order_id = base.order_id
	LEFT JOIN _fact_order__shipping_cost_local_amount AS scla
		ON scla.order_id = base.order_id
    LEFT JOIN stg.lkp_order_classification AS oc
        ON oc.order_id = base.order_id;
-- SELECT * FROM _fact_order__stg_order_payment;

-- Get dates with local time zone based on store location
CREATE OR REPLACE TEMP TABLE _fact_order__local_time_zone_dates (
    order_id INT,
    order_local_datetime TIMESTAMP_TZ(3),
    payment_transaction_local_datetime TIMESTAMP_TZ(3),
    shipped_local_datetime TIMESTAMP_TZ(3),
    order_completion_local_datetime TIMESTAMP_TZ(3),
    bops_pickup_local_datetime TIMESTAMP_TZ(3),
    order_placed_local_datetime TIMESTAMP_TZ(3)
    );
INSERT INTO _fact_order__local_time_zone_dates (
    order_id,
    order_local_datetime,
    payment_transaction_local_datetime,
    shipped_local_datetime,
    order_completion_local_datetime,
    bops_pickup_local_datetime,
    order_placed_local_datetime
    )
SELECT
    stg.order_id,
    CONVERT_TIMEZONE(stz.time_zone, stg.order_hq_datetime_ltz::TIMESTAMP_TZ)
        AS order_local_datetime,
    CONVERT_TIMEZONE(stz.time_zone, stg.payment_transaction_hq_datetime_ltz::TIMESTAMP_TZ)
        AS payment_transaction_local_datetime,
    CONVERT_TIMEZONE(stz.time_zone, stg.shipped_hq_datetime_ltz::TIMESTAMP_TZ)
        AS shipped_local_datetime,
    CONVERT_TIMEZONE(stz.time_zone, COALESCE(stg.ps_completion_hq_datetime_ltz, stg.shipped_hq_datetime_ltz, stg.payment_transaction_hq_datetime_ltz,
        IFF(LOWER(st.store_type) = 'retail' AND LOWER(p.order_status) = 'success', stg.order_hq_datetime_ltz, NULL))::TIMESTAMP_TZ)
        AS order_completion_local_datetime,
    CONVERT_TIMEZONE(stz.time_zone, stg.bops_pickup_datetime_ltz::TIMESTAMP_TZ)
        AS bops_pickup_local_datetime,
    CONVERT_TIMEZONE(stz.time_zone, stg.placed_datetime_ltz::TIMESTAMP_TZ)
        AS order_placed_local_datetime
FROM _fact_order__stg_order_base AS stg
    JOIN _fact_order__stg_order_payment AS p
        ON p.order_id = stg.order_id
	LEFT JOIN reference.store_timezone AS stz
		ON stz.store_id = stg.store_id
    LEFT JOIN stg.dim_store AS st ON st.store_id = stg.store_id;

-- get exchange rates
CREATE OR REPLACE TEMP TABLE _fact_order__exchange_rates (
    order_id INT,
    order_date_usd_conversion_rate NUMBER(18, 6),
    order_date_eur_conversion_rate NUMBER(18, 6),
    payment_transaction_date_usd_conversion_rate NUMBER(18, 6),
    payment_transaction_date_eur_conversion_rate NUMBER(18, 6),
    shipped_date_usd_conversion_rate NUMBER(18, 6),
    shipped_date_eur_conversion_rate NUMBER(18, 6),
    reporting_usd_conversion_rate NUMBER(18, 6),
    reporting_eur_conversion_rate NUMBER(18, 6)
    );
INSERT INTO _fact_order__exchange_rates (
	order_id,
	order_date_usd_conversion_rate,
    order_date_eur_conversion_rate,
    payment_transaction_date_usd_conversion_rate,
    payment_transaction_date_eur_conversion_rate,
    shipped_date_usd_conversion_rate,
    shipped_date_eur_conversion_rate,
    reporting_usd_conversion_rate,
    reporting_eur_conversion_rate
	)
SELECT
    stg.order_id,
    cer_usd_o.exchange_rate AS order_date_usd_conversion_rate,
	cer_eur_o.exchange_rate AS order_date_eur_conversion_rate,
	cer_usd_pt.exchange_rate AS payment_transaction_date_usd_conversion_rate,
	cer_eur_pt.exchange_rate AS payment_transaction_date_eur_conversion_rate,
	cer_usd_s.exchange_rate AS shipped_date_usd_conversion_rate,
	cer_eur_s.exchange_rate AS shipped_date_eur_conversion_rate,
	COALESCE(cer_usd_s.exchange_rate, cer_usd_pt.exchange_rate, cer_usd_o.exchange_rate, 1)
	    AS reporting_usd_conversion_rate,
	COALESCE(cer_eur_s.exchange_rate, cer_eur_pt.exchange_rate, cer_eur_o.exchange_rate, 1)
	    AS reporting_eur_conversion_rate
FROM _fact_order__stg_order_base AS stg
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer_usd_o
		ON cer_usd_o.src_currency = stg.iso_currency_code
		AND cer_usd_o.dest_currency = 'USD'
		AND cer_usd_o.rate_date_pst = CAST(stg.order_hq_datetime_ltz AS date)
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer_eur_o
		ON cer_eur_o.src_currency = stg.iso_currency_code
		AND cer_eur_o.dest_currency = 'EUR'
	    AND cer_eur_o.rate_date_pst = CAST(stg.order_hq_datetime_ltz AS date)
	LEFT JOIN reference.currency_exchange_rate_by_date AS cer_usd_pt
		ON cer_usd_pt.src_currency = stg.iso_currency_code
		AND cer_usd_pt.dest_currency = 'USD'
		AND cer_usd_pt.rate_date_pst = CAST(stg.payment_transaction_hq_datetime_ltz AS date)
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer_eur_pt
		ON cer_eur_pt.src_currency = stg.iso_currency_code
		AND cer_eur_pt.dest_currency = 'EUR'
		AND cer_eur_pt.rate_date_pst = CAST(stg.payment_transaction_hq_datetime_ltz AS date)
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer_usd_s
		ON cer_usd_s.src_currency = stg.iso_currency_code
		AND cer_usd_s.dest_currency = 'USD'
		AND cer_usd_s.rate_date_pst = CAST(stg.shipped_hq_datetime_ltz AS date)
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer_eur_s
		ON cer_eur_s.src_currency = stg.iso_currency_code
		AND cer_eur_s.dest_currency = 'EUR'
        AND cer_eur_s.rate_date_pst = CAST(stg.shipped_hq_datetime_ltz AS date);

CREATE OR REPLACE TEMP TABLE _fact_order__selected_shipping_type AS
SELECT
       stg.order_id,
       dss.order_customer_selected_shipping_key
FROM (
        SELECT
            base.order_id,
            IFF((dosc.order_sales_channel_l1 = 'Retail Order' AND dosc.is_retail_ship_only_order = FALSE) OR dosc.order_classification_l1 = 'Billing Order', TRUE, FALSE) as is_not_applicable,
            IFF(is_not_applicable,'Not Applicable', INITCAP(COALESCE(so.type, os.type, 'Unknown'))) as customer_selected_shipping_type,
            IFF(is_not_applicable,'Not Applicable', INITCAP(COALESCE(so.label, 'Unknown'))) as customer_selected_shipping_service,
            IFF(is_not_applicable,'Not Applicable', INITCAP(COALESCE(so.description, 'Unknown'))) as customer_selected_shipping_description,
            IFF(is_not_applicable, -2, COALESCE(so.cost, -1)) as customer_selected_shipping_price
        FROM _fact_order__stg_order_base base
        JOIN stg.dim_order_sales_channel AS dosc
            ON dosc.order_sales_channel_key = base.order_sales_channel_key
        LEFT JOIN lake_consolidated.ultra_merchant.order_shipping os
            ON base.order_id = os.order_id
            AND os.hvr_is_deleted = 0
        LEFT JOIN lake_consolidated.ultra_merchant.shipping_option so
            ON os.shipping_option_id = so.shipping_option_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY base.order_id ORDER BY os.amount, os.order_shipping_id DESC) = 1 /* If multiple shipment types, choose the most expensive shipping type */
)AS stg
JOIN stg.dim_order_customer_selected_shipping dss
    ON  stg.customer_selected_shipping_type = dss.customer_selected_shipping_type
    AND stg.customer_selected_shipping_service = dss.customer_selected_shipping_service
    AND stg.customer_selected_shipping_description = dss.customer_selected_shipping_description
    AND stg.customer_selected_shipping_price = dss.customer_selected_shipping_price;
-- SELECT * FROM _fact_order__selected_shipping_type;

-- Get key values
CREATE OR REPLACE TEMP TABLE _fact_order__dim_keys (
    order_id INT,
    master_order_id INT,
    currency_key NUMBER(38, 0),
    order_payment_status_key NUMBER(38, 0),
    order_processing_status_key NUMBER(38, 0),
    default_order_membership_classification_key NUMBER(38, 0),
    order_status_key NUMBER(38, 0),
    payment_key NUMBER(38, 0),
    order_customer_selected_shipping_key NUMBER(38, 0)
);

INSERT INTO _fact_order__dim_keys (
	order_id,
    master_order_id,
    currency_key,
    order_payment_status_key,
    order_processing_status_key,
    default_order_membership_classification_key,
    order_status_key,
    payment_key,
    order_customer_selected_shipping_key
    )
SELECT
	stg.order_id,
	stg.master_order_id,
	COALESCE(dcur.currency_key, -1) AS currency_key,
	COALESCE(dopy.order_payment_status_key, -1) AS order_payment_status_key,
	COALESCE(dopr.order_processing_status_key, -1) AS order_processing_status_key,
    COALESCE(domc.order_membership_classification_key, -1) AS default_order_membership_classification_key,
	COALESCE(dost.order_status_key, -1) AS order_status_key,
	COALESCE(dp.payment_key, -1) AS payment_key,
    COALESCE(fosst.order_customer_selected_shipping_key, -1) AS order_customer_selected_shipping_key
FROM _fact_order__stg_order_base AS stg
    JOIN _fact_order__stg_order_payment AS p
        ON p.order_id = stg.order_id
	LEFT JOIN stg.dim_currency AS dcur
		ON LOWER(dcur.iso_currency_code) = LOWER(stg.iso_currency_code)
		AND stg.order_hq_datetime_ltz
		    BETWEEN dcur.effective_start_datetime and dcur.effective_end_datetime
	LEFT JOIN stg.dim_order_payment_status AS dopy
		ON dopy.order_payment_status_code = stg.order_payment_status_code
		AND stg.order_hq_datetime_ltz
		    BETWEEN dopy.effective_start_datetime and dopy.effective_end_datetime
	LEFT JOIN stg.dim_order_processing_status AS dopr
		ON dopr.order_processing_status_code = stg.order_processing_status_code
		AND stg.order_hq_datetime_ltz
		    BETWEEN dopr.effective_start_datetime and dopr.effective_end_datetime
    LEFT JOIN stg.dim_order_membership_classification AS domc
		ON domc.is_activating = p.is_activating
		AND domc.is_guest = p.is_guest
		AND domc.is_vip_membership_trial = p.is_vip_membership_trial
		AND domc.order_membership_classification_key IN (1,2,3,4,7)-- Use original keys for initial (default) classification
		    -- Keys 5 and 6 are sub-categories of keys 2 and 3 which will be updated later, since
		    -- we don't have the necessary information at this time to determine the final key value.
		AND stg.order_hq_datetime_ltz
		    BETWEEN domc.effective_start_datetime and domc.effective_end_datetime
	LEFT JOIN stg.dim_order_status AS dost
		ON LOWER(dost.order_status) = LOWER(p.order_status)
		AND stg.order_hq_datetime_ltz
		    BETWEEN dost.effective_start_datetime and dost.effective_end_datetime
    LEFT JOIN stg.dim_payment AS dp
		ON LOWER(dp.payment_method) = LOWER(COALESCE(p.payment_method, 'Unknown'))
		AND LOWER(dp.raw_creditcard_type) = LOWER(COALESCE(p.raw_creditcard_type, 'Unknown'))
		AND dp.is_prepaid_creditcard = p.is_prepaid_creditcard
		AND dp.is_applepay = p.is_applepay
		AND dp.is_mastercard_checkout = p.is_mastercard_checkout
		AND dp.is_visa_checkout = p.is_visa_checkout
		AND LOWER(dp.funding_type) = LOWER(COALESCE(p.funding_type, 'Unknown'))
		AND LOWER(dp.prepaid_type) = LOWER(COALESCE(p.prepaid_type, 'Unknown'))
		AND LOWER(dp.card_product_type) = LOWER(COALESCE(p.card_product_type, 'Unknown'))
		AND stg.order_hq_datetime_ltz
		    BETWEEN dp.effective_start_datetime and dp.effective_end_datetime
    LEFT JOIN _fact_order__selected_shipping_type fosst
        ON fosst.order_id = stg.order_id;

-- assigning the max payment_key to the master order for dropship splits.  The child orders have more detailed payment info
UPDATE _fact_order__dim_keys AS t
SET t.payment_key = s.payment_key
FROM (
        SELECT dk.master_order_id AS order_id,
               MAX(dk.payment_key) AS payment_key
        FROM _fact_order__dropship_split AS ds
            JOIN _fact_order__dim_keys AS dk
                ON dk.order_id = ds.order_id
        WHERE ds.master_child = 'child'
            AND dk.master_order_id IS NOT NULL
        GROUP BY dk.master_order_id
    ) AS s
WHERE s.order_id = t.order_id;

-- DA-18407
CREATE OR REPLACE TEMP TABLE _fact_order__order_cancel AS
SELECT DISTINCT oc.order_id
FROM lake_consolidated.ultra_merchant.order_cancel oc
         JOIN _fact_order__dim_keys fodk
              ON fodk.order_id = oc.order_id
         JOIN stg.dim_order_status dos
              ON dos.order_status_key = fodk.order_status_key
WHERE dos.order_status = 'Success' AND oc.hvr_is_deleted = 0;

UPDATE _fact_order__dim_keys fodk
SET order_status_key = (SELECT order_status_key FROM stg.dim_order_status WHERE order_status ='Cancelled')
FROM _fact_order__order_cancel fooc
WHERE fodk.order_id = fooc.order_id;

-- get finance assumption measures
-- apply costs like shipping, warehouse, shipping supplies only to product related orders
-- this part considers retail orders with shipping as online orders
CREATE OR REPLACE TEMP TABLE _fact_order__finance_assumptions_stg AS
SELECT
    stg.order_id,
    stg.master_order_id,
    stg.customer_id,
    p.order_status,
    stg.order_sales_channel_key,
    a.payment_transaction_local_amount,
    a.tax_local_amount,
    st.store_brand,
    st.store_region,
    st.store_country,
    CASE WHEN st.store_type = 'Retail' and dosc.is_retail_ship_only_order = TRUE THEN 'Online'
         WHEN st.store_type IN ('Mobile App', 'Group Order') THEN 'Online'
        ELSE st.store_type END AS store_type,
    dosc.order_classification_l2,
    DATE_TRUNC(MONTH, CAST(CONVERT_TIMEZONE(stz.time_zone,
        COALESCE(stg.shipped_hq_datetime_ltz, stg.order_hq_datetime_ltz)::TIMESTAMP_TZ) AS DATE)) AS order_month,
    CASE
        WHEN p.order_status = 'DropShip Split' AND stg.shipped_hq_datetime_ltz IS NOT NULL THEN TRUE
        WHEN p.order_status = 'DropShip Split' AND  stg.order_processing_status_code IN (2100, 2110, 2050, 2060, 2065, 2080, 2350, 2342) THEN TRUE
        ELSE FALSE
    END AS is_dropship_child_success_pending
FROM _fact_order__stg_order_base AS stg
    JOIN _fact_order__stg_order_payment AS p
        ON p.order_id = stg.order_id
	JOIN _fact_order__stg_order_amounts AS a
        ON a.order_id = stg.order_id
	JOIN reference.store_timezone AS stz
        ON stz.store_id = stg.store_id
    JOIN stg.dim_store AS st
        ON st.store_id = stg.store_id
    JOIN stg.dim_order_sales_channel AS dosc
        ON dosc.order_sales_channel_key = stg.order_sales_channel_key
WHERE p.order_status IN ('Success', 'Pending', 'DropShip Split');

/* deleting dropship child orders that weren't success/pending */
DELETE FROM _fact_order__finance_assumptions_stg
WHERE order_status = 'DropShip Split' AND is_dropship_child_success_pending = FALSE;

CREATE OR REPLACE TEMP TABLE _fact_order__finance_assumptions AS
SELECT
    stg.order_id,
    stg.master_order_id,
    stg.store_type,
    CASE WHEN stg.store_type <> 'Retail' THEN fa.variable_gms_cost_per_order END AS estimated_variable_gms_cost_local_amount,
    fa.variable_payment_processing_pct_cash_revenue AS estimated_variable_payment_processing_pct_cash_revenue,
    CASE
        WHEN stg.order_classification_l2 IN ('Exchange', 'Product Order', 'Reship')
            THEN fa.variable_warehouse_cost_per_order END AS estimated_variable_warehouse_cost_local_amount,
    CASE
        WHEN stg.order_classification_l2 IN ('Exchange', 'Product Order', 'Reship') AND stg.store_type <> 'Retail'
            THEN fa.shipping_cost_per_order END AS estimated_shipping_cost_local_amount,
    CASE
        WHEN stg.order_classification_l2 IN ('Exchange', 'Product Order', 'Reship') AND stg.store_type <> 'Retail'
            THEN fa.shipping_supplies_cost_per_order END AS estimated_shipping_supplies_cost_local_amount
FROM _fact_order__finance_assumptions_stg AS stg
    JOIN reference.finance_assumption AS fa
        ON fa.brand = stg.store_brand
        AND IFF(fa.region_type = 'Region', stg.store_region, stg.store_country) = fa.region_type_mapping /* region join */
    AND stg.order_month = fa.financial_date /* month join */
    AND fa.store_type = stg.store_type;

UPDATE _fact_order__finance_assumptions AS t
SET t.estimated_variable_gms_cost_local_amount = s.estimated_variable_gms_cost_local_amount,
    t.estimated_variable_payment_processing_pct_cash_revenue = s.estimated_variable_payment_processing_pct_cash_revenue,
    t.estimated_variable_warehouse_cost_local_amount = s.estimated_variable_warehouse_cost_local_amount,
    t.estimated_shipping_cost_local_amount = s.estimated_shipping_cost_local_amount,
    t.estimated_shipping_supplies_cost_local_amount = s.estimated_shipping_supplies_cost_local_amount
FROM (
    SELECT
        fa.master_order_id AS order_id,
        SUM(estimated_variable_gms_cost_local_amount) AS estimated_variable_gms_cost_local_amount,
        SUM(estimated_variable_payment_processing_pct_cash_revenue) AS estimated_variable_payment_processing_pct_cash_revenue,
        SUM(estimated_variable_warehouse_cost_local_amount) AS estimated_variable_warehouse_cost_local_amount,
        SUM(estimated_shipping_cost_local_amount) AS estimated_shipping_cost_local_amount,
        SUM(estimated_shipping_supplies_cost_local_amount) AS estimated_shipping_supplies_cost_local_amount
    FROM _fact_order__dropship_split AS ds
        JOIN _fact_order__finance_assumptions AS fa
            ON fa.order_id = ds.order_id
    WHERE ds.master_child = 'child'
          AND fa.master_order_id IS NOT NULL
    GROUP BY fa.master_order_id
    ) AS s
WHERE s.order_id = t.order_id;
-- SELECT * FROM _fact_order__finance_assumptions

CREATE OR REPLACE TEMP TABLE _fact_order__test_customers AS
SELECT tc.customer_id
FROM _fact_order__stg_order_base AS stg
    JOIN reference.test_customer AS tc
        ON tc.customer_id = stg.customer_id;

CREATE OR REPLACE TEMP TABLE _fact_order__activation AS
SELECT
    stg.customer_id,
    fa.activation_key,
    fa.membership_event_key,
    fa.activation_local_datetime,
    fa.next_activation_local_datetime,
    fa.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.customer_id ORDER BY fa.activation_sequence_number, fa.activation_local_datetime) AS row_num
FROM (SELECT DISTINCT customer_id FROM _fact_order__stg_order_base) AS stg
    JOIN stg.fact_activation AS fa
    	ON fa.customer_id = stg.customer_id
WHERE NOT NVL(fa.is_deleted, FALSE);

CREATE OR REPLACE TEMP TABLE _fact_order__first_activation AS
SELECT
    stg.order_id,
    stg.customer_id,
    a.activation_key,
    a.membership_event_key,
    a.activation_local_datetime,
    a.next_activation_local_datetime,
    a.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.order_id, stg.customer_id ORDER BY a.row_num) AS row_num
FROM _fact_order__stg_order_base AS stg
    JOIN _fact_order__local_time_zone_dates AS tz
        ON tz.order_id = stg.order_id
    JOIN _fact_order__activation AS a
		ON a.customer_id = stg.customer_id
        AND a.activation_local_datetime <= tz.order_local_datetime;

-- Capture dim/fact reference keys
CREATE OR REPLACE TEMP TABLE _fact_order__ref_keys (
    order_id INT,
    customer_id INT,
    membership_event_key NUMBER(38, 0),
    activation_key NUMBER(38, 0),
    first_activation_key NUMBER(38, 0),
    is_reactivated_vip BOOLEAN,
    is_never_vip BOOLEAN,
    order_membership_classification_key NUMBER(38, 0)
	);

INSERT INTO _fact_order__ref_keys (
	order_id,
    customer_id,
    membership_event_key,
    activation_key,
    first_activation_key,
    is_reactivated_vip,
    is_never_vip,
    order_membership_classification_key
    )
SELECT
    stg.order_id,
    stg.customer_id,
    COALESCE(fa.membership_event_key, fme.membership_event_key, -1) AS membership_event_key,
    COALESCE(fa.activation_key, -1) AS activation_key,
    COALESCE(ffa.activation_key, -1) AS first_activation_key,
    IFF(fa.row_num > 1, TRUE, FALSE) AS is_reactivated_vip,
    IFF(COALESCE(ffa.activation_key, -1) = -1, TRUE, FALSE) AS is_never_vip,
	COALESCE(k.default_order_membership_classification_key, -1) AS order_membership_classification_key
FROM _fact_order__stg_order_base AS stg
    JOIN _fact_order__local_time_zone_dates AS tz
        ON tz.order_id = stg.order_id
    JOIN _fact_order__dim_keys AS k
        ON k.order_id = stg.order_id
    LEFT JOIN _fact_order__activation AS fa
		ON fa.customer_id = stg.customer_id
		AND (tz.order_local_datetime >= fa.activation_local_datetime
		    AND tz.order_local_datetime < fa.next_activation_local_datetime)
    LEFT JOIN stg.fact_membership_event AS fme
		ON fme.customer_id = stg.customer_id
		AND tz.order_local_datetime BETWEEN fme.event_start_local_datetime AND fme.event_end_local_datetime
        AND NOT NVL(fme.is_deleted, FALSE)
    LEFT JOIN _fact_order__first_activation AS ffa
		ON ffa.order_id = stg.order_id
        AND ffa.customer_id = stg.customer_id
		AND ffa.row_num = 1;
-- SELECT * FROM _fact_order__ref_keys;

-- Try to capture more activation keys (DA-15434)
UPDATE _fact_order__ref_keys AS tgt
SET
    tgt.activation_key = COALESCE(fa.activation_key, -1),
    tgt.first_activation_key = IFF(tgt.first_activation_key = -1, COALESCE(fa.activation_key, -1), tgt.first_activation_key),
    tgt.is_reactivated_vip = IFF(fa.row_num > 1, TRUE, FALSE),
    tgt.is_never_vip = IFF(COALESCE(ffa.activation_key, -1) = -1, TRUE, FALSE)
FROM _fact_order__ref_keys AS stg
    JOIN _fact_order__local_time_zone_dates AS tz
        ON tz.order_id = stg.order_id
    JOIN _fact_order__dim_keys AS k
        ON k.order_id = stg.order_id
    JOIN stg.dim_order_membership_classification AS domc
		ON domc.order_membership_classification_key = k.default_order_membership_classification_key
    JOIN stg.dim_order_status AS dost
        ON dost.order_status_key = k.order_status_key
    JOIN stg.fact_membership_event AS fme
		ON fme.membership_event_key = stg.membership_event_key
    JOIN _fact_order__activation AS fa
		ON fa.customer_id = stg.customer_id
		AND DATEDIFF(DAY, tz.order_local_datetime, fa.activation_local_datetime) = 0
    LEFT JOIN _fact_order__first_activation AS ffa
		ON ffa.order_id = stg.order_id
        AND ffa.customer_id = stg.customer_id
		AND ffa.row_num = 1
WHERE tgt.order_id = stg.order_id
    AND stg.activation_key = -1
    AND domc.is_vip
    AND domc.is_activating
    AND dost.order_status IN ('Success', 'Pending')
    AND fme.membership_type_detail != 'Classic';
-- SELECT * FROM _fact_order__ref_keys WHERE order_id = 1237501200

-- Distinguish cancelled VIP guest Purchasers vs Guest (DA-23856)
UPDATE _fact_order__ref_keys AS tgt
SET
    tgt.order_membership_classification_key = CASE
        -- Keys 5 and 6 are subcategories of Keys 2 and 3 respectively
        WHEN domc.is_activating AND NOT domc.is_guest AND ref.is_reactivated_vip THEN 5
        WHEN NOT domc.is_activating AND domc.is_guest AND ref.is_never_vip AND NOT domc.is_vip_membership_trial THEN 6
        ELSE tgt.order_membership_classification_key END
FROM _fact_order__ref_keys AS ref
    JOIN stg.dim_order_membership_classification AS domc
		ON domc.order_membership_classification_key = ref.order_membership_classification_key
WHERE tgt.order_id = ref.order_id
    AND domc.is_activating != domc.is_guest; -- Filter by Keys 2 and 3 since they are the only ones with subcategories
-- SELECT order_membership_classification_key, COUNT(1) FROM _fact_order__ref_keys GROUP BY 1;

---------------------------
-- prepare staging table --
---------------------------

CREATE OR REPLACE TEMP TABLE _fact_order__stg AS
SELECT DISTINCT
	stg.order_id,
	stg.meta_original_order_id,
	rk.membership_event_key,
    rk.activation_key,
    rk.first_activation_key,
	dk.currency_key,
	stg.customer_id,
	stg.store_id,
	stg.cart_store_id,
	stg.membership_brand_id,
    stg.master_order_id,
	dk.order_payment_status_key,
	dk.order_processing_status_key,
	rk.order_membership_classification_key, -- Use _ref_keys instead of _dim_keys temp table for this field
	stg.is_cash,
	stg.is_credit_billing_on_retry,
	stg.shipping_address_id,
	stg.billing_address_id,
	dk.order_status_key,
	stg.order_sales_channel_key,
	dk.payment_key,
    dk.order_customer_selected_shipping_key,
	stg.session_id,
	tz.order_local_datetime,
	tz.payment_transaction_local_datetime,
	tz.shipped_local_datetime,
    tz.order_completion_local_datetime,
	stg.unit_count,
	stg.loyalty_unit_count,
	stg.third_party_unit_count,
	ex.order_date_usd_conversion_rate,
	ex.order_date_eur_conversion_rate,
	ex.payment_transaction_date_usd_conversion_rate,
	ex.payment_transaction_date_eur_conversion_rate,
	ex.shipped_date_usd_conversion_rate,
	ex.shipped_date_eur_conversion_rate,
	ex.reporting_usd_conversion_rate,
	ex.reporting_eur_conversion_rate,
	a.effective_vat_rate,
	a.payment_transaction_local_amount,
	a.subtotal_excl_tariff_local_amount,
	a.tax_local_amount,
	a.tax_cash_local_amount,
	a.tax_credit_local_amount,
	a.cash_credit_local_amount,
	a.cash_credit_count,
	a.cash_membership_credit_local_amount,
	a.cash_membership_credit_count,
	a.cash_refund_credit_local_amount,
	a.cash_refund_credit_count,
	a.cash_giftco_credit_local_amount,
	a.cash_giftco_credit_count,
	a.cash_giftcard_credit_local_amount,
	a.cash_giftcard_credit_count,
    a.token_local_amount,
    a.token_count,
    a.cash_token_local_amount,
    a.cash_token_count,
    a.non_cash_token_local_amount,
    a.non_cash_token_count,
	a.non_cash_credit_local_amount,
	a.non_cash_credit_count,
	a.shipping_revenue_before_discount_local_amount,
	a.shipping_revenue_cash_local_amount,
	a.shipping_revenue_credit_local_amount,
	a.shipping_cost_local_amount,
	a.product_discount_local_amount,
    a.shipping_discount_local_amount,
    a.tariff_revenue_local_amount,
    a.delivery_fee_local_amount,
    a.bounceback_endowment_local_amount,
    a.vip_endowment_local_amount,
    COALESCE(fa.estimated_variable_gms_cost_local_amount, 0) AS estimated_variable_gms_cost_local_amount,
    COALESCE(fa.estimated_variable_warehouse_cost_local_amount, 0) AS estimated_variable_warehouse_cost_local_amount,
    COALESCE(fa.estimated_variable_payment_processing_pct_cash_revenue, 0) AS estimated_variable_payment_processing_pct_cash_revenue,
    COALESCE(fa.estimated_shipping_cost_local_amount, 0) AS estimated_shipping_cost_local_amount,
    COALESCE(fa.estimated_shipping_supplies_cost_local_amount, 0) AS estimated_shipping_supplies_cost_local_amount,
    0 as estimated_landed_cost_local_amount, -- this columns is populated from fact order product cost,
    0 as misc_cogs_local_amount,
    stg.is_deleted,
    stg.administrator_id,
    stg.bops_store_id,
    tz.bops_pickup_local_datetime,
    tz.order_placed_local_datetime,
    IFF(tc.customer_id IS NOT NULL, TRUE, FALSE) AS is_test_customer,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_order__stg_order_base AS stg
    JOIN _fact_order__stg_order_amounts AS a
        ON a.order_id = stg.order_id
	JOIN _fact_order__dim_keys AS dk
		ON dk.order_id = stg.order_id
    JOIN _fact_order__ref_keys AS rk
        ON rk.order_id = stg.order_id
    JOIN _fact_order__exchange_rates AS ex
        ON ex.order_id = stg.order_id
    JOIN _fact_order__local_time_zone_dates AS tz
        ON tz.order_id = stg.order_id
    LEFT JOIN _fact_order__finance_assumptions AS fa
        ON fa.order_id = stg.order_id
    LEFT JOIN _fact_order__test_customers AS tc
        ON tc.customer_id = stg.customer_id;
-- SELECT * FROM _fact_order__stg;

    -- Migrating JF CA to JF US
UPDATE _fact_order__stg
SET store_id = 26, cart_store_id = 26
WHERE store_id = 41;


-- FINAL OUTPUT - Insert into staging table
INSERT INTO stg.fact_order_stg (
	order_id,
    meta_original_order_id,
    membership_event_key,
    activation_key,
    first_activation_key,
	currency_key,
	customer_id,
	store_id,
	cart_store_id,
	membership_brand_id,
    master_order_id,
	order_payment_status_key,
	order_processing_status_key,
	order_membership_classification_key,
    is_cash,
	is_credit_billing_on_retry,
	shipping_address_id,
	billing_address_id,
	order_status_key,
	order_sales_channel_key,
	payment_key,
    order_customer_selected_shipping_key,
	session_id,
	order_local_datetime,
	payment_transaction_local_datetime,
	shipped_local_datetime,
    order_completion_local_datetime,
	unit_count,
	loyalty_unit_count,
    third_party_unit_count,
	order_date_usd_conversion_rate,
	order_date_eur_conversion_rate,
	payment_transaction_date_usd_conversion_rate,
	payment_transaction_date_eur_conversion_rate,
	shipped_date_usd_conversion_rate,
	shipped_date_eur_conversion_rate,
	reporting_usd_conversion_rate,
	reporting_eur_conversion_rate,
	effective_vat_rate,
	payment_transaction_local_amount,
	subtotal_excl_tariff_local_amount,
	tax_local_amount,
	tax_cash_local_amount,
	tax_credit_local_amount,
	cash_credit_local_amount,
	cash_credit_count,
	cash_membership_credit_local_amount,
	cash_membership_credit_count,
	cash_refund_credit_local_amount,
	cash_refund_credit_count,
	cash_giftco_credit_local_amount,
	cash_giftco_credit_count,
	cash_giftcard_credit_local_amount,
	cash_giftcard_credit_count,
    token_local_amount,
    token_count,
    cash_token_local_amount,
    cash_token_count,
    non_cash_token_local_amount,
    non_cash_token_count,
	non_cash_credit_local_amount,
	non_cash_credit_count,
	shipping_revenue_before_discount_local_amount,
	shipping_revenue_cash_local_amount,
	shipping_revenue_credit_local_amount,
	shipping_cost_local_amount,
	product_discount_local_amount,
    shipping_discount_local_amount,
    tariff_revenue_local_amount,
    delivery_fee_local_amount,
    bounceback_endowment_local_amount,
    vip_endowment_local_amount,
    estimated_variable_gms_cost_local_amount,
    estimated_variable_warehouse_cost_local_amount,
    estimated_variable_payment_processing_pct_cash_revenue,
    estimated_shipping_cost_local_amount,
    estimated_shipping_supplies_cost_local_amount,
    estimated_landed_cost_local_amount,
    misc_cogs_local_amount,
    is_deleted,
    administrator_id,
    bops_store_id,
    bops_pickup_local_datetime,
    order_placed_local_datetime,
    is_test_customer,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    order_id,
    meta_original_order_id,
    membership_event_key,
    activation_key,
    first_activation_key,
	currency_key,
	customer_id,
	store_id,
	cart_store_id,
	membership_brand_id,
    master_order_id,
	order_payment_status_key,
	order_processing_status_key,
	order_membership_classification_key,
    is_cash,
	is_credit_billing_on_retry,
	shipping_address_id,
	billing_address_id,
	order_status_key,
	order_sales_channel_key,
	payment_key,
    order_customer_selected_shipping_key,
	session_id,
	order_local_datetime,
	payment_transaction_local_datetime,
	shipped_local_datetime,
    order_completion_local_datetime,
	unit_count,
	loyalty_unit_count,
	third_party_unit_count,
	order_date_usd_conversion_rate,
	order_date_eur_conversion_rate,
	payment_transaction_date_usd_conversion_rate,
	payment_transaction_date_eur_conversion_rate,
	shipped_date_usd_conversion_rate,
	shipped_date_eur_conversion_rate,
	reporting_usd_conversion_rate,
	reporting_eur_conversion_rate,
	effective_vat_rate,
	payment_transaction_local_amount,
	subtotal_excl_tariff_local_amount,
	tax_local_amount,
	tax_cash_local_amount,
	tax_credit_local_amount,
	cash_credit_local_amount,
	cash_credit_count,
	cash_membership_credit_local_amount,
	cash_membership_credit_count,
	cash_refund_credit_local_amount,
	cash_refund_credit_count,
	cash_giftco_credit_local_amount,
	cash_giftco_credit_count,
	cash_giftcard_credit_local_amount,
	cash_giftcard_credit_count,
    token_local_amount,
    token_count,
    cash_token_local_amount,
    cash_token_count,
    non_cash_token_local_amount,
    non_cash_token_count,
	non_cash_credit_local_amount,
	non_cash_credit_count,
	shipping_revenue_before_discount_local_amount,
	shipping_revenue_cash_local_amount,
	shipping_revenue_credit_local_amount,
	shipping_cost_local_amount,
	product_discount_local_amount,
    shipping_discount_local_amount,
    tariff_revenue_local_amount,
    delivery_fee_local_amount,
    bounceback_endowment_local_amount,
    vip_endowment_local_amount,
    estimated_variable_gms_cost_local_amount,
    estimated_variable_warehouse_cost_local_amount,
    estimated_variable_payment_processing_pct_cash_revenue,
    estimated_shipping_cost_local_amount,
    estimated_shipping_supplies_cost_local_amount,
    estimated_landed_cost_local_amount,
    misc_cogs_local_amount,
    is_deleted,
    administrator_id,
    bops_store_id,
    bops_pickup_local_datetime,
    order_placed_local_datetime,
    is_test_customer,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_order__stg
ORDER BY
    order_id;
