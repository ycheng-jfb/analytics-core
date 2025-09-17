SET target_table = 'stg.dim_refund_payment_method';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table,NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), True, False));

SET wm_lake_ultra_merchant_refund = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.refund'));
/*
-- Initial load / manual full refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = current_timestamp()
WHERE table_name = $target_table;
*/

CREATE OR REPLACE TEMP TABLE _payment_method_base (payment_method varchar(25));

INSERT INTO _payment_method_base (payment_method)
-- Full Refresh
SELECT DISTINCT r.payment_method
FROM lake_consolidated.ultra_merchant.refund AS r
WHERE COALESCE(payment_method, '') != ''
    AND $is_full_refresh
UNION
-- Incremental Refresh
SELECT DISTINCT incr.payment_method
FROM (
    SELECT DISTINCT payment_method
    FROM lake_consolidated.ultra_merchant.refund
    WHERE COALESCE(payment_method, '') != ''
        AND meta_update_datetime > $wm_lake_ultra_merchant_refund
    UNION ALL
    SELECT refund_payment_method AS payment_method
    FROM excp.dim_refund_payment_method e
    WHERE e.meta_is_current_excp
        AND e.meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh;

CREATE OR REPLACE TEMP TABLE _refund_payment_method_base AS
SELECT
	CASE LOWER(payment_method)
		WHEN 'store_credit' THEN 'Store Credit'
		WHEN 'membership_token' THEN 'Membership Token'
		WHEN 'moneyorder' THEN 'Money Order'
		WHEN 'creditcard' THEN 'Credit Card'
		WHEN 'cash' THEN 'Cash'
		WHEN 'psp' THEN 'Psp'
		WHEN 'check_request' THEN 'Check Request'
		WHEN 'chargeback' THEN 'Chargeback'
		ELSE INITCAP(payment_method)
	    END AS refund_payment_method,
    payment_method AS source_refund_payment_method
FROM _payment_method_base;
-- SELECT * FROM _refund_payment_method_base;

CREATE OR REPLACE TEMP TABLE _refund_payment_method_type_base AS
SELECT
    rpm.refund_payment_method,
    rpmt.refund_payment_method_type
FROM _refund_payment_method_base AS rpm
    CROSS JOIN (VALUES ('Cash Credit'), ('NonCash Credit'), ('Unknown')) AS rpmt (refund_payment_method_type)
WHERE rpm.refund_payment_method IN ('Membership Token', 'Store Credit')
UNION ALL
SELECT
    rpm.refund_payment_method,
    rpmt.refund_payment_method_type
FROM _refund_payment_method_base AS rpm
    CROSS JOIN (VALUES ('Cash')) AS rpmt (refund_payment_method_type)
WHERE rpm.refund_payment_method IN ('Check Request', 'Cash', 'Chargeback', 'Credit Card', 'Psp');

INSERT INTO stg.dim_refund_payment_method_stg (
    refund_payment_method,
    refund_payment_method_type,
    source_refund_payment_method,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    rpm.refund_payment_method,
    COALESCE(rpmt.refund_payment_method_type, 'Unknown') AS refund_payment_method_type,
    rpm.source_refund_payment_method,
	$execution_start_time,
	$execution_start_time
FROM _refund_payment_method_base AS rpm
    LEFT JOIN _refund_payment_method_type_base AS rpmt
        ON rpmt.refund_payment_method = rpm.refund_payment_method
ORDER BY 1, 2;
