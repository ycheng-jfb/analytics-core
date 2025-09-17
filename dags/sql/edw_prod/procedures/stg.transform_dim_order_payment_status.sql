SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _order_payment_status_base AS
SELECT s.label AS order_payment_status
	,s.statuscode AS order_payment_status_code
FROM lake_consolidated.ultra_merchant.statuscode s
JOIN lake_consolidated.ultra_merchant.statuscode_category sc
	ON s.statuscode BETWEEN sc.range_start AND range_end
WHERE sc.label = 'Order Payment Codes'
UNION
SELECT s.label AS order_payment_status
	, s.statuscode AS order_payment_status_code
FROM excp.dim_order_payment_status se
JOIN lake_consolidated.ultra_merchant.statuscode s
	ON S.statuscode=se.order_payment_status_code
JOIN lake_consolidated.ultra_merchant.statuscode_category sc
	ON s.statuscode BETWEEN sc.range_start AND sc.range_end
WHERE se.meta_is_current_excp
AND se.meta_data_quality = 'error';


INSERT INTO stg.dim_order_payment_status_stg
(
	order_payment_status_code
	,order_payment_status
	,meta_create_datetime
	,meta_update_datetime
)
SELECT order_payment_status_code
	,order_payment_status
	,$execution_start_time
	,$execution_start_time
FROM _order_payment_status_base;
