SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _order_processing_status_base AS
WITH order_processing_status AS
(
	SELECT DISTINCT s.statuscode order_processing_status_code
	,s.label AS order_processing_status
	FROM lake_consolidated.ultra_merchant.statuscode s
	JOIN lake_consolidated.ultra_merchant.statuscode_category sc ON s.statuscode >= sc.range_start
		AND s.statuscode <= range_end
	WHERE UPPER(sc.label) = 'ORDER PROCESSING CODES'
	UNION
	SELECT order_processing_status_code
		,s.label AS order_processing_status
	FROM excp.dim_order_processing_status se
	JOIN lake_consolidated.ultra_merchant.statuscode s ON se.order_processing_status_code=s.statuscode
	    AND se.meta_is_current_excp AND meta_data_quality = 'error'
	JOIN lake_consolidated.ultra_merchant.statuscode_category sc ON s.statuscode >= sc.range_start
		AND s.statuscode <= range_end
	WHERE UPPER(sc.label) = 'ORDER PROCESSING CODES'
)
SELECT order_processing_status_code
	,order_processing_status
FROM order_processing_status os;

INSERT INTO stg.dim_order_processing_status_stg
(
	order_processing_status_code
	,order_processing_status
	,meta_create_datetime
	,meta_update_datetime
)
SELECT order_processing_status_code
	,order_processing_status
	,$execution_start_time
	,$execution_start_time
FROM _order_processing_status_base;
