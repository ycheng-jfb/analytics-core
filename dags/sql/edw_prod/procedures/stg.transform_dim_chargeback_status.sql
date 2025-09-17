SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE temp TABLE _chargeback_status_base AS
SELECT statuscode AS chargeback_status_code,
    label AS chargeback_status
FROM lake_consolidated.ultra_merchant.statuscode s
LEFT JOIN excp.dim_chargeback_status e ON s.statuscode = e.chargeback_status_code
    AND e.meta_is_current_excp AND e.meta_data_quality = 'error'
WHERE (statuscode BETWEEN
		(SELECT range_start FROM lake_consolidated.ultra_merchant.statuscode_category WHERE label = 'Payment Transaction Codes')
		AND
		(SELECT range_end FROM lake_consolidated.ultra_merchant.statuscode_category WHERE label = 'Payment Transaction Codes')
	) OR e.chargeback_status_code IS NOT NULL;


INSERT INTO stg.dim_chargeback_status_stg
(
	chargeback_status_code,
	chargeback_status,
	meta_create_datetime,
	meta_update_datetime
)
SELECT chargeback_status_code,
	chargeback_status,
	$execution_start_time,
	$execution_start_time
FROM _chargeback_status_base;
