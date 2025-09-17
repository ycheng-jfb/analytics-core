SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.dim_return_status';

SET wm_lake_ultra_merchant_statuscode = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.statuscode'));
CREATE OR REPLACE TEMP TABLE _stg_return_status AS
SELECT
	 COALESCE(s.statuscode, -1) AS return_status_code,
	 COALESCE(s.label, 'Unknown') AS return_status
FROM lake_consolidated.ultra_merchant.statuscode s
WHERE statuscode BETWEEN
	(SELECT range_start FROM
	lake_consolidated.ultra_merchant.statuscode_category
	WHERE LOWER(label) = 'return codes')
	AND
	(SELECT range_end FROM
	lake_consolidated.ultra_merchant.statuscode_category
	WHERE LOWER(label) = 'return codes')
AND s.meta_update_datetime > $wm_lake_ultra_merchant_statuscode

UNION

SELECT
	return_status_code,
	COALESCE(s.label, 'Unknown') AS return_status
FROM excp.dim_return_status se
JOIN lake_consolidated.ultra_merchant.statuscode s ON se.return_status_code=s.statuscode
WHERE se.meta_is_current_excp AND se.meta_data_quality = 'error'
	AND statuscode BETWEEN
	(SELECT range_start FROM
	lake_consolidated.ultra_merchant.statuscode_category
	WHERE lower(label) = 'return codes')
	AND
	(SELECT range_end FROM
	lake_consolidated.ultra_merchant.statuscode_category
	WHERE lower(label) = 'return codes');

INSERT INTO stg.dim_return_status_stg
(
	return_status_code,
	return_status,
	meta_create_datetime,
	meta_update_datetime
)
SELECT
	return_status_code,
    return_status,
	$execution_start_time,
	$execution_start_time
FROM _stg_return_status;
