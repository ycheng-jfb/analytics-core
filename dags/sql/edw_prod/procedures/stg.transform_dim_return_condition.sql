SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.dim_return_condition';

SET wm_lake_ultra_merchant_return_disposition = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.return_disposition'));
CREATE OR REPLACE TEMP TABLE _stg_return_condition AS
WITH conditions AS
(
	SELECT
	    DISTINCT d.return_disposition,
		c.return_condition
	FROM
	(
		SELECT DISTINCT
			label return_disposition
		FROM lake_consolidated.ultra_merchant.return_disposition rd
		WHERE rd.meta_update_datetime > $wm_lake_ultra_merchant_return_disposition
		UNION
		SELECT 'NA' return_disposition
	) d
	CROSS JOIN
	(
		SELECT 'New' as return_condition
		UNION
		SELECT 'Used'
		UNION
		SELECT 'Damaged'
	) c
)
SELECT
    return_disposition,
    return_condition
FROM conditions
UNION
SELECT
    return_disposition,
    return_condition
FROM excp.dim_return_condition
WHERE meta_is_current_excp
AND meta_data_quality = 'error';

INSERT INTO stg.dim_return_condition_stg
(
	return_condition,
	return_disposition,
	meta_create_datetime,
	meta_update_datetime
)
SELECT
	return_condition,
	return_disposition,
	$execution_start_time,
	$execution_start_time
FROM _stg_return_condition;
