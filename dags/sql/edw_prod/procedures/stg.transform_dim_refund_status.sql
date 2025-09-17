SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.dim_refund_status';

SET wm_lake_ultra_merchant_statuscode = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.statuscode'));

CREATE OR REPLACE TEMP TABLE _stg_refund_status AS
WITH status_code AS
(
	SELECT
	DISTINCT statuscode AS refund_status_code,
	label AS refund_status
	FROM lake_consolidated.ultra_merchant.statuscode
	WHERE statuscode BETWEEN
		(select range_start
		from lake_consolidated.ultra_merchant.statuscode_category
		where label = 'Refund Codes')
		AND
		(select range_end
		from lake_consolidated.ultra_merchant.statuscode_category
		where label = 'Refund Codes')
	AND meta_update_datetime > $wm_lake_ultra_merchant_statuscode

)
SELECT
    refund_status_code,
	refund_status
FROM status_code

UNION

SELECT
    statuscode AS refund_status_code,
	label AS refund_status
FROM excp.dim_refund_status se
JOIN 	lake_consolidated.ultra_merchant.statuscode s	ON se.refund_status_code=s.statuscode
	WHERE statuscode BETWEEN
		(select range_start
		from lake_consolidated.ultra_merchant.statuscode_category
		where label = 'Refund Codes')
		AND
		(select range_end
		from lake_consolidated.ultra_merchant.statuscode_category
		where label = 'Refund Codes'
		)
		AND se.meta_is_current_excp
        AND se.meta_data_quality = 'error';

INSERT INTO stg.dim_refund_status_stg
(
	refund_status_code,
	refund_status,
	meta_create_datetime,
	meta_update_datetime
)

SELECT
	refund_status_code,
	refund_status,
	$execution_start_time,
	$execution_start_time
FROM _stg_refund_status;
