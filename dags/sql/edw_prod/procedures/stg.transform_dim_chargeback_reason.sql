SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _chargeback_reason_base AS
WITH reasons AS
(
	SELECT LOWER(reason_text) AS chargeback_reason
	FROM lake_consolidated.ultra_merchant.psp_notification_log
	WHERE UPPER(transaction_type) = 'CHARGEBACK'
	UNION
	SELECT LOWER(comments)
	FROM lake_consolidated.ultra_merchant.order_chargeback_log
	UNION
	SELECT LOWER(reason_description)
	FROM lake.oracle_ebs.chargeback_us
    UNION
	SELECT LOWER(reason_description)
	FROM lake.oracle_ebs.chargeback_eu
	UNION
	SELECT LOWER(chargeback_reason)
	FROM excp.dim_chargeback_reason
	WHERE meta_is_current_excp
    AND meta_data_quality = 'error'
)

SELECT chargeback_reason
FROM reasons;

INSERT INTO stg.dim_chargeback_reason_stg
(
	chargeback_reason,
	meta_create_datetime,
	meta_update_datetime
)
SELECT chargeback_reason,
	$execution_start_time,
	$execution_start_time
FROM _chargeback_reason_base;
