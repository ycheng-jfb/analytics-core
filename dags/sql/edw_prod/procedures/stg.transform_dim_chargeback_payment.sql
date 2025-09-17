SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.dim_chargeback_payment';


SET wm_lake_ultra_merchant_psp_notification_log = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.psp_notification_log'));
SET wm_lake_ultra_merchant_payment_transaction_psp = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.payment_transaction_psp'));
SET wm_lake_ultra_merchant_creditcard = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.creditcard'));
SET wm_lake_ultra_merchant_payment = (SELECT  stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.payment'));
SET wm_lake_oracle_ebs_chargeback_us = (SELECT stg.udf_get_watermark($target_table, 'lake.oracle_ebs.chargeback_us'));
SET wm_lake_oracle_ebs_chargeback_eu = (SELECT stg.udf_get_watermark($target_table, 'lake.oracle_ebs.chargeback_eu'));


CREATE OR REPLACE TEMP TABLE _previous_errors AS
SELECT
	chargeback_payment_processor,
	chargeback_payment_method,
	chargeback_payment_type,
	chargeback_payment_bank
FROM excp.dim_chargeback_payment
WHERE meta_is_current_excp
AND meta_data_quality = 'error';

CREATE OR REPLACE TEMPORARY TABLE _chargeback_payment_processor
(
	chargeback_payment_processor VARCHAR(20)
);

INSERT INTO _chargeback_payment_processor
SELECT 'Adyen' AS chargeback_payment_processor
UNION
SELECT 'Litle' AS chargeback_payment_processor
UNION
SELECT CASE WHEN LOWER(source) = 'payal' OR LOWER(source) LIKE 'paypal%' THEN 'Paypal'
           ELSE INITCAP(source)
           END AS chargeback_payment_processor
FROM lake.oracle_ebs.chargeback_eu
WHERE meta_update_datetime > $wm_lake_oracle_ebs_chargeback_eu
UNION
SELECT CASE WHEN LOWER(source) = 'payal' OR LOWER(source) LIKE 'paypal%' THEN 'Paypal'
           ELSE INITCAP(source)
           END AS chargeback_payment_processor
FROM lake.oracle_ebs.chargeback_us
WHERE meta_update_datetime > $wm_lake_oracle_ebs_chargeback_us
UNION
SELECT 'Unknown' AS chargeback_payment_processor;

CREATE OR REPLACE TEMPORARY TABLE _chargeback_payment_method
(
	chargeback_payment_method VARCHAR(50)
);

INSERT INTO _chargeback_payment_method
SELECT 'Unknown' AS chargeback_payment_method
UNION
SELECT payment_method AS chargeback_payment_method
FROM lake_consolidated.ultra_merchant.psp_notification_log
WHERE payment_method IS NOT NULL
	AND payment_method NOT IN ('', CHAR(0))
	AND meta_update_datetime > $wm_lake_ultra_merchant_psp_notification_log
UNION
SELECT response_payment_method AS chargeback_payment_method
FROM lake_consolidated.ultra_merchant.payment_transaction_psp
WHERE response_payment_method IS NOT NULL
	AND response_payment_method NOT IN ('', CHAR(0))
	AND meta_update_datetime > $wm_lake_ultra_merchant_payment_transaction_psp
UNION
SELECT card_type AS chargeback_payment_method
FROM lake_consolidated.ultra_merchant.creditcard
WHERE card_type NOT IN ('', CHAR(0))
	AND card_type IS NOT NULL
	AND meta_update_datetime > $wm_lake_ultra_merchant_creditcard
UNION
SELECT payment_type AS chargeback_payment_method
FROM lake.oracle_ebs.chargeback_us
WHERE payment_type IS NOT NULL
    AND meta_update_datetime > $wm_lake_oracle_ebs_chargeback_us
UNION
SELECT payment_type AS chargeback_payment_method
FROM lake.oracle_ebs.chargeback_eu
WHERE payment_type IS NOT NULL
    AND meta_update_datetime > $wm_lake_oracle_ebs_chargeback_eu
UNION
SELECT chargeback_payment_method
FROM stg.dim_chargeback_payment
UNION
SELECT chargeback_payment_method
FROM _previous_errors;

CREATE OR REPLACE TEMPORARY TABLE _chargeback_payment_type
(
	chargeback_payment_type VARCHAR(50)
);

INSERT INTO _chargeback_payment_type
SELECT 'Unknown' AS chargeback_payment_type
UNION
SELECT payment_method AS chargeback_payment_type
FROM lake_consolidated.ultra_merchant.payment
WHERE payment_method IS NOT NULL
	AND payment_method <> ''
	AND meta_update_datetime > $wm_lake_ultra_merchant_payment
UNION
SELECT chargeback_payment_type
FROM stg.dim_chargeback_payment
UNION
SELECT chargeback_payment_type
FROM _previous_errors;

CREATE OR REPLACE TEMPORARY TABLE _chargeback_payment_bank
(
	chargeback_payment_bank VARCHAR(1000)
);

INSERT INTO _chargeback_payment_bank
SELECT 'Unknown' AS chargeback_payment_bank
UNION
SELECT issuing_bank AS chargeback_payment_bank
FROM lake.oracle_ebs.chargeback_eu
WHERE issuing_bank IS NOT NULL
    AND meta_update_datetime > $wm_lake_oracle_ebs_chargeback_eu
UNION
SELECT issuing_bank AS chargeback_payment_bank
FROM lake.oracle_ebs.chargeback_us
WHERE issuing_bank IS NOT NULL
    AND meta_update_datetime > $wm_lake_oracle_ebs_chargeback_us
UNION
SELECT chargeback_payment_bank
FROM _previous_errors;

CREATE OR REPLACE TEMP TABLE _chargeback_payment_base AS
SELECT
	chargeback_payment_processor,
	chargeback_payment_method,
	chargeback_payment_type,
	chargeback_payment_bank
FROM
(
	SELECT DISTINCT
		chargeback_payment_processor,
		INITCAP(chargeback_payment_method) AS chargeback_payment_method,
		INITCAP(chargeback_payment_type) AS chargeback_payment_type,
		INITCAP(chargeback_payment_bank) AS chargeback_payment_bank
	FROM _chargeback_payment_processor
	CROSS JOIN _chargeback_payment_method
	CROSS JOIN _chargeback_payment_type
	CROSS JOIN _chargeback_payment_bank
	WHERE NOT(chargeback_payment_processor = 'Unknown'
		AND chargeback_payment_method = 'Unknown'
		AND chargeback_payment_type = 'Unknown'
	    AND chargeback_payment_bank = 'Unknown')
) t1;

INSERT INTO stg.dim_chargeback_payment_stg
(
	chargeback_payment_processor
	,chargeback_payment_method
	,chargeback_payment_type
	,chargeback_payment_bank
	,meta_create_datetime
	,meta_update_datetime
)
SELECT
	chargeback_payment_processor
	,chargeback_payment_method
	,chargeback_payment_type
    ,chargeback_payment_bank
	,$execution_start_time
	,$execution_start_time
FROM _chargeback_payment_base;
