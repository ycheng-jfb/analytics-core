SET target_table = 'stg.dim_payment';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
ALTER SESSION SET QUERY_TAG = $target_table;

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
SET wm_lake_ultra_merchant_creditcard = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.creditcard'));
SET wm_lake_ultra_merchant_payment_transaction_creditcard = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.payment_transaction_creditcard'));
SET wm_lake_ultra_merchant_payment_transaction_creditcard_data = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.payment_transaction_creditcard_data'));
SET wm_lake_ultra_merchant_psp = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.psp'));
SET wm_lake_ultra_merchant_payment_transaction_psp = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.payment_transaction_psp'));

/*
SELECT
    $wm_lake_ultra_merchant_order,
    $wm_lake_ultra_merchant_creditcard,
    $wm_lake_ultra_merchant_payment_transaction_creditcard,
    $wm_lake_ultra_merchant_payment_transaction_creditcard_data,
    $wm_lake_ultra_merchant_psp,
    $wm_lake_ultra_merchant_payment_transaction_psp;
*/

CREATE OR REPLACE TEMP TABLE _dim_payment__payment_types AS
WITH payment_types AS (
    SELECT DISTINCT
        o.payment_method,
        cc.card_type AS creditcard_type,
        IFF(ptc.order_id IS NOT NULL, COALESCE(TO_BOOLEAN(ptcd.is_prepaid), FALSE), FALSE) AS is_prepaid_creditcard,
        IFF(LOWER(od.name) = 'masterpass_checkout_payment_id', TRUE, FALSE) AS is_mastercard_checkout,
        IFF(LOWER(od.name) = 'visa_checkout_call_id', TRUE, FALSE) AS is_visa_checkout,
        COALESCE(IFF(ascii(ptcd.funding_type) = 0,NULL,ptcd.funding_type),'Unknown') AS funding_type,
        COALESCE(IFF(ascii(ptcd.prepaid_type) = 0,NULL,ptcd.prepaid_type),'Unknown') AS prepaid_type,
        COALESCE(IFF(ascii(ptcd.card_product_type) = 0,NULL,ptcd.card_product_type),'Unknown') AS card_product_type
    FROM lake_consolidated.ultra_merchant.creditcard AS cc
        JOIN lake_consolidated.ultra_merchant.payment_transaction_creditcard AS ptc
             ON ptc.creditcard_id = cc.creditcard_id
            AND ptc.statuscode = 4001
            AND LOWER(ptc.transaction_type) IN ('prior_auth_capture', 'sale_redirect')
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
             ON o.order_id = ptc.order_id
        LEFT JOIN (
            SELECT order_id, NAME
            FROM lake_consolidated.ultra_merchant.order_detail
            WHERE LOWER(NAME) IN ('masterpass_checkout_payment_id', 'visa_checkout_call_id')
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY datetime_added DESC) = 1
            ) AS od
            ON od.order_id = o.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.payment_transaction_creditcard_data AS ptcd
            ON ptcd.payment_transaction_id = ptc.original_payment_transaction_id
    WHERE COALESCE(cc.card_type, '') <> ''
        AND ($is_full_refresh
            OR o.meta_update_datetime > $wm_lake_ultra_merchant_order
            OR cc.meta_update_datetime > $wm_lake_ultra_merchant_creditcard
            OR ptc.meta_update_datetime > $wm_lake_ultra_merchant_payment_transaction_creditcard
            OR ptcd.meta_update_datetime > $wm_lake_ultra_merchant_payment_transaction_creditcard_data)
    UNION ALL
    SELECT DISTINCT
        o.payment_method,
        cc.card_type AS creditcard_type,
        IFF(ptc.order_id IS NOT NULL, COALESCE(TO_BOOLEAN(ptcd.is_prepaid), FALSE), FALSE) AS is_prepaid_creditcard,
        IFF(LOWER(od.name) = 'masterpass_checkout_payment_id', TRUE, FALSE) AS is_mastercard_checkout,
        IFF(LOWER(od.name) = 'visa_checkout_call_id', TRUE, FALSE) AS is_visa_checkout,
        COALESCE(IFF(ascii(ptcd.funding_type) = 0,NULL,ptcd.funding_type),'Unknown') AS funding_type,
        COALESCE(IFF(ascii(ptcd.prepaid_type) = 0,NULL,ptcd.prepaid_type),'Unknown') AS prepaid_type,
        COALESCE(IFF(ascii(ptcd.card_product_type) = 0,NULL,ptcd.card_product_type),'Unknown') AS card_product_type
    FROM lake_consolidated.ultra_merchant.creditcard AS cc
        JOIN lake_consolidated.ultra_merchant.payment_transaction_creditcard AS ptc
             ON ptc.creditcard_id = cc.creditcard_id
            AND LOWER(ptc.transaction_type) = 'auth_only'
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
             ON o.order_id = ptc.order_id
        LEFT JOIN (
            SELECT order_id, NAME
            FROM lake_consolidated.ultra_merchant.order_detail
            WHERE LOWER(NAME) IN ('masterpass_checkout_payment_id', 'visa_checkout_call_id')
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY datetime_added DESC) = 1
            ) AS od
            ON od.order_id = o.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.payment_transaction_creditcard_data AS ptcd
            ON ptcd.payment_transaction_id = ptc.payment_transaction_id
    WHERE COALESCE(cc.card_type, '') <> ''
        AND ($is_full_refresh
            OR o.meta_update_datetime > $wm_lake_ultra_merchant_order
            OR cc.meta_update_datetime > $wm_lake_ultra_merchant_creditcard
            OR ptc.meta_update_datetime > $wm_lake_ultra_merchant_payment_transaction_creditcard
            OR ptcd.meta_update_datetime > $wm_lake_ultra_merchant_payment_transaction_creditcard_data)
    UNION ALL
    SELECT DISTINCT
        o.payment_method,
        p.type AS creditcard_type,
        FALSE AS is_prepaid_creditcard,
        IFF(LOWER(od.name) = 'masterpass_checkout_payment_id', TRUE, FALSE) AS is_mastercard_checkout,
        IFF(LOWER(od.name) = 'visa_checkout_call_id', TRUE, FALSE) AS is_visa_checkout,
        'Unknown' AS funding_type,
        'Unknown' AS prepaid_type,
        'Unknown' AS card_product_type
    FROM lake_consolidated.ultra_merchant.psp AS p
        JOIN lake_consolidated.ultra_merchant.payment_transaction_psp AS ptp
            ON ptp.psp_id = p.psp_id
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.order_id = ptp.order_id
        LEFT JOIN (
            SELECT order_id, name
            FROM lake_consolidated.ultra_merchant.order_detail
            WHERE LOWER(name) IN ('masterpass_checkout_payment_id', 'visa_checkout_call_id')
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY datetime_added DESC) = 1
            ) AS od
            ON od.order_id = o.order_id
    WHERE COALESCE(p.type, '') <> ''
        AND ($is_full_refresh
            OR o.meta_update_datetime > $wm_lake_ultra_merchant_order
            OR p.meta_update_datetime > $wm_lake_ultra_merchant_psp
            OR ptp.meta_update_datetime > $wm_lake_ultra_merchant_payment_transaction_psp)
    UNION ALL
    SELECT 'Cash', 'Not Applicable', FALSE, FALSE, FALSE, 'Unknown', 'Unknown', 'Unknown'
    UNION ALL
    SELECT 'None', 'Not Applicable', FALSE, FALSE, FALSE, 'Unknown', 'Unknown', 'Unknown'
	UNION ALL
    SELECT 'Credit Card', 'Not Applicable', FALSE, FALSE, FALSE, 'Unknown', 'Unknown', 'Unknown'
    UNION ALL
    SELECT 'Credit Card', 'Unknown', FALSE, FALSE, FALSE, 'Unknown', 'Unknown', 'Unknown'
	UNION ALL
    SELECT 'Psp', 'Not Applicable', FALSE, FALSE, FALSE, 'Unknown', 'Unknown', 'Unknown'
    UNION ALL
    SELECT 'Psp', 'Unknown', FALSE, FALSE, FALSE, 'Unknown', 'Unknown', 'Unknown'
    UNION ALL
    SELECT DISTINCT
        payment_method,
        creditcard_type,
        is_prepaid_creditcard,
        is_mastercard_checkout,
        is_visa_checkout,
        funding_type,
        prepaid_type,
        card_product_type
    FROM excp.dim_payment
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    )
SELECT DISTINCT
    CASE
        WHEN LOWER(payment_method)='moneyorder' THEN 'Money Order'
        WHEN LOWER(payment_method)='creditcard' THEN 'Credit Card'
        ELSE INITCAP(payment_method)
    END AS payment_method,
    CASE
        WHEN LOWER(creditcard_type) IN ('mastercard', 'mc') THEN 'Master Card'
        WHEN LOWER(creditcard_type) IN ('vi', 'visadankort', 'visasaraivacard') THEN 'Visa'
        WHEN LOWER(creditcard_type) = 'paypallitle' THEN 'Paypal'
        WHEN LOWER(creditcard_type) IN ('bijcard', 'directdebit_nl', 'ideal', 'cartebancaire', 'elv', 'sepadirectdebit', 'diners') THEN 'Other'
        ELSE INITCAP(creditcard_type)
    END AS raw_creditcard_type,
    CASE
        WHEN raw_creditcard_type ILIKE '%%Master Card%%' OR raw_creditcard_type ILIKE 'mc%' THEN 'Mastercard'
        WHEN raw_creditcard_type ILIKE '%%amex%%' THEN 'American Express'
        WHEN raw_creditcard_type ILIKE '%%paypal%%' THEN 'Paypal'
        WHEN raw_creditcard_type ILIKE 'AP_%%' THEN INITCAP(SUBSTR(raw_creditcard_type,4))
        WHEN raw_creditcard_type ILIKE 'visa%' THEN 'Visa'
        ELSE raw_creditcard_type
    END AS creditcard_type,
    IFF(raw_creditcard_type ILIKE 'AP_%%', TRUE, FALSE) AS is_applepay,
    is_prepaid_creditcard,
    IFF(is_applepay, FALSE, is_mastercard_checkout) AS is_mastercard_checkout,
    IFF(is_applepay OR raw_creditcard_type ILIKE '%%paypal%%', FALSE, is_visa_checkout) AS is_visa_checkout,
    INITCAP(LOWER(funding_type)) AS funding_type,
    INITCAP(LOWER(prepaid_type)) AS prepaid_type,
    INITCAP(LOWER(card_product_type)) AS card_product_type
FROM payment_types;
-- SELECT * FROM _dim_payment__payment_types;

-- Add new data to the reference.payment_method_mapping table
INSERT INTO reference.payment_method_mapping (
    payment_method,
    raw_creditcard_type,
    creditcard_type,
    is_prepaid_creditcard,
    funding_type,
    prepaid_type,
    card_product_type
    )
SELECT DISTINCT
    p.payment_method,
    p.raw_creditcard_type,
    p.creditcard_type,
    IFF(p.is_prepaid_creditcard, TRUE, FALSE) AS is_prepaid_creditcard,
    p.funding_type,
    p.prepaid_type,
    p.card_product_type
FROM _dim_payment__payment_types AS p
    LEFT JOIN reference.payment_method_mapping AS m
        ON UPPER(m.payment_method) = UPPER(p.payment_method)
        AND UPPER(m.raw_creditcard_type) = UPPER(p.raw_creditcard_type)
        AND m.is_prepaid_creditcard = p.is_prepaid_creditcard
        AND UPPER(m.funding_type) = UPPER(p.funding_type)
        AND UPPER(m.prepaid_type) = UPPER(p.prepaid_type)
        AND UPPER(m.card_product_type) = UPPER(p.card_product_type)
WHERE m.payment_method IS NULL;

INSERT INTO stg.dim_payment_stg (
    payment_method,
    raw_creditcard_type,
    creditcard_type,
    is_applepay,
    is_prepaid_creditcard,
    is_mastercard_checkout,
    is_visa_checkout,
    funding_type,
    prepaid_type,
    card_product_type,
	meta_create_datetime,
    meta_update_datetime
    )
SELECT DISTINCT
    p.payment_method,
    p.raw_creditcard_type,
    p.creditcard_type,
    p.is_applepay,
    p.is_prepaid_creditcard,
    p.is_mastercard_checkout,
    p.is_visa_checkout,
    p.funding_type,
    p.prepaid_type,
    p.card_product_type,
    $execution_start_time,
	$execution_start_time
FROM _dim_payment__payment_types AS p;
