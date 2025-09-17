SET process_from_date = (
        SELECT COALESCE(DATEADD('MONTH', -5, MAX(order_month)), '2019-01-01')
        FROM edw_prod.reporting.payment_dataset_final_output
    );

SET datetime_added = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3);


CREATE OR REPLACE TEMPORARY TABLE _fleu_retail_orders AS
SELECT fo.order_id
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_store ds ON ds.store_id = fo.store_id
    AND ds.store_type = 'Retail'
    AND store_region = 'EU'
WHERE fo.order_local_datetime::DATE >= GREATEST('2021-03-07'::DATE, $process_from_date);

CREATE OR REPLACE TEMPORARY TABLE _payment_method_order_base AS
SELECT fo.order_id, dp.payment_method, dp.raw_creditcard_type, dp.creditcard_type
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_store ds ON ds.store_id = fo.store_id
    AND ds.store_region = 'EU'
JOIN edw_prod.data_model_jfb.dim_payment dp ON dp.payment_key = fo.payment_key
WHERE order_local_datetime::DATE >= $process_from_date
    AND dp.payment_method = 'Psp'
    AND raw_creditcard_type IN ('Other', 'Unknown');

CREATE OR REPLACE TEMPORARY TABLE _all_transactions AS
SELECT ob.order_id, MAX(p.type) AS creditcard_type
FROM _payment_method_order_base ob
JOIN lake_jfb.ultra_merchant.payment_transaction_psp psp ON psp.order_id = ob.order_id
LEFT JOIN _fleu_retail_orders fro ON ob.order_id = fro.order_id
JOIN lake_jfb.ultra_merchant.psp p ON p.psp_id = psp.psp_id
WHERE LOWER(psp.transaction_type) IN ('prior_auth_capture', 'sale_redirect', IFF(fro.order_id IS NOT NULL, 'auth_redirect', 'sale_redirect'))
		AND psp.statuscode IN (4001, 4040) /* Success and 4040 chargeback */
GROUP BY ob.order_id;

INSERT INTO _all_transactions
SELECT ob.order_id,
       p.type AS creditcard_type
FROM _payment_method_order_base ob
JOIN lake_jfb.ultra_merchant.payment_transaction_psp psp ON psp.order_id = ob.order_id
JOIN lake_jfb.ultra_merchant.psp p ON p.psp_id = psp.psp_id
LEFT JOIN _all_transactions so ON so.order_id = ob.order_id
WHERE so.order_id IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ob.order_id ORDER BY
            CASE LOWER(psp.transaction_type) WHEN 'auth_only' THEN 1
                WHEN 'prior_auth_capture' THEN 2
                WHEN 'auth_sepa' THEN 3
                WHEN 'credit' THEN 4
                WHEN 'auth_redirect' THEN 5
            END, psp.datetime_modified DESC) = 1;

CREATE OR REPLACE TEMPORARY TABLE _updated_payment_methods AS
SELECT t.order_id,
    CASE WHEN LOWER(t.creditcard_type) IN ('bijcard', 'mcsuperpremiumdebit', 'mcsuperpremiumcredit', 'mcstandarddebit', 'mcstandardcredit', 'mcpurchasingcredit',
                                           'mcpremiumdebit', 'mcpremiumcredit', 'mcfleetcredit', 'mcdebit', 'mccredit', 'mccorporatedebit', 'mccorporatecredit',
                                           'mccommercialdebit', 'mccommercialcredit', 'mastercard', 'mc', 'maestro') THEN 'Mastercard'
    WHEN LOWER(t.creditcard_type) IN ('braintreepaypal', 'paypallitle', 'paypal') THEN 'Paypal'
    WHEN LOWER(t.creditcard_type) IN ('cartebancaire') THEN 'Carte Bancaire'
    WHEN LOWER(t.creditcard_type) IN ('directdebit_nl', 'elv', 'sepadirectdebit', 'sepaadyenapi') THEN 'Direct Debit'
    WHEN LOWER(t.creditcard_type) IN ('ideal') THEN 'iDeal'
    WHEN LOWER(t.creditcard_type) IN ('vpay', 'visasuperpremiumdebit', 'visasuperpremiumcredit', 'visastandarddebit', 'visastandardcredit', 'visapurchasingdebit',
                                    'visapurchasingcredit', 'visapremiumdebit', 'visapremiumcredit', 'visacorporatedebit', 'visacorporatecredit',
                                    'visacommercialsuperpremiumdebit', 'visacommercialsuperpremiumcredit', 'visacommercialpremiumdebit', 'visacommercialpremiumcredit',
                                    'visacommercialdebit', 'visacommercialcredit', 'visadankort', 'visasaraivacard', 'visa', 'electron') THEN 'Visa'
    END AS creditcard_type
FROM _all_transactions t;

CREATE OR REPLACE TEMPORARY TABLE _updated_creditcard AS
SELECT DISTINCT DATE_TRUNC('MONTH', date_next_billing_start)::DATE AS billing_month_date,
    updated_creditcard_id AS creditcard_id,
    cub.gateway_account_id
FROM lake_jfb.ultra_merchant.creditcard_update_batch cub
JOIN lake_jfb.ultra_merchant.creditcard_update_transaction cut ON cut.creditcard_update_batch_id = cub.creditcard_update_batch_id
    AND updated_creditcard_id IS NOT NULL;

CREATE OR REPLACE TEMPORARY TABLE _psp_rnk AS
SELECT fo.order_id,
    gateway_name,
    sc.label,
    ptc.response_reason_text,
    ptc.response_result_text,
    ptc.amount,
    ROW_NUMBER() OVER(PARTITION BY fo.order_id ORDER BY amount DESC, ptc.datetime_added DESC, ptc.datetime_modified DESC) AS rnk
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing', 'Product Order')
JOIN edw_prod.data_model_jfb.dim_order_status dos ON dos.order_status_key = fo.order_status_key
    AND dos.order_status <> 'Success'
JOIN lake_jfb.ultra_merchant.payment_transaction_psp ptc ON ptc.order_id = fo.order_id
    AND ptc.transaction_type IN ('AUTH_ONLY', 'PRIOR_AUTH_CAPTURE', 'SALE_REDIRECT')
JOIN lake_jfb.ultra_merchant.statuscode sc ON sc.statuscode = ptc.statuscode
WHERE fo.order_local_datetime::DATE >= $process_from_date;

CREATE OR REPLACE TEMPORARY TABLE _psp AS
SELECT fo.order_id,
    MAX(pr.amount) AS payment_amount,
    MAX(pr.gateway_name) AS payment_gateway,
    MAX(pr.label) AS payment_status,
    MAX(pr.response_result_text) AS response_result_text,
    MAX(pr.response_reason_text) AS response_reason_text,
    MAX(ptc.datetime_modified) AS datetime_modified,
    COUNT(IFF(transaction_type = 'AUTH_ONLY', fo.order_id, NULL)) AS auth_only,
    COUNT(IFF(transaction_type = 'SALE_REDIRECT', fo.order_id, NULL)) AS sale_redirect,
    COUNT(IFF(transaction_type = 'VOID', fo.order_id, NULL)) AS void,
    COUNT(IFF(transaction_type = 'PRIOR_AUTH_CAPTURE', fo.order_id, NULL)) AS prior_auth_capture,
    COUNT(IFF(transaction_type = 'AUTH_REDIRECT', fo.order_id, NULL)) AS auth_redirect,
    COUNT(IFF(transaction_type = 'CREDIT', fo.order_id, NULL)) AS credit,
    COUNT(IFF(transaction_type = 'AUTH_SEPA', fo.order_id, NULL)) AS auth_sepa,
    COUNT(IFF(transaction_type = 'INIT_SESSION', fo.order_id, NULL)) AS init_session,
    COUNT(IFF(transaction_type = 'VERIFY', fo.order_id, NULL)) AS verify
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing', 'Product Order')
JOIN lake_jfb.ultra_merchant.payment_transaction_psp ptc ON ptc.order_id = fo.order_id
LEFT JOIN _psp_rnk pr ON pr.order_id = fo.order_id
    AND pr.rnk = 1
WHERE fo.order_local_datetime::DATE >= $process_from_date
GROUP BY fo.order_id;

CREATE OR REPLACE TEMPORARY TABLE _credit_card_rnk AS
SELECT fo.order_id,
    gateway_name,
    sc.label,
    ptc.response_reason_text,
    ptc.response_result_text,
    ptc.amount,
    ROW_NUMBER() OVER(PARTITION BY fo.order_id ORDER BY amount DESC, ptc.datetime_added DESC, ptc.datetime_modified DESC) AS rnk
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing', 'Product Order')
JOIN edw_prod.data_model_jfb.dim_order_status dos ON dos.order_status_key = fo.order_status_key
    AND dos.order_status <> 'Success'
JOIN lake_jfb.ultra_merchant.payment_transaction_creditcard ptc ON ptc.order_id = fo.order_id
    AND ptc.transaction_type IN ('AUTH_ONLY', 'PRIOR_AUTH_CAPTURE', 'SALE_REDIRECT')
JOIN lake_jfb.ultra_merchant.statuscode sc ON sc.statuscode = ptc.statuscode
WHERE fo.order_local_datetime::DATE >= $process_from_date;

CREATE OR REPLACE TEMPORARY TABLE _credit_card AS
SELECT fo.order_id,
    MAX(ccr.amount) AS payment_amount,
    MAX(ccr.gateway_name) AS payment_gateway,
    MAX(ccr.label) AS payment_status,
    MAX(ccr.response_result_text) AS response_result_text,
    MAX(ccr.response_reason_text) AS response_reason_text,
    MAX(ptc.datetime_modified) AS datetime_modified,
    MAX(uc.gateway_account_id) AS gateway_account_id,
    MAX(IFF(uc.creditcard_id IS NOT NULL, TRUE, FALSE)) AS is_billed_on_updated_card,
    COUNT(IFF(transaction_type = 'AUTH_ONLY', fo.order_id, NULL)) AS auth_only,
    COUNT(IFF(transaction_type = 'SALE_REDIRECT', fo.order_id, NULL)) AS sale_redirect,
    COUNT(IFF(transaction_type = 'VOID', fo.order_id, NULL)) AS void,
    COUNT(IFF(transaction_type = 'PRIOR_AUTH_CAPTURE', fo.order_id, NULL)) AS prior_auth_capture,
    COUNT(IFF(transaction_type = 'AUTH_REVERSE', fo.order_id, NULL)) AS auth_reverse,
    COUNT(IFF(transaction_type = 'CREDIT', fo.order_id, NULL)) AS credit,
    COUNT(IFF(transaction_type = 'AUTH_DEFER', fo.order_id, NULL)) AS auth_defer
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing', 'Product Order')
JOIN lake_jfb.ultra_merchant.payment_transaction_creditcard ptc ON ptc.order_id = fo.order_id
LEFT JOIN _updated_creditcard uc ON uc.creditcard_id = ptc.creditcard_id
    AND uc.gateway_account_id = ptc.gateway_account_id
    AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing')
    AND uc.billing_month_date = DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE
LEFT JOIN _credit_card_rnk ccr ON ccr.order_id = fo.order_id
    AND ccr.rnk = 1
WHERE fo.order_local_datetime::DATE >= $process_from_date
GROUP BY fo.order_id;

CREATE OR REPLACE TEMPORARY TABLE _cash AS
SELECT fo.order_id,
    MAX(ptc.datetime_modified) AS datetime_modified,
    COUNT(IFF(transaction_type = 'SALE', fo.order_id, NULL)) AS sale,
    COUNT(IFF(transaction_type = 'CREDIT', fo.order_id, NULL)) AS credit
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing', 'Product Order')
JOIN lake_jfb.ultra_merchant.payment_transaction_cash ptc ON ptc.order_id = fo.order_id
WHERE fo.order_local_datetime::DATE >= $process_from_date
GROUP BY fo.order_id;

CREATE OR REPLACE TEMPORARY TABLE _retry_schedule_queue AS
SELECT brsq.order_id,
    MAX(cycle) AS cycle
FROM edw_prod.data_model_jfb.fact_order fo
JOIN lake_jfb_view.ultra_merchant.billing_retry_schedule_queue brsq ON brsq.order_id = fo.order_id
JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing')
JOIN lake_jfb_view.ultra_merchant.billing_retry_schedule_queue_log brsql ON brsql.billing_retry_schedule_queue_id = brsq.billing_retry_schedule_queue_id
WHERE fo.order_local_datetime::DATE >= $process_from_date
GROUP BY brsq.order_id;

BEGIN;

DELETE FROM reporting.payment_dataset_final_output
WHERE order_date >= $process_from_date;

INSERT INTO reporting.payment_dataset_final_output(
    order_month,
    order_date,
    payment_date,
    store_full_name,
    store_country,
    store_region,
    store_brand,
    store_type,
    payment_method,
    creditcard_type,
    order_status,
    order_classification_l1,
    order_classification_l2,
    membership_order_type_l1,
    membership_order_type_l2,
    membership_order_type_l3,
    cycle,
    is_retry,
    gateway_account_id,
    is_billed_on_updated_card,
    payment_gateway,
    transaction_status,
    response_result_text,
    response_reason_text,
    orders,
    payment_transaction_eur_amount,
    payment_transaction_usd_amount,
    auth_only_attempts,
    auth_only_orders,
    sale_redirect_attempts,
    sale_redirect_orders,
    void_attempts,
    void_orders,
    prior_auth_capture_attempts,
    prior_auth_capture_orders,
    auth_redirect_attempts,
    auth_redirect_orders,
    credit_attempts,
    credit_orders,
    auth_sepa_attempts,
    auth_sepa_orders,
    init_session_attempts,
    init_session_orders,
    verify_attempts,
    verify_orders,
    auth_reverse_attempts,
    auth_reverse_orders,
    auth_defer_attempts,
    auth_defer_orders,
    sale_attempts,
    sale_orders,
    meta_create_datetime,
    meta_update_datetime
)
SELECT DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE AS order_month,
    fo.order_local_datetime::DATE as order_date,
    COALESCE(fo.payment_transaction_local_datetime, fo.order_completion_local_datetime, psp.datetime_modified, cc.datetime_modified, c.datetime_modified, fo.order_local_datetime)::DATE AS payment_date,
    ds.store_full_name,
    ds.store_country,
    ds.store_region,
    CASE WHEN gender = 'M' AND registration_local_datetime >= '2020-01-01' AND ds.store_brand_abbr = 'FL' THEN 'Fabletics Men'
        WHEN ds.store_brand_abbr = 'FL' THEN 'Fabletics Women'
        ELSE ds.store_brand END AS store_brand,
    ds.store_type,
    dp.payment_method,
    CASE WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('bijcard', 'mcsuperpremiumdebit', 'mcsuperpremiumcredit', 'mcstandarddebit', 'mcstandardcredit', 'mcpurchasingcredit',
                                           'mcpremiumdebit', 'mcpremiumcredit', 'mcfleetcredit', 'mcdebit', 'mccredit', 'mccorporatedebit', 'mccorporatecredit',
                                           'mccommercialdebit', 'mccommercialcredit', 'mastercard', 'mc', 'maestro') THEN 'Mastercard'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('braintreepaypal', 'paypallitle', 'paypal') THEN 'Paypal'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('cartebancaire') THEN 'Carte Bancaire'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('directdebit_nl', 'elv', 'sepadirectdebit', 'sepaadyenapi') THEN 'Direct Debit'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('ideal') THEN 'iDeal'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('vpay', 'visasuperpremiumdebit', 'visasuperpremiumcredit', 'visastandarddebit', 'visastandardcredit', 'visapurchasingdebit',
                                    'visapurchasingcredit', 'visapremiumdebit', 'visapremiumcredit', 'visacorporatedebit', 'visacorporatecredit',
                                    'visacommercialsuperpremiumdebit', 'visacommercialsuperpremiumcredit', 'visacommercialpremiumdebit', 'visacommercialpremiumcredit',
                                    'visacommercialdebit', 'visacommercialcredit', 'visadankort', 'visasaraivacard', 'visa', 'electron') THEN 'Visa'
    ELSE COALESCE(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type), 'Unknown')
    END AS creditcard_type,
    dos.order_status,
    dosc.order_classification_l1,
    dosc.order_classification_l2,
    domc.membership_order_type_l1,
    domc.membership_order_type_l2,
    domc.membership_order_type_l3,
    IFF(dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing'), COALESCE(rsq.cycle, 0), 0) AS cycle,
    IFF(cycle > 0, TRUE, FALSE) AS is_retry,
    COALESCE(cc.gateway_account_id, -1) AS gateway_account_id,
    COALESCE(cc.is_billed_on_updated_card, FALSE) AS is_billed_on_updated_card,
    COALESCE(psp.payment_gateway, cc.payment_gateway, 'Unknown') AS payment_gateway,
    IFF(dos.order_status = 'Success', 'NA', COALESCE(psp.payment_status, cc.payment_status, 'Unknown')) AS transaction_status,
    IFF(dos.order_status = 'Success', 'NA', COALESCE(psp.response_result_text, cc.response_result_text, 'Unknown')) AS response_result_text,
    IFF(dos.order_status = 'Success', 'NA', COALESCE(psp.response_reason_text, cc.response_reason_text, 'Unknown')) AS response_reason_text,
    COUNT(1) AS orders,
    SUM(IFF(payment_transaction_local_amount = 0, COALESCE(psp.payment_amount, cc.payment_amount, 0), payment_transaction_local_amount) * COALESCE(payment_transaction_date_eur_conversion_rate, 1)) AS payment_transaction_eur_amount,
    SUM(IFF(payment_transaction_local_amount = 0, COALESCE(psp.payment_amount, cc.payment_amount, 0), payment_transaction_local_amount) * COALESCE(payment_transaction_date_usd_conversion_rate, 1)) AS payment_transaction_usd_amount,
    SUM(COALESCE(psp.auth_only, cc.auth_only, 0)) AS auth_only_attempts,
    COUNT(COALESCE(psp.order_id, cc.order_id)) AS auth_only_orders,
    SUM(COALESCE(psp.sale_redirect, cc.sale_redirect, 0)) AS sale_redirect_attempts,
    COUNT(COALESCE(psp.order_id, cc.order_id)) AS sale_redirect_orders,
    SUM(COALESCE(psp.void, cc.void, 0)) AS void_attempts,
    COUNT(COALESCE(psp.order_id, cc.order_id)) AS void_orders,
    SUM(COALESCE(psp.prior_auth_capture, cc.prior_auth_capture, 0)) AS prior_auth_capture_attempts,
    COUNT(COALESCE(psp.order_id, cc.order_id)) AS prior_auth_capture_orders,
    SUM(COALESCE(psp.auth_redirect, 0)) AS auth_redirect_attempts,
    COUNT(psp.order_id) AS auth_redirect_orders,
    SUM(COALESCE(psp.credit, cc.credit, c.credit, 0)) AS credit_attempts,
    COUNT(COALESCE(psp.order_id, cc.order_id, c.order_id)) AS credit_orders,
    SUM(COALESCE(psp.auth_sepa, 0)) AS auth_sepa_attempts,
    COUNT(psp.order_id) AS auth_sepa_orders,
    SUM(COALESCE(psp.init_session, 0)) AS init_session_attempts,
    COUNT(psp.order_id) AS init_session_orders,
    SUM(COALESCE(psp.verify, 0)) AS verify_attempts,
    COUNT(psp.order_id) AS verify_orders,
    SUM(COALESCE(cc.auth_reverse, 0)) AS auth_reverse_attempts,
    COUNT(cc.order_id) AS auth_reverse_orders,
    SUM(COALESCE(cc.auth_defer, 0)) AS auth_defer_attempts,
    COUNT(cc.order_id) AS auth_defer_orders,
    SUM(COALESCE(c.sale, 0)) AS sale_attempts,
    COUNT(c.order_id) AS sale_orders,
    $datetime_added AS meta_create_datetime,
    $datetime_added AS meta_update_datetime
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_order_status dos ON dos.order_status_key = fo.order_status_key
JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing', 'Product Order')
JOIN edw_prod.data_model_jfb.dim_store ds ON ds.store_id = fo.store_id
JOIN edw_prod.data_model_jfb.dim_payment dp ON dp.payment_key = fo.payment_key
JOIN edw_prod.data_model_jfb.dim_order_membership_classification domc ON domc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model_jfb.dim_customer dc ON dc.customer_id = fo.customer_id
LEFT JOIN _updated_payment_methods upm ON upm.order_id = fo.order_id
LEFT JOIN _psp psp ON psp.order_id = fo.order_id
    AND dp.payment_method = 'Psp'
LEFT JOIN _credit_card cc ON cc.order_id = fo.order_id
    AND dp.payment_method = 'Credit Card'
LEFT JOIN _cash c ON c.order_id = fo.order_id
    AND dp.payment_method = 'Cash'
LEFT JOIN _retry_schedule_queue rsq ON rsq.order_id = fo.order_id
WHERE fo.order_local_datetime::DATE >= $process_from_date
GROUP BY DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE,
    fo.order_local_datetime::DATE,
    COALESCE(fo.payment_transaction_local_datetime, fo.order_completion_local_datetime, psp.datetime_modified, cc.datetime_modified, c.datetime_modified, fo.order_local_datetime)::DATE,
    ds.store_full_name,
    ds.store_country,
    ds.store_region,
    CASE WHEN gender = 'M' AND registration_local_datetime >= '2020-01-01' AND ds.store_brand_abbr = 'FL' THEN 'Fabletics Men'
        WHEN ds.store_brand_abbr = 'FL' THEN 'Fabletics Women'
        ELSE ds.store_brand END,
    ds.store_type,
    dp.payment_method,
    CASE WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('bijcard', 'mcsuperpremiumdebit', 'mcsuperpremiumcredit', 'mcstandarddebit', 'mcstandardcredit', 'mcpurchasingcredit',
                                           'mcpremiumdebit', 'mcpremiumcredit', 'mcfleetcredit', 'mcdebit', 'mccredit', 'mccorporatedebit', 'mccorporatecredit',
                                           'mccommercialdebit', 'mccommercialcredit', 'mastercard', 'mc', 'maestro') THEN 'Mastercard'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('braintreepaypal', 'paypallitle', 'paypal') THEN 'Paypal'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('cartebancaire') THEN 'Carte Bancaire'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('directdebit_nl', 'elv', 'sepadirectdebit', 'sepaadyenapi') THEN 'Direct Debit'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('ideal') THEN 'iDeal'
    WHEN LOWER(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type)) IN ('vpay', 'visasuperpremiumdebit', 'visasuperpremiumcredit', 'visastandarddebit', 'visastandardcredit', 'visapurchasingdebit',
                                    'visapurchasingcredit', 'visapremiumdebit', 'visapremiumcredit', 'visacorporatedebit', 'visacorporatecredit',
                                    'visacommercialsuperpremiumdebit', 'visacommercialsuperpremiumcredit', 'visacommercialpremiumdebit', 'visacommercialpremiumcredit',
                                    'visacommercialdebit', 'visacommercialcredit', 'visadankort', 'visasaraivacard', 'visa', 'electron') THEN 'Visa'
    ELSE COALESCE(IFF(upm.order_id IS NOT NULL, upm.creditcard_type, dp.creditcard_type), 'Unknown')
    END,
    dos.order_status,
    dosc.order_classification_l1,
    dosc.order_classification_l2,
    domc.membership_order_type_l1,
    domc.membership_order_type_l2,
    domc.membership_order_type_l3,
    IFF(dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing'), COALESCE(rsq.cycle, 0), 0),
    IFF(cycle > 0, TRUE, FALSE),
    COALESCE(cc.gateway_account_id, -1),
    COALESCE(cc.is_billed_on_updated_card, FALSE),
    COALESCE(psp.payment_gateway, cc.payment_gateway, 'Unknown'),
    IFF(dos.order_status = 'Success', 'NA', COALESCE(psp.payment_status, cc.payment_status, 'Unknown')),
    IFF(dos.order_status = 'Success', 'NA', COALESCE(psp.response_result_text, cc.response_result_text, 'Unknown')),
    IFF(dos.order_status = 'Success', 'NA', COALESCE(psp.response_reason_text, cc.response_reason_text, 'Unknown'));

COMMIT;
