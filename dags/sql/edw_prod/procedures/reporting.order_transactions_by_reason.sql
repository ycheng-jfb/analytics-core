
SET process_from_date = (
        SELECT COALESCE(DATEADD('MONTH', -3, MAX(month_date)), '2019-01-01')
        FROM edw_prod.reporting.order_transactions_by_reason
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

CREATE OR REPLACE TEMPORARY TABLE _is_credit_billing_on_retry AS
SELECT fo.order_id,
    TRUE AS is_credit_billing_on_retry
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND order_classification_l2 IN ('Credit Billing', 'Token Billing')
JOIN lake_jfb.ultra_merchant.billing_retry_schedule_queue brsq ON brsq.order_id = fo.order_id
WHERE fo.order_local_datetime >= $process_from_date
    AND brsq.datetime_last_retry IS NOT NULL
    AND brsq.statuscode != 4209
    QUALIFY ROW_NUMBER() OVER(PARTITION BY fo.order_id ORDER BY brsq.datetime_modified DESC, brsq.datetime_added DESC) = 1;

BEGIN;

DELETE FROM edw_prod.reporting.order_transactions_by_reason
WHERE month_date >= $process_from_date;

INSERT INTO edw_prod.reporting.order_transactions_by_reason (
    month_date,
    order_date,
    transaction_date,
    payment_date,
    store_region,
    store_country,
    store_type,
    store_brand,
    store_full_name,
    payment_method,
    creditcard_type,
    order_classification_l1,
    order_classification_l2,
    order_status,
    membership_order_type_l1,
    membership_order_type_l2,
    membership_order_type_l3,
    is_credit_billing_on_retry,
    transaction_type,
    platform,
    browser,
    attempt_status,
    response_result_text,
    response_reason_text,
    attempts,
    customers,
    orders,
    meta_create_datetime,
    meta_update_datetime
)
SELECT DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE AS month_date,
    fo.order_local_datetime::DATE AS order_date,
    COALESCE(ptcc.datetime_added, ptp.datetime_added, ptc.datetime_added, fo.payment_transaction_local_datetime)::DATE AS transaction_date,
    fo.payment_transaction_local_datetime::DATE as payment_date,
    ds.store_region,
    ds.store_country,
    ds.store_type,
    CASE WHEN gender = 'M' AND registration_local_datetime >= '2020-01-01' AND ds.store_brand_abbr = 'FL' THEN 'Fabletics Men'
        WHEN ds.store_brand_abbr = 'FL' THEN 'Fabletics Women'
        ELSE ds.store_brand END AS store_brand,
    ds.store_full_name,
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
    dosc.order_classification_l1,
    dosc.order_classification_l2,
    dos.order_status,
    domc.membership_order_type_l1,
    domc.membership_order_type_l2,
    domc.membership_order_type_l3,
    COALESCE(icbor.is_credit_billing_on_retry, FALSE) AS is_credit_billing_on_retry,
    COALESCE(ptcc.transaction_type, ptp.transaction_type, ptc.transaction_type) AS transaction_type,
    COALESCE(s.platform, 'Unknown') AS platform,
    COALESCE(s.browser, 'Unknown') AS browser,
    sc.label AS attempt_status,
    COALESCE(ptcc.response_result_text, ptp.response_result_text) AS response_result_text,
    COALESCE(ptcc.response_reason_text, ptp.response_reason_text) AS response_reason_text,
    COUNT(1) AS attempts,
    COUNT(DISTINCT fo.customer_id) AS customers,
    COUNT(DISTINCT fo.order_id) AS orders,
    $datetime_added AS meta_create_datetime,
    $datetime_added AS meta_update_datetime
FROM edw_prod.data_model_jfb.fact_order fo
JOIN edw_prod.data_model_jfb.dim_store ds ON ds.store_id = fo.store_id
JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND order_classification_l2 IN ('Credit Billing', 'Token Billing', 'Product Order')
JOIN edw_prod.data_model_jfb.dim_order_status dos ON dos.order_status_key = fo.order_status_key
JOIN edw_prod.data_model_jfb.dim_order_membership_classification domc ON domc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model_jfb.dim_payment dp ON dp.payment_key = fo.payment_key
JOIN edw_prod.data_model_jfb.dim_customer dc ON dc.customer_id = fo.customer_id
LEFT JOIN _updated_payment_methods upm ON upm.order_id = fo.order_id
LEFT JOIN lake_jfb_view.ultra_merchant.payment_transaction_creditcard ptcc ON ptcc.order_id = fo.order_id
    AND dp.payment_method = 'Credit Card'
LEFT JOIN lake_jfb_view.ultra_merchant.payment_transaction_psp ptp ON ptp.order_id = fo.order_id
    AND dp.payment_method = 'Psp'
LEFT JOIN lake_jfb_view.ultra_merchant.payment_transaction_cash ptc ON ptc.order_id = fo.order_id
    AND dp.payment_method = 'Cash'
left JOIN lake_jfb_view.ultra_merchant.statuscode sc ON sc.statuscode = COALESCE(ptcc.statuscode, ptp.statuscode, ptc.statuscode)
LEFT JOIN reporting_base_prod.shared.session s ON s.meta_original_session_id = fo.session_id
LEFT JOIN _is_credit_billing_on_retry icbor ON fo.order_id = icbor.order_id
WHERE order_local_datetime::DATE >= $process_from_date
GROUP BY DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE,
    fo.order_local_datetime::DATE,
    COALESCE(ptcc.datetime_added, ptp.datetime_added, ptc.datetime_added, fo.payment_transaction_local_datetime)::DATE,
    fo.payment_transaction_local_datetime::DATE,
    ds.store_region,
    ds.store_country,
    ds.store_type,
    CASE WHEN gender = 'M' AND registration_local_datetime >= '2020-01-01' AND ds.store_brand_abbr = 'FL' THEN 'Fabletics Men'
        WHEN ds.store_brand_abbr = 'FL' THEN 'Fabletics Women'
        ELSE ds.store_brand END,
    ds.store_full_name,
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
    dosc.order_classification_l1,
    dosc.order_classification_l2,
    dos.order_status,
    domc.membership_order_type_l1,
    domc.membership_order_type_l2,
    domc.membership_order_type_l3,
    COALESCE(icbor.is_credit_billing_on_retry, FALSE),
    COALESCE(ptcc.transaction_type, ptp.transaction_type, ptc.transaction_type),
    COALESCE(s.platform, 'Unknown'),
    COALESCE(s.browser, 'Unknown'),
    sc.label,
    COALESCE(ptcc.response_result_text, ptp.response_result_text),
    COALESCE(ptcc.response_reason_text, ptp.response_reason_text);

COMMIT;
