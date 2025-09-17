SET datetime_added = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(9);

CREATE OR REPLACE TEMPORARY TABLE _vips AS
SELECT ds.store_name,
    ds.store_full_name,
    ds.store_brand,
    ds.store_region,
    ds.store_type,
    ds.store_country,
    0 AS update_phone,
    a.phone_digits,
    0 AS update_default_address,
    a.firstname,
    a.lastname,
    c2.firstname AS customer_firstname,
    c2.lastname AS customer_lastname,
    a.address1,
    a.city,
    a.state,
    dc.customer_id,
    a.zip,
    0 AS update_billing_address,
    ab.city AS billing_city,
    ab.address1 AS billing_address1,
    ab.firstname AS billing_firstname,
    ab.lastname AS billing_lastname,
    ab.zip AS billing_zip,
    p.payment_method,
    COALESCE(c2c.tokenized_card_num, psp2.token) AS card_num,
    COALESCE(c2c.cnh, psp2.token) AS cnh,
    dp.is_prepaid_creditcard,
    v.activation_local_datetime::DATE AS activation_local_datetime,
    v.order_id,
    v.is_retail_vip,
    v.is_reactivated_vip,
    IFF(store_brand = 'Fabletics' AND dc.gender = 'M', 'M', 'F') AS customer_gender
FROM data_model.dim_customer dc
JOIN data_model.dim_store ds ON ds.store_id = dc.store_id
JOIN lake_consolidated_view.ultra_merchant.customer c2 ON c2.customer_id = dc.customer_id
JOIN data_model.fact_activation v ON v.customer_id = dc.customer_id
LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON a.address_id = c2.default_address_id
LEFT JOIN lake_consolidated_view.ultra_merchant.payment p ON p.order_id = v.order_id
LEFT JOIN lake_consolidated_view.ultra_merchant.payment_transaction_creditcard cc ON cc.payment_transaction_id = p.capture_payment_transaction_id
    AND p.payment_method = 'creditcard'
LEFT JOIN lake_consolidated_view.ultra_merchant.creditcard c2c ON c2c.creditcard_id = cc.creditcard_id
LEFT JOIN lake_consolidated_view.ultra_merchant.payment_transaction_psp psp ON psp.payment_transaction_id = p.capture_payment_transaction_id
    AND p.payment_method = 'psp'
LEFT JOIN lake_consolidated_view.ultra_merchant.psp psp2 ON psp2.psp_id = psp.psp_id
LEFT JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = v.order_id
LEFT JOIN data_model.fact_order fo ON fo.order_id = o.order_id
LEFT JOIN data_model.dim_payment dp ON dp.payment_key = fo.payment_key
LEFT JOIN lake_consolidated_view.ultra_merchant.address ab ON ab.address_id = o.billing_address_id
WHERE v.activation_local_datetime >= '2018-01-01';

CREATE OR REPLACE TEMPORARY TABLE _repeat_orders AS
SELECT ds.store_name,
    ds.store_full_name,
    ds.store_brand,
    ds.store_region,
    ds.store_type,
    ds.store_country,
    fo.order_local_datetime::DATE AS order_local_datetime,
    fo.customer_id,
    COALESCE(cc.cnh, psp.token) AS cnh,
    a.city AS billing_city,
    a.address1 AS billing_address1,
    a.firstname AS billing_firstname,
    a.lastname AS billing_lastname,
    a.zip AS billing_zip
FROM data_model.fact_order fo
JOIN data_model.dim_store ds ON ds.store_id = fo.store_id
JOIN lake_consolidated_view.ultra_merchant.address a ON a.address_id = fo.billing_address_id
JOIN data_model.dim_order_membership_classification domc ON domc.order_membership_classification_key = fo.order_membership_classification_key
    AND domc.membership_order_type_l3 = 'Repeat VIP'
JOIN data_model.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l2 IN ('Product Order', 'Credit Billing', 'Token Billing')
LEFT JOIN lake_consolidated_view.ultra_merchant.payment p ON p.order_id = fo.order_id
LEFT JOIN lake_consolidated_view.ultra_merchant.payment_transaction_creditcard ptc ON ptc.payment_transaction_id = p.capture_payment_transaction_id
    AND p.payment_method = 'creditcard'
LEFT JOIN lake_consolidated_view.ultra_merchant.creditcard cc ON cc.creditcard_id = ptc.creditcard_id
LEFT JOIN lake_consolidated_view.ultra_merchant.payment_transaction_psp ptp ON ptp.payment_transaction_id = p.capture_payment_transaction_id
    AND p.payment_method = 'psp'
LEFT JOIN lake_consolidated_view.ultra_merchant.psp psp ON psp.psp_id = ptp.psp_id
WHERE fo.order_local_datetime >= '2018-01-01';

UPDATE _vips
SET phone_digits = NULL,
    update_phone = 1
WHERE phone_digits IN ('1111111111', '5555555555', '8888888888', '0000000000', '9999999999')
   OR LENGTH(phone_digits) < 10;

UPDATE _vips
SET firstname = NULL,
    lastname = NULL,
    address1 = NULL,
    zip = NULL,
    city = NULL,
    update_default_address = 1
WHERE (firstname = 'Guest' AND lastname = 'Guest')
    OR (firstname = '[removed]' AND lastname = '[removed]')
    OR (firstname ILIKE '%test%' AND lastname ILIKE '%test%')
    OR address1 IS NULL;

UPDATE _vips
SET billing_firstname = NULL,
    billing_lastname = NULL,
    billing_address1 = NULL,
    billing_zip = NULL,
    billing_city = NULL,
    update_billing_address = 1
WHERE (billing_firstname = 'Guest' AND billing_lastname = 'Guest')
    OR (billing_firstname = '[removed]' AND billing_lastname = '[removed]')
    OR (billing_firstname ILIKE '%test%' AND billing_lastname ILIKE '%test%')
    OR billing_address1 IS NULL;

UPDATE _repeat_orders
SET billing_firstname = NULL,
    billing_lastname = NULL,
    billing_address1 = NULL,
    billing_zip = NULL,
    billing_city = NULL
WHERE (billing_firstname = 'Guest' AND billing_lastname = 'Guest')
    OR (billing_firstname = '[removed]' AND billing_lastname = '[removed]')
    OR (billing_firstname ILIKE '%test%' AND billing_lastname ILIKE '%test%')
    OR billing_address1 IS NULL;

UPDATE _vips
SET billing_firstname = ''
WHERE billing_firstname IS NULL
    AND billing_lastname IS NOT NULL
    AND billing_address1 IS NOT NULL;

UPDATE _vips
SET firstname = ''
WHERE firstname IS NULL
  AND lastname IS NOT NULL
  AND address1 IS NOT NULL;

SET lookback = 6;

CREATE OR REPLACE TEMPORARY TABLE _all_combos_lookback AS
SELECT CAST(CAST($lookback AS VARCHAR) || 'M' AS VARCHAR(55))    AS lookback_window,
    v.store_name,
    v.store_full_name,
    v.store_brand,
    v.store_region,
    v.store_type,
    v.store_country,
    v.update_phone,
    v.phone_digits,
    v.update_default_address,
    v.firstname,
    v.lastname,
    v.customer_firstname,
    v.customer_lastname,
    v.address1,
    v.city,
    v.state,
    v.customer_id,
    v.zip,
    v.update_billing_address,
    v.billing_city,
    v.billing_address1,
    v.billing_firstname,
    v.billing_lastname,
    v.billing_zip,
    v.payment_method,
    v.card_num,
    v.cnh,
    v.is_prepaid_creditcard,
    v.activation_local_datetime,
    v.order_id,
    v.is_retail_vip,
    v.is_reactivated_vip,
    v.customer_gender,
    IFF(phone.customer_id IS NOT NULL, 1, 0) AS activating_phone_match,
    IFF(cnh.customer_id IS NOT NULL, 1, 0) AS activating_cnh_match,
    IFF(defaddress.customer_id IS NOT NULL, 1, 0) AS activating_defaddress_match,
    IFF(billingaddress.customer_id IS NOT NULL, 1, 0) AS activating_billingaddress_match,
    IFF(repeat_billing_address.customer_id IS NOT NULL, 1, 0) AS repeat_billing_match,
    IFF(repeat_cnh.customer_id IS NOT NULL, 1, 0) AS repeat_cnh_match
FROM _vips v
LEFT JOIN (
    SELECT f.customer_id, activation_local_datetime::DATE as activation_date
    FROM _vips f
    WHERE EXISTS(
        SELECT 1
        FROM _vips flus
        WHERE flus.store_name = f.store_name
            AND flus.customer_id != f.customer_id
            AND flus.phone_digits = f.phone_digits
            AND f.activation_local_datetime > flus.activation_local_datetime
            AND flus.activation_local_datetime > DATEADD(MONTH, -$lookback, f.activation_local_datetime)
    )
) phone ON phone.customer_id = v.customer_id
    and phone.activation_date = v.activation_local_datetime::DATE
LEFT JOIN (
    SELECT f.customer_id, activation_local_datetime::DATE as activation_date
    FROM _vips f
    WHERE EXISTS(
        SELECT 1
        FROM _vips flus
        WHERE flus.store_name = f.store_name
            AND flus.customer_id != f.customer_id
            AND flus.cnh = f.cnh
            AND f.activation_local_datetime > flus.activation_local_datetime
            AND flus.activation_local_datetime > DATEADD(MONTH, -$lookback, f.activation_local_datetime)
    )
) cnh ON cnh.customer_id = v.customer_id
    and cnh.activation_date = v.activation_local_datetime::DATE
LEFT JOIN (
    SELECT f.customer_id, activation_local_datetime::DATE as activation_date
    FROM _vips f
    WHERE EXISTS(
        SELECT 1
        FROM _vips flus
        WHERE flus.store_name = f.store_name
            AND flus.customer_id != f.customer_id
            AND f.activation_local_datetime > flus.activation_local_datetime
            AND LEFT(flus.customer_firstname, 4) = LEFT(customer_firstname, 4)
            AND LEFT(flus.customer_lastname, 4) = LEFT(f.customer_lastname, 4)
            AND flus.address1 = f.address1
            AND flus.activation_local_datetime > DATEADD(MONTH, -$lookback, f.activation_local_datetime)
    )
) defaddress ON defaddress.customer_id = v.customer_id
    and defaddress.activation_date = v.activation_local_datetime::DATE
LEFT JOIN (
    SELECT f.customer_id, activation_local_datetime::DATE as activation_date
    FROM _vips f
    WHERE EXISTS(
        SELECT 1
        FROM _vips flus
        WHERE flus.store_name = f.store_name
            AND flus.customer_id != f.customer_id
            AND f.activation_local_datetime > flus.activation_local_datetime
            AND LEFT(flus.billing_firstname, 4) = LEFT(f.billing_firstname, 4)
            AND LEFT(flus.billing_lastname, 4) = LEFT(f.billing_lastname, 4)
            AND flus.billing_address1 = f.billing_address1
            AND flus.billing_zip = f.billing_zip
            AND flus.activation_local_datetime > DATEADD(MONTH, -$lookback, f.activation_local_datetime)
    )
) AS billingaddress ON billingaddress.customer_id = v.customer_id
    and billingaddress.activation_date = v.activation_local_datetime::DATE
LEFT JOIN (
    SELECT f.customer_id, activation_local_datetime::DATE as activation_date
    FROM _vips f
    WHERE EXISTS(
        SELECT 1
        FROM _repeat_orders flus
        WHERE flus.store_name = f.store_name
            AND flus.customer_id != f.customer_id
            AND LEFT(flus.billing_firstname, 4) = LEFT(f.customer_firstname, 4)
            AND LEFT(flus.billing_lastname, 4) = LEFT(f.customer_lastname, 4)
            AND flus.billing_address1 = f.address1
            AND flus.billing_zip = f.billing_zip
            AND f.activation_local_datetime > flus.order_local_datetime
            AND flus.order_local_datetime > DATEADD(MONTH, -$lookback, f.activation_local_datetime)
    )
) repeat_billing_address ON repeat_billing_address.customer_id = v.customer_id
    and repeat_billing_address.activation_date = v.activation_local_datetime::DATE
-- matches a repeat vip's CNH (on product or billings) from a different customer
LEFT JOIN(
    SELECT f.customer_id, activation_local_datetime::DATE as activation_date
    FROM _vips f
    WHERE EXISTS(
        SELECT 1
        FROM _repeat_orders flus
        WHERE flus.store_name = f.store_name
        AND flus.customer_id != f.customer_id
        AND flus.cnh = f.cnh
        AND f.activation_local_datetime > flus.order_local_datetime
        AND flus.order_local_datetime > DATEADD(MONTH, -$lookback, f.activation_local_datetime)
    )
) repeat_cnh ON repeat_cnh.customer_id = v.customer_id
and repeat_cnh.activation_date = v.activation_local_datetime::DATE;

DELETE
FROM _all_combos_lookback
WHERE activation_local_datetime < '2018-01-01';

BEGIN;

TRUNCATE TABLE reporting.vip_level_gamers;

INSERT INTO reporting.vip_level_gamers (
    lookback_window,
    store_name,
    store_full_name,
    store_brand,
    store_region,
    store_type,
    store_country,
    activating_billingaddress_match,
    activating_cnh_match,
    activating_defaddress_match,
    activating_phone_match,
    repeat_billing_match,
    repeat_cnh_match,
    activation_local_datetime,
    is_reactivated_vip,
    is_retail_vip,
    customer_gender,
    vips,
    meta_create_datetime,
    meta_update_datetime
)
SELECT lookback_window,
    store_name,
    store_full_name,
    store_brand,
    store_region,
    store_type,
    store_country,
    activating_billingaddress_match,
    activating_cnh_match,
    activating_defaddress_match,
    activating_phone_match,
    repeat_billing_match,
    repeat_cnh_match,
    activation_local_datetime,
    is_reactivated_vip,
    is_retail_vip,
    customer_gender,
    COUNT(1) AS vips,
    $datetime_added AS meta_create_datetime,
    $datetime_added AS meta_update_datetime
FROM _all_combos_lookback
GROUP BY lookback_window,
    store_name,
    store_full_name,
    store_brand,
    store_region,
    store_type,
    store_country,
    activating_billingaddress_match,
    activating_cnh_match,
    activating_defaddress_match,
    activating_phone_match,
    repeat_billing_match,
    repeat_cnh_match,
    activation_local_datetime,
    is_reactivated_vip,
    is_retail_vip,
    customer_gender;

COMMIT;
