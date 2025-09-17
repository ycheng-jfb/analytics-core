CREATE OR REPLACE VIEW data_model_sxf.dim_payment
(
    payment_key,
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
) AS
SELECT
    payment_key,
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
FROM stg.dim_payment;
