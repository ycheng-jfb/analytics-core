CREATE OR REPLACE VIEW data_model.dim_chargeback_payment
(
    chargeback_payment_key,
    chargeback_payment_processor,
    chargeback_payment_method,
    chargeback_payment_type,
    chargeback_payment_bank,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    chargeback_payment_key,
    chargeback_payment_processor,
    chargeback_payment_method,
    chargeback_payment_type,
    chargeback_payment_bank,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_chargeback_payment;
