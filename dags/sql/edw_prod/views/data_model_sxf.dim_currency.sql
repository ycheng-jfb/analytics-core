CREATE OR REPLACE VIEW data_model_sxf.dim_currency AS
SELECT
    currency_key,
    iso_currency_code,
	currency_exchange_rate_type,
	currency_name,
	currency_symbol,
	--effective_start_datetime,
    --effective_end_datetime,
    --is_current,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_currency
WHERE is_current;
