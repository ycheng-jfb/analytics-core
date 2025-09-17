CREATE OR REPLACE VIEW data_model_fl.dim_order_status AS
SELECT
    order_status_key,
    order_status,
	--effective_start_datetime,
	--effective_end_datetime,
	--is_current,
	meta_create_datetime,
	meta_update_datetime
FROM stg.dim_order_status
WHERE is_current;
