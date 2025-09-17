CREATE OR REPLACE VIEW data_model_jfb.dim_customer_action_type AS
SELECT
    customer_action_type_key,
    customer_action_type,
	--effective_start_datetime,
	--effective_end_datetime,
	--is_current,
	meta_create_datetime,
	meta_update_datetime
FROM stg.dim_customer_action_type
WHERE is_current;
