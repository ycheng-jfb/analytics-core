CREATE OR REPLACE VIEW data_model_jfb.dim_membership_event_type AS
SELECT
    membership_event_type_key,
    membership_event_type,
	--effective_start_datetime,
	--effective_end_datetime,
	--is_current,
	meta_create_datetime,
	meta_update_datetime
FROM stg.dim_membership_event_type
WHERE is_current;
