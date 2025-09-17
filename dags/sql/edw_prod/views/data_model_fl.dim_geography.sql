CREATE OR REPLACE VIEW data_model_fl.dim_geography AS
SELECT
    geography_key,
    sub_region,
	region,
	--effective_start_datetime,
	--effective_end_datetime,
	--is_current,
	meta_create_datetime,
	meta_update_datetime
FROM stg.dim_geography
WHERE is_current;
