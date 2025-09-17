CREATE TRANSIENT TABLE IF NOT EXISTS public.meta_table_dependency_watermark (
	table_name VARCHAR(255),
	dependent_table_name VARCHAR(255),
	high_watermark_datetime TIMESTAMP_LTZ,
	new_high_watermark_datetime TIMESTAMP_LTZ,
	meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    PRIMARY KEY (table_name, dependent_table_name)
);
