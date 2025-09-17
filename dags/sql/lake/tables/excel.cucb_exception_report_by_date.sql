CREATE TRANSIENT TABLE IF NOT EXISTS excel.cucb_exception_report_by_date (
	exception_subject VARCHAR(16777216),
	ref_type VARCHAR(16777216),
	ref_num VARCHAR(16777216),
	po VARCHAR(16777216),
	item VARCHAR(16777216),
	exception_name VARCHAR(16777216),
	excep_detail VARCHAR(16777216),
	status VARCHAR(16777216),
	create_by_user_name VARCHAR(16777216),
	create_time TIMESTAMP_NTZ(9),
	last_update_time TIMESTAMP_NTZ(9),
	last_update_from VARCHAR(16777216),
	division_name VARCHAR(16777216),
	last_reply VARCHAR(16777216),
	meta_row_hash NUMBER(38,0),
	meta_create_datetime TIMESTAMP_LTZ(3),
	meta_update_datetime TIMESTAMP_LTZ(3)
);
