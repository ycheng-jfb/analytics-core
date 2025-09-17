CREATE OR REPLACE TABLE reference.historical_po_landed_cost (
	order_line_id NUMBER(38,0) NOT NULL,
	estimated_landed_cost_local_amount NUMBER(19,4),
	po_cost_local_amount NUMBER(19,4),
	lpn_po_cost_local_amount NUMBER(19,4),
	misc_cogs_local_amount NUMBER(19,4),
	lpn_code VARCHAR(255),
	cost_source_id NUMBER(38,0),
	cost_source VARCHAR(16777216),
	meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	meta_original_order_line_id NUMBER(38,0),
	primary key (order_line_id)
);
