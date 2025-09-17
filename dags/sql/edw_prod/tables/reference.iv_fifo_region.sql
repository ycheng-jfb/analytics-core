CREATE TABLE reference.iv_fifo_region (
    interid VARCHAR(5),
	trxloctn VARCHAR(11),
	region VARCHAR(2),
	meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    PRIMARY KEY (interid, trxloctn, region)
);
