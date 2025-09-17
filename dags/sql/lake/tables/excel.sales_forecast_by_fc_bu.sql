CREATE TABLE IF NOT EXISTS lake_stg.excel.sales_forecast_by_fc_bu_stg (
	year VARCHAR(16777216),
	month VARCHAR(16777216),
	day VARCHAR(16777216),
	date DATE,
	city VARCHAR(16777216),
	bu VARCHAR(16777216),
	orders NUMBER(38,0),
	units NUMBER(38,0),
	promos_and_notes VARCHAR(16777216)
);

CREATE TABLE IF NOT EXISTS lake.excel.sales_forecast_by_fc_bu (
    sales_forecast_by_fc_bu_key INT IDENTITY,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR,
    date DATE,
    city VARCHAR,
    bu VARCHAR,
    orders INTEGER,
    units INTEGER,
    promos_and_notes VARCHAR,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3),
    PRIMARY KEY (date, city, bu)
);
