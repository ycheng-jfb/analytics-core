CREATE TABLE IF NOT EXISTS reference.gl_store_country
(
   gl_country_code INT,
   gl_store_country VARCHAR,
   reporting_store_country VARCHAR,
   meta_create_datetime  TIMESTAMP_LTZ DEFAULT current_timestamp,
   meta_update_datetime  TIMESTAMP_LTZ DEFAULT current_timestamp
);
