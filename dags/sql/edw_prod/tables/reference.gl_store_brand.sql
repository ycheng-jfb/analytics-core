CREATE TABLE IF NOT EXISTS reference.gl_store_brand
(
   gl_company_code INT,
   gl_store_brand VARCHAR,
   reporting_store_brand VARCHAR,
   meta_create_datetime  TIMESTAMP_LTZ DEFAULT current_timestamp,
   meta_update_datetime  TIMESTAMP_LTZ DEFAULT current_timestamp
);
