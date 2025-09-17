CREATE TABLE IF NOT EXISTS reference.gl_trx_store
(
   transaction_name varchar,
   store_country varchar,
   store_brand varchar,
   meta_create_datetime  TIMESTAMP_LTZ DEFAULT current_timestamp,
   meta_update_datetime  TIMESTAMP_LTZ DEFAULT current_timestamp
);
