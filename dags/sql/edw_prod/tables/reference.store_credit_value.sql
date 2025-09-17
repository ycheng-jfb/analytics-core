CREATE OR REPLACE TABLE reference.store_credit_value
(
    store_brand                          VARCHAR(100),
    store_country                        VARCHAR(10),
    equivalent_credit_value_local_amount NUMERIC(38, 2),
    meta_create_datetime DATETIME default current_timestamp,
    meta_update_datetime DATETIME default current_timestamp
);
