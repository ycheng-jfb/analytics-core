CREATE OR REPLACE TABLE  iterable.public.orphan_contacts (
    itbl_user_id STRING,
    bento_customer_id NUMBER(38,0),
    firstname VARCHAR(25),
    lastname VARCHAR(25),
    email VARCHAR(75),
    store_id NUMBER(38,0),
    store_group_id NUMBER(38,0),
    phone_number STRING,
    store_brand STRING,
    membership_state STRING,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);
