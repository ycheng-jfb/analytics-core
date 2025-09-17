-- DROP TABLE IF EXISTS reference.test_customer;

CREATE TABLE reference.test_customer (
    customer_id NUMBER(38),
    email VARCHAR(75),
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (customer_id)
);

GRANT OWNERSHIP ON TABLE reference.test_customer TO ROLE __DB_RW__EDW COPY CURRENT GRANTS;

-- ALTER TABLE reference.test_customer ADD COLUMN new_column INT;
