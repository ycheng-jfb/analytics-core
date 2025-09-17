CREATE OR REPLACE TABLE reference.payment_method_mapping (
    payment_method VARCHAR(50),
    raw_creditcard_type VARCHAR(50),
    creditcard_type VARCHAR(50),
    is_prepaid_creditcard BOOLEAN,
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP,
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP
    );
