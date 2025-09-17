CREATE TABLE reference.country_timezone (
    name VARCHAR UNIQUE,
    alpha2_code VARCHAR PRIMARY KEY,
    alpha3_code VARCHAR UNIQUE,
    numeric_code VARCHAR UNIQUE,
    apolitical_name VARCHAR UNIQUE,
    default_timezone VARCHAR,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp
);
