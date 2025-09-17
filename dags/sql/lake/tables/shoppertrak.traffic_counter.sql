USE DATABASE lake;

CREATE TABLE IF NOT EXISTS lake.shoppertrak.traffic_counter (
    site_id INT,
    customer_site_id INT,
    date DATE,
    time TIME,
    traffic_enters INT,
    traffic_exits INT,
    imputation_indicator VARCHAR(50),
    datetime_added DATE,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3),
    PRIMARY KEY (site_id, customer_site_id, date)
);
