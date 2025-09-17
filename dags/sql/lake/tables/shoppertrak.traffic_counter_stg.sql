USE DATABASE lake;

CREATE TABLE IF NOT EXISTS lake.shoppertrak.traffic_counter_stg (
    site_id INT,
    customer_site_id INT,
    date DATE,
    time TIME,
    traffic_enters INT,
    traffic_exits INT,
    imputation_indicator VARCHAR(50),
    datetime_added DATE
);
