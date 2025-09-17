CREATE OR REPLACE TRANSIENT TABLE validation.fact_order_and_fact_return_line_missing_assumptions
(
    edw_table     VARCHAR(16),
    store_brand   VARCHAR(20),
    store_region  VARCHAR(32),
    store_country VARCHAR(50),
    store_type    VARCHAR(20),
    order_month   DATE
);
