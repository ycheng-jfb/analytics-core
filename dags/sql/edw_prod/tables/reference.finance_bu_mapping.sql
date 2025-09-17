CREATE OR REPLACE TABLE reference.finance_bu_mapping
(
    bu                                  VARCHAR,
    report_mapping                      VARCHAR,
    region_type                         VARCHAR,
    store_brand                         VARCHAR,
    region_type_mapping                 VARCHAR,
    local_currency                      VARCHAR,
    gender                              VARCHAR,
    store_type                          VARCHAR,
    is_assumption_mapping               BOOLEAN,
    returned_product_resaleable_percent NUMBER(18, 4),
    start_date                          DATE          DEFAULT 1900 - 01 - 01,
    end_date                            DATE          DEFAULT 9999 - 12 - 31,
    meta_create_datetime                TIMESTAMP_LTZ DEFAULT current_timestamp,
    meta_update_datetime                TIMESTAMP_LTZ DEFAULT current_timestamp,
    PRIMARY KEY (bu)
);
