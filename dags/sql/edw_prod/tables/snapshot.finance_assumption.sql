CREATE OR REPLACE TABLE snapshot.finance_assumption
(
    bu                                           VARCHAR(255),
    financial_date                               DATE,
    brand                                        VARCHAR(255),
    gender                                       VARCHAR(255),
    region_type                                  VARCHAR(255),
    region_type_mapping                          VARCHAR(255),
    store_type                                   VARCHAR(255),
    local_currency                               VARCHAR(255),
    shipping_supplies_cost_per_order             NUMBER(19, 11),
    shipping_cost_per_order                      NUMBER(19, 11),
    variable_warehouse_cost_per_order            NUMBER(19, 11),
    variable_gms_cost_per_order                  NUMBER(19, 11),
    variable_payment_processing_pct_cash_revenue NUMBER(19, 11),
    return_shipping_cost_per_order               NUMBER(19, 11),
    product_markdown_percent                     NUMBER(19, 11),
    returned_product_resaleable_percent          NUMBER(18, 4),
    snapshot_datetime                            TIMESTAMP_LTZ(3)

);
