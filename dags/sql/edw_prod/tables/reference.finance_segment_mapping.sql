CREATE OR REPLACE TABLE reference.finance_segment_mapping
(
    metric_type                          VARCHAR(255),
    event_store_id                       NUMBER(38, 0), -- for orders will be the order_store_id,
    -- for billings will be the order_store_id (never maps to retail stores)
    -- for acquistion metrics will be equal to the vip_store_id
    vip_store_id                         NUMBER(38, 0),
    is_retail_vip                        BOOLEAN,
    customer_gender                      VARCHAR(255),
    is_cross_promo                       BOOLEAN,
    finance_specialty_store              VARCHAR(255),
    -- report specific fields
    mapping_start_date                   DATE,
    mapping_end_date                     DATE,
    is_ddd_consolidated                  BOOLEAN,
    ddd_consolidated_currency_type       VARCHAR(255),
    is_ddd_individual                    BOOLEAN,
    ddd_individual_currency_type         VARCHAR(255),
    is_ddd_hyperion                      BOOLEAN,
    ddd_hyperion_currency_type           VARCHAR(255),
    is_retail_attribution_ddd            BOOLEAN,
    retail_attribution_ddd_currency_type VARCHAR(255),
    is_daily_cash_usd                    BOOLEAN,
    daily_cash_usd_currency_type         VARCHAR(255),
    is_daily_cash_eur                    BOOLEAN,
    daily_cash_eur_currency_type         VARCHAR(255),
    is_vip_tenure                        BOOLEAN,
    vip_tenure_currency_type             VARCHAR(255),
    is_lead_to_vip_waterfall             BOOLEAN,
    lead_to_vip_waterfall_currency_type  VARCHAR(255),
    is_weekly_kpi                        BOOLEAN,
    weekly_kpi_currency_type             VARCHAR(255),
    report_mapping                       VARCHAR(255),
    business_unit                        VARCHAR(255),
    store_brand                          VARCHAR(255)
);
