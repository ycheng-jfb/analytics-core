CREATE TRANSIENT TABLE IF NOT EXISTS reporting.sxf.daily_bop_eop_counts(
    date DATE,
    store_reporting_name VARCHAR(50),
    store_brand_name VARCHAR(25),
    store_name VARCHAR(30),
    store_region_abbr VARCHAR(25),
    store_country_abbr VARCHAR(25),
    vip_activation_store_name VARCHAR(50),
    vip_activation_store_type VARCHAR(10),
    omni_channel VARCHAR(25),
    vip_cohort DATE,
    Tenure VARCHAR(30),
    Activation_Type VARCHAR(30),
    membership_Type VARCHAR(100),
    bop_vip_count NUMBER(38,0),
    eop_vip_count NUMBER(38,0)
  );
