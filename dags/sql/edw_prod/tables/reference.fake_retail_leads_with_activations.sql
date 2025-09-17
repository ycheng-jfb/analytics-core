CREATE OR REPLACE TABLE reference.fake_retail_leads_with_activations
(
    customer_id                  NUMBER(38,0),
    meta_create_datetime         TIMESTAMPLTZ DEFAULT current_timestamp,
    meta_update_datetime         TIMESTAMPLTZ DEFAULT current_timestamp
);
