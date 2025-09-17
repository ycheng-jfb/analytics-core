CREATE OR REPLACE VIEW data_model.fact_customer_action_period
AS
SELECT
    fcap.customer_id,
    fcap.period_month_date,
    fcap.period_name,
    fcap.store_id,
    fcap.store_name,
    fcap.store_brand,
    fcap.store_brand_abbr,
    fcap.store_country,
    fcap.store_region,
    fcap.customer_action_category,
    fcap.vip_cohort_month_date,
    --meta_row_hash,
    fcap.meta_create_datetime,
    fcap.meta_update_datetime
FROM stg.fact_customer_action_period AS fcap
    LEFT JOIN stg.dim_customer AS dc
        ON fcap.customer_id = dc.customer_id
    LEFT JOIN stg.dim_store AS ds
        ON fcap.store_id = ds.store_id
WHERE ds.store_brand NOT IN ('Legacy')
    AND NOT NVL(dc.is_test_customer, False);
