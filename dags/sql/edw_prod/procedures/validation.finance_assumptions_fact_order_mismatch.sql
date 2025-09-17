TRUNCATE TABLE validation.finance_assumptions_fact_order_mismatch;

CREATE OR REPLACE TEMP TABLE _fact_order_base AS
SELECT fo.order_id,
       fo.master_order_id,
       ds.store_brand,
       ds.store_region,
       ds.store_country,
       ds.store_id,
       dosc.order_classification_l2,
       dosc.is_third_party,
       CASE
           WHEN ds.store_type = 'Retail' AND dosc.is_retail_ship_only_order = TRUE THEN 'Online'
           WHEN ds.store_type = 'Mobile App' THEN 'Online'
           ELSE ds.store_type END                                                 AS store_type,
       DATE_TRUNC(MONTH, CAST(
           COALESCE(fo.shipped_local_datetime, fo.order_local_datetime) AS DATE)) AS order_month,
       fo.estimated_shipping_supplies_cost_local_amount,
       fo.estimated_shipping_cost_local_amount,
       fo.estimated_variable_gms_cost_local_amount,
       fo.estimated_variable_warehouse_cost_local_amount,
       fo.estimated_variable_payment_processing_pct_cash_revenue
FROM stg.fact_order fo
         JOIN stg.dim_order_status dos
              ON fo.order_status_key = dos.order_status_key
         JOIN stg.dim_store ds
              ON ds.store_id = fo.store_id
         JOIN stg.dim_order_sales_channel AS dosc
              ON dosc.order_sales_channel_key = fo.order_sales_channel_key
WHERE dos.order_status IN ('Success', 'Pending')
  AND fo.order_local_datetime >= '2022-01-01 00:00:00.000 -08:00';


CREATE OR REPLACE TEMP TABLE _third_party AS
SELECT DISTINCT fob.order_id
FROM _fact_order_base fob
         JOIN stg.fact_order fo
              ON fo.master_order_id = fob.order_id
WHERE fo.order_status_key = 8;


CREATE OR REPLACE TEMP TABLE _fact_order_assumption_difference AS
SELECT DISTINCT fob.order_id,
                fa.bu,
                fob.order_month,
                fa.variable_payment_processing_pct_cash_revenue  - fob.estimated_variable_payment_processing_pct_cash_revenue                                        AS diff_variable_payment_processing,
                IFF(fob.store_type <> 'Retail',
                    fa.variable_gms_cost_per_order - fob.estimated_variable_gms_cost_local_amount,
                    0)                                                                                            AS diff_variable_gms_cost,
                IFF(fob.order_classification_l2 IN ('Exchange', 'Product Order', 'Reship'),
                    fa.variable_warehouse_cost_per_order - fob.estimated_variable_warehouse_cost_local_amount,
                    0)                                                                                            AS diff_variable_warehouse_cost,
                IFF(fob.order_classification_l2 IN ('Exchange', 'Product Order', 'Reship') AND
                    fob.store_type <> 'Retail', fa.shipping_cost_per_order - fob.estimated_shipping_cost_local_amount,
                    0)                                                                                            AS diff_variable_shipping_cost,
                IFF(fob.order_classification_l2 IN ('Exchange', 'Product Order', 'Reship') AND
                    fob.store_type <> 'Retail',
                    fa.shipping_supplies_cost_per_order - fob.estimated_shipping_supplies_cost_local_amount,
                    0)                                                                                            AS diff_shipping_supplies_cost
FROM _fact_order_base fob
         LEFT JOIN reference.finance_assumption AS fa
                   ON fa.brand = fob.store_brand
                       AND IFF(fa.region_type = 'Region', fob.store_region, fob.store_country) =
                           fa.region_type_mapping
                       AND fob.order_month = fa.financial_date
                       AND fa.store_type = fob.store_type
WHERE fob.order_id NOT IN (select order_id from _third_party);

INSERT INTO validation.finance_assumptions_fact_order_mismatch
SELECT
    bu,
    order_month,
    SUM(IFF(ABS(diff_shipping_supplies_cost) > 0.5
    OR ABS(diff_variable_gms_cost) > 0.5
    OR ABS(diff_variable_payment_processing) > 0.5
    OR ABS(diff_variable_shipping_cost) > 0.5
    OR ABS(diff_variable_warehouse_cost) > 0.5, 1, 0)) AS variance_record_count,
    COUNT(*)                                     AS total_record_count
FROM _fact_order_assumption_difference
GROUP BY bu, order_month
HAVING variance_record_count > 10
ORDER BY bu, order_month;
