CREATE OR REPLACE VIEW reporting.retail_attribution_ddd_output AS
WITH _store_detail AS (
    SELECT fsm.oracle_store_id, ds.store_id, rs.retail_store
    FROM reference.finance_store_mapping fsm
    JOIN data_model.dim_store ds ON fsm.store_id = ds.store_id
    JOIN (SELECT DISTINCT retail_store FROM reporting.retail_attribution_final_output
          WHERE store_brand IN ('Fabletics', 'Yitty')
          AND store_region = 'NA') rs ON rs.retail_store = ds.store_full_name
)

SELECT 'FL+SC-' || IFF(ra.gender = 'M', 'M', 'W') || '-R-OREV-US' AS report_mapping,
       ra.month_date,
       SUM(ra.product_net_revenue)                                AS product_net_revenue,
       SUM(ra.billed_credit_net_billings)                         AS billed_credit_net_billings,
       SUM(ra.landed_product_cost)                                AS landed_product_cost,
       SUM(ra.shipping_cost)                                      AS shipping_cost
FROM reporting.retail_attribution_final_output ra
         JOIN _store_detail sd ON ra.retail_store = sd.retail_store
WHERE store_brand IN ('Fabletics', 'Yitty')
  AND store_region = 'NA'
  AND month_date >= '2022-01-01'
GROUP BY 1, 2

UNION ALL

SELECT 'FL+SC-' || IFF(ra.gender = 'M', 'M', 'W') || '-R-OREV-US-' || sd.oracle_store_id AS report_mapping,
       ra.month_date,
       SUM(ra.product_net_revenue)                                                       AS product_net_revenue,
       SUM(ra.billed_credit_net_billings)                                                AS billed_credit_net_billings,
       SUM(ra.landed_product_cost)                                                       AS landed_product_cost,
       SUM(ra.shipping_cost)                                                             AS shipping_cost
FROM reporting.retail_attribution_final_output ra
         JOIN _store_detail sd ON ra.retail_store = sd.retail_store
WHERE store_brand IN ('Fabletics', 'Yitty')
  AND store_region = 'NA'
  AND month_date >= '2022-01-01'
GROUP BY 1, 2
;
