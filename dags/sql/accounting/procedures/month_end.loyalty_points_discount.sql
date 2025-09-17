-- item sold cost per point -- price per point
CREATE OR REPLACE TRANSIENT TABLE month_end.loyalty_points_discount AS
SELECT promo_id AS                                                                                 promo_id,
       DATE_TRUNC(MONTH, CONVERT_TIMEZONE('America/Los_Angeles', fo.shipped_local_datetime))::DATE month_date,
       fo.store_id,
       dc.gender,
       SUM(od.amount)                                                                              discount_amount,
       COUNT(*)                                                                                    order_count
FROM edw_prod.data_model.fact_order fo
         JOIN lake_consolidated_view.ultra_merchant.order_discount od ON od.order_id = fo.order_id
         JOIN edw_prod.data_model.dim_customer dc
              ON dc.customer_id = fo.customer_id
WHERE fo.order_status_key = 1 --'Success'
  AND promo_id IS NOT NULL
GROUP BY 1, 2, 3, 4;

ALTER TABLE month_end.loyalty_points_discount
    SET DATA_RETENTION_TIME_IN_DAYS = 0;
