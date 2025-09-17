CREATE OR REPLACE TRANSIENT TABLE month_end.retail_discount_order AS
SELECT date_trunc(MONTH, order_completion_local_datetime::DATE) AS month_date,
       st.store_brand,
       st.store_region,
       st.store_full_name,
       st.store_id,
       osc.is_retail_ship_only_order,
       fo.order_id,
       dp.promo_code,
       iff((dp.promo_code ILIKE '%team40%'
                OR dp.promo_code ILIKE '%team50%'
                OR dp.promo_code ILIKE '%teamtfg%'),
           'Employee Discount',
           'Normal Promo')                                      AS discount_type,
       'Local Net VAT'                                          AS currency_type,
       fo.product_subtotal_local_amount,
       fo.product_discount_local_amount,
       fo.shipping_revenue_local_amount,
       fo.non_cash_credit_local_amount,
       fo.product_gross_revenue_local_amount
FROM edw_prod.data_model.fact_order fo
         JOIN edw_prod.data_model.dim_order_status os ON os.order_status_key = fo.order_status_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = fo.store_id
         JOIN edw_prod.data_model.fact_order_discount od ON od.order_id = fo.order_id
         JOIN edw_prod.data_model.dim_order_sales_channel osc
              ON osc.order_sales_channel_key = fo.order_sales_channel_key
         JOIN edw_prod.data_model.dim_promo_history dp ON dp.promo_history_key = od.promo_history_key
WHERE store_type = 'Retail'
  AND os.order_status = 'Success'
  AND month_date >= '2021-01-01';

ALTER TABLE month_end.retail_discount_order
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO month_end.retail_discount_order_snapshot
SELECT month_date,
       store_brand,
       store_region,
       store_full_name,
       store_id,
       is_retail_ship_only_order,
       order_id,
       promo_code,
       discount_type,
       currency_type,
       product_subtotal_local_amount,
       product_discount_local_amount,
       shipping_revenue_local_amount,
       non_cash_credit_local_amount,
       product_gross_revenue_local_amount,
       CURRENT_TIMESTAMP                    AS snapshot_datetime
FROM month_end.retail_discount_order;

DELETE
FROM month_end.retail_discount_order_snapshot
WHERE snapshot_datetime < DATEADD(MONTH, -12, getdate());
