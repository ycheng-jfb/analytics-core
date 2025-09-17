CREATE OR REPLACE VIEW reporting.pending_orders_30_days AS
SELECT fo.order_id,
       fo.customer_id,
       ds.store_id,
       ds.store_brand,
       ds.store_type,
       ds.store_country,
       dosc.order_sales_channel_key,
       dosc.order_sales_channel_l2,
       dosc.order_classification_l2,
       fo.order_local_datetime,
       fo.product_subtotal_local_amount,
       fo.cash_gross_revenue_local_amount,
       fo.unit_count,
       fo.token_count,
       fo.token_local_amount,
       datediff(DAY, cast(fo.order_local_datetime AS DATE), current_date) AS days_in_pending
FROM data_model.fact_order fo
         JOIN data_model.dim_store ds
              ON fo.store_id = ds.store_id
         JOIN data_model.dim_order_sales_channel dosc
              ON fo.order_sales_channel_key = dosc.order_sales_channel_key
         JOIN data_model.dim_order_status dos
              ON fo.order_status_key = dos.order_status_key
WHERE dos.order_status = 'Pending'
  AND datediff(DAY, cast(fo.order_local_datetime AS DATE), current_date) >= 30
