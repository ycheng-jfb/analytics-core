CREATE OR REPLACE TRANSIENT TABLE month_end.retail_ship_only_sales AS
SELECT fo.store_id                                                                           sales_retail_store,
       ro.store_brand,
       ro.store_country,
       iff(st.store_id IS NULL, 'Unclassified',
           iff(st.store_name = 'JustFab Retail (Glendale)', 'Glendale', st.store_full_name)) sales_retail_location,
       ro.order_id,
       o.capture_payment_transaction_id                                                      payment_transaction_id,
       convert_timezone('America/Los_Angeles', o.date_shipped)                               date_shipped,
       date_trunc(MONTH, date_shipped)::date                                                 shipped_month, ro.units,
       o.customer_id,
       ro.creditcard                                                                         cash_collected,
       ro.subtotal,
       ro.discount,
       ro.subtotal - ro.discount                                                             net_rev,
       o.credit                                                                              credit_used,
       ro.shipping                                                                           shipping_rev,
       ro.tax                                                                                tax_collected,
       ro.estimated_cost_product_usd                                                         product_cost,
       ro.cost_shipping                                                                      shipping_cost
FROM month_end.retail_orders ro
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON ro.order_id = o.order_id
         JOIN edw_prod.stg.fact_order fo ON ro.order_id = fo.order_id
         LEFT JOIN edw_prod.stg.dim_store st ON st.store_id = fo.store_id
WHERE date_shipped IS NOT NULL;

ALTER TABLE month_end.retail_ship_only_sales SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO month_end.retail_ship_only_sales_snapshot
SELECT sales_retail_store,
       sales_retail_location,
       order_id,
       payment_transaction_id,
       date_shipped,
       shipped_month,
       units,
       customer_id,
       cash_collected,
       subtotal,
       discount,
       net_rev,
       credit_used,
       shipping_rev,
       tax_collected,
       product_cost,
       shipping_cost,
       CURRENT_TIMESTAMP AS snapshot_timestamp,
       store_brand,
       store_country
FROM month_end.retail_ship_only_sales;

DELETE
FROM month_end.retail_ship_only_sales_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
