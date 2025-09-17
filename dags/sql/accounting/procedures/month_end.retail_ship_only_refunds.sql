CREATE OR REPLACE TEMPORARY TABLE _retail_refund AS
SELECT DISTINCT s.order_id
FROM month_end.retail_ship_only_sales s
         JOIN lake_consolidated_view.ultra_merchant.retail_return rr ON rr.order_id = s.order_id;

CREATE OR REPLACE TRANSIENT TABLE month_end.retail_ship_only_refunds AS
SELECT r.refund_id,
       s.sales_retail_store,
       s.sales_retail_location                                                    retail_sales_location,
       s.store_brand,
       s.store_country,
       s.order_id,
       s.payment_transaction_id,
       r.payment_transaction_id                                                   return_payment_transaction_id,
       s.date_shipped,
       date_trunc(MONTH, s.date_shipped) ::date                                   month_shipped, iff(rr.order_id IS NOT NULL, 'Retail', 'Online') AS return_location,
       iff(r.payment_method = 'store_credit', r.product_refund - r.tax_refund, 0) store_credit_refund,
       iff(r.payment_method = 'creditcard', r.product_refund - r.tax_refund, 0)   creditcard_refund,
       iff(r.payment_method = 'cash', r.product_refund - r.tax_refund, 0)         cash_refund,
       CAST(r.datetime_refunded AS DATE)                                          date_refunded,
       date_trunc(MONTH, r.datetime_refunded)::date                               month_refunded, r.product_refund - r.tax_refund total_refund
FROM month_end.retail_ship_only_sales s
         JOIN lake_consolidated_view.ultra_merchant.refund r ON r.order_id = s.order_id
         LEFT JOIN _retail_refund rr ON rr.order_id = r.order_id
WHERE r.statuscode = 4570;

ALTER TABLE month_end.retail_ship_only_refunds SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO month_end.retail_ship_only_refunds_snapshot
SELECT refund_id,
       sales_retail_store,
       retail_sales_location,
       order_id,
       payment_transaction_id,
       return_payment_transaction_id,
       date_shipped,
       month_shipped,
       return_location,
       store_credit_refund,
       creditcard_refund,
       cash_refund,
       date_refunded,
       month_refunded,
       total_refund,
       CURRENT_TIMESTAMP AS snapshot_timestamp,
       store_brand,
       store_country
FROM month_end.retail_ship_only_refunds;
--
-- alter table month_end.retail_ship_only_refunds_snapshot
-- add brand varchar default null, country varchar default null
-- ;
--
-- update month_end.retail_ship_only_refunds_snapshot r
-- set brand = ds.store_brand, country = ds.store_country
-- from  EDW_PROD.stg.dim_store ds
-- where ds.store_id = r.sales_retail_store

DELETE
FROM month_end.retail_ship_only_refunds_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
