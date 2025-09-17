-- this is buying at the store but no size and people placing order online at store

CREATE OR REPLACE TEMPORARY TABLE _stores AS
SELECT store_id, store_brand, store_country
FROM edw_prod.stg.dim_store s
WHERE s.store_type = 'Retail'
  AND s.store_country = 'US'
  AND s.store_brand IN ('Fabletics', 'Savage X');

------------------------------------------------------------------------
-- putting exchange rates to convert source currency to USD by day into a table
-- doing in a temp table because when trying to do in the final join, the query was taking 30+ minutes
CREATE OR REPLACE TEMPORARY TABLE _exchange_rate AS
SELECT src_currency,
       dd.full_date,
       er.exchange_rate
FROM edw_prod.reference.currency_exchange_rate er
         JOIN edw_prod.data_model.dim_date dd ON er.effective_start_datetime <= dd.full_date
    AND er.effective_end_datetime > dd.full_date
WHERE dest_currency = 'USD'
  AND dd.full_date < CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _orders AS
SELECT o.order_id, s.store_brand, s.store_country
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN _stores s ON o.store_id = s.store_id
WHERE CONVERT_TIMEZONE('America/Los_Angeles', o.datetime_added) >= '2015-09-11'

UNION

SELECT o.order_id, ds.store_brand, ds.store_country
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.stg.fact_order fo
              ON fo.order_id = o.order_id
         JOIN edw_prod.data_model.dim_order_sales_channel doc
              ON doc.order_sales_channel_key = fo.order_sales_channel_key
         JOIN edw_prod.stg.dim_store ds ON o.store_id = ds.store_id
    AND ds.store_brand IN ('Fabletics', 'Savage X')
WHERE doc.is_retail_ship_only_order = TRUE
  AND CONVERT_TIMEZONE('America/Los_Angeles', o.datetime_added) >= '2015-09-11';

--getting retail uniform orders
CREATE OR REPLACE TEMPORARY TABLE _retail_u AS
SELECT oc.order_id
FROM lake_consolidated_view.ultra_merchant.order_classification oc
WHERE oc.order_type_id = 27;

--deleting retail uniform orders from base orders
DELETE
FROM _orders o
    USING _retail_u r
WHERE o.order_id = r.order_id;

CREATE OR REPLACE TEMPORARY TABLE _order_data AS
SELECT fo.store_id,
--         bo.STORE_BRAND,
--         bo.STORE_COUNTRY,
       fo.order_id,
       doc.order_classification_l1                                                         order_classification,
       IFF(fo.order_membership_classification_key = 2, 'Activating', 'Non-Activating') AS  activating,
       fo.unit_count                                                                       units,
       o.subtotal                                                                          subtotal,
       o.discount                                                                          discount,
       o.shipping                                                                          shipping,
       o.tax                                                                               tax,
       fo.shipping_cost_local_amount                                                       cost_shipping,
       o.credit                                                                            credit,
       IFF(o.payment_method <> 'cash', ZEROIFNULL(fo.payment_transaction_local_amount), 0) creditcard,
       IFF(o.payment_method = 'cash', ZEROIFNULL(fo.payment_transaction_local_amount),
           0)                                                                              cash, --ISNULL(ptc.payment_transaction_cash_gross_of_vat_local_amount, 0) / (1 + ISNULL(o.effective_vat_rate, 0))
       dp.payment_method,
       fo.shipped_local_datetime,
       fo.payment_transaction_local_datetime,
       fo.order_local_datetime,
       c.customer_id,
       IFF(fo.order_membership_classification_key IN (1, 3), 1, 0)                         ecom,
       IFF(doc.order_classification_l1 = 'Product Order' AND fo.order_membership_classification_key = 2, 1,
           0)                                                                              vip_activation,
       CASE
           WHEN fo.order_membership_classification_key IN (1, 3) THEN 'PAYG Retail'
           WHEN doc.order_classification_l1 = 'Product Order' AND fo.order_membership_classification_key = 2
               THEN 'Activating'
           WHEN doc.order_classification_l1 = 'Product Order' AND
                fo.order_membership_classification_key NOT IN (1, 2) THEN 'Repeat'
           WHEN doc.order_classification_l1 <> 'Product Order' THEN 'Repeat'
           ELSE 'Unknown'
           END                                                                             order_segment,
       c.datetime_added::DATE                                                              customer_datetime_added,
       CONVERT_TIMEZONE('America/Los_Angeles', fo.order_local_datetime)                    order_datetime_added
FROM _orders bo
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = bo.order_id
         JOIN edw_prod.data_model.fact_order fo ON fo.order_id = bo.order_id
         JOIN edw_prod.data_model.dim_payment dp ON dp.payment_key = fo.payment_key
         JOIN edw_prod.data_model.dim_order_sales_channel doc
              ON doc.order_sales_channel_key = fo.order_sales_channel_key
         JOIN edw_prod.data_model.dim_order_membership_classification dod
              ON fo.order_membership_classification_key = dod.order_membership_classification_key
         LEFT JOIN lake_consolidated_view.ultra_merchant.customer c ON c.customer_id = o.customer_id;

--original store_id
CREATE OR REPLACE TEMPORARY TABLE _original_store AS
SELECT o.order_id,
       od.value                                                               retail_store_id,
       ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY od.datetime_added) rnk
FROM _orders o
         JOIN lake_consolidated_view.ultra_merchant.order_detail od ON od.order_id = o.order_id
    AND od.name = 'retail_store_id';

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.retail_orders AS
SELECT ds.store_id,
       ds.store_brand,
       ds.store_country,
       COALESCE(os.retail_store_id, ds.store_id)                                  mapped_store_id,
       o.order_id,
       o.order_classification,
       o.activating,
       o.units,
       o.subtotal,
       o.discount,
       o.shipping,
       o.tax,
       fopc.estimated_landed_cost_local_amount *
       COALESCE(exchs.exchange_rate, exchp.exchange_rate, excho.exchange_rate, 1) estimated_cost_product_usd,
       o.cost_shipping,
       o.credit,
       o.creditcard,
       o.cash,
       o.payment_method,
       o.customer_id,
       o.ecom,
       o.vip_activation,
       o.order_segment,
       o.customer_datetime_added,
       o.order_datetime_added,
       CURRENT_TIMESTAMP                                                          tbl_datetime_added
FROM _order_data o
         LEFT JOIN edw_prod.stg.fact_order_product_cost fopc ON o.order_id = fopc.order_id
         JOIN edw_prod.stg.dim_store ds ON ds.store_id = o.store_id
         LEFT JOIN _original_store os ON os.order_id = o.order_id
    AND os.rnk = 1 AND LEN(retail_store_id) > 1
         LEFT JOIN _exchange_rate exchs
                   ON exchs.src_currency = ds.store_currency -- add in exchange rate to convert to USD
                       AND o.shipped_local_datetime::DATE = exchs.full_date
         LEFT JOIN _exchange_rate exchp
                   ON exchp.src_currency = ds.store_currency -- add in exchange rate to convert to USD
                       AND o.payment_transaction_local_datetime::DATE = exchp.full_date
         LEFT JOIN _exchange_rate excho
                   ON excho.src_currency = ds.store_currency -- add in exchange rate to convert to USD
                       AND o.order_local_datetime::DATE = excho.full_date;

ALTER TABLE reporting_base_prod.shared.retail_orders
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

CREATE OR REPLACE TEMPORARY TABLE _retail_store_id AS
SELECT order_id,
       value retail_store_id,
       datetime_added
FROM lake_consolidated_view.ultra_merchant.order_detail
WHERE name = 'retail_store_id';

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.retail_ship_only_sales AS
SELECT fo.store_id                                                                           sales_retail_store,
       ro.store_brand,
       ro.store_country,
       IFF(st.store_id IS NULL, 'Unclassified',
           IFF(st.store_name = 'JustFab Retail (Glendale)', 'Glendale', st.store_full_name)) sales_retail_location,
       ro.order_id,
       o.capture_payment_transaction_id                                                      payment_transaction_id,
       CONVERT_TIMEZONE('America/Los_Angeles', o.date_shipped)                               date_shipped,
       DATE_TRUNC(MONTH, date_shipped)::DATE                                                 shipped_month,
       ro.units,
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
FROM reporting_base_prod.shared.retail_orders ro
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON ro.order_id = o.order_id
         JOIN edw_prod.stg.fact_order fo ON ro.order_id = fo.order_id
         LEFT JOIN edw_prod.stg.dim_store st ON st.store_id = fo.store_id
WHERE date_shipped IS NOT NULL;

ALTER TABLE reporting_base_prod.shared.retail_ship_only_sales
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

CREATE OR REPLACE TEMPORARY TABLE _retail_refund AS
SELECT DISTINCT s.order_id
FROM reporting_base_prod.shared.retail_ship_only_sales s
         JOIN lake_consolidated_view.ultra_merchant.retail_return rr ON rr.order_id = s.order_id;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.retail_ship_only_refunds AS
SELECT r.refund_id,
       s.sales_retail_store,
       s.sales_retail_location                                                    retail_sales_location,
       s.store_brand,
       s.store_country,
       s.order_id,
       s.payment_transaction_id,
       r.payment_transaction_id                                                   return_payment_transaction_id,
       s.date_shipped,
       DATE_TRUNC(MONTH, s.date_shipped) ::DATE                                   month_shipped,
       IFF(rr.order_id IS NOT NULL, 'Retail', 'Online') AS                        return_location,
       IFF(r.payment_method = 'store_credit', r.product_refund - r.tax_refund, 0) store_credit_refund,
       IFF(r.payment_method = 'creditcard', r.product_refund - r.tax_refund, 0)   creditcard_refund,
       IFF(r.payment_method = 'cash', r.product_refund - r.tax_refund, 0)         cash_refund,
       CAST(r.datetime_refunded AS DATE)                                          date_refunded,
       DATE_TRUNC(MONTH, r.datetime_refunded)::DATE                               month_refunded,
       r.product_refund - r.tax_refund                                            total_refund
FROM reporting_base_prod.shared.retail_ship_only_sales s
         JOIN lake_consolidated_view.ultra_merchant.refund r ON r.order_id = s.order_id
         LEFT JOIN _retail_refund rr ON rr.order_id = r.order_id
WHERE r.statuscode = 4570;

ALTER TABLE reporting_base_prod.shared.retail_ship_only_refunds
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

--snapshots
-- create table REPORTING_BASE_PROD.SHARED.retail_ship_only_sales_snapshot as
INSERT INTO reporting_base_prod.shared.retail_ship_only_sales_snapshot
SELECT sales_retail_store
     , sales_retail_location
     , order_id
     , payment_transaction_id
     , date_shipped
     , shipped_month
     , units
     , customer_id
     , cash_collected
     , subtotal
     , discount
     , net_rev
     , credit_used
     , shipping_rev
     , tax_collected
     , product_cost
     , shipping_cost
     , CURRENT_TIMESTAMP AS snapshot_timestamp
     , store_brand
     , store_country
FROM reporting_base_prod.shared.retail_ship_only_sales;

DELETE
FROM reporting_base_prod.shared.retail_ship_only_sales_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());

-- create table REPORTING_BASE_PROD.SHARED.retail_ship_only_refunds_snapshot as
INSERT INTO reporting_base_prod.shared.retail_ship_only_refunds_snapshot
SELECT refund_id
     , sales_retail_store
     , retail_sales_location
     , order_id
     , payment_transaction_id
     , return_payment_transaction_id
     , date_shipped
     , month_shipped
     , return_location
     , store_credit_refund
     , creditcard_refund
     , cash_refund
     , date_refunded
     , month_refunded
     , total_refund
     , CURRENT_TIMESTAMP AS snapshot_timestamp
     , store_brand
     , store_country
FROM reporting_base_prod.shared.retail_ship_only_refunds;
--
-- alter table REPORTING_BASE_PROD.SHARED.retail_ship_only_refunds_snapshot
-- add brand varchar default null, country varchar default null
-- ;
--
-- update REPORTING_BASE_PROD.SHARED.retail_ship_only_refunds_snapshot r
-- set brand = ds.store_brand, country = ds.store_country
-- from  EDW_PROD.stg.dim_store ds
-- where ds.store_id = r.sales_retail_store

DELETE
FROM reporting_base_prod.shared.retail_ship_only_refunds_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
