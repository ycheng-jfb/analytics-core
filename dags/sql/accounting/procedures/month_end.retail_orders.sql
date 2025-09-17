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
WHERE convert_timezone('America/Los_Angeles', o.datetime_added) >= '2015-09-11'

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
  AND convert_timezone('America/Los_Angeles', o.datetime_added) >= '2015-09-11';

--getting retail uniform orders
CREATE OR REPLACE TEMPORARY TABLE _retail_u AS
SELECT oc.order_id
FROM lake_consolidated_view.ultra_merchant.order_classification oc
WHERE oc.order_type_id = 27;

--deleting retail uniform orders from base orders
DELETE
FROM _orders o USING _retail_u r
WHERE o.order_id = r.order_id;

CREATE OR REPLACE TEMPORARY TABLE _order_data AS
SELECT fo.store_id,
--         bo.STORE_BRAND,
--         bo.STORE_COUNTRY,
       fo.order_id,
       doc.order_classification_l1                                                         order_classification,
       iff(fo.order_membership_classification_key = 2, 'Activating', 'Non-Activating') AS  activating,
       fo.unit_count                                                                       units,
       o.subtotal                                                                          subtotal,
       o.discount                                                                          discount,
       o.shipping                                                                          shipping,
       o.tax                                                                               tax,
       fo.shipping_cost_local_amount                                                       cost_shipping,
       o.credit                                                                            credit,
       iff(o.payment_method <> 'cash', zeroifnull(fo.payment_transaction_local_amount), 0) creditcard,
       iff(o.payment_method = 'cash', zeroifnull(fo.payment_transaction_local_amount), 0)  cash, --ISNULL(ptc.payment_transaction_cash_gross_of_vat_local_amount, 0) / (1 + ISNULL(o.effective_vat_rate, 0))
       dp.payment_method,
       fo.shipped_local_datetime,
       fo.payment_transaction_local_datetime,
       fo.order_local_datetime,
       c.customer_id,
       iff(fo.order_membership_classification_key IN (1, 3), 1, 0)                         ecom,
       iff(doc.order_classification_l1 = 'Product Order'
               AND fo.order_membership_classification_key = 2, 1, 0)                       vip_activation,
       CASE
           WHEN fo.order_membership_classification_key IN (1, 3) THEN 'PAYG Retail'
           WHEN doc.order_classification_l1 = 'Product Order'
                    AND fo.order_membership_classification_key = 2
               THEN 'Activating'
           WHEN doc.order_classification_l1 = 'Product Order' AND
                fo.order_membership_classification_key NOT IN (1, 2) THEN 'Repeat'
           WHEN doc.order_classification_l1 <> 'Product Order' THEN 'Repeat'
           ELSE 'Unknown'
           END                                                                             order_segment,
       c.datetime_added::date                                                              customer_datetime_added,
       convert_timezone('America/Los_Angeles', fo.order_local_datetime)                    order_datetime_added
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
       row_number() over (partition BY O.ORDER_ID ORDER BY OD.DATETIME_ADDED) rnk
FROM _orders o
         JOIN lake_consolidated_view.ultra_merchant.order_detail od ON od.order_id = o.order_id
    AND od.name = 'retail_store_id';

CREATE OR REPLACE TRANSIENT TABLE month_end.retail_orders AS
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
                       AND o.payment_transaction_local_datetime:: DATE = exchp.full_date
         LEFT JOIN _exchange_rate excho
                   ON excho.src_currency = ds.store_currency -- add in exchange rate to convert to USD
                       AND o.order_local_datetime:: DATE = excho.full_date;

ALTER TABLE month_end.retail_orders SET DATA_RETENTION_TIME_IN_DAYS = 0;
