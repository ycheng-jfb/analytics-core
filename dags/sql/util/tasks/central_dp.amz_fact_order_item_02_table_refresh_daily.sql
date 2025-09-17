CREATE OR REPLACE TASK util.tasks_central_dp.amz_fact_order_item_02_table_refresh_daily
    WAREHOUSE = da_wh_analytics
    AFTER util.tasks_central_dp.amz_dim_product_01_table_refresh_daily
    AS BEGIN
USE reporting_prod;

CREATE OR REPLACE TEMP TABLE _refunded_orders AS
SELECT DISTINCT amazon_order_id, CONVERT_TIMEZONE('America/Los_Angeles', posted_date) refund_posted_datetime
FROM lake_view.amazon_selling_partner.financial_shipment_event
WHERE event_type ILIKE 'refund_event';

CREATE OR REPLACE TEMP TABLE _amazon_today_orders AS
SELECT fse.amazon_order_id                                      AS order_id,
       o.order_id                                               AS tfg_order_id,
       mname.marketplace_id,
       fse.marketplace_name,
       s.store_id                                               AS store_id,
       CONVERT_TIMEZONE('America/Los_Angeles', fse.posted_date) AS purchase_datetime,
       CONVERT_TIMEZONE('America/Los_Angeles', fse.posted_date) AS latest_ship_datetime,
       CONVERT_TIMEZONE('America/Los_Angeles', fse.posted_date)    posted_datetime,
       settlement_id,
       settlement_end_date                                      AS settled_datetime,
       fse.marketplace_name                                     AS sales_channel,
       order_item_id,
       fce.currency_code,
       oi.asin,
       fsei.seller_sku                                          AS amz_seller_sku,
       quantity_shipped                                         AS quantity_ordered,
       quantity_shipped,
       fce.currency_amount                                      AS item_price_amount_local,
       fce.currency_amount * cer.exchange_rate                  AS item_price_amount_usd
FROM lake_view.amazon_selling_partner.financial_shipment_event fse
         LEFT JOIN lake_view.amazon_selling_partner.financial_shipment_event_item fsei
                   ON fse._fivetran_id = fsei.financial_shipment_event_id
         LEFT JOIN lake_view.amazon_selling_partner.financial_fee_component ffe
                   ON fsei._fivetran_id = ffe.linked_to_id
         LEFT JOIN lake_view.amazon_selling_partner.financial_charge_component fce
                   ON fsei._fivetran_id = fce.linked_to_id
                       AND fce.charge_type ILIKE 'Principal'
         LEFT JOIN (SELECT DISTINCT marketplace_id, sales_channel FROM lake_view.amazon_selling_partner.orders) mname
                   ON fse.marketplace_name = mname.sales_channel
         LEFT JOIN (SELECT DISTINCT asin, seller_sku FROM lake_view.amazon_selling_partner.order_item) oi
                   ON fsei.seller_sku = oi.seller_sku
         LEFT JOIN reporting_prod.amazon.settlement_report sr
                   ON fse.amazon_order_id = sr.order_id AND fsei.order_item_id = sr.order_item_code AND
                      transaction_type ILIKE 'order'
         LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date cer
                   ON cer.src_currency = fce.currency_code AND cer.dest_currency = 'USD'
                       AND CONVERT_TIMEZONE('America/Los_Angeles', fse.posted_date)::DATE = cer.rate_date_pst
         LEFT JOIN lake_consolidated.ultra_merchant.order_detail od
                   ON fse.amazon_order_id = od.value AND od.name = 'amazon-today-buyerOrderId'
         LEFT JOIN lake_consolidated.ultra_merchant."ORDER" o ON od.order_id = o.order_id
         LEFT JOIN edw_prod.stg.dim_store s ON o.store_id = s.store_id
WHERE event_type = 'shipment_event'
  AND ffe.fee_type ILIKE '%omni%'
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY amazon_order_id, order_item_id ORDER BY CONVERT_TIMEZONE('America/Los_Angeles', sr.posted_date) DESC) =
    1;


CREATE OR REPLACE TEMP TABLE _amazon_ecom_orders AS
SELECT DISTINCT o.amazon_order_id                                                                                         AS order_id,
                o.fulfillment_channel,
                o.marketplace_id,
                o.sales_channel                                                                                           AS marketplace_name,
                o.order_status,
                o.buyer_info_buyer_email,
                CONVERT_TIMEZONE('America/Los_Angeles', o.purchase_date)                                                     purchase_datetime,
                CONVERT_TIMEZONE('America/Los_Angeles', o.latest_ship_date)                                                  latest_ship_datetime,
                CONVERT_TIMEZONE('America/Los_Angeles', sr.posted_date)                                                   AS posted_date,
                settlement_id,
                settlement_end_date                                                                                       AS settled_datetime,
                RANK() OVER (PARTITION BY o.buyer_info_buyer_email ORDER BY purchase_datetime ASC, o.amazon_order_id ASC) AS buyer_order_rank,
                CASE
                    WHEN buyer_info_buyer_email IS NULL THEN 'NA'
                    WHEN buyer_order_rank = 1 THEN 'First Purchase'
                    WHEN buyer_order_rank > 1 THEN 'Repeat Purchase'
                    END                                                                                                   AS first_repeat,
                o.sales_channel,
                oi.order_item_id,
                oi.item_price_currency_code,
                oi.asin,
                oi.seller_sku                                                                                             AS amz_seller_sku,
                oi.quantity_ordered,
                oi.quantity_shipped,
                oi.item_price_amount                                                                                      AS item_price_amount_local,
                oi.item_price_amount * cer.exchange_rate                                                                  AS item_price_amount_usd,
                oi.item_tax_amount * cer.exchange_rate                                                                    AS item_tax_amount_usd,
                oi.shipping_price_amount * cer.exchange_rate                                                              AS shipping_price_amount_usd,
                oi.shipping_tax_amount * cer.exchange_rate                                                                AS shipping_tax_amount_usd,
                oi.shipping_discount_amount * cer.exchange_rate                                                           AS shipping_discount_amount_usd,
                oi.shipping_discount_tax_amount * cer.exchange_rate                                                       AS shipping_discount_tax_amount_usd,
                oi.promotion_discount_amount * cer.exchange_rate                                                          AS promotion_discount_amount_usd,
                oi.promotion_discount_tax_amount * cer.exchange_rate                                                      AS promotion_discount_tax_amount_usd
FROM lake_view.amazon_selling_partner.orders o
         LEFT OUTER JOIN lake_view.amazon_selling_partner.order_item oi ON o.amazon_order_id = oi.amazon_order_id
         LEFT JOIN reporting_prod.amazon.settlement_report sr
                   ON o.amazon_order_id = sr.order_id AND oi.order_item_id = sr.order_item_code AND
                      transaction_type ILIKE 'order'
         LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date cer
                   ON cer.src_currency = oi.item_price_currency_code AND cer.dest_currency = 'USD'
                       AND CONVERT_TIMEZONE('America/Los_Angeles', o.purchase_date)::DATE = cer.rate_date_pst
WHERE CONVERT_TIMEZONE('America/Los_Angeles', o.purchase_date) < CURRENT_DATE()
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o.amazon_order_id, order_item_id ORDER BY CONVERT_TIMEZONE('America/Los_Angeles', sr.posted_date) DESC) =
    1;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.amazon.fact_order_item AS
SELECT order_id,
       tfg_order_id,
       fulfillment_channel,
       marketplace_id,
       marketplace_name,
       order_status,
       record_source,
       store_id,
       buyer_info_buyer_email,
       purchase_datetime,
       latest_ship_datetime,
       posted_date,
       settlement_id,
       settled_datetime,
       IFF(buyer_info_buyer_email IS NULL, NULL, buyer_order_rank) AS buyer_order_rank,
       first_repeat,
       sales_channel,
       order_item_id,
       item_price_currency_code,
       asin,
       amz_seller_sku,
       quantity_ordered,
       quantity_shipped,
       item_price_amount_local,
       item_price_amount_usd,
       item_tax_amount_usd,
       shipping_price_amount_usd,
       shipping_tax_amount_usd,
       shipping_discount_amount_usd,
       shipping_discount_tax_amount_usd,
       promotion_discount_amount_usd,
       promotion_discount_tax_amount_usd,
       IFF(ro.amazon_order_id IS NULL, FALSE, TRUE)                   is_returned,
       refund_posted_datetime,
       CURRENT_TIMESTAMP                                           AS meta_create_datetime
FROM (SELECT order_id,
             -1            AS tfg_order_id,
             fulfillment_channel,
             marketplace_id,
             marketplace_name,
             order_status,
             'Amazon Ecom' AS record_source,
             5201          AS store_id,
             buyer_info_buyer_email,
             purchase_datetime,
             latest_ship_datetime,
             posted_date,
             settlement_id,
             settled_datetime,
             buyer_order_rank,
             first_repeat,
             sales_channel,
             order_item_id,
             item_price_currency_code,
             asin,
             amz_seller_sku,
             quantity_ordered,
             quantity_shipped,
             item_price_amount_local,
             item_price_amount_usd,
             item_tax_amount_usd,
             shipping_price_amount_usd,
             shipping_tax_amount_usd,
             shipping_discount_amount_usd,
             shipping_discount_tax_amount_usd,
             promotion_discount_amount_usd,
             promotion_discount_tax_amount_usd
      FROM _amazon_ecom_orders
      UNION ALL
      SELECT order_id,
             tfg_order_id,
             NULL AS fulfillment_channel,
             marketplace_id,
             marketplace_name,
             'Shipped' AS order_status,
             'Amazon Today' AS record_source,
             store_id,
             NULL AS buyer_info_buyer_email,
             purchase_datetime,
             latest_ship_datetime,
             posted_datetime,
             settlement_id,
             settled_datetime,
             NULL AS buyer_order_rank,
             'NA' AS first_repeat,
             sales_channel,
             order_item_id,
             currency_code,
             asin,
             amz_seller_sku,
             quantity_ordered,
             quantity_shipped,
             item_price_amount_local,
             item_price_amount_usd,
             NULL AS item_tax_amount_usd,
             NULL AS shipping_price_amount_usd,
             NULL AS shipping_tax_amount_usd,
             NULL AS shipping_discount_amount_usd,
             NULL AS shipping_discount_tax_amount_usd,
             NULL AS promotion_discount_amount_usd,
             NULL AS promotion_discount_tax_amount_usd
      FROM _amazon_today_orders) orders
         LEFT JOIN _refunded_orders ro
                   ON orders.order_id = ro.amazon_order_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY orders.order_id, order_item_id ORDER BY record_source ASC) = 1;

END;
