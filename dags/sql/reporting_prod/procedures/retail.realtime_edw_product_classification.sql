SET edw_start_date = DATEADD(DAY, -7, CURRENT_DATE());
SET edw_end_date = CURRENT_DATE();

CREATE OR REPLACE TEMP TABLE _store_location_timezone AS
SELECT DISTINCT st.store_id, rc.configuration_value, _sf_tz.sf_timezone, rc.retail_configuration_id
FROM lake_consolidated.ultra_merchant.store st
JOIN lake.ultra_warehouse.retail_location rl ON st.store_id = rl.store_id
JOIN lake.ultra_warehouse.retail_location_configuration rc ON rl.retail_location_id = rc.retail_location_id
JOIN (SELECT 'HST' AS timezone,'Pacific/Honolulu' AS sf_timezone UNION
      SELECT 'GMT' AS timezone,'GMT' AS sf_timezone UNION
      SELECT 'EST' AS timezone,'US/Eastern' AS sf_timezone UNION
      SELECT 'PST' AS timezone,'America/Los_Angeles' AS sf_timezone UNION
      SELECT 'CST' AS timezone,'America/Chicago' AS sf_timezone UNION
      SELECT 'MST' AS timezone,'America/Phoenix' AS sf_timezone
     ) _sf_tz ON _sf_tz.timezone = rc.configuration_value
where retail_configuration_id = 15;

CREATE OR REPLACE TEMP TABLE _dim_store_location_timezone AS
SELECT
    COALESCE(slt.sf_timezone, store_time_zone) AS sf_timezone,
    ds.*
FROM edw_prod.data_model.dim_store ds
LEFT JOIN _store_location_timezone slt ON slt.store_id = ds.store_id;

CREATE OR REPLACE TEMP TABLE _edw_orders AS
SELECT
    fo.store_id AS store_id,
    fo.order_id,
    fol.order_line_id,
    fol.product_id,
    TIME_SLICE(convert_timezone(dsl.sf_timezone,fo.order_local_datetime)::timestamp_ntz, 30,'minute', 'start') AS slot,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l1 = 'Activating VIP', fol.item_quantity, 0)) AS activating_unit_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l1 = 'Activating VIP', fol.product_gross_revenue_local_amount*fol.order_date_usd_conversion_rate, 0)) AS activating_revenue_net_vat_usd,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 IN ('Repeat VIP', 'First Guest', 'Repeat Guest'), fol.item_quantity, 0)) AS nonactivating_unit_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 IN ('Repeat VIP', 'First Guest', 'Repeat Guest'), fol.product_gross_revenue_local_amount*fol.order_date_usd_conversion_rate, 0)) AS nonactivating_revenue_net_vat_usd,
    SUM(IFF(dsc.order_classification_l2 IN ('Credit Billing','Token Billing') AND os.order_status = 'Success', fol.cash_gross_revenue_local_amount,0)) AS successful_billing_cash_gross_revenue,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'product order',fol.product_margin_pre_return_local_amount,0)) AS product_margin_pre_return,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'reship',fol.reporting_landed_cost_local_amount,0)) AS product_order_reship_product_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'reship',fol.shipping_cost_local_amount,0)) AS product_order_reship_shipping_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'reship',fol.estimated_shipping_supplies_cost_local_amount,0)) AS product_order_reship_shipping_supplies_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'exchange',fol.reporting_landed_cost_local_amount,0)) AS product_order_exchange_product_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'exchange',fol.shipping_cost_local_amount,0)) AS product_order_exchange_shipping_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'exchange',fol.estimated_shipping_supplies_cost_local_amount,0)) AS product_order_exchange_shipping_supplies_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) IN ('product order','reship','exchange'),fol.misc_cogs_local_amount,0)) AS product_order_misc_cogs_amount
FROM edw_prod.data_model.fact_order fo
LEFT JOIN edw_prod.data_model.fact_order_line fol ON fol.order_id=fo.order_id
JOIN _dim_store_location_timezone dsl ON dsl.store_id = fo.store_id
JOIN edw_prod.data_model.dim_store st ON st.store_id = fo.store_id
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_order_sales_channel dsc ON dsc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model.dim_order_status os ON os.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_membership_classification mc
    ON mc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model.fact_activation fa ON fa.activation_key = fo.activation_key
WHERE os.order_status IN ('Pending','Success')
    AND (dsc.order_classification_l1 = 'Product Order' or dsc.order_classification_l2 IN ('Credit Billing','Token Billing'))
    AND convert_timezone(dsl.sf_timezone,fo.order_local_datetime)::timestamp_ntz < $edw_end_date
    AND convert_timezone(dsl.sf_timezone,fo.order_local_datetime)::timestamp_ntz >= $edw_start_date
GROUP BY
    fo.store_id,
    fo.order_id,
    fol.product_id,
    fol.order_line_id,
    TIME_SLICE(convert_timezone(dsl.sf_timezone,fo.order_local_datetime)::timestamp_ntz, 30,'minute', 'start');


CREATE OR REPLACE TEMP TABLE _edw_refunds AS
SELECT
    r.store_id,
    time_slice(convert_timezone(dsl.sf_timezone, r.refund_completion_local_datetime)::TIMESTAMP_NTZ, 30,'minute','start') AS slot,
    r.order_id,
    units.order_line_id,
    units.product_id,
    COALESCE(units.units, 0) AS refund_units,
    r.refund_product_local_amount,
    fo.order_date_usd_conversion_rate,
    SUM(iff(dsc.order_classification_l1 = 'Product Order' AND
            drpm.refund_payment_method NOT IN ('Store Credit', 'Membership Token'),
            (r.refund_product_local_amount + r.refund_freight_local_amount) * fo.order_date_usd_conversion_rate, 0))
        + SUM(iff(dsc.order_classification_l1 = 'Product Order' AND
                  drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND
                  drpm.refund_payment_method_type = 'Cash Credit',
                  (r.refund_product_local_amount + r.refund_freight_local_amount) *
                  fo.order_date_usd_conversion_rate, 0)) AS refund_gross_vat_local,
    SUM(iff(dsc.order_classification_l2 IN ('Credit Billing', 'Token Billing') AND os.order_status = 'Success',
            (r.refund_product_local_amount + r.refund_freight_local_amount) *
            fo.order_date_usd_conversion_rate, 0)) AS successful_billing_refund_net_vat_usd,
    SUM(iff(dsc.order_classification_l1 = 'Product Order',
            r.cash_store_credit_refund_local_amount + r.unknown_store_credit_refund_local_amount,
            0)) AS product_order_cash_credit_refund_amount,
    SUM(iff(dsc.order_classification_l1 = 'Product Order', r.cash_refund_local_amount,
            0)) AS product_order_cash_refund_amount
FROM edw_prod.data_model.fact_refund r
JOIN edw_prod.data_model.dim_refund_payment_method drpm ON r.refund_payment_method_key = drpm.refund_payment_method_key
JOIN _dim_store_location_timezone dsl ON dsl.store_id = r.store_id
JOIN edw_prod.data_model.dim_refund_status ds ON ds.refund_status_key = r.refund_status_key
LEFT JOIN (SELECT return_id, product_id, order_line_id, SUM(COALESCE(frl.return_item_quantity, 0)) AS units
        FROM edw_prod.data_model.fact_return_line frl
        GROUP BY return_id, product_id, order_line_id) AS units
    ON iff(r.return_id > 0, r.return_id, 0) = units.return_id
JOIN edw_prod.data_model.fact_order fo ON fo.order_id = r.order_id
JOIN edw_prod.data_model.dim_store st ON st.store_id = fo.store_id
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_order_sales_channel dsc ON dsc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model.dim_order_status os ON os.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_membership_classification mc ON mc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model.fact_activation fa ON fa.activation_key = fo.activation_key
--          LEFT JOIN edw_prod.data_model.dim_product dp ON units.product_id = dp.product_id
--          LEFT JOIN lake.excel.fl_merch_items_ubt fmiu ON dp.product_sku = fmiu.sku
WHERE os.order_status IN ('Pending', 'Success')
    AND (dsc.order_classification_l1 = 'Product Order' OR dsc.order_classification_l2 IN ('Credit Billing', 'Token Billing'))
    AND ds.refund_status = 'Refunded'
    AND CONVERT_TIMEZONE(dsl.sf_timezone, r.refund_completion_local_datetime)::TIMESTAMP_NTZ < $edw_end_date
    AND CONVERT_TIMEZONE(dsl.sf_timezone , r.refund_completion_local_datetime)::TIMESTAMP_NTZ >= $edw_start_date
GROUP BY all;

CREATE OR REPLACE TEMP TABLE _edw_returns AS
SELECT
    frl.store_id,
    time_slice(convert_timezone(dsl.sf_timezone, frl.return_completion_local_datetime)::TIMESTAMP_NTZ, 30,'minute','start') AS slot, -- return time
    frl.order_id,
    frl.order_line_id,
    frl.product_id,
    SUM(frl.return_item_quantity) AS return_units,
    SUM(frl.estimated_return_shipping_cost_local_amount) AS product_order_return_shipping_cost_amount,
    SUM(frl.estimated_returned_product_cost_local_amount_resaleable) AS product_order_cost_product_returned_resaleable_amount
FROM edw_prod.data_model.fact_return_line frl
JOIN _dim_store_location_timezone dsl ON dsl.store_id = frl.store_id
JOIN edw_prod.data_model.dim_return_status AS rs ON rs.return_status_key = frl.return_status_key
JOIN edw_prod.data_model.fact_order fo ON fo.order_id = frl.order_id
JOIN edw_prod.data_model.dim_store st ON st.store_id = fo.store_id
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_order_sales_channel dsc ON dsc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model.dim_order_status os ON os.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_membership_classification mc
    ON mc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model.fact_activation fa ON fa.activation_key = fo.activation_key
WHERE os.order_status IN ('Pending', 'Success')
  AND (dsc.order_classification_l1 = 'Product Order' OR dsc.order_classification_l2 IN ('Credit Billing', 'Token Billing'))
  AND LOWER(rs.return_status) = 'resolved'
  AND CONVERT_TIMEZONE(dsl.sf_timezone, frl.return_completion_local_datetime)::TIMESTAMP_NTZ < $edw_end_date
  AND CONVERT_TIMEZONE(dsl.sf_timezone, frl.return_completion_local_datetime)::TIMESTAMP_NTZ >= $edw_start_date
GROUP BY all;

CREATE OR REPLACE TEMP TABLE _edw_chargebacks AS
SELECT
    fc.store_id,
    time_slice(convert_timezone(dsl.sf_timezone, fc.chargeback_datetime)::TIMESTAMP_NTZ, 30, 'minute', 'start') AS slot,
    fc.order_id,
    fol.product_id,
    fol.order_line_id,
    SUM(iff(dsc.order_classification_l1 = 'Product Order', fc.chargeback_local_amount, 0)) AS product_order_cash_chargeback_amount
FROM edw_prod.data_model.fact_chargeback fc
JOIN _dim_store_location_timezone dsl ON dsl.store_id = fc.store_id
JOIN edw_prod.data_model.fact_order fo ON fo.order_id = fc.order_id
LEFT JOIN edw_prod.data_model.fact_order_line fol ON fol.order_id = fo.order_id
JOIN edw_prod.data_model.dim_store st ON st.store_id = fo.store_id
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_order_sales_channel dsc ON dsc.order_sales_channel_key = fol.order_sales_channel_key
JOIN edw_prod.data_model.dim_order_status os ON os.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_membership_classification mc ON mc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model.fact_activation fa ON fa.activation_key = fo.activation_key
WHERE os.order_status IN ('Pending', 'Success')
    AND (dsc.order_classification_l1 = 'Product Order' OR dsc.order_classification_l2 IN ('Credit Billing', 'Token Billing'))
    AND CONVERT_TIMEZONE(dsl.sf_timezone, fc.chargeback_datetime)::TIMESTAMP_NTZ < $edw_end_date
    AND CONVERT_TIMEZONE(dsl.sf_timezone, fc.chargeback_datetime)::TIMESTAMP_NTZ >= $edw_start_date
GROUP BY all;

CREATE OR REPLACE TEMP TABLE _edw_orders_gm AS
SELECT
    COALESCE(o.store_id, rd.store_id, rt.store_id, cb.store_id) AS store_id,
    COALESCE(o.product_id, rd.product_id, rt.product_id, cb.product_id ) AS product_id,
    COALESCE(o.order_line_id ,rd.order_line_id, rt.order_line_id, cb.order_line_id ) AS order_line_id,
    COALESCE(o.slot, rd.slot, rt.slot, cb.slot) AS slot,
    SUM(o.activating_unit_count + o.nonactivating_unit_count) AS units,
    SUM(rt.return_units) AS return_units,
    SUM(rd.refund_units) AS refund_units,
    SUM(o.successful_billing_cash_gross_revenue) AS successful_billing_cash_gross_revenue,
    SUM(o.activating_revenue_net_vat_usd + o.nonactivating_revenue_net_vat_usd) AS product_gross_revenue_local_amount,
    SUM(IFNULL(product_margin_pre_return,0)
            - IFNULL(product_order_cash_credit_refund_amount,0)
            - IFNULL(product_order_cash_refund_amount,0)
            - IFNULL(product_order_cash_chargeback_amount,0)
            - IFNULL(product_order_return_shipping_cost_amount,0)
            + IFNULL(product_order_cost_product_returned_resaleable_amount,0)
            - IFNULL(product_order_reship_product_cost_amount,0)
            - IFNULL(product_order_reship_shipping_cost_amount,0)
            - IFNULL(product_order_reship_shipping_supplies_cost_amount,0)
            - IFNULL(product_order_exchange_product_cost_amount,0)
            - IFNULL(product_order_exchange_shipping_cost_amount,0)
            - IFNULL(product_order_exchange_shipping_supplies_cost_amount,0)
            - IFNULL(product_order_misc_cogs_amount,0)) AS product_gross_profit
FROM _edw_orders o
FULL JOIN _edw_refunds rd ON rd.store_id = o.store_id
    AND rd.product_id = o.product_id
    AND rd.order_line_id = o.order_line_id
    AND rd.slot = o.slot
FULL JOIN _edw_returns rt ON rt.STORE_ID = o.STORE_ID
    AND rt.product_id = o.product_id
    AND rt.order_line_id = o.order_line_id
    AND rt.slot = o.slot
FULL JOIN _edw_chargebacks cb ON cb.STORE_ID = o.STORE_ID
    AND cb.product_id = o.product_id
    AND cb.order_line_id = o.order_line_id
    AND cb.slot = o.slot
GROUP BY ALL;

CREATE OR REPLACE TEMP TABLE _edw_scaffold AS
SELECT DISTINCT store_id, slot, product_id, order_line_id
FROM _edw_orders_gm
UNION
SELECT DISTINCT store_id, slot, product_id, order_line_id
FROM _edw_refunds;

CREATE OR REPLACE TEMP TABLE _product_category AS
SELECT DISTINCT
    product_segment,
    category AS raw_category,
    CASE WHEN CONTAINS(category,'JACKET') THEN 'JACKET'
         WHEN CONTAINS(category,'BOTTOM') THEN 'BOTTOM'
         WHEN CONTAINS(category,'TOP') THEN 'TOP'
         WHEN CONTAINS(category,'ONEPIECES') or CONTAINS(category,'ONE PIECES') THEN 'ONEPIECES'
         WHEN CONTAINS(category,'BRA') THEN 'BRA'
         WHEN CONTAINS(category,'UNDERWEAR') or CONTAINS(category,'PANTIES') THEN 'UNDERWEAR'
         WHEN category IN ('BAGS', 'HATS', 'MISC ACC', 'SOCKS', 'SHOES', 'SCRUB ACCESSORIES') THEN 'ACCESSORIES'
        ELSE category END AS category,
    sku
FROM lake.excel.fl_merch_items_ubt
ORDER BY 1, 2;

CREATE OR REPLACE TEMP TABLE _edw_orders_gm_women AS
SELECT DISTINCT pc.category, o.* FROM _edw_orders_gm o
 LEFT JOIN edw_prod.data_model.dim_product dp ON o.product_id = dp.product_id
 LEFT JOIN _product_category pc ON dp.product_sku = pc.sku
where pc.product_segment in  ('FLWOMENS', 'YITTY', 'SCWOMENS')
    AND pc.category!='ACCESSORIES';

CREATE OR REPLACE TEMP TABLE _edw_orders_gm_men AS
SELECT DISTINCT pc.category, o.* FROM _edw_orders_gm o
 LEFT JOIN edw_prod.data_model.dim_product dp ON o.product_id = dp.product_id
 LEFT JOIN _product_category pc ON dp.product_sku = pc.sku
WHERE pc.product_segment in ('FLMENS', 'SCMENS');

CREATE OR REPLACE TEMP TABLE _edw_orders_gm_other AS
SELECT DISTINCT IFF(UPPER(dp.product_type) = 'BAG FEE', 'ACCESSORIES', pc.category) AS category, o.* from _edw_orders_gm o
 LEFT JOIN edw_prod.data_model.dim_product dp ON o.product_id = dp.product_id
 LEFT JOIN _product_category pc ON dp.product_sku = pc.sku
WHERE pc.product_segment = 'FLACCESSORIES'
   OR (pc.product_segment = 'YITTY' AND pc.category='ACCESSORIES')
   OR (pc.product_segment = 'FLWOMENS' AND pc.category='ACCESSORIES')
   OR UPPER(dp.product_type) = 'BAG FEE';


TRUNCATE TABLE reporting_prod.retail.realtime_edw_product_classification;


INSERT INTO reporting_prod.retail.realtime_edw_product_classification
(
    store_code,
    slot,
    date,
    event_store_location,
    women_tops_dollar,
    women_tops_units,
    women_tops_refund_units,
    women_tops_return_units,
    women_bottoms_dollar,
    women_bottoms_units,
    women_bottoms_refund_units,
    women_bottoms_return_units,
    women_onesie_dollar,
    women_onesie_units,
    women_onesie_refund_units,
    women_onesie_return_units,
    women_jackets_dollar,
    women_jackets_units,
    women_jackets_refund_units,
    women_jackets_return_units,
    women_bra_dollar,
    women_bra_units,
    women_bra_refund_units,
    women_bra_return_units,
    women_under_dollar,
    women_under_units,
    women_under_refund_units,
    women_under_return_units,
    women_swim_dollar,
    women_swim_units,
    women_swim_refund_units,
    women_swim_return_units,
    men_tops_dollar,
    men_tops_units,
    men_tops_refund_units,
    men_tops_return_units,
    men_bottoms_dollar,
    men_bottoms_units,
    men_bottoms_refund_units,
    men_bottoms_return_units,
    men_jackets_dollar,
    men_jackets_units,
    men_jackets_refund_units,
    men_jackets_return_units,
    men_under_dollar,
    men_under_units,
    men_under_refund_units,
    men_under_return_units,
    accessories_dollar,
    accessories_units,
    accessories_refund_units,
    accessories_return_units
)
SELECT cs.store_retail_location_code AS store_code,
       s.slot,
       DATE_TRUNC('day', s.slot) AS date,
       cs.store_type AS event_store_location,
       SUM(IFF(ow.category IN ('TOP'), ow.product_gross_revenue_local_amount,0)) AS womens_tops_dollar,
       SUM(IFF(ow.category IN ('TOP'), ow.units, 0)) AS womens_tops_units,
       SUM(IFF(ow.category IN ('TOP'), ow.refund_units, 0)) AS womens_tops_refund_units,
       SUM(IFF(ow.category IN ('TOP'), ow.return_units, 0)) AS womens_tops_return_units,

       SUM(IFF(ow.category IN ('BOTTOM'), ow.product_gross_revenue_local_amount,0)) AS womens_bottoms_dollar,
       SUM(IFF(ow.category IN ('BOTTOM'), ow.units, 0)) AS womens_bottoms_units,
       SUM(IFF(ow.category IN ('BOTTOM'), ow.refund_units, 0)) AS womens_bottoms_refund_units,
       SUM(IFF(ow.category IN ('BOTTOM'), ow.return_units, 0)) AS womens_bottoms_return_units,

       SUM(IFF(ow.category IN ('ONEPIECES'), ow.product_gross_revenue_local_amount,0)) AS womens_onesie_dollar,
       SUM(IFF(ow.category IN ('ONEPIECES'), ow.units, 0))  AS womens_onesie_units,
       SUM(IFF(ow.category IN ('ONEPIECES'), ow.refund_units, 0)) AS womens_onesie_refund_units,
       SUM(IFF(ow.category IN ('ONEPIECES'), ow.return_units, 0)) AS womens_onesie_return_units,

       SUM(IFF(ow.category IN ('JACKET'), ow.product_gross_revenue_local_amount,0)) AS womens_jackets_dollar,
       SUM(IFF(ow.category IN ('JACKET'), ow.units, 0)) AS womens_jackets_units,
       SUM(IFF(ow.category IN ('JACKET'), ow.refund_units, 0)) AS womens_jackets_refund_units,
       SUM(IFF(ow.category IN ('JACKET'), ow.return_units, 0)) AS womens_jackets_return_units,

       SUM(IFF(ow.category IN ('BRA'), ow.product_gross_revenue_local_amount,0)) AS womens_bra_dollar,
       SUM(IFF(ow.category IN ('BRA'), ow.units, 0)) AS womens_bra_units,
       SUM(IFF(ow.category IN ('BRA'), ow.refund_units, 0)) AS womens_bra_refund_units,
       SUM(IFF(ow.category IN ('BRA'), ow.return_units, 0)) AS womens_bra_return_units,

       SUM(IFF(ow.category IN ('UNDERWEAR'), ow.product_gross_revenue_local_amount,0)) AS womens_under_dollar,
       SUM(IFF(ow.category IN ('UNDERWEAR'), ow.units, 0)) AS womens_under_units,
       SUM(IFF(ow.category IN ('UNDERWEAR'), ow.refund_units, 0)) AS womens_under_refund_units,
       SUM(IFF(ow.category IN ('UNDERWEAR'), ow.return_units, 0)) AS womens_under_return_units,

       SUM(IFF(ow.category IN ('SWIMWEAR'), ow.product_gross_revenue_local_amount,0)) AS womens_swim_dollar,
       SUM(IFF(ow.category IN ('SWIMWEAR'), ow.units, 0)) AS womens_swim_units,
       SUM(IFF(ow.category IN ('SWIMWEAR'), ow.refund_units, 0)) AS womens_swim_refund_units,
       SUM(IFF(ow.category IN ('SWIMWEAR'), ow.return_units, 0)) AS womens_swim_return_units,

       SUM(IFF(om.category IN ('TOP'), om.product_gross_revenue_local_amount,0)) AS mens_tops_dollar,
       SUM(IFF(om.category IN ('TOP'), om.units, 0)) AS mens_tops_units,
       SUM(IFF(om.category IN ('TOP'), om.refund_units, 0)) AS mens_tops_refund_units,
       SUM(IFF(om.category IN ('TOP'), om.return_units, 0)) AS mens_tops_return_units,

       SUM(IFF(om.category IN ('BOTTOM'), om.product_gross_revenue_local_amount,0)) AS mens_bottoms_dollar,
       SUM(IFF(om.category IN ('BOTTOM'), om.units, 0)) AS mens_bottoms_units,
       SUM(IFF(om.category IN ('BOTTOM'), om.refund_units, 0)) AS mens_bottoms_refund_units,
       SUM(IFF(om.category IN ('BOTTOM'), om.return_units, 0)) AS mens_bottoms_return_units,

       SUM(IFF(om.category IN ('JACKET'), om.product_gross_revenue_local_amount,0)) AS mens_jackets_dollar,
       SUM(IFF(om.category IN ('JACKET'), om.units, 0)) AS mens_jackets_units,
       SUM(IFF(om.category IN ('JACKET'), om.refund_units, 0)) AS mens_jackets_refund_units,
       SUM(IFF(om.category IN ('JACKET'), om.return_units, 0)) AS mens_jackets_return_units,

       SUM(IFF(om.category IN ('UNDERWEAR'), om.product_gross_revenue_local_amount,0)) AS mens_under_dollar,
       SUM(IFF(om.category IN ('UNDERWEAR'), om.units, 0)) AS mens_under_units,
       SUM(IFF(om.category IN ('UNDERWEAR'), om.refund_units, 0)) AS mens_under_refund_units,
       SUM(IFF(om.category IN ('UNDERWEAR'), om.return_units, 0)) AS mens_under_return_units,

       SUM(IFF(ot.category IN ('ACCESSORIES'), ot.product_gross_revenue_local_amount, 0)) AS accessories_dollar,
       SUM(IFF(ot.category IN ('ACCESSORIES'), ot.units, 0))  AS accessories_units,
       SUM(IFF(ot.category IN ('ACCESSORIES'), ot.refund_units, 0)) AS accessories_refund_units,
       SUM(IFF(ot.category IN ('ACCESSORIES'), ot.return_units, 0)) AS accessories_return_units
FROM  _edw_scaffold s
JOIN edw_prod.data_model.dim_store cs ON s.store_id = cs.store_id
LEFT JOIN _edw_orders_gm_women ow ON ow.store_id = s.store_id
     AND s.slot = ow.slot
     AND s.order_line_id = ow.order_line_id
     AND s.product_id = ow.product_id
LEFT JOIN _edw_orders_gm_men om ON om.store_id = s.store_id
    AND s.slot = om.slot
    AND s.order_line_id = om.order_line_id
    AND s.product_id = om.product_id
LEFT JOIN _edw_orders_gm_other ot ON ot.store_id = s.store_id
    AND s.slot = ot.slot
    AND s.order_line_id = ot.order_line_id
    AND s.product_id = ot.product_id
WHERE cs.store_brand IN ('Fabletics')
GROUP BY ALL;
