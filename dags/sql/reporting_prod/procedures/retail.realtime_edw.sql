
SET edw_start_date = dateadd(day,-30,current_date());
SET edw_end_date = current_date();

------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE _workday_employee_ids AS
SELECT
    associate_id,
    try_to_number(workday_id) as workday_id,
    start_date as start_datetime,
    COALESCE(lead(start_date) over (partition by associate_id order by start_date),current_timestamp) as end_datetime
FROM (SELECT distinct
        a.administrator_id as associate_id,
        COALESCE(e.employee_id, e2.employee_id ,t2.workday_id) as workday_id,
        COALESCE(a.DATE_ADDED,e.START_DATE,t2.start_datetime) as START_DATE
    FROM lake.ultra_identity.administrator a
    LEFT JOIN lake.WORKDAY.EMPLOYEES e
        on a.login = e.employee_id
        AND e.company IN ('170 JF Retail Services','190 Savage X Fenty','192 Savage X Retail')
    LEFT JOIN lake.WORKDAY.EMPLOYEES e2
        on a.login != e2.employee_id
        and lower(trim(a.firstname))=lower(trim(e2.FIRST_NAME))
        AND lower(trim(a.lastname))=lower(trim(e2.last_NAME))
        AND e2.company IN ('170 JF Retail Services','190 Savage X Fenty','192 Savage X Retail')
        and ((a.login!='NRamos') or (a.login='NRamos' and year(e2.start_date) = '2021'))
    left JOIN (
        SELECT distinct
            COALESCE(try_to_number(trim(up_for_euid.value,'"')),-2) AS associate_id,
            COALESCE(trim(up_for_wdid.value,'"'),emp.employee_id) as workday_id,
            up_for_wdid.datetime_added AS start_datetime
        FROM lake_view.ultra_identity.user_property AS up_for_euid
        LEFT JOIN lake_view.ultra_identity.user_property AS up_for_wdid
            ON up_for_euid.user_id = up_for_wdid.user_id
            AND up_for_wdid.key = 'workdayEmployeeId'
        JOIN lake_view.ultra_identity.user AS u
            ON u.user_id = up_for_euid.user_id
        LEFT JOIN lake.workday.employees emp
            ON lower(trim(emp.first_name))=lower(trim(u.firstname))
            AND lower(trim(emp.last_name))=lower(trim(u.lastname))
            AND emp.company in ('170 JF Retail Services','190 Savage X Fenty','192 Savage X Retail')
        WHERE up_for_euid.key = 'ecomUserId'
      ) t2 on t2.associate_id = a.administrator_id
) abc
;

create or replace  temporary  table _order_tracking_id_null as
-- ecom issue , John Blayter is helping on this. Roshan to backfill after they make updates on their end
select order_id
from lake_consolidated.ultra_merchant."ORDER"
where ORDER_TRACKING_ID is null
    and datetime_modified >= dateadd(day,-1,$edw_start_date)
union
select order_id
from edw_prod.data_model.fact_order
where administrator_id =-1
    and order_local_datetime >= dateadd(day,-1,$edw_start_date)
union
select r.order_id
from edw_prod.data_model.fact_order fo
join edw_prod.data_model.fact_refund r on r.order_id = fo.order_id
where administrator_id =-1
    and refund_completion_local_datetime >= dateadd(day,-1,$edw_start_date)
;

create or replace  temporary  table _missing_administrator_ids as
select cw.administrator_id,o.order_id
from lake_consolidated.ultra_merchant."ORDER" o
JOIN lake_consolidated.ultra_merchant.SESSION s  on s.SESSION_ID=o.session_id
JOIN lake_consolidated.ultra_merchant.CART c on c.SESSION_ID=s.SESSION_ID
JOIN lake_consolidated.ultra_merchant.CART_WAREHOUSE cw on c.CART_ID=cw.cart_id
join _order_tracking_id_null otn on o.ORDER_ID=otn.order_id;


create or replace temp table _store_location_timezone as
select
distinct st.STORE_ID, rc.CONFIGURATION_VALUE,_sf_tz.sf_timezone
from lake_consolidated.ultra_merchant.store st
join lake.ULTRA_WAREHOUSE.RETAIL_LOCATION rl on st.STORE_ID = rl.STORE_ID
join lake.ULTRA_WAREHOUSE.RETAIL_LOCATION_CONFIGURATION rc on rl.RETAIL_LOCATION_ID = rc.RETAIL_LOCATION_ID
join (SELECT 'HST' as timezone,'Pacific/Honolulu' as sf_timezone union
      SELECT 'GMT' as timezone,'GMT' as sf_timezone union
      SELECT 'EST' as timezone,'US/Eastern' as sf_timezone union
      SELECT 'PST' as timezone,'America/Los_Angeles' as sf_timezone union
      SELECT 'CST' as timezone,'America/Chicago' as sf_timezone union
      SELECT 'MST' as timezone,'America/Phoenix' as sf_timezone
     ) _sf_tz ON _sf_tz.timezone = rc.CONFIGURATION_VALUE
where RETAIL_CONFIGURATION_ID = 15
;

create or replace temp table _dim_store_location_timezone as
select coalesce(slt.sf_timezone,STORE_TIME_ZONE) as sf_timezone,ds.*
from edw_prod.DATA_MODEL.DIM_STORE ds
left join _store_location_timezone slt on slt.store_id = ds.store_id
;

CREATE OR REPLACE TEMP TABLE _edw_orders AS
SELECT
    fa.sub_store_id AS vip_store_id,
    fo.store_id AS event_store_id,
    dc.gender AS customer_gender,
    dc.is_cross_promo AS fk_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end AS finance_sub_store,
    TIME_SLICE(convert_timezone(dsl.sf_timezone,fo.order_local_datetime)::timestamp_ntz, 30,'minute', 'start') AS slot,
    COALESCE(w.workday_id,-1) AS employee_number,
    MAX(fo.meta_create_datetime) AS max_refresh_time,
    SUM(IFF(dsc.order_classification_l2 IN ('Credit Billing','Token Billing') AND order_status = 'Success', cash_gross_revenue_local_amount,0)) AS successful_billing_cash_gross_revenue,
    COUNT(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l1 = 'Activating VIP', fo.order_id, null)) AS activating_order_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l1 = 'Activating VIP', unit_count, 0)) AS activating_unit_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l1 = 'Activating VIP', product_gross_revenue_local_amount*fo.order_date_usd_conversion_rate, 0)) AS activating_revenue_net_vat_usd,
    COUNT(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 IN ('Repeat VIP', 'First Guest', 'Repeat Guest'), fo.order_id, null)) AS nonactivating_order_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 IN ('Repeat VIP', 'First Guest', 'Repeat Guest'), unit_count, 0)) AS nonactivating_unit_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 IN ('Repeat VIP', 'First Guest', 'Repeat Guest'), product_gross_revenue_local_amount*fo.order_date_usd_conversion_rate, 0)) AS nonactivating_revenue_net_vat_usd,
    -- the next three are subsets of 'Non-Activating'
    COUNT(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 = 'First Guest', fo.order_id, null)) AS first_guest_order_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 = 'First Guest', unit_count, 0)) AS first_guest_unit_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 = 'First Guest', product_gross_revenue_local_amount*fo.order_date_usd_conversion_rate, 0)) AS first_guest_revenue_net_vat_usd,
    COUNT(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 = 'Repeat Guest', fo.order_id, null)) AS repeat_guest_order_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 = 'Repeat Guest', unit_count, 0)) AS repeat_guest_unit_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 = 'Repeat Guest', product_gross_revenue_local_amount*fo.order_date_usd_conversion_rate, 0)) AS repeat_guest_revenue_net_vat_usd,
    COUNT(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 = 'Repeat VIP', fo.order_id, null)) AS repeat_vip_order_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 = 'Repeat VIP', unit_count, 0)) AS repeat_vip_unit_count,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 = 'Repeat VIP', product_gross_revenue_local_amount*fo.order_date_usd_conversion_rate, 0)) AS repeat_vip_revenue_net_vat_usd,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'product order',fo.product_margin_pre_return_local_amount,0)) AS product_margin_pre_return,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'reship',reporting_landed_cost_local_amount,0)) AS product_order_reship_product_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'reship',shipping_cost_local_amount,0)) AS product_order_reship_shipping_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'reship',estimated_shipping_supplies_cost_local_amount,0)) AS product_order_reship_shipping_supplies_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'exchange',reporting_landed_cost_local_amount,0)) AS product_order_exchange_product_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'exchange',shipping_cost_local_amount,0)) AS product_order_exchange_shipping_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) = 'exchange',estimated_shipping_supplies_cost_local_amount,0)) AS product_order_exchange_shipping_supplies_cost_amount,
    SUM(IFF(LOWER(dsc.order_classification_l1) IN ('product order','reship','exchange'),misc_cogs_local_amount,0)) AS product_order_misc_cogs_amount
    FROM edw_prod.data_model.fact_order fo
join _dim_store_location_timezone dsl on dsl.store_id = fo.store_id
LEFT JOIN _missing_administrator_ids m ON fo.order_id = m.order_id
LEFT JOIN _workday_employee_ids w ON w.associate_id= iff(fo.administrator_id = -1,m.administrator_id ,fo.administrator_id)  AND fo.order_local_datetime >= w.start_datetime AND fo.order_local_datetime < w.end_datetime
JOIN edw_prod.data_model.dim_store st ON st.store_id = fo.store_id
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_order_sales_channel dsc ON dsc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model.dim_order_status os ON os.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_membership_classification mc ON mc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model.fact_activation fa ON fa.activation_key = fo.activation_key
WHERE os.order_status IN ('Pending','Success')
    AND (dsc.order_classification_l1 = 'Product Order' or dsc.order_classification_l2 IN ('Credit Billing','Token Billing'))
    AND convert_timezone(dsl.sf_timezone,fo.order_local_datetime)::timestamp_ntz < $edw_end_date
    AND convert_timezone(dsl.sf_timezone,fo.order_local_datetime)::timestamp_ntz >= $edw_start_date
GROUP BY fa.sub_store_id,
    fo.store_id,
    dc.gender,
    dc.is_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end,
    TIME_SLICE(convert_timezone(dsl.sf_timezone,fo.order_local_datetime)::timestamp_ntz, 30,'minute', 'start'),
    COALESCE(w.workday_id,-1);

CREATE OR REPLACE TEMP TABLE _edw_vips as
SELECT
    fa.sub_store_id AS vip_store_id,
    fa.sub_store_id AS event_store_id,
    dc.gender AS customer_gender,
    dc.is_cross_promo AS fk_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end AS finance_sub_store,
    time_slice(convert_timezone(dsl.sf_timezone,fa.activation_local_datetime)::timestamp_ntz,
        30,'minute', 'start') AS slot,
    COALESCE(w.workday_id,-1) AS employee_number,
    COUNT(*) AS vips
FROM edw_prod.data_model.fact_activation fa
JOIN edw_prod.data_model.fact_order fo ON fo.order_id=fa.order_id
join _dim_store_location_timezone dsl on dsl.store_id = fo.store_id
LEFT JOIN _missing_administrator_ids m ON fo.order_id = m.order_id
LEFT JOIN _workday_employee_ids w ON w.associate_id= iff(fo.administrator_id = -1,m.administrator_id ,fo.administrator_id)  AND fa.activation_local_datetime >= w.start_datetime AND fa.activation_local_datetime < w.end_datetime
JOIN edw_prod.data_model.dim_store st ON st.store_id = fa.store_id
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fa.customer_id
WHERE convert_timezone(dsl.sf_timezone,fa.activation_local_datetime)::timestamp_ntz  < $edw_end_date
AND convert_timezone(dsl.sf_timezone,fa.activation_local_datetime)::timestamp_ntz >= $edw_start_date
and fo.order_id <> -1
GROUP BY fa.sub_store_id,
    fa.sub_store_id,
    dc.gender,
    dc.is_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end,
    time_slice(convert_timezone(dsl.sf_timezone,fa.activation_local_datetime)::timestamp_ntz,
        30,'minute', 'start'),
    COALESCE(w.workday_id,-1);


CREATE OR REPLACE TEMP TABLE _edw_refunds as
SELECT
    fa.sub_store_id AS vip_store_id,
    fo.store_id AS event_store_id,
    dc.gender AS customer_gender,
    dc.is_cross_promo AS fk_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end AS finance_sub_store,
    time_slice(convert_timezone(dsl.sf_timezone,r.refund_completion_local_datetime)::timestamp_ntz, 30,'minute', 'start') AS slot, -- refund time
    COALESCE(w.workday_id,-1) AS employee_number,
    COUNT(distinct r.order_id) AS refund_orders,
    SUM(COALESCE(units.units,0)) AS refund_units,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' and drpm.refund_payment_method NOT IN ('Store Credit','Membership Token'),(r.refund_product_local_amount + r.refund_freight_local_amount )* fo.order_date_usd_conversion_rate,0))
    + SUM(IFF(dsc.order_classification_l1 = 'Product Order' and drpm.refund_payment_method IN ('Store Credit','Membership Token') and drpm.refund_payment_method_type = 'Cash Credit',(r.refund_product_local_amount + r.refund_freight_local_amount )* fo.order_date_usd_conversion_rate,0))
    AS refund_gross_vat_local,
    SUM(IFF(dsc.order_classification_l2 IN ('Credit Billing','Token Billing') AND order_status = 'Success',(r.refund_product_local_amount + r.refund_freight_local_amount )* fo.order_date_usd_conversion_rate,0)) AS successful_billing_refund_net_vat_usd,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND mc.membership_order_type_l3 IN ('Repeat VIP', 'First Guest', 'Repeat Guest'),(r.refund_product_local_amount + r.refund_freight_local_amount )* fo.order_date_usd_conversion_rate,0)) AS nonactivating_product_refund_net_vat_usd,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order' AND  mc.membership_order_type_l3 IN ('Activating VIP') ,(r.refund_product_local_amount + r.refund_freight_local_amount )* fo.order_date_usd_conversion_rate,0)) AS activating_product_refund_net_vat_usd,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order',r.cash_store_credit_refund_local_amount + r.unknown_store_credit_refund_local_amount,0)) AS product_order_cash_credit_refund_amount,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order',r.cash_refund_local_amount,0)) AS product_order_cash_refund_amount
FROM edw_prod.data_model.fact_refund r
JOIN edw_prod.data_model.dim_refund_payment_method drpm on r.refund_payment_method_key =drpm.refund_payment_method_key
join _dim_store_location_timezone dsl on dsl.store_id = r.store_id
JOIN edw_prod.data_model.dim_refund_status ds ON ds.refund_status_key= r.refund_status_key
LEFT JOIN (SELECT return_id, SUM(COALESCE(frl.return_item_quantity,0)) AS units FROM edw_prod.data_model.fact_return_line frl GROUP BY return_id) AS units ON IFF(r.return_id>0,r.return_id,0) =units.return_id
JOIN edw_prod.data_model.fact_order fo ON fo.order_id = r.order_id
LEFT JOIN _missing_administrator_ids m ON fo.order_id = m.order_id
LEFT JOIN _workday_employee_ids w ON w.associate_id= iff(fo.administrator_id = -1,m.administrator_id ,fo.administrator_id)  AND r.refund_completion_local_datetime >= w.start_datetime AND r.refund_completion_local_datetime < w.end_datetime
JOIN edw_prod.data_model.dim_store st ON st.store_id = fo.store_id
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_order_sales_channel dsc ON dsc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model.dim_order_status os ON os.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_membership_classification mc ON mc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model.fact_activation fa ON fa.activation_key = fo.activation_key
WHERE os.order_status IN ('Pending','Success')
    AND (dsc.order_classification_l1 = 'Product Order' or dsc.order_classification_l2 IN ('Credit Billing','Token Billing'))
    AND ds.refund_status='Refunded'
    AND convert_timezone(dsl.sf_timezone,r.refund_completion_local_datetime)::timestamp_ntz < $edw_end_date
    AND convert_timezone(dsl.sf_timezone,r.refund_completion_local_datetime)::timestamp_ntz >= $edw_start_date
GROUP BY fa.sub_store_id,
    fo.store_id,
    dc.gender,
    dc.is_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end,
    time_slice(convert_timezone(dsl.sf_timezone,r.refund_completion_local_datetime)::timestamp_ntz, 30,'minute', 'start'), -- refund time
    COALESCE(w.workday_id,-1);


CREATE OR REPLACE TEMP TABLE _edw_returns as
SELECT
    fa.sub_store_id AS vip_store_id,
    fo.store_id AS event_store_id,
    dc.gender AS customer_gender,
    dc.is_cross_promo AS fk_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end AS finance_sub_store,
    time_slice(convert_timezone(dsl.sf_timezone,frl.return_completion_local_datetime)::timestamp_ntz, 30,'minute', 'start') AS slot, -- return time
    COALESCE(w.workday_id,-1) AS employee_number,
    SUM(frl.estimated_return_shipping_cost_local_amount) AS product_order_return_shipping_cost_amount,
    SUM(frl.estimated_returned_product_cost_local_amount_resaleable) AS product_order_cost_product_returned_resaleable_amount
FROM edw_prod.data_model.fact_return_line frl
join _dim_store_location_timezone dsl on dsl.store_id = frl.store_id
JOIN edw_prod.data_model.dim_return_status AS rs ON rs.return_status_key = frl.return_status_key
JOIN edw_prod.data_model.fact_order fo ON fo.order_id = frl.order_id
LEFT JOIN _missing_administrator_ids m ON fo.order_id = m.order_id
LEFT JOIN _workday_employee_ids w ON w.associate_id= iff(fo.administrator_id = -1,m.administrator_id ,fo.administrator_id)  AND frl.return_completion_local_datetime >= w.start_datetime AND frl.return_completion_local_datetime < w.end_datetime
JOIN edw_prod.data_model.dim_store st ON st.store_id = fo.store_id
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_order_sales_channel dsc ON dsc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model.dim_order_status os ON os.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_membership_classification mc ON mc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model.fact_activation fa ON fa.activation_key = fo.activation_key
WHERE os.order_status IN ('Pending','Success')
    AND (dsc.order_classification_l1 = 'Product Order' or dsc.order_classification_l2 IN ('Credit Billing','Token Billing'))
    AND LOWER(rs.return_status) = 'resolved'
    AND convert_timezone(dsl.sf_timezone,frl.return_completion_local_datetime)::timestamp_ntz < $edw_end_date
    AND convert_timezone(dsl.sf_timezone,frl.return_completion_local_datetime)::timestamp_ntz >= $edw_start_date
GROUP BY fa.sub_store_id,
    fo.store_id,
    dc.gender,
    dc.is_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end,
    time_slice(convert_timezone(dsl.sf_timezone,frl.return_completion_local_datetime)::timestamp_ntz, 30,'minute', 'start'), -- refund time
    COALESCE(w.workday_id,-1);
 
 
CREATE OR REPLACE TEMP TABLE _edw_chargebacks as
SELECT
    fa.sub_store_id AS vip_store_id,
    fo.store_id AS event_store_id,
    dc.gender AS customer_gender,
    dc.is_cross_promo AS fk_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end AS finance_sub_store,
    time_slice(convert_timezone(dsl.sf_timezone,fc.chargeback_datetime)::timestamp_ntz, 30,'minute', 'start') AS slot, -- return time
    COALESCE(w.workday_id,-1) AS employee_number,
    SUM(IFF(dsc.order_classification_l1 = 'Product Order',fc.chargeback_local_amount,0)) AS product_order_cash_chargeback_amount
FROM edw_prod.data_model.fact_chargeback fc
join _dim_store_location_timezone dsl on dsl.store_id = fc.store_id
JOIN edw_prod.data_model.fact_order fo ON fo.order_id = fc.order_id
LEFT JOIN _missing_administrator_ids m ON fo.order_id = m.order_id
LEFT JOIN _workday_employee_ids w ON w.associate_id= iff(fo.administrator_id = -1,m.administrator_id ,fo.administrator_id)  AND fc.chargeback_datetime >= w.start_datetime AND fc.chargeback_datetime < w.end_datetime
JOIN edw_prod.data_model.dim_store st ON st.store_id = fo.store_id
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_order_sales_channel dsc ON dsc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model.dim_order_status os ON os.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_membership_classification mc ON mc.order_membership_classification_key = fo.order_membership_classification_key
JOIN edw_prod.data_model.fact_activation fa ON fa.activation_key = fo.activation_key
WHERE os.order_status IN ('Pending','Success')
    AND (dsc.order_classification_l1 = 'Product Order' or dsc.order_classification_l2 IN ('Credit Billing','Token Billing'))
    AND convert_timezone(dsl.sf_timezone,fc.chargeback_datetime)::timestamp_ntz < $edw_end_date
    AND convert_timezone(dsl.sf_timezone,fc.chargeback_datetime)::timestamp_ntz >= $edw_start_date
GROUP BY fa.sub_store_id,
    fo.store_id,
    dc.gender,
    dc.is_cross_promo,
    CASE WHEN dc.customer_id IN (50362353130, 87518041030) THEN 'BF'
        WHEN dc.specialty_country_code IN ('AT','BE','AU') THEN dc.specialty_country_code
        WHEN st.store_brand IN ('FabKids','ShoeDazzle') AND specialty_country_code = 'CA' THEN 'CA'
        ELSE 'none' end,
    time_slice(convert_timezone(dsl.sf_timezone,fc.chargeback_datetime)::timestamp_ntz, 30,'minute', 'start'), -- refund time
    COALESCE(w.workday_id,-1);   
    

create or replace temp table _edw_orders_gm as
select 
coalesce(o.VIP_STORE_ID ,rd.VIP_STORE_ID,rt.VIP_STORE_ID,cb.VIP_STORE_ID ) as VIP_STORE_ID,
coalesce(o.EVENT_STORE_ID ,rd.EVENT_STORE_ID,rt.EVENT_STORE_ID,cb.EVENT_STORE_ID ) as EVENT_STORE_ID,
coalesce(o.CUSTOMER_GENDER ,rd.CUSTOMER_GENDER,rt.CUSTOMER_GENDER,cb.CUSTOMER_GENDER ) as CUSTOMER_GENDER,
coalesce(o.FK_CROSS_PROMO ,rd.FK_CROSS_PROMO,rt.FK_CROSS_PROMO,cb.FK_CROSS_PROMO ) as FK_CROSS_PROMO,
coalesce(o.FINANCE_SUB_STORE ,rd.FINANCE_SUB_STORE,rt.FINANCE_SUB_STORE,cb.FINANCE_SUB_STORE ) as FINANCE_SUB_STORE,
coalesce(o.slot ,rd.slot,rt.slot,cb.slot ) as slot,
coalesce(o.employee_number ,rd.employee_number,rt.employee_number,cb.employee_number) as employee_number,
sum(SUCCESSFUL_BILLING_CASH_GROSS_REVENUE) as SUCCESSFUL_BILLING_CASH_GROSS_REVENUE,
sum(ACTIVATING_ORDER_COUNT) as ACTIVATING_ORDER_COUNT,
sum(ACTIVATING_UNIT_COUNT) as ACTIVATING_UNIT_COUNT,
sum(ACTIVATING_REVENUE_NET_VAT_USD) as ACTIVATING_REVENUE_NET_VAT_USD,
sum(NONACTIVATING_ORDER_COUNT) as NONACTIVATING_ORDER_COUNT,
sum(NONACTIVATING_UNIT_COUNT) as NONACTIVATING_UNIT_COUNT,
sum(NONACTIVATING_REVENUE_NET_VAT_USD) as NONACTIVATING_REVENUE_NET_VAT_USD,
sum(FIRST_GUEST_ORDER_COUNT) as FIRST_GUEST_ORDER_COUNT,
sum(FIRST_GUEST_UNIT_COUNT) as FIRST_GUEST_UNIT_COUNT,
sum(FIRST_GUEST_REVENUE_NET_VAT_USD) as FIRST_GUEST_REVENUE_NET_VAT_USD,
sum(REPEAT_GUEST_ORDER_COUNT) as REPEAT_GUEST_ORDER_COUNT,
sum(REPEAT_GUEST_UNIT_COUNT) as REPEAT_GUEST_UNIT_COUNT,
sum(REPEAT_GUEST_REVENUE_NET_VAT_USD) as REPEAT_GUEST_REVENUE_NET_VAT_USD,
sum(REPEAT_VIP_ORDER_COUNT) as REPEAT_VIP_ORDER_COUNT,
sum(REPEAT_VIP_UNIT_COUNT) as REPEAT_VIP_UNIT_COUNT,
sum(REPEAT_VIP_REVENUE_NET_VAT_USD) as REPEAT_VIP_REVENUE_NET_VAT_USD,
max(MAX_REFRESH_TIME) as MAX_REFRESH_TIME,
sum(IFNULL(product_margin_pre_return,0)
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
         - IFNULL(product_order_misc_cogs_amount,0))
        AS product_gross_profit   
from _edw_orders o   
FULL JOIN _edw_refunds rd 
on rd.VIP_STORE_ID = o.VIP_STORE_ID
and rd.EVENT_STORE_ID = o.EVENT_STORE_ID
and rd.CUSTOMER_GENDER = o.CUSTOMER_GENDER
and rd.FK_CROSS_PROMO = o.FK_CROSS_PROMO
and rd.FINANCE_SUB_STORE = o.FINANCE_SUB_STORE
and rd.slot = o.slot
and rd.employee_number = o.employee_number
FULL JOIN _edw_returns rt 
on rt.VIP_STORE_ID = o.VIP_STORE_ID
and rt.EVENT_STORE_ID = o.EVENT_STORE_ID
and rt.CUSTOMER_GENDER = o.CUSTOMER_GENDER
and rt.FK_CROSS_PROMO = o.FK_CROSS_PROMO
and rt.FINANCE_SUB_STORE = o.FINANCE_SUB_STORE
and rt.slot = o.slot
and rt.employee_number = o.employee_number
FULL JOIN _edw_chargebacks cb 
on cb.VIP_STORE_ID = o.VIP_STORE_ID
and cb.EVENT_STORE_ID = o.EVENT_STORE_ID
and cb.CUSTOMER_GENDER = o.CUSTOMER_GENDER
and cb.FK_CROSS_PROMO = o.FK_CROSS_PROMO
and cb.FINANCE_SUB_STORE = o.FINANCE_SUB_STORE
and cb.slot = o.slot
and cb.employee_number = o.employee_number
group by 
coalesce(o.VIP_STORE_ID ,rd.VIP_STORE_ID,rt.VIP_STORE_ID,cb.VIP_STORE_ID ),
coalesce(o.EVENT_STORE_ID ,rd.EVENT_STORE_ID,rt.EVENT_STORE_ID,cb.EVENT_STORE_ID ),
coalesce(o.CUSTOMER_GENDER ,rd.CUSTOMER_GENDER,rt.CUSTOMER_GENDER,cb.CUSTOMER_GENDER ),
coalesce(o.FK_CROSS_PROMO ,rd.FK_CROSS_PROMO,rt.FK_CROSS_PROMO,cb.FK_CROSS_PROMO ),
coalesce(o.FINANCE_SUB_STORE ,rd.FINANCE_SUB_STORE,rt.FINANCE_SUB_STORE,cb.FINANCE_SUB_STORE ),
coalesce(o.slot ,rd.slot,rt.slot,cb.slot ),
coalesce(o.employee_number ,rd.employee_number,rt.employee_number,cb.employee_number)
;

CREATE OR REPLACE TEMP TABLE _edw_scaffold as
SELECT distinct
    vip_store_id, event_store_id, customer_gender, finance_sub_store, fk_cross_promo, slot,employee_number
FROM _edw_orders_gm
UNION
SELECT distinct
    vip_store_id, event_store_id, customer_gender, finance_sub_store, fk_cross_promo, slot, employee_number
FROM _edw_vips
UNION
SELECT distinct
    vip_store_id, event_store_id, customer_gender, finance_sub_store, fk_cross_promo, slot, employee_number
FROM _edw_refunds;

TRUNCATE TABLE reporting_prod.retail.realtime_edw;
INSERT INTO reporting_prod.retail.realtime_edw
(
event_store_brand,
event_store_region,
event_store_country,
event_store_location,
event_store_name,
vip_activation_location,
vip_store_name,
customer_gender,
fk_cross_promo,
finance_sub_store,
slot,
date,
employee_number,
store_code,
successful_billing_cash_gross_revenue,
activating_order_count,
activating_unit_count,
activating_revenue,
nonactivating_order_count,
nonactivating_unit_count,
nonactivating_revenue_,
first_guest_order_count,
first_guest_unit_count,
first_guest_revenue,
repeat_guest_order_count,
repeat_guest_unit_count,
repeat_guest_revenue,
repeat_vip_order_count,
repeat_vip_unit_count,
repeat_vip_revenue,
product_gross_revenue,
gross_margin,  
guest_order_count,
order_count,
unit_count,
product_refund,
billing_refund,
nonactivating_product_refund,
activating_product_refund,
refund_units,
refund_orders,
vips,
max_refresh_time
)
SELECT
    cs.store_brand AS event_store_brand,
    cs.store_region AS event_store_region,
    cs.store_country AS event_store_country,
    cs.store_type AS event_store_location,
    cs.store_full_name AS event_store_name,
    vs.store_type AS vip_activation_location,
    vs.store_full_name AS vip_store_name,
    s.customer_gender,
    s.fk_cross_promo,
    s.finance_sub_store,
    s.slot,
    date_trunc('day',s.slot) AS date,
    s.employee_number,
    cs.store_retail_location_code AS store_code,
    ifnull(o.successful_billing_cash_gross_revenue,0) AS successful_billing_cash_gross_revenue,
    ifnull(o.activating_order_count,0) AS activating_order_count,
    ifnull(o.activating_unit_count,0) AS activating_unit_count,
    ifnull(o.activating_revenue_net_vat_usd,0) AS activating_revenue,
    ifnull(o.nonactivating_order_count,0) AS nonactivating_order_count,
    ifnull(o.nonactivating_unit_count,0) AS nonactivating_unit_count,
    ifnull(o.nonactivating_revenue_net_vat_usd,0) AS nonactivating_revenue_,    

    ifnull(o.first_guest_order_count,0) AS first_guest_order_count,
    ifnull(o.first_guest_unit_count,0) AS first_guest_unit_count,
    ifnull(o.first_guest_revenue_net_vat_usd,0) AS first_guest_revenue,

    ifnull(o.repeat_guest_order_count,0) AS repeat_guest_order_count,
    ifnull(o.repeat_guest_unit_count,0) AS repeat_guest_unit_count,
    ifnull(o.repeat_guest_revenue_net_vat_usd,0) AS repeat_guest_revenue,

    ifnull(o.repeat_vip_order_count,0) AS repeat_vip_order_count,
    ifnull(o.repeat_vip_unit_count,0) AS repeat_vip_unit_count,
    ifnull(o.repeat_vip_revenue_net_vat_usd,0) AS repeat_vip_revenue,

    ifnull(o.activating_revenue_net_vat_usd,0) + ifnull(o.nonactivating_revenue_net_vat_usd,0) AS product_gross_revenue,
    ifnull(o.product_gross_profit,0) AS gross_margin,
    ifnull(o.first_guest_order_count,0) + ifnull(o.repeat_guest_order_count,0) AS guest_order_count,
    ifnull(o.activating_order_count,0) + ifnull(o.nonactivating_order_count,0)  AS order_count,
    ifnull(o.activating_unit_count,0) + ifnull(o.nonactivating_unit_count,0) AS unit_count,
    
    ifnull(r.refund_gross_vat_local,0) AS product_refund,
    ifnull(r.successful_billing_refund_net_vat_usd,0) AS billing_refund,
    ifnull(r.nonactivating_product_refund_net_vat_usd,0) AS nonactivating_product_refund,
    ifnull(r.activating_product_refund_net_vat_usd,0) AS activating_product_refund,
    ifnull(r.refund_units,0) AS refund_units,
    ifnull(refund_orders,0) AS refund_orders,

    COALESCE(v.vips, 0) AS vips,
    null AS max_refresh_time
FROM _edw_scaffold s
JOIN edw_prod.data_model.dim_store cs ON s.event_store_id = cs.store_id
LEFT JOIN edw_prod.data_model.dim_store vs ON s.vip_store_id = vs.store_id
LEFT JOIN _edw_orders_gm o ON s.event_store_id = o.event_store_id
    AND s.vip_store_id = o.vip_store_id
    AND s.customer_gender = o.customer_gender
    AND s.fk_cross_promo = o.fk_cross_promo
    AND s.finance_sub_store = o.finance_sub_store
    AND s.slot = o.slot
    AND s.employee_number = o.employee_number
LEFT JOIN _edw_vips v
    ON v.event_store_id = s.event_store_id
    AND v.vip_store_id = s.vip_store_id
    AND v.customer_gender = s.customer_gender
    AND v.fk_cross_promo=s.fk_cross_promo
    AND v.finance_sub_store = s.finance_sub_store
    AND v.slot = s.slot
    AND v.employee_number = s.employee_number
LEFT JOIN _edw_refunds r ON r.event_store_id = s.event_store_id
    AND r.vip_store_id = s.vip_store_id
    AND r.customer_gender = s.customer_gender
    AND r.fk_cross_promo=s.fk_cross_promo
    AND r.finance_sub_store = s.finance_sub_store
    AND r.slot = s.slot
    AND r.employee_number = s.employee_number
WHERE cs.store_brand IN ('FabKids','JustFab','ShoeDazzle','Savage X','Fabletics');