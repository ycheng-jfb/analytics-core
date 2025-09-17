SET curr_time = CURRENT_TIMESTAMP();


CREATE TABLE IF NOT EXISTS reporting_base_prod.shared.med61_metrics_validation
(
    date DATE,
    store_name VARCHAR,
    leads INT,
    vips INT,
    fb_spend INT,
    activating_order_count INT,
    leads_vips_fb_refresh_dt timestamp,
    act_order_refresh_dt timestamp
);


CREATE OR REPLACE TEMP TABLE _med61_realtime_acquisition AS
SELECT date_hour::DATE date,
       store_name,
       SUM(leads) leads,
       SUM(vips) vips,
       SUM(fb_spend)::INT fb_spend,
       max(date_hour) leads_vips_fb_refresh_dt
FROM reporting_base_prod.shared.med61_realtime_acquisition
WHERE date_hour::DATE = CURRENT_DATE()
GROUP BY 1, 2;


CREATE OR REPLACE TEMP TABLE _comp_acquisition_metrics AS
SELECT n.date,
       n.store_name,
       n.leads recent_leads_count,
       o.leads prev_leads_count,
       n.leads - o.leads leads_delta,
       n.vips recent_vips_count,
       o.vips prev_vips_count,
       n.vips - o.vips vips_delta,
       n.fb_spend recent_fb_spend,
       o.fb_spend prev_fb_spend,
       n.fb_spend - o.fb_spend fb_spend_delta,
       n.leads_vips_fb_refresh_dt leads_vips_fb_recent_refresh_dt,
       o.leads_vips_fb_refresh_dt leads_vips_fb_prev_refresh_dt
FROM reporting_base_prod.shared.med61_metrics_validation o
    JOIN _med61_realtime_acquisition n ON n.date = o.date
            AND n.store_name = o.store_name;


CREATE OR REPLACE TEMP TABLE _med61_realtime_orders AS
SELECT date_hour::DATE date,
       store_name,
       SUM(activating_order_count) activating_order_count,
       max(date_hour) act_order_refresh_dt
FROM reporting_base_prod.shared.med61_realtime_orders
WHERE date_hour::DATE = CURRENT_DATE()
GROUP BY 1, 2;


CREATE OR REPLACE TEMP TABLE _comp_order_metrics AS
SELECT n.date,
       n.store_name,
       n.activating_order_count recent_activating_order_count,
       o.activating_order_count prev_activating_order_count,
       nvl(n.activating_order_count,0) - nvl(o.activating_order_count,0) AS activating_order_count_delta,
       n.act_order_refresh_dt act_order_recent_refresh_dt,
       o.act_order_refresh_dt act_order_prev_refresh_dt       
FROM reporting_base_prod.shared.med61_metrics_validation o
    JOIN _med61_realtime_orders n ON n.date = o.date
            AND n.store_name = o.store_name;


DELETE
    FROM reporting_base_prod.shared.med61_metrics_validation
    WHERE date = CURRENT_DATE();


INSERT
    INTO reporting_base_prod.shared.med61_metrics_validation
SELECT COALESCE(a.date, o.date) AS date,
       COALESCE(a.store_name, o.store_name) AS store_name,
       nvl(leads, 0) AS leads,
       nvl(vips, 0) AS vips,
       nvl(fb_spend, 0) AS fb_spend,
       nvl(activating_order_count, 0) AS activating_order_count,
       a.leads_vips_fb_refresh_dt,
       o.act_order_refresh_dt
FROM _med61_realtime_acquisition a
     FULL JOIN _med61_realtime_orders o ON a.date = o.date
            AND a.store_name = o.store_name;


CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.med61_validation_result AS
SELECT COALESCE(a.date, o.date) AS date,
       COALESCE(a.store_name, o.store_name) AS store_name,
       prev_leads_count,
       recent_leads_count,
       IFF(NVL(leads_delta, 0) = 0, 'Not Updated', 'Updated') leads,
       prev_vips_count,
       recent_vips_count,
       IFF(NVL(vips_delta, 0) = 0, 'Not Updated', 'Updated') vips,
       recent_fb_spend,
       prev_fb_spend,
       IFF(NVL(fb_spend_delta, 0) = 0, 'Not Updated', 'Updated') fb_spend,
       leads_vips_fb_prev_refresh_dt,
       leads_vips_fb_recent_refresh_dt,
       recent_activating_order_count,
       prev_activating_order_count,
       IFF(NVL(activating_order_count_delta, 0) = 0, 'Not Updated', 'Updated') activating_order_count,
       act_order_recent_refresh_dt,
       act_order_prev_refresh_dt   
    FROM _comp_acquisition_metrics a
             FULL JOIN _comp_order_metrics o ON a.date = o.date
            AND a.store_name = o.store_name
    WHERE (((HOUR($curr_time) BETWEEN 9 AND 20) AND
            (NVL(leads_delta, 0) = 0 OR NVL(vips_delta, 0) = 0 OR NVL(fb_spend_delta, 0) = 0
                OR NVL(activating_order_count_delta, 0) = 0)
           ) OR (NVL(fb_spend_delta, 0) = 0 OR NVL(activating_order_count_delta, 0) = 0)
          );


SELECT date,
       store_name,
       prev_leads_count,
       recent_leads_count,
       leads,
       prev_vips_count,
       recent_vips_count,
       vips,
       prev_fb_spend,
       recent_fb_spend,
       fb_spend,
       leads_vips_fb_prev_refresh_dt   lvf_prev_ref_dt,
       leads_vips_fb_recent_refresh_dt lvf_recent_ref_dt,
       prev_activating_order_count     AS prev_ao_count,
       recent_activating_order_count   AS recent_ao_count,
       activating_order_count          AS ao_count,
       act_order_prev_refresh_dt       AS ao_prev_refresh_dt,
       act_order_recent_refresh_dt     AS ao_recent_refresh_dt 
FROM reporting_base_prod.shared.med61_validation_result
    WHERE store_name = 'Fabletics US';