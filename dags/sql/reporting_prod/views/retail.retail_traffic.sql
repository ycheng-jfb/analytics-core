CREATE OR REPLACE VIEW reporting_prod.retail.retail_traffic(
	STORE_ID,
	SITE_NAME,
	DATE,
	INCOMING_VISITOR_TRAFFIC,
	INCOMING_FITTINGROOM_TRAFFIC,
	ADJUSTED_INCOMING_VISITOR_TRAFFIC,
	ADJUSTED_INCOMING_FITTINGROOM_TRAFFIC
) as
WITH _retail_location AS (
    SELECT *
    FROM lake.ultra_warehouse.retail_location
    WHERE retail_location_code NOT ILIKE '%FLRT%'
)
SELECT
    store_id,
    site_name,
    date,
    SUM(incoming_visitor_traffic) AS incoming_visitor_traffic,
    SUM(incoming_fittingroom_traffic) AS incoming_fittingroom_traffic,
    SUM(adjusted_incoming_visitor_traffic) AS adjusted_incoming_visitor_traffic,
    SUM(adjusted_incoming_fittingroom_traffic) AS adjusted_incoming_fittingroom_traffic
FROM(
    SELECT
        rl.store_id,
        st.store_full_name AS site_name,
        tc.date AS date,
        sum(tc.traffic_exits) AS incoming_visitor_traffic,
        0 AS incoming_fittingroom_traffic,
        0 AS adjusted_incoming_visitor_traffic,
        0 AS adjusted_incoming_fittingroom_traffic
    FROM lake.shoppertrak.traffic_counter tc
    JOIN _retail_location rl ON tc.customer_site_id = rl.retail_location_code
    JOIN edw_prod.data_model_fl.dim_store st on st.store_id = rl.store_id
    LEFT JOIN lake.countwise.sites s ON s.customer_id = rl.retail_location_code --look into that
    LEFT JOIN reporting_prod.retail.shoppertrak_pilot_backfill spb on spb.store_id = rl.store_id and spb.date = tc.date
    LEFT JOIN reporting_prod.retail.countwise_adjustments ca on ca.store_id = rl.store_id
    WHERE spb.store_id is null
    AND st.store_brand_abbr = 'FL'
    AND tc.date > coalesce(ca.date_ended,'1900-01-01')
    GROUP BY rl.store_id,
        st.store_full_name,
        tc.date
    UNION ALL
    SELECT
        rl.store_id,
        st.store_full_name AS site_name,
        c.date_t::date AS date,
        round(SUM(IFF(trim(l.location) IN ('FrontEntrance', 'Entrance'), outgoing, 0)*((1-reduction_value)+1)),0) as incoming_visitor_traffic,
        round(SUM(IFF(trim(l.location) = 'FittingRoom', incoming, 0)*((1-reduction_value)+1)),0) as fittingroom_traffic,
        SUM(IFF(trim(l.location) IN ('FrontEntrance', 'Entrance'), incoming, 0)) as adjusted_incoming_visitor_traffic,
        SUM(IFF(trim(l.location) = 'FittingRoom', incoming, 0)) as adjusted_incoming_fittingroom_traffic
    FROM lake.countwise.counts c
    JOIN lake.countwise.location l ON l.location_id = c.location_id
        AND trim(l.location) IN ('FrontEntrance', 'Entrance', 'FittingRoom')
    JOIN lake.countwise.sites s ON s.site_id = l.site_id
    JOIN _retail_location rl ON rl.retail_location_code = s.customer_id
    JOIN edw_prod.data_model.dim_store st on st.store_id = rl.store_id
    LEFT JOIN  (SELECT tc.*
                FROM lake.shoppertrak.traffic_counter tc
                    JOIN _retail_location rl ON tc.customer_site_id = rl.retail_location_code
                    LEFT JOIN reporting_prod.retail.shoppertrak_pilot_backfill spb on spb.store_id = rl.store_id and spb.date = tc.date
                    LEFT JOIN reporting_prod.retail.countwise_adjustments ca on ca.store_id = rl.store_id
                WHERE spb.store_id is null
                    AND tc.date > ca.date_ended) tc ON tc.customer_site_id = rl.retail_location_code and tc.date = c.date_t::date
    LEFT JOIN reporting_prod.retail.countwise_adjustments ca on ca.store_id = rl.store_id
    WHERE c.is_active_hours = 1
    AND tc.customer_site_id IS NULL--only using countwise data where shoppertrak data doesn't exist
    AND st.store_brand_abbr = 'FL'
    GROUP BY
        rl.store_id,
        st.store_full_name,
        c.date_t::date
    UNION ALL
     SELECT spb.store_id,
           st.store_full_name as site_name,
           date,
           sum(traffic) as incoming_visitor_traffic,
           0 as incoming_fittingroom_traffic,
           0 AS adjusted_incoming_visitor_traffic,
           0 AS adjusted_incoming_fittingroom_traffic
    FROM reporting_prod.retail.shoppertrak_pilot_backfill spb
    JOIN edw_prod.data_model.dim_store st on st.store_id = spb.store_id
    WHERE st.store_brand_abbr = 'FL'
    GROUP BY
        spb.store_id,
        st.store_full_name,
        date
 ) traffic

GROUP BY store_id, site_name, date
ORDER BY date DESC, store_id;
