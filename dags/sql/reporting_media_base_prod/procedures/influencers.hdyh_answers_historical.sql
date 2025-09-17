INSERT INTO reporting_media_base_prod.influencers.hdyh_answers_historical_new

WITH _lists        AS (SELECT DISTINCT hdyh_global_list_id,
                                       group_name                                                   AS list_name,
                                       global_code,
                                       split_part(lower(global_code), '_', 1)                       AS sub_brand,
                                       CASE WHEN global_code ILIKE '%quiz%' THEN 'single_step'
                                            WHEN global_code ILIKE '%influencer%' THEN 'tiered'
                                            WHEN global_code ILIKE '%general%'
                                                THEN 'tiered' END                                   AS hdyh_implementation,
                                       lower(list_data)                                             AS list_data,
                                       store_group_id,
                                       --TIERED HDYH LAUNCH DATE
                                       CASE WHEN current_date <= '2025-03-01'
                                                THEN iff(hdyh_implementation = 'single_step', 1, 0) --
                                       -- WHEN current_date > '2025-03-01' AND current_date <= '2025-05-01' THEN iff(hdyh_implementation IN ('tiered', 'single_step'), 1, 0)
                                       -- WHEN current_date > '2025-07-01' THEN iff(hdyh_implementation = 'tiered', 1, 0)
                                            ELSE iff(hdyh_implementation = 'single_step', 1, 0) END AS on_fl_site
                       FROM lake_consolidated.ultra_merchant.hdyh_global_list)


-- this gets who is in each list at a given time BUT its tied to this manual recording of the list ids which is bad
-- and we have to create
   , _global_lists AS (SELECT meta_company_id, store_group_id, hdyh_global_list_id, customer_referrer_id
                       FROM lake_consolidated.ultra_merchant.hdyh_global_list_referrer
                       WHERE hvr_is_deleted = 0
                       UNION ALL
                       --analytics algo plus assigned doesn't come through the same way. is_influencer indicates if it came from the algo
                       SELECT meta_company_id, r.store_group_id, hdyh_global_list_id, customer_referrer_id
                       FROM lake_consolidated.ultra_merchant.customer_referrer r
                            LEFT JOIN (SELECT DISTINCT store_group_id, hdyh_global_list_id
                                       FROM _lists
                                       WHERE list_data ILIKE '%analytics_algorithm%') l
                                      ON l.store_group_id = r.store_group_id
                       WHERE r.store_group_id IN (16) --, 34) -- don't add sx influencers here and below
                         AND active = 1
                         AND is_influencer = TRUE
                       UNION ALL
                       -- grab other brands
                       SELECT meta_company_id, store_group_id, NULL AS hdyh_global_list_id, customer_referrer_id
                       FROM lake_consolidated.ultra_merchant.customer_referrer r
                       WHERE meta_company_id != 20
                         AND active = 1)

SELECT sg.label            AS store_group_label,
       country_code,
       r.store_group_id,
       r.hdyh_global_list_id,
       l.list_name,
       l.global_code,
       hdyh_implementation,
       sub_brand,
       l.list_data,
       r.customer_referrer_id,
       cr.label            AS hdyh,
       cr.customer_referrer_type_id,
       rt.label            AS referrer_type,
       cr.group_name,
       on_fl_site,
       current_timestamp() AS timestamp
FROM _global_lists r
     LEFT JOIN _lists l ON l.store_group_id = r.store_group_id AND l.hdyh_global_list_id = r.hdyh_global_list_id
     LEFT JOIN lake_consolidated_view.ultra_merchant.store_group sg ON sg.store_group_id = r.store_group_id
     LEFT JOIN lake_consolidated.ultra_merchant.customer_referrer cr
               ON r.customer_referrer_id = cr.customer_referrer_id AND r.store_group_id = cr.store_group_id
     LEFT JOIN lake_consolidated.ultra_merchant.customer_referrer_type rt
               ON cr.customer_referrer_type_id = rt.customer_referrer_type_id
WHERE (on_fl_site = 1 OR l.hdyh_global_list_id IS NULL)
ORDER BY global_code;
