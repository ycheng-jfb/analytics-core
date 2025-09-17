CREATE OR REPLACE TRANSIENT TABLE reporting_media_prod.dbo.friction_finder_inventory_top_skus AS
SELECT s.*,
       leads,
       vips,
       vips_from_leads_d1,
       lead_to_vip,
       lead_to_vip_d1,
       pct_top_skus_oos,
       pct_top_skus_oos_30d
FROM reporting_media_prod.dbo.inventory_score_top_skus s
JOIN reporting_media_prod.dbo.inventory_score_aggregates a
     ON s.date = a.date
     AND s.brand = a.brand
     AND s.item_status = a.item_status;
