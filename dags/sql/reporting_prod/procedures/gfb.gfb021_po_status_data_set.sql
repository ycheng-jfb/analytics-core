CREATE OR REPLACE TEMPORARY TABLE _po_data AS
SELECT pds.business_unit
     , pds.region
     , pds.country
     , pds.product_sku
     , pds.sku
     , mdp.image_url
     , pds.po_number
     , pds.show_room
     , mdp.department_detail
     , pds.po_type
     , pds.warehouse
     , mdp.description
     , mdp.color
     , mdp.subcategory
     , pds.arrival_date
     , pds.expected_deliver_to_warehouse_date
     , pds.po_qty
     , pds.warehouse_id
     , pds.exact_launch_date
     , mdp.subclass

     , SUM(qty_receipt) AS qty_receipt
FROM reporting_prod.gfb.gfb_po_data_set pds
         LEFT JOIN reporting_prod.gfb.merch_dim_product mdp
                   ON mdp.business_unit = pds.business_unit
                       AND mdp.region = pds.region
                       AND mdp.country = pds.country
                       AND mdp.product_sku = pds.product_sku
GROUP BY pds.business_unit
       , pds.region
       , pds.country
       , pds.product_sku
       , pds.sku
       , mdp.image_url
       , pds.po_number
       , pds.show_room
       , mdp.department_detail
       , pds.po_type
       , pds.warehouse
       , mdp.description
       , mdp.color
       , mdp.subcategory
       , pds.arrival_date
       , pds.expected_deliver_to_warehouse_date
       , pds.po_qty
       , pds.warehouse_id
       , pds.exact_launch_date
       , mdp.subclass;



CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb021_po_status_data_set AS
SELECT po.business_unit
     , po.region
     , po.country
     , po.product_sku
     , po.image_url
     , po.po_number
     , po.show_room
     , po.department_detail
     , po.po_type
     , po.warehouse
     , po.description
     , po.color
     , po.subcategory
     , po.arrival_date
     , po.expected_deliver_to_warehouse_date
     , po.sku
     , po.exact_launch_date
     , po.subclass


     , SUM(idc.qty_onhand)            AS qty_onhand
     , SUM(idc.qty_available_to_sell) AS qty_available_to_sell
     , SUM(po.qty_receipt)            AS qty_receipt
     , SUM(po.po_qty)                 AS po_qty
FROM _po_data po
         LEFT JOIN reporting_prod.gfb.gfb_inventory_data_set_current idc
                   ON idc.business_unit = po.business_unit
                       AND idc.region = po.region
                       AND idc.country = po.country
                       AND idc.warehouse_id = po.warehouse_id
                       AND idc.sku = po.sku
GROUP BY po.business_unit
       , po.region
       , po.country
       , po.product_sku
       , po.image_url
       , po.po_number
       , po.show_room
       , po.department_detail
       , po.po_type
       , po.warehouse
       , po.description
       , po.color
       , po.subcategory
       , po.arrival_date
       , po.expected_deliver_to_warehouse_date
       , po.sku
       , po.exact_launch_date
       , po.subclass
;
