set start_date = DATEADD(YEAR, -2, DATE_TRUNC(YEAR, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _edw_orders AS
SELECT DISTINCT fo.master_order_id, fo.order_id
FROM edw_prod.data_model_jfb.fact_order_line fol
         JOIN edw_prod.data_model_jfb.fact_order fo
              ON fol.order_id = fo.order_id
         JOIN edw_prod.data_model_jfb.dim_product dm
              ON dm.product_id = fol.product_id
WHERE dm.membership_brand_id IN (10, 11, 12, 13);

CREATE OR REPLACE TEMPORARY TABLE _rma_data AS
SELECT edw_prod.stg.udf_unconcat_brand(rma.product_id) AS product_id,
       rma1.statuscode,
       edw_prod.stg.udf_unconcat_brand(rma1.order_id)  AS order_id,
       eo.master_order_id,
       ml.rma_transit_datetime                         AS transit_date,
       rma.order_line_id,
       drr.return_reason
FROM lake_jfb.ultra_merchant.rma_product rma
         LEFT JOIN edw_prod.data_model_jfb.dim_return_reason drr
                   ON drr.return_reason_id = rma.return_reason_id
         JOIN lake_jfb.ultra_merchant.rma rma1
              ON rma.rma_id = rma1.rma_id
         JOIN _edw_orders eo
              ON eo.order_id = edw_prod.stg.udf_unconcat_brand(rma1.order_id)
         JOIN (SELECT DISTINCT object_id AS rma_id, datetime_added AS rma_transit_datetime
               FROM lake_jfb.ultra_merchant.statuscode_modification_log
               WHERE object = 'rma'
                 AND to_statuscode = 4656) ml
              ON edw_prod.stg.udf_unconcat_brand(rma1.rma_id) = ml.rma_id
         JOIN (SELECT DISTINCT product_id
               FROM edw_prod.data_model_jfb.dim_product
               WHERE membership_brand_id IN (10, 11, 12, 13)) dp
              ON edw_prod.stg.udf_unconcat_brand(rma.product_id) = dp.product_id
where transit_date::DATE >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _orders AS
SELECT mdp.sub_brand,
       mdp.department_detail,
       mdp.subcategory,
       mdp.subclass,
       mdp.gender,
       ol.customer_id,
       ol.order_id,
       ol.product_id,
       ol.product_sku,
       mdp.large_img_url,
       mdp.style_name,
       DATE_TRUNC('month', mdp.show_room::DATE)                              AS show_room,
       IFF(ol.order_type != 'vip activating', 'Nonactivating', 'Activating') AS order_type,
       ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN reporting_prod.gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE mdp.sub_brand != 'JFB'
    and ol.order_date >= $start_date;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb082_mm_returns_data AS
SELECT DISTINCT rd.transit_date, rd.return_reason, rd.order_line_id, o.*
FROM _rma_data rd
         JOIN _orders o
              ON COALESCE(rd.master_order_id, rd.order_id) = o.order_id
                  AND rd.product_id = o.product_id;
