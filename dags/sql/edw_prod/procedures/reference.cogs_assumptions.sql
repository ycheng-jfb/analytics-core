SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
CREATE OR REPLACE TEMP TABLE _cogs_assumptions
AS
SELECT
    nvl(stg.store_id, ca.store_id) AS store_id,
    nvl(stg.store_full_name, ca.store_name) AS store_name,
    nvl(stg.store_brand, ca.store_brand_name) AS store_brand_name,
    nvl(stg.store_brand_abbr, ca.store_brand) AS store_brand,
    nvl(stg.store_country, ca.store_country) AS store_country,
    nvl(stg.store_region, ca.store_region) AS store_region,
    nvl(stg.store_type, ca.store_type) AS store_type,
    nvl(stg.currency_code, ca.currency_code) AS currency_code,
    nvl(stg.returned_product_resaleable_percent, ca.returned_product_resaleable_percent) AS returned_product_resaleable_percent,
    nvl(stg.returned_product_damaged_percent, ca.returned_product_damaged_percent) AS returned_product_damaged_percent,
    nvl(stg.freight_cost_per_return_shipment, ca.freight_cost_per_return_shipment) AS freight_cost_per_return_shipment,
    nvl(stg.shipping_supplies_cost_per_outbound_order, ca.shipping_supplies_cost_per_outbound_order) AS shipping_supplies_cost_per_outbound_order,
    nvl(stg.freight_cost_per_outbound_shipment, ca.freight_cost_per_outbound_shipment) AS freight_cost_per_outbound_shipment,
    nvl(stg.product_cost_markdown_adjustment_percent, ca.product_cost_markdown_adjustment_percent) AS product_cost_markdown_adjustment_percent,
    nvl(stg.variable_gms_cost_per_order, ca.variable_gms_cost_per_order) AS variable_gms_cost_per_order,
    nvl(stg.variable_warehouse_cost_per_unit, ca.variable_warehouse_cost_per_unit) AS variable_warehouse_cost_per_unit,
    nvl(stg.payment_processing_cost_percent, ca.payment_processing_cost_percent) AS payment_processing_cost_percent,
    nvl(stg.start_date, ca.start_date) AS start_Date,
    NULL AS end_date,
    NULL AS meta_row_hash,
    row_number() OVER (PARTITION BY nvl(stg.store_id, ca.store_id) ORDER BY nvl(stg.start_date, ca.start_date)) AS rno
FROM (
          SELECT
              ds.store_id,
              ds.store_full_name,
              ds.store_brand,
              ds.store_brand_abbr AS store_brand_abbr,
              ds.store_region,
              ds.store_country,
              ds.store_type,
              lca.currency_code,
              lca.returned_product_resaleable_percent,
              lca.returned_product_damaged_percent,
              lca.freight_cost_per_return_shipment,
              lca.shipping_supplies_cost_per_outbound_order,
              lca.freight_cost_per_outbound_shipment,
              lca.product_cost_markdown_adjustment_percent,
              lca.variable_gms_cost_per_order,
              lca.variable_warehouse_cost_per_unit,
              lca.payment_processing_cost_percent,
              lca.start_date,
              row_number() OVER (PARTITION BY ds.store_id, lca.start_date ORDER BY lca.start_date) AS rno
          FROM lake.gsheet.cogs_assumptions lca
          JOIN stg.dim_store ds
               ON upper(lca.store_country) = upper(ds.store_country)
                   AND upper(lca.store_brand) = upper(ds.store_brand)
                   AND upper(lca.store_type) = upper(ds.store_type)

      ) AS stg
FULL JOIN reference.cogs_assumptions ca
    ON ca.store_id = stg.store_id
    AND ca.start_date = stg.start_date
    AND stg.rno = 1;


UPDATE _cogs_assumptions cas
SET cas.end_date = ca.end_date,
    cas.meta_row_hash = ca.meta_row_hash,
    cas.start_date = IFF(cas.rno = 1,'1900-01-01',cas.start_date)
FROM (
        SELECT
            store_id,
            start_date,
            nvl(DATEADD(DAY ,-1, LEAD(start_date) OVER (PARTITION BY store_id ORDER BY start_date)), '9999-12-31') as end_Date,
            hash(
                currency_code,
                returned_product_resaleable_percent,
                returned_product_damaged_percent,
                freight_cost_per_return_shipment,
                shipping_supplies_cost_per_outbound_order,
                freight_cost_per_outbound_shipment,
                product_cost_markdown_adjustment_percent,
                variable_gms_cost_per_order,
                variable_warehouse_cost_per_unit,
                payment_processing_cost_percent,
                nvl(DATEADD(DAY ,-1, LEAD(start_date) OVER (PARTITION BY store_id ORDER BY start_date)), '9999-12-31')
                ) AS meta_row_hash
        FROM _cogs_assumptions ca
    ) AS ca
WHERE ca.store_id = cas.store_id
AND ca.start_date = cas.start_date;

MERGE INTO reference.cogs_assumptions t
    USING _cogs_assumptions s ON s.store_id = t.store_id AND s.start_date = t.start_date
    WHEN NOT MATCHED THEN INSERT
        (
             store_id,
             store_name,
             store_brand_name,
             store_brand,
             store_country,
             store_region,
             store_type,
             currency_code,
             returned_product_resaleable_percent,
             returned_product_damaged_percent,
             freight_cost_per_return_shipment,
             shipping_supplies_cost_per_outbound_order,
             freight_cost_per_outbound_shipment,
             product_cost_markdown_adjustment_percent,
             variable_gms_cost_per_order,
             variable_warehouse_cost_per_unit,
             payment_processing_cost_percent,
             start_date,
             end_date,
             meta_row_hash,
             meta_update_datetime,
             meta_create_datetime
            )
        VALUES
        (
             store_id,
             store_name,
             store_brand_name,
             store_brand,
             store_country,
             store_region,
             store_type,
             currency_code,
             returned_product_resaleable_percent,
             returned_product_damaged_percent,
             freight_cost_per_return_shipment,
             shipping_supplies_cost_per_outbound_order,
             freight_cost_per_outbound_shipment,
             product_cost_markdown_adjustment_percent,
             variable_gms_cost_per_order,
             variable_warehouse_cost_per_unit,
             payment_processing_cost_percent,
             start_date,
             end_date,
             meta_row_hash,
             $execution_start_time,
             $execution_start_time)
    WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
        THEN UPDATE
        SET
             t.store_name = s.store_name,
             t.store_brand_name = s.store_brand_name,
             t.store_brand = s.store_brand,
             t.store_country = s.store_country,
             t.store_region = s.store_region,
             t.store_type = s.store_type,
             t.currency_code = s.currency_code,
             t.returned_product_resaleable_percent = s.returned_product_resaleable_percent,
             t.returned_product_damaged_percent = s.returned_product_damaged_percent,
             t.freight_cost_per_return_shipment = s.freight_cost_per_return_shipment,
             t.shipping_supplies_cost_per_outbound_order = s.shipping_supplies_cost_per_outbound_order,
             t.freight_cost_per_outbound_shipment = s.freight_cost_per_outbound_shipment,
             t.product_cost_markdown_adjustment_percent = s.product_cost_markdown_adjustment_percent,
             t.variable_gms_cost_per_order = s.variable_gms_cost_per_order,
             t.variable_warehouse_cost_per_unit = s.variable_warehouse_cost_per_unit,
             t.payment_processing_cost_percent = s.payment_processing_cost_percent,
             t.end_date = s.end_date,
             t.meta_row_hash = s.meta_row_hash,
             t.meta_update_datetime = $execution_start_time;
