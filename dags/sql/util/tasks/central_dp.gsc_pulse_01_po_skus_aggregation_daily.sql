CREATE OR REPLACE TASK util.tasks_central_dp.gsc_pulse_01_po_skus_aggregation_daily
    WAREHOUSE =da_wh_analytics
    SCHEDULE = 'USING CRON 07 05,09,13,17 * * * America/Los_Angeles'
    ERROR_INTEGRATION =SNOWFLAKE_TASK_SNS_INTEGRATION
    AS
BEGIN

    USE SCHEMA reporting_base_prod.gsc;


    CREATE OR REPLACE TEMPORARY TABLE _lcd AS

    SELECT po_number,
           sku,
           show_room,
           units_received,
           actual_landed_cost_per_unit,
           fully_landed,
           ROW_NUMBER() OVER (PARTITION BY po_number, sku ORDER BY po_number, sku, units_received DESC) AS rn
    FROM reporting_prod.gsc.landed_cost_dataset lcd;


    DELETE
    FROM _lcd
    WHERE rn > 1;

/***********************
sort centric data
for material & fabric
department
carry over
************************/
    CREATE OR REPLACE TEMPORARY TABLE _cen AS
    SELECT style,
           tfg_color_sku_segment,
           showroom,
           tfg_composition_list,
           fabric_category,
           ROW_NUMBER() OVER (PARTITION BY style, tfg_color_sku_segment ORDER BY showroom DESC) AS style_color_rn,
           ROW_NUMBER() OVER (PARTITION BY style ORDER BY showroom DESC)                        AS style_rn
    FROM gsc.centric_style_hierarchy
    WHERE NVL(NULLIF(tfg_composition_list, ''), '-1') != '-1'
      AND NVL(NULLIF(fabric_category, ''), '-1') != '-1';

    CREATE OR REPLACE TEMPORARY TABLE _cen_dept AS
    SELECT style,
           tfg_color_sku_segment,
           showroom,
           centric_department,
           centric_subdepartment,
           centric_category,
           centric_class,
           centric_subclass,
           ROW_NUMBER() OVER (PARTITION BY style, tfg_color_sku_segment ORDER BY showroom DESC) AS style_color_rn_dept,
           ROW_NUMBER() OVER (PARTITION BY style ORDER BY showroom DESC)                        AS style_rn_dept
    FROM gsc.centric_style_hierarchy
    WHERE NVL(NULLIF(centric_department, ''), '-1') != '-1'
      AND NVL(NULLIF(centric_subdepartment, ''), '-1') != '-1'
      AND NVL(NULLIF(centric_category, ''), '-1') != '-1'
      AND NVL(NULLIF(centric_class, ''), '-1') != '-1'
      AND NVL(NULLIF(centric_subclass, ''), '-1') != '-1';

    CREATE OR REPLACE TEMPORARY TABLE _cen_co1 AS
    SELECT style,
           tfg_color_sku_segment,
           showroom,
           carry_over
        ,
           CASE
               WHEN RIGHT(showroom, 2) IN ('01', '02', '03')
                   THEN 'Q1'
               WHEN RIGHT(showroom, 2) IN ('04', '05', '06')
                   THEN 'Q2'
               WHEN RIGHT(showroom, 2) IN ('07', '08', '09')
                   THEN 'Q3'
               WHEN RIGHT(showroom, 2) IN ('10', '11', '12')
                   THEN 'Q4'
               END                                                                              AS showroom_quarter,
           ROW_NUMBER() OVER (PARTITION BY style, tfg_color_sku_segment ORDER BY showroom DESC) AS showrooms_ordered
    FROM gsc.centric_style_hierarchy;

    CREATE OR REPLACE TEMPORARY TABLE _bluec AS
    SELECT bc.po_id,
           NVL(bc.po_dtl_id_merlin, bc.po_num_bc || bc.po_line_number)                                           AS po_dtl_id,
           bc.sku,
           bc.division_name,
           bc.show_room,
           bc.date_launch,
           COALESCE(UPPER(TRIM(bc.po_num_merlin)),
                    UPPER(TRIM(bc.po_number)))                                                                   AS po_number,
           bc.color,
           bc.description                                                                                        AS gp_description,
           bc.qty,
           bc.vendor_id,
           bc.xfd,
           bc.fc_delivery,
           TO_DATE(TO_VARCHAR(bc.delivery))                                                                      AS delivery,
           bc.department,
           IFF(bc.inco_term = 'EXW', 'XWORKS', bc.inco_term)                                                     AS inco_term,
           NVL(bc.freight_drayage, 0) + NVL(bc.freight_shipping, 0) +
           NVL(bc.freight_transload, 0)                                                                          AS freight,
           bc.duty,
           bc.cmt,
           bc.date_create,
           bc.date_update,
           bc.cost,
           bc.region_id,
           UPPER(TRIM(bc.freight_method))                                                                        AS freight_method,
           UPPER(TRIM(bc.shipping_port))                                                                         AS shipping_port,
           bc.gender,
           i.wms_class                                                                                           AS wms_code,
           bc.style_name,
           bc.vend_name,
           UPPER(TRIM(bc.brand))                                                                                 AS brand,
           bc.line_status,
           bc.country_origin,
           bc.hts_code,
           bc.duty_percentage,
           bc.style                                                                                              AS plm_style,
           COALESCE(csh.fabric_category, NULLIF(bc.fabric_category, ''), cen_sc.fabric_category, cen_s.fabric_category,
                    psd_h.fabric_category)                                                                       AS fabric_category,
           COALESCE(csh.tfg_composition_list, NULLIF(bc.composition, ''), cen_sc.tfg_composition_list,
                    cen_s.tfg_composition_list,
                    psd_h.material_content)                                                                      AS material_content,
           bc.buyer,
           CASE
               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'KY' AND
                    UPPER(TRIM(bc.division_id)) IN ('SXF', 'FL', 'Y') AND bc.date_launch < '2024-07-01'
                   THEN 154
               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'KY' AND
                    bc.date_launch < '2024-07-01'
                   THEN 107
               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'KY' AND
                    bc.date_launch >= '2024-07-01'
                   THEN 154
               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'EU'
                   THEN 221
               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'UK'
                   THEN 366
               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'LA'
                   THEN 231
               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'MX2'
                   THEN 466
               END                                                                                               AS warehouse_id,
--bc.WAREHOUSE_ID,
           bc.dimension,
           bc.subclass,
           CASE
               WHEN UPPER(TRIM(bc.warehouse_id_origin)) = 'KY' AND UPPER(TRIM(bc.division_id)) IN ('SXF', 'FL', 'Y') AND
                    bc.date_launch < '2024-07-01'
                   THEN 154
               WHEN UPPER(TRIM(bc.warehouse_id_origin)) = 'KY' AND bc.date_launch < '2024-07-01'
                   THEN 107
               WHEN UPPER(TRIM(bc.warehouse_id_origin)) = 'KY' AND bc.date_launch >= '2024-07-01'
                   THEN 154
               WHEN UPPER(TRIM(bc.warehouse_id_origin)) = 'EU'
                   THEN 221
               WHEN UPPER(TRIM(bc.warehouse_id_origin)) = 'UK'
                   THEN 366
               WHEN UPPER(TRIM(bc.warehouse_id_origin)) = 'LA'
                   THEN 231
               WHEN UPPER(TRIM(bc.warehouse_id_origin)) = 'MX2'
                   THEN 466
               END                                                                                               AS warehouse_id_origin,
           csh.tfg_rank                                                                                          AS style_rank,
           bc.tariff                                                                                             AS primary_tariff,
           awv.avg_volume,
           bc.meta_create_datetime,
           bc.meta_update_datetime,
           NVL(bc.cost, 0) + NVL(bc.freight_drayage, 0) + NVL(bc.freight_shipping, 0) + NVL(bc.freight_transload, 0) +
           NVL(bc.misc_cost, 0) + (NVL(bc.cost, 0) * NVL(bc.duty_percentage, 0)) +
           (NVL(bc.cost, 0) * NVL(bc.tariff_pct, 0))                                                             AS landed_cost,
           bc.official_factory_name,
           UPPER(TRIM(bc.po_type))                                                                               AS po_type,
           gid.ghost_qty                                                                                         AS qty_ghosted,
           bc.hts_uk                                                                                             AS uk_hts_code,
           bc.is_retail,
           bc.factory_city,
           bc.factory_state,
           bc.factory_zip_code,
           bc.factory_country,
           bc.hts_ca                                                                                             AS ca_hts_code,
           bc.class,
           bc.commission,
           bc.inspection,
           bc.sc_cat                                                                                             AS sc_category,
           bc.size_name,
           bc.payment_term                                                                                       AS payment_terms,
           bc.msrp_eur                                                                                           AS msrp_eu,
           bc.msrp_gbp                                                                                           AS msrp_gb,
           bc.msrp_usd                                                                                           AS msrp_us,
           awv.avg_weight,
           iwv.avg_volume                                                                                        AS actual_volume,
           iwv.avg_weight                                                                                        AS actual_weight,
           bc.is_cancelled,
           lcd.actual_landed_cost_per_unit,
           NVL(bc.cost, 0) + NVL(bc.freight_drayage, 0) + NVL(bc.freight_shipping, 0) + NVL(bc.freight_transload, 0) +
           NVL(bc.cmt, 0) + (NVL(bc.cost, 0) * NVL(bc.duty_percentage, 0)) +
           (NVL(bc.cost, 0) * NVL(bc.tariff_pct, 0)) + (NVL(bc.cost, 0) * NVL(bc.commission_pct, 0)) +
           NVL(bc.inspection, 0)                                                                                 AS landed_cost_estimated,
           CASE
               WHEN lcd.actual_landed_cost_per_unit > 0 AND UPPER(TRIM(lcd.fully_landed)) = 'Y'
                   THEN lcd.actual_landed_cost_per_unit
               ELSE NVL(bc.cost, 0) + NVL(bc.freight_drayage, 0) + NVL(bc.freight_shipping, 0) +
                    NVL(bc.freight_transload, 0) + NVL(bc.cmt, 0) + (NVL(bc.cost, 0) * NVL(bc.duty_percentage, 0)) +
                    (NVL(bc.cost, 0) * NVL(bc.tariff_pct, 0)) + (NVL(bc.cost, 0) * NVL(bc.commission_pct, 0)) +
                    NVL(bc.inspection, 0)
               END                                                                                               AS reporting_landed_cost,

           bc.factory_address2                                                                                   AS factory_street_number,
           bc.factory_address1                                                                                   AS factory_street_name,
           bc.wholesaler_id,
           bc.wholesaler_name,
           COALESCE(csh.centric_department, cen_sc_d.centric_department,
                    cen_s_d.centric_department)                                                                  AS centric_department,
           COALESCE(csh.centric_subdepartment, cen_sc_d.centric_subdepartment,
                    cen_s_d.centric_subdepartment)                                                               AS centric_subdepartment,
           COALESCE(csh.centric_category, cen_sc_d.centric_category,
                    cen_s_d.centric_category)                                                                    AS centric_category,
           COALESCE(csh.centric_class, cen_sc_d.centric_class,
                    cen_s_d.centric_class)                                                                       AS centric_class,
           COALESCE(csh.centric_subclass, cen_sc_d.centric_subclass,
                    cen_s_d.centric_subclass)                                                                    AS centric_subclass,
           bc.start_date,
           bc.po_line_number,
           'Blue Cherry'                                                                                         AS po_source,
           bc.centric_url                                                                                        AS centric_style_id,
           bc.colorfamily                                                                                        AS color_family,
           bc.po_size_range                                                                                      AS size_scale,
           bc.agent_id,
           bc.agent_name,
           bc.approved_date,
           bc.basesku,
           bc.blank_sku,
           bc.cancelled_qty,
           bc.carryover,
           bc.collection,
           bc.commission_pct,
           bc.composition,
           bc.currency_code,
           bc.curvy,
           bc.ean,
           bc.factory_address3,
           bc.factory_address4,
           bc.freight_shipping,
           bc.freight_transload,
           bc.graphic,
           bc.hts_eu,
           bc.member_price,
           bc.non_member_price,
           bc.performance,
           bc.plussize,
           bc.po_size_range,
           bc.issue_qty,
           bc.intran_qty,
           bc.recv_qty,
           bc.retail_price,
           bc.tariff_pct,
           bc.uom,
           bc.upc,
           bc.vendor_address1,
           bc.vendor_address2,
           bc.vendor_address3,
           bc.vendor_address4,
           bc.vendor_city,
           bc.vendor_country,
           bc.vendor_state,
           bc.vendor_zip_code,
           bc.po_dtl_id_merlin,
           IFF(bc.line_status = 'ISSUE', CURRENT_TIMESTAMP(), NULL)                                              AS issue_datetime,
           bc.po_num_bc,
           bc.division_id                                                                                        AS division_id_bc,
           bc.color_code,
           bc.label_code,
           bc.us_compound_rate,
           bc.po_status,
           --iff(coalesce(rec.received_qty, rb.received_qty) >= (bc.qty * .8), coalesce(rec.received_date, rb.received_date), null) as received_date_80_percent,
           -- coalesce(rec.received_date, rb.received_date) as received_date,
           --coalesce(rec.received_qty,rb.received_qty) as received_qty,
           bc.open_seq,
           bc.po_dtl_mod,
           bc.plan_qty,
           ROW_NUMBER() OVER (PARTITION BY bc.po_number, bc.sku, bc.po_line_number ORDER BY bc.date_update DESC) AS po_sku_count,
           UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id)))                                      AS final_fc,
           UPPER(TRIM(bc.warehouse_id_origin))                                                                   AS first_fc,
           csh.color                                                                                             AS centric_color_name,
           IFF(bc.line_status = 'ISSUE', bc.cmt, NULL)                                                           AS original_issue_cmt,
           IFF(bc.line_status = 'ISSUE', bc.qty, NULL)                                                           AS original_issue_qty,
           IFF(bc.line_status = 'ISSUE', bc.cost, NULL)                                                          AS original_issue_cost,
           bc.open_seq || bc.sizebucket                                                                          AS bc_po_line_number,
           IFF(NVL(csh.tfg_color_sku_segment, '-1') = '-1', 1, 0)                                                AS centric_data_null,
           bc.sizebucket                                                                                         AS size_bucket,
           COALESCE(cen_co.carry_over, cen_co1.carry_over)                                                       AS carry_over,
           tc.cbm * bc.qty                                                                                       AS centric_cbm,
           bc.factory_id,
           bc.is_prepack,
           bc.prepack_sku,
           bc.prepack_po_line_number
    FROM lake_view.bluecherry.tfg_po_dtl bc
             LEFT JOIN lake_view.ultra_warehouse.item i
                       ON bc.sku = i.item_number
             LEFT JOIN reporting_base_prod.gsc.centric_style_hierarchy csh
                       ON UPPER(TRIM(bc.style)) = UPPER(TRIM(csh.style))
                           AND SPLIT_PART(bc.sku, '-', 2) = TRIM(csh.tfg_color_sku_segment)
                           AND TRIM(bc.show_room) = TRIM(csh.showroom)
                           AND NVL(csh.tfg_composition_list || csh.fabric_category, '-1') != '-1'
             LEFT JOIN reporting_base_prod.gsc.lc_avg_item_weight_volume iwv
                       ON bc.sku = iwv.item_number
             LEFT JOIN reporting_base_prod.gsc.lc_avg_weight_volume awv
                       ON UPPER(TRIM(bc.division_id)) = UPPER(TRIM(awv.division_id)) --FL
                           AND UPPER(TRIM(i.wms_class)) = UPPER(TRIM(awv.wms_code))
                           AND UPPER(TRIM(bc.subclass)) = UPPER(TRIM(awv.style_type))
             LEFT JOIN _lcd lcd
                       ON UPPER(TRIM(bc.po_number)) = UPPER(TRIM(lcd.po_number))
                           AND UPPER(TRIM(bc.sku)) = UPPER(TRIM(lcd.sku))
             LEFT JOIN reporting_prod.gfc.ghost_inventory_dataset gid
                       ON
                           CASE
                               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'KY' AND
                                    UPPER(TRIM(bc.division_id)) IN ('SXF', 'FL', 'Y') AND bc.date_launch < '2024-07-01'
                                   THEN 154
                               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'KY' AND
                                    bc.date_launch < '2024-07-01'
                                   THEN 107
                               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'KY' AND
                                    bc.date_launch >= '2024-07-01'
                                   THEN 154
                               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'EU'
                                   THEN 221
                               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'UK'
                                   THEN 366
                               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'LA'
                                   THEN 231
                               WHEN UPPER(TRIM(COALESCE(bc.warehouse_id_override, bc.warehouse_id))) = 'MX2'
                                   THEN 466
                               END = gid.warehouse_id
                               AND i.item_id = gid.item_id

             LEFT JOIN (SELECT * FROM _cen WHERE style_color_rn = 1) cen_sc
                       ON UPPER(TRIM(bc.style)) = UPPER(TRIM(cen_sc.style))
                           AND SPLIT_PART(bc.sku, '-', 2) = TRIM(cen_sc.tfg_color_sku_segment)


             LEFT JOIN (SELECT * FROM _cen WHERE style_rn = 1) cen_s
                       ON UPPER(TRIM(bc.style)) = UPPER(TRIM(cen_s.style))

             LEFT JOIN reporting_base_prod.gsc.po_skus_data_history psd_h
                       ON bc.po_number = psd_h.po_number
                           AND bc.sku = psd_h.sku
                           AND bc.po_dtl_id_merlin = psd_h.po_dtl_id

             LEFT JOIN (SELECT * FROM _cen_dept WHERE style_color_rn_dept = 1) cen_sc_d
                       ON UPPER(TRIM(bc.style)) = UPPER(TRIM(cen_sc_d.style))
                           AND SPLIT_PART(bc.sku, '-', 2) = TRIM(cen_sc_d.tfg_color_sku_segment)


             LEFT JOIN (SELECT * FROM _cen_dept WHERE style_rn_dept = 1) cen_s_d
                       ON UPPER(TRIM(bc.style)) = UPPER(TRIM(cen_s_d.style))


             LEFT JOIN _cen_co1 cen_co
                       ON UPPER(TRIM(bc.style)) = UPPER(TRIM(cen_co.style))
                           AND SPLIT_PART(bc.sku, '-', 2) = TRIM(cen_co.tfg_color_sku_segment)
                           AND TRIM(bc.show_room) = TRIM(cen_co.showroom)

             LEFT JOIN _cen_co1 cen_co1
                       ON UPPER(TRIM(bc.style)) = UPPER(TRIM(cen_co1.style))
                           AND SPLIT_PART(bc.sku, '-', 2) = TRIM(cen_co1.tfg_color_sku_segment)
                           AND CASE
                                   WHEN RIGHT(bc.show_room, 2) IN ('01', '02', '03')
                                       THEN 'Q1'
                                   WHEN RIGHT(bc.show_room, 2) IN ('04', '05', '06')
                                       THEN 'Q2'
                                   WHEN RIGHT(bc.show_room, 2) IN ('07', '08', '09')
                                       THEN 'Q3'
                                   WHEN RIGHT(bc.show_room, 2) IN ('10', '11', '12')
                                       THEN 'Q4'
                                   END = cen_co1.showroom_quarter
                           AND cen_co1.showrooms_ordered = 1

             LEFT JOIN lake_view.centric.tfg_cbm_kg_class tc
                       ON UPPER(TRIM(IFF(bc.division_name = 'LINGERIE', 'SAVAGE', bc.division_name))) =
                          UPPER(TRIM(tc.division))
                           AND UPPER(TRIM(COALESCE(csh.centric_department, cen_sc_d.centric_department,
                                                   cen_s_d.centric_department))) = UPPER(TRIM(tc.department))
                           AND UPPER(TRIM(bc.sc_cat)) = UPPER(TRIM(tc.sccategory))
                           AND UPPER(TRIM(COALESCE(csh.centric_subdepartment, cen_sc_d.centric_subdepartment,
                                                   cen_s_d.centric_subdepartment))) = UPPER(TRIM(tc.subdept))
                           AND UPPER(TRIM(COALESCE(csh.centric_class, cen_sc_d.centric_class, cen_s_d.centric_class))) =
                               UPPER(TRIM(tc.class))

             LEFT JOIN reporting_base_prod.reference.po_override ovr
                       ON bc.po_number = ovr.po_number

    WHERE bc.po_dtl_mod > (SELECT MAX(po_dtl_mod) AS mmd FROM gsc.po_skus_data)
       OR ovr.po_number IS NOT NULL
    ;


/*********************
error mitigation
**********************/

    INSERT INTO gsc.aggregation_errors
        (error, process_or_table, field_list, field_values)
    SELECT DISTINCT 'SKU IS NULL'                                                                                   AS error
                  , 'PO/SKUs data aggregation'                                                                      AS process_or_table
                  , 'PO/SKU/bc_po_line_number/PO_dtl_Id'                                                            AS field_list
                  , po_number || '/' || sku || '/' || TO_VARCHAR(bc_po_line_number) || '/' ||
                    TO_VARCHAR(po_dtl_id)                                                                           AS field_values
    FROM _bluec
    WHERE NVL(sku, 'error') = 'error';

    DELETE
    FROM _bluec
    WHERE NVL(sku, 'error') = 'error';

    INSERT INTO gsc.aggregation_errors
        (error, process_or_table, field_list, field_values)
    SELECT DISTINCT 'DUPLICATED AT PO/SKU/LINE_NUMBER GRAIN'                                                        AS error
                  , 'PO/SKUs data aggregation'                                                                      AS process_or_table
                  , 'PO/SKU/bc_po_line_number/PO_dtl_Id'                                                            AS field_list
                  , po_number || '/' || sku || '/' || TO_VARCHAR(bc_po_line_number) || '/' ||
                    TO_VARCHAR(po_dtl_id)                                                                           AS field_values
    FROM _bluec
    WHERE po_sku_count > 1;


    DELETE
    FROM _bluec
    WHERE po_sku_count > 1;


    INSERT INTO gsc.aggregation_errors
        (error, process_or_table, field_list, field_values)
    SELECT DISTINCT 'WAREHOUSE_ID_ORIGIN NULL'                                                                      AS error
                  , 'PO/SKUs data aggregation'                                                                      AS process_or_table
                  , 'PO/SKU/bc_po_line_number/PO_dtl_Id'                                                            AS field_list
                  , po_number || '/' || sku || '/' || TO_VARCHAR(bc_po_line_number) || '/' ||
                    TO_VARCHAR(po_dtl_id)                                                                           AS field_values
    FROM _bluec
    WHERE NVL(warehouse_id_origin, -9) = -9;

    DELETE
    FROM _bluec
    WHERE NVL(warehouse_id_origin, -9) = -9;

    INSERT INTO gsc.aggregation_errors
        (error, process_or_table, field_list, field_values)
    SELECT 'CENTRIC DATA NULL'                                      AS error
         , 'PO/SKUs data aggregation'                               AS process_or_table
         , 'PO/SKU/bc_po_line_number/plm_style/show_room/PO_dtl_Id' AS field_list
         , po_number || '/' || sku || '/' || TO_VARCHAR(bc_po_line_number) || '/' || plm_style || '/' || show_room || '/' ||
           TO_VARCHAR(po_dtl_id)                             AS field_values
    FROM _bluec
    WHERE centric_data_null = 1
      AND qty > 0
      AND is_cancelled = 0;


    MERGE INTO gsc.po_skus_data tgt
        USING (SELECT DISTINCT *
               FROM _bluec src) src
        ON tgt.po_dtl_id = src.po_dtl_id
        WHEN MATCHED THEN UPDATE
            SET
                tgt.sku = src.sku,
                tgt.division_id = src.division_name,
                tgt.show_room = src.show_room,
                tgt.date_launch = src.date_launch,
                tgt.po_number = src.po_number,
                tgt.color = src.color,
                tgt.gp_description = src.gp_description,
                tgt.qty = src.qty,
                tgt.vendor_id = src.vendor_id,
                tgt.xfd = src.xfd,
                tgt.fc_delivery = src.fc_delivery,
                tgt.delivery = src.delivery,
                tgt.department = src.department,
                tgt.inco_term = src.inco_term,
                tgt.freight = src.freight,
                tgt.duty = src.duty,
                tgt.cmt = src.cmt,
                tgt.date_update = src.date_update,
                tgt.cost = src.cost,
                tgt.region_id = src.region_id,
                tgt.freight_method = src.freight_method,
                tgt.shipping_port = src.shipping_port,
                tgt.gender = src.gender,
                tgt.wms_code = src.wms_code,
                tgt.style_name = src.style_name,
                tgt.vend_name = src.vend_name,
                tgt.brand = src.brand,
                tgt.line_status = src.line_status,
                tgt.country_origin = src.country_origin,
                tgt.hts_code = src.hts_code,
                tgt.duty_percentage = src.duty_percentage,
                tgt.plm_style = src.plm_style,
                tgt.fabric_category = src.fabric_category,
                tgt.material_content = src.material_content,
                tgt.buyer = src.buyer,
                tgt.warehouse_id = src.warehouse_id,
                tgt.dimension = src.dimension,
                tgt.subclass = src.subclass,
                tgt.warehouse_id_origin = src.warehouse_id_origin,
                tgt.style_rank = src.style_rank,
                tgt.primary_tariff = src.primary_tariff,
                tgt.avg_volume = src.avg_volume,
                tgt.landed_cost = src.landed_cost,
                tgt.official_factory_name = src.official_factory_name,
                tgt.po_type = src.po_type,
                tgt.qty_ghosted = src.qty_ghosted,
                tgt.uk_hts_code = src.uk_hts_code,
                tgt.is_retail = src.is_retail,
                tgt.factory_city = src.factory_city,
                tgt.factory_state = src.factory_state,
                tgt.factory_zip_code = src.factory_zip_code,
                tgt.factory_country = src.factory_country,
                tgt.ca_hts_code = src.ca_hts_code,
                tgt.class = src.class,
                tgt.commission = src.commission,
                tgt.inspection = src.inspection,
                tgt.sc_category = src.sc_category,
                tgt.size_name = src.size_name,
                tgt.payment_terms = src.payment_terms,
                tgt.msrp_eu = src.msrp_eu,
                tgt.msrp_gb = src.msrp_gb,
                tgt.msrp_us = src.msrp_us,
                tgt.avg_weight = src.avg_weight,
                tgt.actual_volume = src.actual_volume,
                tgt.actual_weight = src.actual_weight,
                tgt.is_cancelled = src.is_cancelled,
                tgt.actual_landed_cost_per_unit = src.actual_landed_cost_per_unit,
                tgt.landed_cost_estimated = src.landed_cost_estimated,
                tgt.reporting_landed_cost = src.reporting_landed_cost,

                tgt.factory_street_number = src.factory_street_number,
                tgt.factory_street_name = src.factory_street_name,
                tgt.wholesaler_name = src.wholesaler_name,
                tgt.centric_department = src.centric_department,
                tgt.centric_subdepartment = src.centric_subdepartment,
                tgt.centric_category = src.centric_category,
                tgt.centric_class = src.centric_class,
                tgt.centric_subclass = src.centric_subclass,
                tgt.start_date = src.start_date,
                tgt.po_line_number = src.po_line_number,
                tgt.po_source = src.po_source,
                tgt.centric_style_id = src.centric_style_id,
                tgt.color_family = src.color_family,
                tgt.size_scale = src.size_scale,
                tgt.agent_id = src.agent_id,
                tgt.agent_name = src.agent_name,
                tgt.approved_date = src.approved_date,
                tgt.basesku = src.basesku,
                tgt.blank_sku = src.blank_sku,
                tgt.cancelled_qty = src.cancelled_qty,
                tgt.carryover = src.carryover,
                tgt.collection = src.collection,
                tgt.commission_pct = src.commission_pct,
                tgt.composition = src.composition,
                tgt.currency_code = src.currency_code,
                tgt.curvy = src.curvy,
                tgt.ean = src.ean,
                tgt.factory_address3 = src.factory_address3,
                tgt.factory_address4 = src.factory_address4,
                tgt.freight_shipping = src.freight_shipping,
                tgt.freight_transload = src.freight_transload,
                tgt.graphic = src.graphic,
                tgt.hts_eu = src.hts_eu,
                tgt.member_price = src.member_price,
                tgt.non_member_price = src.non_member_price,
                tgt.performance = src.performance,
                tgt.plussize = src.plussize,
                tgt.po_size_range = src.po_size_range,
                tgt.issue_qty = src.issue_qty,
                tgt.intran_qty = src.intran_qty,
                tgt.recv_qty = src.recv_qty,
                tgt.retail_price = src.retail_price,
                tgt.tariff_pct = src.tariff_pct,
                tgt.uom = src.uom,
                tgt.upc = src.upc,
                tgt.vendor_address1 = src.vendor_address1,
                tgt.vendor_address2 = src.vendor_address2,
                tgt.vendor_address3 = src.vendor_address3,
                tgt.vendor_address4 = src.vendor_address4,
                tgt.vendor_city = src.vendor_city,
                tgt.vendor_country = src.vendor_country,
                tgt.vendor_state = src.vendor_state,
                tgt.vendor_zip_code = src.vendor_zip_code,
                tgt.po_dtl_id_merlin = src.po_dtl_id_merlin,
                tgt.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(9),
                tgt.issue_datetime = NVL(tgt.issue_datetime, src.issue_datetime),
                tgt.po_num_bc = src.po_num_bc,
                tgt.division_id_bc = src.division_id_bc,
                tgt.color_code = src.color_code,
                tgt.label_code = src.label_code,
                tgt.us_compound_rate = src.us_compound_rate,
                tgt.po_status_text = src.po_status,
                --tgt.received_date_80_percent = src.received_date_80_percent,
                --tgt.received_date = src.received_date,
                tgt.open_seq = src.open_seq,
                tgt.po_dtl_mod = src.po_dtl_mod,
                tgt.plan_qty = src.plan_qty,
                tgt.final_fc = src.final_fc,
                tgt.first_fc = src.first_fc,
                tgt.centric_color_name = src.centric_color_name,
                tgt.original_issue_cmt = NVL(tgt.original_issue_cmt, src.original_issue_cmt),
                tgt.original_issue_qty = NVL(tgt.original_issue_qty, src.original_issue_qty),
                tgt.original_issue_cost = NVL(tgt.original_issue_cost, src.original_issue_cost),
                tgt.bc_po_line_number = src.bc_po_line_number,
                tgt.size_bucket = src.size_bucket,
                tgt.carry_over = src.carry_over,
                tgt.centric_cbm = src.centric_cbm,
                tgt.factory_id = src.factory_id,
                tgt.is_prepack = src.is_prepack,
                tgt.prepack_sku = src.prepack_sku,
                tgt.prepack_po_line_number = src.prepack_po_line_number
        WHEN NOT MATCHED THEN INSERT
            (po_id,
             po_dtl_id,
             sku,
             division_id,
             show_room,
             date_launch,
             po_number,
             color,
             gp_description,
             qty,
             vendor_id,
             xfd,
             fc_delivery,
             delivery,
             department,
             inco_term,
             freight,
             duty,
             cmt,
             date_create,
             date_update,
             cost,
             region_id,
             freight_method,
             shipping_port,
             gender,
             wms_code,
             style_name,
             vend_name,
             brand,
             line_status,
             country_origin,
             hts_code,
             duty_percentage,
             plm_style,
             fabric_category,
             material_content,
             buyer,
             warehouse_id,
             dimension,
             subclass,
             warehouse_id_origin,
             style_rank,
             primary_tariff,
             avg_volume,
             landed_cost,
             official_factory_name,
             original_end_ship_window_date,
             po_type,
             qty_ghosted,
             uk_hts_code,
             is_retail,
             factory_city,
             factory_state,
             factory_zip_code,
             factory_country,
             ca_hts_code,
             class,
             commission,
             inspection,
             sc_category,
             size_name,
             payment_terms,
             msrp_eu,
             msrp_gb,
             msrp_us,
             avg_weight,
             actual_volume,
             actual_weight,
             original_showroom,
             is_cancelled,
             actual_landed_cost_per_unit,
             landed_cost_estimated,
             reporting_landed_cost,
             factory_street_number,
             factory_street_name,
             wholesaler_name,
             centric_department,
             centric_subdepartment,
             centric_category,
             centric_class,
             centric_subclass,
             start_date,
             po_line_number,
             po_source,
             centric_style_id,
             color_family,
             size_scale,
             agent_id,
             agent_name,
             approved_date,
             basesku,
             blank_sku,
             cancelled_qty,
             carryover,
             collection,
             commission_pct,
             composition,
             currency_code,
             curvy,
             ean,
             factory_address3,
             factory_address4,
             freight_shipping,
             freight_transload,
             graphic,
             hts_eu,
             member_price,
             non_member_price,
             performance,
             plussize,
             po_size_range,
             issue_qty,
             intran_qty,
             recv_qty,
             retail_price,
             tariff_pct,
             uom,
             upc,
             vendor_address1,
             vendor_address2,
             vendor_address3,
             vendor_address4,
             vendor_city,
             vendor_country,
             vendor_state,
             vendor_zip_code,
             po_dtl_id_merlin,
             issue_datetime,
             po_num_bc,
             division_id_bc,
             color_code,
             label_code,
             us_compound_rate,
             po_status_text,
                --received_date_80_percent,
                --received_date,
             open_seq,
             po_dtl_mod,
             plan_qty,
             final_fc,
             first_fc,
             centric_color_name,
             original_issue_cmt,
             original_issue_qty,
             original_issue_cost,
             bc_po_line_number,
             size_bucket,
             carry_over,
             centric_cbm,
             factory_id,
             is_prepack,
             prepack_sku,
             prepack_po_line_number
                )
            VALUES (src.po_id,
                    src.po_dtl_id,
                    src.sku,
                    src.division_name,
                    src.show_room,
                    src.date_launch,
                    src.po_number,
                    src.color,
                    src.gp_description,
                    src.qty,
                    src.vendor_id,
                    src.xfd,
                    src.fc_delivery,
                    src.delivery,
                    src.department,
                    src.inco_term,
                    src.freight,
                    src.duty,
                    src.cmt,
                    src.date_create,
                    src.date_update,
                    src.cost,
                    src.region_id,
                    src.freight_method,
                    src.shipping_port,
                    src.gender,
                    src.wms_code,
                    src.style_name,
                    src.vend_name,
                    src.brand,
                    src.line_status,
                    src.country_origin,
                    src.hts_code,
                    src.duty_percentage,
                    src.plm_style,
                    src.fabric_category,
                    src.material_content,
                    src.buyer,
                    src.warehouse_id,
                    src.dimension,
                    src.subclass,
                    src.warehouse_id_origin,
                    src.style_rank,
                    src.primary_tariff,
                    src.avg_volume,
                    src.landed_cost,
                    src.official_factory_name,
                    src.xfd,
                    src.po_type,
                    src.qty_ghosted,
                    src.uk_hts_code,
                    src.is_retail,
                    src.factory_city,
                    src.factory_state,
                    src.factory_zip_code,
                    src.factory_country,
                    src.ca_hts_code,
                    src.class,
                    src.commission,
                    src.inspection,
                    src.sc_category,
                    src.size_name,
                    src.payment_terms,
                    src.msrp_eu,
                    src.msrp_gb,
                    src.msrp_us,
                    src.avg_weight,
                    src.actual_volume,
                    src.actual_weight,
                    src.show_room,
                    src.is_cancelled,
                    src.actual_landed_cost_per_unit,
                    src.landed_cost_estimated,
                    src.reporting_landed_cost,
                    src.factory_street_number,
                    src.factory_street_name,
                    src.wholesaler_name,
                    src.centric_department,
                    src.centric_subdepartment,
                    src.centric_category,
                    src.centric_class,
                    src.centric_subclass,
                    src.start_date,
                    src.po_line_number,
                    src.po_source,
                    src.centric_style_id,
                    src.color_family,
                    src.size_scale,
                    src.agent_id,
                    src.agent_name,
                    src.approved_date,
                    src.basesku,
                    src.blank_sku,
                    src.cancelled_qty,
                    src.carryover,
                    src.collection,
                    src.commission_pct,
                    src.composition,
                    src.currency_code,
                    src.curvy,
                    src.ean,
                    src.factory_address3,
                    src.factory_address4,
                    src.freight_shipping,
                    src.freight_transload,
                    src.graphic,
                    src.hts_eu,
                    src.member_price,
                    src.non_member_price,
                    src.performance,
                    src.plussize,
                    src.po_size_range,
                    src.issue_qty,
                    src.intran_qty,
                    src.recv_qty,
                    src.retail_price,
                    src.tariff_pct,
                    src.uom,
                    src.upc,
                    src.vendor_address1,
                    src.vendor_address2,
                    src.vendor_address3,
                    src.vendor_address4,
                    src.vendor_city,
                    src.vendor_country,
                    src.vendor_state,
                    src.vendor_zip_code,
                    src.po_dtl_id_merlin,
                    src.issue_datetime,
                    src.po_num_bc,
                    src.division_id_bc,
                    src.color_code,
                    src.label_code,
                    src.us_compound_rate,
                    src.po_status,
                       --src.received_date_80_percent,
                       --src.received_date,
                    src.open_seq,
                    src.po_dtl_mod,
                    src.plan_qty,
                    src.final_fc,
                    src.first_fc,
                    src.centric_color_name,
                    src.original_issue_cmt,
                    src.original_issue_qty,
                    src.original_issue_cost,
                    src.bc_po_line_number,
                    src.size_bucket,
                    src.carry_over,
                    src.centric_cbm,
                    src.factory_id,
                    src.is_prepack,
                    src.prepack_sku,
                    src.prepack_po_line_number);

    CREATE OR REPLACE TEMPORARY TABLE _rn AS

    SELECT psd.po_dtl_id
         , psd.po_number
         , psd.sku
         , psd.issue_qty
         , psd.open_seq
         , psd.po_line_number
         , ROW_NUMBER() OVER (PARTITION BY psd.po_number, psd.sku, NVL(bc_po_line_number, po_dtl_id) ORDER BY psd.po_number, psd.sku, NVL(psd.issue_qty, 0) DESC) AS row_number
    FROM gsc.po_skus_data psd
             JOIN (SELECT DISTINCT po_number, sku FROM _bluec) bc
                  ON psd.po_number = bc.po_number
                      AND psd.sku = bc.sku
    WHERE 1 = 1
    ORDER BY psd.po_number, psd.sku, psd.issue_qty DESC;

    UPDATE gsc.po_skus_data
    SET row_number = _rn.row_number
    FROM _rn
    WHERE gsc.po_skus_data.po_dtl_id = _rn.po_dtl_id;

/***********receipt data************/
    UPDATE gsc.po_skus_data
    SET received_date            = rd.received_date
      , received_date_80_percent = IFF((gsc.po_skus_data.qty * .8) <= (rd.received_qty + rd.correction_qty),
                                       rd.received_date, NULL)
      , meta_update_datetime     = CURRENT_TIMESTAMP()
    FROM gsc.receipt_data rd
    WHERE gsc.po_skus_data.po_dtl_id = rd.po_dtl_id
      AND (gsc.po_skus_data.received_date IS NULL
        OR gsc.po_skus_data.received_date_80_percent IS NULL
        );


/***********landed cost ******************/
    CREATE OR REPLACE TEMPORARY TABLE _lcd AS

    SELECT po_dtl_id,
           actual_landed_cost_per_unit
    FROM reporting_prod.gsc.landed_cost_dataset lcd
    WHERE lcd.meta_update_datetime >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
      AND fully_landed = 'Y';


    UPDATE gsc.po_skus_data
    SET actual_landed_cost_per_unit = lcd.actual_landed_cost_per_unit
      , reporting_landed_cost       = lcd.actual_landed_cost_per_unit
      , meta_update_datetime        = CURRENT_TIMESTAMP()
    FROM _lcd lcd
    WHERE gsc.po_skus_data.po_dtl_id = lcd.po_dtl_id;

END;
