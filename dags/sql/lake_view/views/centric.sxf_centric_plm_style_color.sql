CREATE VIEW LAKE_VIEW.CENTRIC.SXF_CENTRIC_PLM_STYLE_COLOR AS (
    SELECT colorway.id AS color_id,
       colorway.the_cnl AS color_cnl,
       REPLACE(SUBSTRING(colorway.tfg_division, POSITION(':' IN colorway.tfg_division),
            LENGTH(colorway.tfg_division)), ':', '') AS color_tfgdivision,
       style.id AS style_id,
       style.the_cnl AS style_cnl,
       style.code AS style_code,
       style.node_name AS style_name,
       style.tfg_classifier0 AS style_dept_id,
       dept.node_name AS style_dept_name,
       style.tfg_classifier1 AS style_subdept_id,
       subdept.node_name AS style_subdept_name,
       style.tfg_classifier2 AS style_class_id,
       class.node_name AS style_class_name,
       style.classifier3 AS style_subclass_id,
       subclass.node_name AS style_subclass_name,
       style.tfg_collection AS style_collection_id,
       lookup_collection.node_name AS style_collection_name,
       style.actual_size_range AS style_size_range_id,
       sizerange.node_name AS style_size_range_name,
       REPLACE(SUBSTRING(t_style.text_value, POSITION(':' IN t_style.text_value),
            LENGTH(t_style.text_value)), ':', '') AS style_size_type,
       REPLACE(SUBSTRING(style.tfg_core_fashion, POSITION(':' IN style.tfg_core_fashion),
            LENGTH(style.tfg_core_fashion)), ':', '') AS style_core_fashion,
       REPLACE(SUBSTRING(style.tfg_gender, POSITION(':' IN style.tfg_gender),
            LENGTH(style.tfg_gender)), ':', '') AS style_gender,
       REPLACE(SUBSTRING(style.tfg_coverage, POSITION(':' IN style.tfg_coverage),
            LENGTH(style.tfg_coverage)), ':','') AS style_coverage,
       REPLACE(SUBSTRING(colorway.tfg_color_family, POSITION(':' IN colorway.tfg_color_family),
            LENGTH(colorway.tfg_color_family)), ':', '') AS colorway_color_family,
       colorspec.node_name AS colorway_color_name,
       printspec.node_name AS colorway_print_name,
       style.parent_season::VARCHAR AS style_parent_season,
       season.tfg_season_sort AS style_tfg_season_sort,
       LISTAGG(r_style.map_key, '|') WITHIN GROUP (ORDER BY r_style.map_key) AS main_mat_order_agg,
       LISTAGG(mat.node_name, '|') WITHIN GROUP (ORDER BY r_style.map_key) AS main_mat_name_agg,
       LISTAGG(REPLACE(SUBSTRING(mat.default_material_family,
            POSITION(':' IN mat.default_material_family),
            LENGTH(mat.default_material_family)), ':', ''),
            '|') WITHIN GROUP (ORDER BY r_style.map_key) AS main_mat_family_agg

    FROM ed_colorway AS colorway
         JOIN ed_style AS style ON colorway.the_parent_id = style.id
         LEFT JOIN ed_classifier0 AS dept ON style.tfg_classifier0 = dept.id
         LEFT JOIN ed_classifier1 AS subdept ON style.tfg_classifier1 = subdept.id
         LEFT JOIN ed_classifier2 AS class ON style.tfg_classifier2 = class.id
         LEFT JOIN ed_classifier3 AS subclass ON style.classifier3 = subclass.id
         LEFT JOIN ed_lookup_item AS lookup_collection ON style.tfg_collection =
            lookup_collection.id
         LEFT JOIN ed_size_range AS sizerange ON style.actual_size_range = sizerange.id
         LEFT JOIN et_style AS t_style ON t_style.id = style.id AND t_style.attr_id::text =
            'tfgMissyVsCurvy'::text
         LEFT JOIN ed_color_specification AS colorspec ON colorway.color_specification =
            colorspec.id
         LEFT JOIN ed_print_design_color AS printspec ON colorway.color_specification =
            printspec.id
         LEFT JOIN er_style AS r_style ON r_style.id = style.id AND r_style.attr_id =
            'BOMMainMaterials'
         LEFT JOIN ed_material AS mat ON r_style.ref_id = mat.id
         LEFT JOIN ed_season AS season ON style.parent_season = season.parent_season

    WHERE colorway.tfg_division = 'tfgDivision:SXF'

    GROUP BY color_id,
         color_cnl,
         color_tfgdivision,
         style_id,
         style_cnl,
         style_code,
         style_name,
         style_dept_id,
         style_dept_name,
         style_subdept_id,
         style_subdept_name,
         style_class_id,
         style_class_name,
         style_subclass_id,
         style_subclass_name,
         style_collection_id,
         style_collection_name,
         style_size_range_id,
         style_size_range_name,
         style_size_type,
         style_core_fashion,
         style_gender,
         style_coverage,
         colorway_color_family,
         colorway_color_name,
         colorway_print_name,
         style_parent_season,
         style_tfg_season_sort
);
