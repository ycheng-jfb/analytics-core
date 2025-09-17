CREATE OR REPLACE TEMPORARY TABLE _style_raw AS
SELECT DISTINCT
    s.style_number,
    pd.color,
    stfcfc.choice                                 AS fabric_category,
    m.material                                    AS material_content,
    smc.percentage::VARCHAR || '% ' || m.material AS material_percent
FROM lake_view.jf_portal.po_hdr AS h
JOIN lake_view.jf_portal.po_dtl AS pd
    ON h.po_id = pd.po_id
JOIN lake_view.merlin.style AS s
    ON pd.style = s.style_number
JOIN lake_view.merlin.merlin_object_base AS mob
    ON s.oid = mob.oid
    AND NVL(mob.gc_record, -1) = -1
LEFT JOIN lake_view.merlin.color AS col
    ON UPPER(TRIM(pd.color)) = UPPER(TRIM(col.name))
    AND pd.color_sku_seg = col.nrf_code
    AND NVL(col.gc_record, -1) = -1
LEFT JOIN lake_view.merlin.style_color AS scol
    ON s.oid = scol.style
    AND col.oid = scol.color
    AND NVL(scol.gc_record, -1) = -1
LEFT JOIN lake_view.merlin.style_tech_field_choice AS stfcfc
    ON s.fabric_category = stfcfc.oid
    AND NVL(stfcfc.gc_record, -1) = -1
LEFT JOIN lake_view.merlin.style_material_content AS smc
    ON scol.oid = smc.style_color
    AND NVL(smc.gc_record, -1) = -1
LEFT JOIN lake_view.merlin.style_material AS m
    ON smc.material = m.oid
    AND NVL(m.gc_record, -1) = -1
WHERE NVL(s.date_update, s.date_create) >= DATEADD(MONTH, -1, CURRENT_TIMESTAMP())
    AND NVL(m.material, 'MIKEWASHERE') != 'MIKEWASHERE';

MERGE INTO gsc.style_material_content AS tgt
USING (
    SELECT
        l.style_number,
        l.color,
        l.fabric_category,
        LISTAGG(DISTINCT l.material_percent, ' / ')
            WITHIN GROUP (ORDER BY l.material_percent DESC )
            AS material_content
    FROM _style_raw AS l
    GROUP BY
        l.style_number,
        l.color,
        l.fabric_category
    ORDER BY
        l.style_number ASC,
        l.color ASC,
        l.fabric_category ASC
    ) AS src
ON tgt.style_number = src.style_number
    AND tgt.color = src.color
WHEN MATCHED AND (
    COALESCE(tgt.fabric_category, '') = COALESCE(src.fabric_category, '')
    OR COALESCE(tgt.material_content, '') = COALESCE(src.material_content, '')
    )
THEN UPDATE
SET
    tgt.fabric_category = src.fabric_category,
    tgt.material_content = src.material_content,
    tgt.meta_update_datetime = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        style_number,
        color,
        fabric_category,
        material_content
    )
    VALUES (
        src.style_number,
        src.color,
        src.fabric_category,
        src.material_content
);
