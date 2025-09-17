SET date_from = (
    SELECT DATEADD(DAY, 1, MAX(shipped_date))
    FROM gsc.shipped_items_data);
SET date_to = TO_DATE(CURRENT_TIMESTAMP());

MERGE INTO gsc.shipped_items_data AS tgt
USING (
    SELECT
        im.item_id AS item_id_um,
        it.item_id AS item_id_uw,
        it.item_number,
        TO_DATE(ib.date_shipped) AS shipped_date,
        TO_NUMBER(SPLIT_PART(TO_CHAR(ib.date_shipped), '-', 1) ||
        SPLIT_PART(TO_CHAR(ib.date_shipped), '-', 2)) AS period,
        i.foreign_store_id,
        i.warehouse_id,
        CASE
            WHEN warehouse_id IN (107, 154, 421, 231)
            THEN 'US'
            WHEN warehouse_id = 109
            THEN 'CA'
            WHEN warehouse_id IN (221, 366)
            THEN 'EU'
            ELSE 'na'
        END AS fc,
        SUM(ii.quantity) AS units
        FROM lake_view.ultra_warehouse.invoice AS i
        JOIN lake_view.ultra_warehouse.invoice_box AS ib
            ON i.invoice_id = ib.invoice_id
        JOIN lake_view.ultra_warehouse.invoice_item AS ii
            ON i.invoice_id = ii.invoice_id
        JOIN lake_view.ultra_warehouse.item AS it
            ON ii.item_id = it.item_id
        JOIN lake_consolidated_view.ultra_merchant.item AS im
            ON it.item_number = im.item_number
        WHERE ib.status_code_id = 51
            AND i.status_code_id = 41
            AND NVL(UPPER(it.wms_class), 'NOCLASS') NOT IN ('CONSUMABLE', 'INSERT', 'MISCELLANEOUS', 'NOCLASS')
            AND ib.date_shipped BETWEEN $date_from AND $date_to
        GROUP BY
            im.item_id,
            it.item_id,
            it.item_number,
            TO_DATE(ib.date_shipped),
            TO_NUMBER(SPLIT_PART(TO_CHAR(ib.date_shipped), '-', 1) ||
            SPLIT_PART(TO_CHAR(ib.date_shipped), '-', 2)),
            i.foreign_store_id,
            i.warehouse_id,
            CASE
                WHEN warehouse_id IN (107, 154, 421, 231)
                THEN 'US'
                WHEN warehouse_id = 109
                THEN 'CA'
                ELSE 'EU'
            END
           ) src
    ON tgt.item_id_um = src.item_id_um
        AND tgt.foreign_store_id = src.foreign_store_id
        AND tgt.warehouse_id = src.warehouse_id
        AND tgt.shipped_date = src.shipped_date
    WHEN MATCHED AND (
        COALESCE(tgt.item_number, '') <> COALESCE(src.item_number, '')
        OR COALESCE(tgt.units, -1) <> COALESCE(src.units, -1)
        OR COALESCE(tgt.period, -1) <> COALESCE(src.period, -1)
        OR COALESCE(tgt.fc, '') <> COALESCE(src.fc, '')
        )
    THEN
    UPDATE SET
        tgt.item_number = src.item_number,
        tgt.units = src.units,
        tgt.period = src.period,
        tgt.fc = src.fc
    WHEN NOT MATCHED THEN
    INSERT (
        item_id_um,
        item_id_uw,
        item_number,
        shipped_date,
        foreign_store_id,
        warehouse_id,
        units,
        period,
        fc
    )
    VALUES (
        src.item_id_um,
        src.item_id_uw,
        src.item_number,
        src.shipped_date,
        src.foreign_store_id,
        src.warehouse_id,
        src.units,
        src.period,
        src.fc
);
