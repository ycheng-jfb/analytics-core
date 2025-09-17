SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _gsc_po_detail_dataset_stg AS
SELECT
    pdd.po_id,
    pdd.po_dtl_id,
    pdd.po_number,
    pdd.po_line_number,
    pdd.sku,
    LOWER(pdd.brand) AS brand,
    pdd.po_status_text,
    pdd.qty,
    nvl(pdd.freight, 0) AS freight,
    nvl(pdd.duty, 0) AS duty,
    nvl(pdd.cmt, 0) AS cmt,
    nvl(pdd.cost, 0) AS cost,
    nvl(pdd.landed_cost_estimated, 0) AS landed_cost_estimated,
    NVL(pdd.actual_landed_cost, 0) AS actual_landed_cost,
    NVL(pdd.reporting_landed_cost, 0) AS reporting_landed_cost,
    pdd.fc_delivery,
    pdd.xfd,
    pdd.delivery,
    pdd.date_launch,
    pdd.date_create,
    pdd.warehouse_id,
    pdd.po_status_id,
    pdd.line_status,
    pdd.show_room,
    FALSE AS is_deleted
FROM reporting_prod.gsc.po_detail_dataset pdd;

BEGIN TRANSACTION;

-- soft delete orphan records
UPDATE reference.gsc_po_detail_dataset pdd
SET
    pdd.is_deleted = TRUE,
    pdd.meta_row_hash = hash(pdd.po_id,pdd.po_dtl_id,pdd.po_number,pdd.po_line_number,pdd.sku,pdd.brand,pdd.po_status_text,
        pdd.qty,pdd.freight,pdd.duty,pdd.cmt,pdd.cost,pdd.landed_cost_estimated,pdd.fc_delivery,pdd.xfd,pdd.delivery,
        pdd.date_launch, pdd.po_status_id,pdd.line_status,pdd.show_room,pdd.date_create,pdd.warehouse_id,
        pdd.actual_landed_cost,pdd.reporting_landed_cost,TRUE),
    pdd.meta_update_datetime = $execution_start_time
WHERE NOT NVL(pdd.is_deleted,FALSE)
    AND NOT EXISTS(
    SELECT 1 FROM _gsc_po_detail_dataset_stg AS base
    WHERE pdd.po_id = base.po_id
    AND pdd.po_dtl_id = base.po_dtl_id
);

MERGE INTO reference.gsc_po_detail_dataset t
USING (
        SELECT
            po_id,
            po_dtl_id,
            po_number,
            sku,
            po_line_number,
            warehouse_id,
            brand,
            po_status_text,
            qty,
            freight,
            duty,
            cmt,
            cost,
            landed_cost_estimated,
            actual_landed_cost,
            reporting_landed_cost,
            fc_delivery,
            xfd,
            delivery,
            date_launch,
            date_create,
            po_status_id,
            line_status,
            show_room,
            is_deleted,
            hash(
                    po_id,
                    po_dtl_id,
                    po_number,
                    sku,
                    po_line_number,
                    warehouse_id,
                    brand,
                    po_status_text,
                    qty,
                    freight,
                    duty,
                    cmt,
                    cost,
                    landed_cost_estimated,
                    actual_landed_cost,
                    reporting_landed_cost,
                    fc_delivery,
                    xfd,
                    delivery,
                    date_launch,
                    date_create,
                    po_status_id,
                    line_status,
                    show_room,
                    is_deleted
                ) as meta_row_hash
        FROM _gsc_po_detail_dataset_stg
      ) s
    ON t.po_id = s.po_id
    AND t.po_dtl_id = s.po_dtl_id
WHEN NOT MATCHED
THEN INSERT
    (
        po_id,
        po_dtl_id,
        po_number,
        sku,
        po_line_number,
        warehouse_id,
        brand,
        po_status_text,
        qty,
        freight,
        duty,
        cmt,
        cost,
        landed_cost_estimated,
        actual_landed_cost,
        reporting_landed_cost,
        fc_delivery,
        xfd,
        delivery,
        date_launch,
        date_create,
        po_status_id,
        line_status,
        show_room,
        is_deleted,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
    )
    VALUES
    (
        po_id,
        po_dtl_id,
        po_number,
        sku,
        po_line_number,
        warehouse_id,
        brand,
        po_status_text,
        qty,
        freight,
        duty,
        cmt,
        cost,
        landed_cost_estimated,
        actual_landed_cost,
        reporting_landed_cost,
        fc_delivery,
        xfd,
        delivery,
        date_launch,
        date_create,
        po_status_id,
        line_status,
        show_room,
        is_deleted,
        meta_row_hash,
        $execution_start_time,
        $execution_start_time
     )
WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
THEN UPDATE
    SET
        t.po_number = s.po_number,
        t.sku = s.sku,
        t.po_line_number = s.po_line_number,
        t.warehouse_id = s.warehouse_id,
        t.brand = s.brand,
        t.po_status_text = s.po_status_text,
        t.qty = s.qty,
        t.freight = s.freight,
        t.duty = s.duty,
        t.cmt = s.cmt,
        t.cost = s.cost,
        t.landed_cost_estimated = s.landed_cost_estimated,
        t.actual_landed_cost = s.actual_landed_cost,
        t.reporting_landed_cost = s.reporting_landed_cost,
        t.fc_delivery = s.fc_delivery,
        t.xfd = s.xfd,
        t.delivery = s.delivery,
        t.date_launch = s.date_launch,
        t.date_create = s.date_create,
        t.po_status_id = s.po_status_id,
        t.line_status = s.line_status,
        t.show_room = s.show_room,
        t.meta_row_hash = s.meta_row_hash,
        t.is_deleted = s.is_deleted,
        t.meta_update_datetime = $execution_start_time;

COMMIT;
