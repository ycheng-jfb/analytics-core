CREATE OR REPLACE VIEW  lake_view.ultra_warehouse.case_item AS
SELECT *
FROM lake.ultra_warehouse.case_item l
WHERE NVL(hvr_is_deleted, 0) = 0;
