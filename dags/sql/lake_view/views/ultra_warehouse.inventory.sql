CREATE VIEW lake_view.ultra_warehouse.inventory AS
SELECT *
FROM lake.ultra_warehouse.inventory l
WHERE NVL(hvr_is_deleted, 0) = 0;
