CREATE OR REPLACE VIEW reference.finance_store_mapping AS
WITH _lake_finance_store_mapping AS (
    SELECT CONCAT('RG', fsm.region_seg) AS hyperion_store_id,
           fsm.region_seg               AS oracle_store_id,
           fsm.region_seg_desc          AS oracle_store_name,
           CAST(fsm.store_id AS INT)    AS store_id,
           ds.store_full_name,
           ds.store_type,
           fsm.meta_row_hash,
           fsm.meta_create_datetime,
           fsm.meta_update_datetime
    FROM lake.fpa.finance_store_mapping fsm
             JOIN stg.dim_store ds
                  ON fsm.store_id = ds.store_id
    WHERE fsm.region_seg <> 'TBD'
)


SELECT hyperion_store_id,
       oracle_store_id,
       oracle_store_name,
       store_id,
       store_full_name,
       store_type,
       meta_row_hash,
       meta_create_datetime,
       meta_update_datetime
FROM _lake_finance_store_mapping

UNION ALL

SELECT hyperion_store_id,
       oracle_store_id,
       oracle_store_name,
       CAST(ds.store_id AS INT) AS store_id,
       ds.store_full_name,
       ds.store_type,
       fsm.meta_row_hash,
       fsm.meta_create_datetime,
       fsm.meta_update_datetime
FROM lake.gsheet.finance_store_mapping fsm
         JOIN stg.dim_store ds
              ON fsm.store_id = ds.store_id
WHERE fsm.hyperion_store_id NOT IN (SELECT hyperion_store_id FROM _lake_finance_store_mapping)
  AND lower(hyperion_store_id) <> 'none'
ORDER BY hyperion_store_id;

/*
 the initial mappings were handled manually in lake.gsheet.finance_store_mapping
 then it is automated and we created a process to ingest this data from lake.fpa.finance_store_mapping
 However, some of the mappings in lake.gsheet is not available in lake.fpa. Therefore, we are using a CTE
 to implement the below logic which prioritizes lake.fpa then lake.gsheet
 */
