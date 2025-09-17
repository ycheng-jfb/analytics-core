SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _miracle_mile_landed_costs__base AS
SELECT DISTINCT style,
                colorway_cnl
FROM lake.centric.tfg_marketplace_costs_history;


CREATE OR REPLACE TEMP TABLE _miracle_mile_landed_costs__stg AS (
WITH landed_costs_history AS (
    SELECT
         base.style,
         base.colorway_cnl,
         tmc.landed_cost,
         tmc.the_created_at,
         tmc.is_current,
         ROW_NUMBER() OVER (PARTITION BY base.style,base.colorway_cnl ORDER BY the_created_at ASC) AS rn
    FROM _miracle_mile_landed_costs__base base
    JOIN lake.centric.tfg_marketplace_costs_history tmc
       ON base.style = tmc.style
       AND base.colorway_cnl = tmc.colorway_cnl)

SELECT style,
       colorway_cnl,
       landed_cost,
       CASE
           WHEN rn = 1 THEN '1900-01-01'
           ELSE the_created_at
           END                                                 AS effective_start_datetime,
       COALESCE(LEAD(the_created_at, 1)
                     OVER (PARTITION BY style,
                         colorway_cnl ORDER BY the_created_at ASC) -
                INTERVAL '1 millisecond',
                '9999-12-31')                                  AS effective_end_datetime,
       IFF(effective_end_datetime = '9999-12-31', TRUE, FALSE) AS is_current,
       rn,
       HASH(style, colorway_cnl, landed_cost,
            effective_start_datetime,
            effective_end_datetime,
            is_current)                                        AS meta_row_hash,
       $execution_start_time                                   AS meta_create_datetime,
       $execution_start_time                                   AS meta_update_datetime
FROM landed_costs_history);


MERGE INTO reference.miracle_miles_landed_cost t
    USING _miracle_mile_landed_costs__stg s
    ON EQUAL_NULL(t.style, s.style)
        AND EQUAL_NULL(s.colorway_cnl, t.colorway_cnl)
        AND EQUAL_NULL(s.effective_start_datetime::timestamp_ntz, t.effective_start_datetime::timestamp_ntz)
    WHEN NOT MATCHED THEN
        INSERT (style,
                colorway_cnl,
                landed_cost,
                effective_start_datetime,
                effective_end_datetime,
                is_current,
                meta_row_hash,
                meta_create_datetime,
                meta_update_datetime)
            VALUES (style,
                    colorway_cnl,
                    landed_cost,
                    effective_start_datetime,
                    effective_end_datetime,
                    is_current,
                    meta_row_hash,
                    meta_create_datetime,
                    meta_update_datetime)
    WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN
        UPDATE
            SET t.effective_end_datetime = s.effective_end_datetime,
                t.is_current = s.is_current,
                t.meta_row_hash = s.meta_row_hash,
                t.meta_update_datetime = s.meta_update_datetime;
