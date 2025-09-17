CREATE OR REPLACE TEMP TABLE _fake_retail_leads_with_activations
AS
SELECT
    DISTINCT fa.customer_id,
             fa.meta_original_customer_id
FROM stg.fact_registration fr
JOIN stg.fact_activation fa
    ON fa.customer_id = fr.customer_id
WHERE fr.is_fake_retail_registration
AND fr.customer_id NOT IN (SELECT customer_id FROM reference.fake_retail_leads_with_activations);


MERGE INTO reference.fake_retail_leads_with_activations AS t
USING _fake_retail_leads_with_activations AS s
    ON s.customer_id = t.customer_id
    WHEN NOT MATCHED THEN INSERT
    (
        customer_id,
        meta_original_customer_id,
        meta_create_datetime,
        meta_update_datetime
    )
    VALUES
    (
        customer_id,
        meta_original_customer_id,
        CURRENT_TIMESTAMP::timestamp_ltz,
        CURRENT_TIMESTAMP::timestamp_ltz
    );

SELECT
    customer_id AS fake_retail_leads_with_activations
FROM _fake_retail_leads_with_activations;

/*
run once for the dbsplit
ALTER TABLE reference.fake_retail_leads_with_activations
ADD COLUMN meta_original_customer_id NUMBER(38,0);

UPDATE reference.fake_retail_leads_with_activations
SET meta_original_customer_id = customer_id;

UPDATE reference.fake_retail_leads_with_activations
SET customer_id = NULL;

UPDATE reference.fake_retail_leads_with_activations f
SET f.customer_id = c.customer_id
FROM lake_consolidated.ultra_merchant.customer c
WHERE f.meta_original_customer_id = c.meta_original_customer_id;

*/
