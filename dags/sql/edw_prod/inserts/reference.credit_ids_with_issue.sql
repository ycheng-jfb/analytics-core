
/*
Below code is to concat store_credit_id with company_id to accommodate the db split project.
It is a ONE TIME RUN ONLY
*/

ALTER TABLE reference.credit_ids_with_issue
ADD meta_original_store_credit_id NUMERIC(38,0);

UPDATE reference.credit_ids_with_issue
SET meta_original_store_credit_id = store_credit_id;

UPDATE reference.credit_ids_with_issue
SET store_credit_id = NULL;

UPDATE reference.credit_ids_with_issue h
SET h.store_credit_id = sc.store_credit_id
FROM lake_consolidated.ultra_merchant.store_credit sc
WHERE h.meta_original_store_credit_id = sc.meta_original_store_credit_id;
