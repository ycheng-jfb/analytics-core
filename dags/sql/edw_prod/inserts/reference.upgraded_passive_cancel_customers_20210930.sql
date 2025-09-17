/*

This table is loaded by Dwiti.
This table is being used to ignore membership signals in stg.fact_membership. Please check DA-19065
for more information.

 */
/*

This table is loaded by Dwiti.
This table is being used to ignore membership signals in stg.fact_membership. Please check DA-19065
for more information.

Below is the update statement to concat customer_id to accommodate db split project.
It is a ONE TIME RUN ONLY

 */

ALTER TABLE reference.upgraded_passive_cancel_customers_20210930
ADD COLUMN meta_original_customer_id INT;

UPDATE reference.upgraded_passive_cancel_customers_20210930
SET meta_original_customer_id= customer_id;

UPDATE reference.upgraded_passive_cancel_customers_20210930
SET customer_id = -1;

UPDATE reference.upgraded_passive_cancel_customers_20210930 r
SET r.customer_id = c.customer_id
FROM lake_consolidated.ultra_merchant.customer c
WHERE r.meta_original_customer_id = c.meta_original_customer_id;
