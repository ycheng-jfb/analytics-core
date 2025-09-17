/*
 On Feb 2023, DBOps passive cancelled pending JFB customers which caused a spike on the passive cancelled member counts.
 I have loaded this table with the below script. This table will be used to ignore these passive cancellations in EDW.
 Check DA-25698 for more details

 */
CREATE OR REPLACE TEMP TABLE _all_customer_list AS
SELECT DISTINCT fme.customer_id, fme.store_id
FROM data_model.fact_membership_event fme
         JOIN data_model.dim_store ds
              ON ds.store_id = fme.store_id
         JOIN lake_consolidated.ultra_merchant.membership m
              ON m.customer_id = fme.customer_id
                  AND CAST(event_start_local_datetime AS DATE) >= '2023-02-01'
                  AND CAST(event_start_local_datetime AS DATE) < '2023-03-01'
                  AND membership_event_type = 'Cancellation'
                  AND membership_type_detail = 'Passive'
WHERE fme.store_id IN (26, 41);


CREATE OR REPLACE TEMP TABLE _real_cancellation AS
WITH _customer_list AS (SELECT membership_id, fme.store_id, fme.customer_id
                        FROM data_model.fact_membership_event fme
                                 JOIN lake_consolidated.ultra_merchant.membership m
                                      ON m.customer_id = fme.customer_id
                        WHERE fme.store_id IN (26, 41)
                          AND CAST(event_start_local_datetime AS DATE) >= '2023-02-01'
                          AND CAST(event_start_local_datetime AS DATE) < '2023-03-01'
                          AND membership_event_type = 'Cancellation'
                          AND membership_type_detail = 'Passive')
SELECT DISTINCT cl.membership_id, cl.customer_id
FROM lake_consolidated.ultra_merchant.statuscode_modification_log ml
         JOIN _customer_list cl
              ON cl.membership_id = ml.object_id
WHERE object = 'membership'
  AND to_statuscode = '3925'
  AND CAST(datetime_added AS DATE) >= '2023-02-01';

TRUNCATE TABLE reference.pending_passive_cancel_customers_20230201;

INSERT INTO reference.pending_passive_cancel_customers_20230201 (customer_id, meta_create_datetime, meta_update_datetime)
SELECT customer_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM _all_customer_list
WHERE customer_id NOT IN (SELECT customer_id FROM _real_cancellation);
