/*
Historical data is loaded from reference.fact_membership_event_archive .
This data is copied from archive_edw01_edw.dbo.fact_membership_event_first_time.

Below code is to concat customer_id to accommodate the db split project.
It is a ONE TIME RUN ONLY
*/

ALTER TABLE reference.fact_membership_event_archive
ADD COLUMN meta_original_customer_id INT;

UPDATE reference.fact_membership_event_archive fmea
SET fmea.meta_original_customer_id = fmea.customer_id;

UPDATE reference.fact_membership_event_archive fmea
SET fmea.customer_id =NULL;

UPDATE reference.fact_membership_event_archive fme
SET fme.customer_id = concat(meta_original_customer_id, company_id)
FROM lake_fl.reference.dim_store ds
WHERE fme.store_id =ds.store_id;

-- DA-24994 -- Guest Purchasing Member
UPDATE reference.fact_membership_event_archive
SET membership_type_detail = 'Regular'
WHERE membership_event_type = 'Guest Purchasing Member'
and (membership_type_detail = 'Unknown' OR membership_type_detail IS NULL);

UPDATE reference.fact_membership_event_archive
SET membership_type_detail ='Unidentified'
WHERE customer_id IN (SELECT customer_id
                      FROM stg.dim_customer
                      WHERE email ILIKE ANY ('%retail.fabletics.com', '%retail.savagex.com'))
AND membership_event_type = 'Guest Purchasing Member';

-- DA-24994 -- Deactivated Leads
CREATE OR REPLACE TEMP TABLE _reactivation_customer_ids AS
SELECT customer_id,
       store_id,
       membership_event_type,
       membership_type_detail,
       event_datetime,
       meta_original_customer_id
FROM reference.fact_membership_event_archive
WHERE membership_event_type = 'Registration'
  AND membership_type_detail = 'Reactivated';


CREATE OR REPLACE TEMP TABLE _reactivation_original_customer_ids AS
SELECT cl.original_customer_id AS customer_id,
       store_id,
       'Deactivated Lead'      AS membership_event_type,
       'Deactivated'           AS membership_type_detail,
       event_datetime,
       meta_original_customer_id
FROM lake_consolidated.ultra_merchant.customer_link cl
         JOIN _reactivation_customer_ids r
              ON r.customer_id = cl.current_customer_id;

INSERT INTO reference.fact_membership_event_archive(
    customer_id,
    store_id,
    membership_event_type,
    membership_type_detail,
    event_datetime,
    meta_data_quality,
    meta_create_datetime,
    meta_update_datetime,
    meta_original_customer_id
 )
SELECT customer_id,
       store_id,
       membership_event_type,
       membership_type_detail,
       event_datetime,
       NULL AS meta_data_quality,
       CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3),
       CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3),
       meta_original_customer_id
FROM _reactivation_original_customer_ids;



/* If a cancelled vip starts making guest purchases after their cancellation, we want to create
   a guest purchaser membership_event_type with a "previous vip" membership_type_detail
 */
CREATE OR REPLACE TEMP TABLE _customer_cancellations AS
SELECT *
FROM (
    SELECT
        customer_id,
        meta_original_customer_id,
        store_id,
        LAG(membership_event_type) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime) AS prior_event,
        membership_event_type,
        membership_type_detail,
        event_start_local_datetime,
        event_end_local_datetime
    FROM stg.fact_membership_event
    WHERE COALESCE(is_deleted, FALSE) = FALSE
)
WHERE membership_event_type = 'Cancellation'
    AND prior_event = 'Activation'
    AND CONVERT_TIMEZONE('America/Los_Angeles', event_start_local_datetime)::TIMESTAMP_NTZ < '2016-01-01';

INSERT INTO reference.fact_membership_event_archive(
    customer_id,
    store_id,
    membership_event_type,
    membership_type_detail,
    event_datetime,
    meta_data_quality,
    meta_create_datetime,
    meta_update_datetime,
    meta_original_customer_id
 )
SELECT
    cc.customer_id,
    cc.store_id,
    'Guest Purchasing Member',
    'Previous VIP',
    CONVERT_TIMEZONE('America/Los_Angeles', fo.order_local_datetime),
    NULL AS meta_data_quality,
    CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3),
    CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3),
    cc.meta_original_customer_id
FROM _customer_cancellations AS cc
    JOIN stg.fact_order AS fo
        ON fo.customer_id = cc.customer_id
        AND fo.order_local_datetime >= cc.event_start_local_datetime
        AND fo.order_local_datetime < cc.event_end_local_datetime
    JOIN stg.dim_order_sales_channel AS osc ON osc.order_sales_channel_key = fo.order_sales_channel_key
    JOIN stg.dim_order_membership_classification AS omc ON omc.order_membership_classification_key = fo.order_membership_classification_key
WHERE osc.order_classification_l2 = 'Product Order'
    AND fo.order_status_key = 1
    AND omc.membership_order_type_l2 = 'Guest'
    AND CONVERT_TIMEZONE('America/Los_Angeles', fo.order_local_datetime)::TIMESTAMP_NTZ < '2016-01-01';
