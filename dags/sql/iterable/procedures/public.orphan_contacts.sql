SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _distinct_phone_number AS (
    SELECT usr.phone_number,
       usr.user_id AS itbl_user_id,
       usr.project_id,
       ROW_NUMBER() OVER (PARTITION BY usr.phone_number ORDER BY usr.profile_updated_at) AS r_no
    FROM campaign_event_data.org_3223.users usr
    WHERE usr.phone_number IS NOT NULL
            AND usr.email IS NULL
                AND usr.profile_updated_at > $low_watermark_ltz
    QUALIFY r_no = 1
);

CREATE OR REPLACE TEMP TABLE _distinct_customer AS (
    SELECT itbl_user_id,
           a.customer_id,
           dc.first_name as firstname,
           dc.last_name as lastname,
           dc.email,
           dc.store_id,
           phone_number,
           phone_digits,
           csr.store_group_id
    FROM _distinct_phone_number AS ct
    JOIN lake_consolidated_view.ultra_merchant.address a
            ON RIGHT(ct.phone_number, 10) = a.phone_digits
    join iterable.public.customer_store_mapping csr on csr.project_id = ct.project_id
    join edw_prod.data_model.dim_store c_store on c_store.store_group_id = csr.store_group_id
    join edw_prod.data_model.dim_customer dc on dc.customer_id = a.customer_id
        and dc.store_id = c_store.store_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.phone_digits ORDER BY a.meta_update_datetime) = 1
);

CREATE OR REPLACE TEMP TABLE _orphan_contacts_membership_status AS (
    SELECT dc.ITBL_USER_ID,
        dc.CUSTOMER_ID,
        dc.FIRSTNAME,
        dc.LASTNAME,
        dc.EMAIL,
        dc.STORE_ID,
        dc.PHONE_NUMBER,
        dc.PHONE_DIGITS,
        dc.STORE_GROUP_ID,
        store.store_brand,
        m.membership_state
    FROM _distinct_customer dc
        JOIN edw_prod.data_model.dim_store store
        ON store.store_id = dc.store_id
        JOIN edw_prod.data_model.fact_membership_event m
        ON m.customer_id = dc.customer_id
            AND CONVERT_TIMEZONE('America/Los_Angeles',m.event_start_local_datetime)::DATETIME <= CURRENT_TIMESTAMP()
            AND CONVERT_TIMEZONE('America/Los_Angeles',m.event_end_local_datetime)::DATETIME > CURRENT_TIMESTAMP()
);

MERGE INTO iterable.public.orphan_contacts t
USING (
        SELECT
            itbl_user_id,
            edw_prod.stg.udf_unconcat_brand(customer_id) as bento_customer_id,
            firstname,
            lastname,
            email,
            store_id,
            store_group_id,
            phone_number,
            store_brand,
            membership_state,
            hash(*) AS meta_row_hash,
            CURRENT_TIMESTAMP AS meta_create_datetime,
            CURRENT_TIMESTAMP AS meta_update_datetime
        FROM _orphan_contacts_membership_status
) s ON equal_null(t.itbl_user_id, s.itbl_user_id)
    AND equal_null(t.bento_customer_id, s.bento_customer_id)
    AND equal_null(t.phone_number, s.phone_number)
WHEN NOT MATCHED THEN INSERT (
    itbl_user_id, bento_customer_id, firstname, lastname, email, store_id, store_group_id, phone_number, store_brand, membership_state, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    itbl_user_id, bento_customer_id, firstname, lastname, email, store_id, store_group_id, phone_number, store_brand, membership_state, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE
SET t.firstname = s.firstname,
    t.lastname = s.lastname,
    t.email = s.email,
    t.store_id = s.store_id,
    t.store_group_id = s.store_group_id,
    t.store_brand = s.store_brand,
    t.membership_state = s.membership_state,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime
;
