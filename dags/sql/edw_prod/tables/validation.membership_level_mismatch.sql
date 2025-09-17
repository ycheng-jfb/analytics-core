CREATE OR REPLACE TRANSIENT TABLE validation.membership_level_mismatch
(
    customer_id          NUMBER(38, 0),
    edw_membership_level VARCHAR(50),
    src_membership_level VARCHAR(100)
);
