CREATE OR REPLACE TEMPORARY TABLE _shipped_order_metrics AS
SELECT *
FROM month_end.gaap_refund_waterfalls_order_breakout_detail;

--keeping the structure for the refund part
ALTER TABLE _shipped_order_metrics
DROP COLUMN individual_bu, STORE_ID, SUBTOTAL, discount, tax, shipping, credit, actual_ship_date;

------------------------------------------------------------------- MAKE SURE ALL REFUND TYPES SHOW UP

CREATE OR REPLACE TEMPORARY TABLE _refund_types
(
    refund_type VARCHAR(20)
);

INSERT INTO _refund_types
VALUES ('Cash Refund'),
       ('Store Credit Refund'),
       ('Token Refund');


CREATE OR REPLACE TRANSIENT TABLE month_end.gaap_refund_waterfalls_order_detail AS
SELECT DISTINCT s.*, r.*
FROM _shipped_order_metrics s
         LEFT OUTER JOIN (SELECT * FROM _refund_types) AS r;

ALTER TABLE month_end.gaap_refund_waterfalls_order_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO month_end.gaap_refund_waterfalls_order_detail_snapshot
(individual_bu,
 store_id,
 membership_store_id,
 membership_store,
 order_type,
 date_shipped,
 order_id,
 gross_rev_after_discount,
 total_cash_and_cash_credit,
 refund_type,
 snapshot_timestamp)
SELECT DISTINCT NULL                 individual_bu,
                NULL                 store_id,
                membership_store_id,
                membership_store,
                order_type,
                date_shipped,
                order_id,
                gross_rev_after_discount,
                total_cash_and_cash_credit,
                refund_type,
                CURRENT_TIMESTAMP AS snapshot_timestamp
FROM month_end.gaap_refund_waterfalls_order_detail;

DELETE
FROM month_end.gaap_refund_waterfalls_order_detail_snapshot
WHERE snapshot_timestamp < DATEADD(month, -18, GETDATE());
