CREATE OR REPLACE TRANSIENT TABLE month_end.gaap_refund_waterfalls_full AS
SELECT o.*,
       r.refund_id,
       r.refund_total,
       r.monthoffset,
       r.refund_month,
       r.payment_method,
       CASE
           WHEN ROW_NUMBER() OVER (PARTITION BY o.order_id, o.refund_type ORDER BY o.order_id DESC, r.refund_month DESC, r.refund_id DESC) =
                1 THEN 1
           ELSE 0 END                                                              order_count,
       ZEROIFNULL(CASE
                      WHEN ROW_NUMBER() OVER (PARTITION BY o.order_id, o.refund_type ORDER BY o.order_id DESC, r.refund_month DESC, r.refund_id DESC) =
                           1
                          THEN o.total_cash_and_cash_credit
                      ELSE 0 END)                                                  order_total,
       ZEROIFNULL(rg.refund_group)                                                 refund_group,
       ZEROIFNULL(og.order_total_group)                                            order_total_group,
       ZEROIFNULL(og.order_count_group)                                            order_count_group,
       IFF(order_total_group = 0, 0, ZEROIFNULL(refund_group / order_total_group)) refund_percentage
FROM month_end.gaap_refund_waterfalls_order_detail o
         LEFT JOIN month_end.gaap_refund_waterfalls_refund_detail r
                   ON r.order_id = o.order_id
                       AND o.refund_type = r.refund_type
         LEFT JOIN (SELECT membership_store, refund_type, order_type, date_shipped, SUM(refund_total) refund_group
                    FROM month_end.gaap_refund_waterfalls_refund_detail
                    GROUP BY membership_store, refund_type, order_type, date_shipped) rg
                   ON rg.membership_store = o.membership_store
                       AND rg.refund_type = o.refund_type
                       AND rg.order_type = o.order_type
                       AND rg.date_shipped = o.date_shipped
         LEFT JOIN (SELECT membership_store,
                           refund_type,
                           order_type,
                           date_shipped,
                           SUM(total_cash_and_cash_credit) order_total_group,
                           COUNT(*)                        order_count_group
                    FROM month_end.gaap_refund_waterfalls_order_detail
                    GROUP BY membership_store, refund_type, order_type, date_shipped) og
                   ON og.membership_store = o.membership_store
                       AND og.refund_type = o.refund_type
                       AND og.order_type = o.order_type
                       AND og.date_shipped = o.date_shipped;

ALTER TABLE month_end.gaap_refund_waterfalls_full
    SET DATA_RETENTION_TIME_IN_DAYS = 0;
