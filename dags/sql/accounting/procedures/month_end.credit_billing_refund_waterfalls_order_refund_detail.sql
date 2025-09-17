CREATE OR REPLACE TRANSIENT TABLE month_end.credit_billing_refund_waterfalls_order_refund_detail AS
SELECT rd.individual_bu,
       rd.payment_month,
       refund_month,
       refund_month_offset,
       rd.date_refunded,
       COALESCE(rd.date_refunded, rd.refund_month)     AS date_refunded_not_null,
       cash_collected,
       COALESCE(SUM(credit_billing_refund_as_cash), 0) AS credit_billing_refund_as_cash
FROM month_end.credit_billing_refund_waterfalls_refund_detail rd
         LEFT JOIN (SELECT individual_bu, payment_month, SUM(cash_collected) AS cash_collected
                    FROM month_end.credit_billing_refund_waterfalls_order_detail
                    GROUP BY 1, 2) od ON od.individual_bu = rd.individual_bu AND rd.payment_month = od.payment_month
GROUP BY 1, 2, 3, 4, 5, 6, 7;

ALTER TABLE month_end.credit_billing_refund_waterfalls_order_refund_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;
