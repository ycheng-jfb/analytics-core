TRUNCATE TABLE validation.edw_credit_equivalent_count;
INSERT INTO validation.edw_credit_equivalent_count
SELECT *
FROM (SELECT 'stg.dim_credit', COUNT(*) AS record_count
      FROM data_model.dim_credit
      WHERE credit_issued_equivalent_count IS NULL
      UNION ALL
      SELECT 'fact_credit_event', COUNT(*)
      FROM data_model.fact_credit_event
      WHERE credit_activity_equivalent_count IS NULL
      UNION ALL
      SELECT 'fact_order_credit', COUNT(*)
      FROM data_model.fact_order_credit
      WHERE (billed_cash_credit_redeemed_local_amount <> 0 AND
             ZEROIFNULL(billed_cash_credit_redeemed_equivalent_count) = 0)
         OR billed_cash_credit_redeemed_equivalent_count IS NULL)
WHERE record_count > 0;
