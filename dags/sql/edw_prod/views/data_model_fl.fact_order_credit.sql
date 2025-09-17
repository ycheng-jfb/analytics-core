CREATE OR REPLACE VIEW data_model_fl.fact_order_credit AS
SELECT stg.udf_unconcat_brand(order_id) AS order_id,
       credit_redeemed_local_amount,
       noncash_credit_redeemed_local_amount,
       cash_credit_redeemed_local_amount,
       IFNULL(other_cash_credit_sub_direct_gift_card_redeemed_local_amount,0)
           + IFNULL(other_cash_credit_sub_gift_card_redeemed_local_amount,0) AS cash_gift_card_redeemed_local_amount,
       billed_cash_credit_redeemed_local_amount,
       refund_cash_credit_redeemed_local_amount,
       other_cash_credit_redeemed_local_amount,
       billed_cash_credit_redeemed_same_month_local_amount,
       billed_cash_credit_redeemed_equivalent_count,
       billed_cash_credit_sub_credit_redeemed_local_amount,
       billed_cash_credit_sub_converted_to_token_redeemed_local_amount,
       billed_cash_credit_sub_converted_to_variable_redeemed_local_amount,
       billed_cash_credit_sub_giftco_roundtrip_redeemed_local_amount,
       billed_cash_credit_sub_token_redeemed_local_amount,
       other_cash_credit_sub_direct_gift_card_redeemed_local_amount,
       other_cash_credit_sub_gift_card_redeemed_local_amount,
       other_cash_credit_sub_legacy_credit_redeemed_local_amount,
       other_cash_credit_sub_other_credit_redeemed_local_amount,
       refund_cash_credit_sub_credit_redeemed_local_amount,
       refund_cash_credit_sub_token_redeemed_local_amount,
       noncash_credit_sub_credit_redeemed_local_amount,
       noncash_credit_sub_direct_gift_card_redeemed_local_amount,
       noncash_credit_sub_gift_card_redeemed_local_amount,
       noncash_credit_sub_token_redeemed_local_amount,
       meta_create_datetime,
       meta_update_datetime
FROM stg.fact_order_credit
WHERE NOT is_deleted
  AND is_test_customer = FALSE
  AND is_test_order = FALSE
  AND substring(order_id, -2) = '20';
