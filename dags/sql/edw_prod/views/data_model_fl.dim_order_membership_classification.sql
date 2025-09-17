CREATE OR REPLACE VIEW data_model_fl.dim_order_membership_classification
(
    order_membership_classification_key,
    membership_order_type_l1,
    membership_order_type_l2,
    membership_order_type_l3,
    membership_order_type_l4,
    first_repeat_order_type,
    is_vip,
    is_vip_membership_trial,
    meta_create_datetime,
    meta_update_datetime

)  AS
SELECT
       order_membership_classification_key,
       --is_activating,
       --is_guest,
       membership_order_type_l1,
       membership_order_type_l2,
       membership_order_type_l3,
       membership_order_type_l4,
       first_repeat_order_type,
       is_vip,
       is_vip_membership_trial,
       --is_repeat_customer,
       --effective_start_datetime,
       --effective_end_datetime,
       --is_current,
       meta_create_datetime,
       meta_update_datetime
FROM stg.dim_order_membership_classification
WHERE is_current;
