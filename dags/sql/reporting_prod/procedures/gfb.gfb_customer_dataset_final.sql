set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


delete from REPORTING_PROD.GFB.customer_dataset a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.customer_dataset
select
a.customer_id
    ,a.month_date
    ,a.activation_local_date
    ,a.activation_local_datetime
    ,a.store_id
    ,a.region
    ,a.country
    ,a.store_type
    ,a.customer_gender
    ,a.customer_birth_year
    ,a.age
    ,a.cancel_local_date
    ,a.cancel_local_datetime
    ,a.cancel_type
    ,a.days_to_cancel
    ,a.churned_flag
    ,a.customer_state_province
    ,a.mobile_app_downloaded_flag
    ,a.how_did_you_hear
    ,a.how_did_you_hear_condensed
    ,a.lead_age_at_activation_in_days
    ,a.lead_age_at_activation_buckets
    ,a.reactivation_count
    ,a.APPAREL_SIZE
    ,a.APPAREL_CORE_SIZE_FLAG
    ,a.FOOTWEAR_SIZE
    ,a.FOOTWEAR_CORE_SIZE_FLAG
    ,a.activating_order_id
    ,a.activating_payment_method
    ,a.ACTIVATING_PROMO_OFFER
    ,a.activating_store_type

    --cycle action
    ,ca.SUCCESSFUL_BILLING_COUNT
    ,ca.FAILED_BILLING_COUNT
    ,ca.SKIP_COUNT
    ,ca.MERCH_PURCHASE_COUNT

    --email action
    ,ea.OPEN_COUNT
    ,ea.CLICK_COUNT
    ,ea.OPTOUT_COUNT
    ,ea.send_count AS email_count
    ,ea.days_between_optout_and_cancel

    --gms actions
    ,ga.GMS_CONTACT_FLAG
    ,ga.GMS_CONTACT_FLAG_RETENTION
    ,ga.gms_contact_flag_order_and_shipping
    ,ga.gms_contact_flag_general_questions
    ,ga.gms_contact_flag_other
    ,ga.gms_contact_flag_billing
    ,ga.gms_contact_flag_acCOUNT_maintenance
    ,ga.gms_contact_flag_skip_month
    ,ga.gms_contact_flag_how_membership_works
    ,ga.gms_contact_flag_returns
    ,ga.gms_contact_flag_website_issues
    ,ga.gms_contact_count

    --last order
    ,lo.LAST_ORDER_DATE
    ,lo.LAST_ORDER_COUNT
    ,lo.LAST_ORDER_UNITS
    ,lo.LAST_ORDER_REVENUE
    ,lo.LAST_ORDER_CREDIT_REDEMPTIONS
    ,lo.LAST_ORDER_SHIPPING_TIME
    ,lo.LAST_CREDIT_BILLING_DATE

    --loyalty action
    ,la.POINTS_EARNED
    ,la.POINTS_REDEEMED
    ,la.POINTS_EXPIRED
    ,la.POINTS_AVAILABLE
    ,la.LAST_REDEEMED_POINTS_DATE

    --misc action
    ,ma.PDP_VIEWS
    ,ma.IS_FAVORITES
    ,ma.FAVORITES_COUNT

    --preorder action
    ,pa.PRE_ORDER_COUNT

    --product purchase
    ,pp.ORDER_COUNT
    ,pp.UNIT_SALES
    ,pp.SUBTOTAL
    ,pp.DISCOUNT
    ,pp.cash_collected_count
    ,pp.PRODUCT_REVENUE_EXCL_SHIPPING
    ,pp.PRODUCT_COST
    ,pp.AOV_EXCL_SHIPPING
    ,pp.upt
    ,pp.NON_ACTIVATING_AOV_EXCL_SHIPPING
    ,pp.NON_ACTIVATING_UPT
    ,pp.AUR_EXCL_SHIPPING
    ,pp.DISCOUNT_RATE
    ,pp.IS_SHOPPED_DRESS
    ,pp.IS_SHOPPED_TOPS
    ,pp.IS_SHOPPED_PUMPS
    ,pp.IS_SHOPPED_ANKLE_BOOTS
    ,pp.IS_SHOPPED_FLAT_BOOTS
    ,pp.IS_SHOPPED_HEELED_SANDALS
    ,pp.IS_SHOPPED_SANDALS_DRESSY
    ,pp.IS_SHOPPED_JACKETS
    ,pp.IS_SHOPPED_BOOTIES
    ,pp.IS_SHOPPED_HEELED_BOOTS
    ,pp.IS_SHOPPED_FOOTWEAR
    ,pp.IS_SHOPPED_APPAREL
    ,pp.IS_SHOPPED_HANDBAGS
    ,pp.IS_SHOPPED_ACCESSORIES
    ,pp.repeat_product_revenue_excl_shipping
    ,pp.repeat_order_count
    ,pp.repeat_qty_sold
    ,pp.cash_collected_amount
    ,pp.items_returned
    ,pp.activating_product_revenue_excl_shipping
    ,pp.activating_qty_sold
    ,pp.activating_discount
    ,pp.discount_excl_activation
    ,pp.total_exchange_units
    ,pp.activating_exchange_units
    ,pp.repeat_exchange_units

    --product review
    ,pr.REVIEW_COUNT
    ,pr.REVIEW_AVG_RECOMMENDED_SCORE
    ,pr.REVIEW_AVG_STYLE_SCORE
    ,pr.REVIEW_AVG_COMFORT_SCORE
    ,pr.REVIEW_AVG_QUALITY_VALUE_SCORE
    ,pr.REVIEW_AVG_SIZE_FIT_SCORE
    ,pr.REVIEW_AVG_NPS_SCORE
    ,pr.REVIEW_MIN_RECOMMENDED_SCORE
    ,pr.REVIEW_MIN_STYLE_SCORE
    ,pr.REVIEW_MIN_COMFORT_SCORE
    ,pr.REVIEW_MIN_QUALITY_VALUE_SCORE
    ,pr.REVIEW_MIN_SIZE_FIT_SCORE
    ,pr.REVIEW_MIN_NPS_SCORE
    ,pr.IS_LEFT_REVIEW

    --promo usage
    ,pu.promo_orders
    ,pu.repeat_promo_orders
    ,pu.orders
    ,pu.repeat_orders
    ,pu.PERCENT_ORDERS_WITH_PROMO
    ,pu.PERCENT_REPEAT_ORDERS_WITH_PROMO

    --psource
    ,ps.PSOURCE_COUNT
    ,ps.PSOURCE_DISTINCT_COUNT
    ,ps.AVG_PSOURCES_PER_ORDER
    ,ps.AVG_DISTINCT_PSOURCES_PER_ORDER

    --sms
    ,sms.OPTIN_STATUS
    ,sms.MESSAGE_LINK_COUNT
    ,sms.CLICK_COUNT AS SMS_CLICK_COUNT
from REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE a
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_CYCLE_ACTION ca
    on ca.CUSTOMER_ID = a.CUSTOMER_ID
    and ca.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_EMAIL_ACTION ea
    on ea.CUSTOMER_ID = a.CUSTOMER_ID
    and ea.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_GMS_ACTIONS ga
    on ga.CUSTOMER_ID = a.CUSTOMER_ID
    and ga.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_LAST_ORDER lo
    on lo.CUSTOMER_ID = a.CUSTOMER_ID
    and lo.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_LOYALTY_ACTION la
    on la.CUSTOMER_ID = a.CUSTOMER_ID
    and la.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_MISC_ACTION ma
    on ma.CUSTOMER_ID = a.CUSTOMER_ID
    and ma.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_PREORDER_ACTION pa
    on pa.CUSTOMER_ID = a.CUSTOMER_ID
    and pa.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_PRODUCT_PURCHASES pp
    on pp.CUSTOMER_ID = a.CUSTOMER_ID
    and pp.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_PRODUCT_REVIEW pr
    on pr.CUSTOMER_ID = a.CUSTOMER_ID
    and pr.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_PROMO_USAGE pu
    on pu.CUSTOMER_ID = a.CUSTOMER_ID
    and pu.MONTH_DATE = a.MONTH_DATE
left join REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_PSOURCES ps
    on ps.CUSTOMER_ID = a.CUSTOMER_ID
    and ps.MONTH_DATE = a.MONTH_DATE
left join reporting_PROD.gfb.customer_dataset_sms_engagement sms
    on sms.CUSTOMER_ID = a.CUSTOMER_ID
    and sms.MONTH_DATE = a.MONTH_DATE
where
    a.MONTH_DATE >= $low_watermark_ltz;
