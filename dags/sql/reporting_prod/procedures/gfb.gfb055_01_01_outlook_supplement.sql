set outlook_date = (select max(date) from REPORTING_PROD.GFB.gfb055_01_outlook_metrics);
set forecast_budget_month = (select max(month) from REPORTING_PROD.GFB.gfb055_01_outlook_metrics);

CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb055_01_01_outlook_supplement as
select
    a.BUSINESS_UNIT
    ,$outlook_date as outlook_date
    ,$forecast_budget_month as forecast_budget_month

    --cash topline
    ,a.CREDIT_BILLING
    + a.ACTIVATING_PRODUCT_REVENUE
    + a.REPEAT_PRODUCT_REVENUE
    - a.CREDIT_REDEEMED
    - a.REFUND_CREDIT_REDEEMED_AMOUNT as gross_cash_revenue
    ,a.chargeback as chargebacks
    ,gross_cash_revenue - chargebacks - a.CASH_NET_REVENUE as cash_refund
    ,a.CASH_NET_REVENUE
    ,(cash_refund + chargebacks)/gross_cash_revenue as refund_chargeback_as_percent_of_gross_cash
    ,a.CASH_GROSS_MARGIN_AMOUNT
    ,a.CASH_GROSS_MARGIN_PERCENT
    ,a.MEDIA_SPEND
    ,a.CONTRIBUTION_AFTER_MEDIA

    --Customer Acquisition
    ,a.CPL
    ,a.TOTAL_VIP_CAC as vip_cpa
    ,a.M1_LEAD_TO_VIP_PERCENT
    ,a.M1_VIP
    ,a.MEDIA_SPEND/a.TOTAL_VIP_CAC - a.M1_VIP as aged_lead_conversion
    ,a.NEW_VIP
    ,a.CANCELS
    ,a.NEW_VIP - a.CANCELS as vip_net_change
    ,a.EOM_VIP_COUNT
    ,vip_net_change/a.EOM_VIP_COUNT as growth_percent

    --Activating Orders
    ,a.ACTIVATING_PRODUCT_REVENUE
    ,a.ACTIVATING_CASH_NET_REVENUE as activating_gaap_net_revenue
    ,a.ACTIVATING_ORDER_COUNT
    ,a.ACTIVATING_AOV_INCL_SHIPPING
    ,a.ACTIVATING_UPT
    ,a.ACTIVATING_DISCOUNT_PERCENT
    ,a.ACTIVATING_PRODUCT_REVENUE * a.ACTIVATING_PRODUCT_MARGIN_PERCENT as activating_product_margin
    ,a.ACTIVATING_PRODUCT_MARGIN_PERCENT

    --Repeat Orders
    ,a.REPEAT_PRODUCT_REVENUE
    ,a.REPEAT_GAAP_NET_REVENUE
    ,a.REPEAT_ORDER_COUNT
    ,a.REPEAT_AOV_INCL_SHIPPING
    ,a.REPEAT_UPT
    ,a.REPEAT_DISCOUNT_PERCENT
    ,a.REPEAT_PRODUCT_REVENUE * a.REPEAT_PRODUCT_MARGIN_PERCENT as repeat_product_margin
    ,a.REPEAT_PRODUCT_MARGIN_PERCENT
    ,a.REPEAT_CASH_NET_REVENUE
    ,a.REPEAT_CASH_GROSS_MARGIN_AMOUNT
    ,a.REPEAT_GROSS_MARGIN_PERCENT

    --Deferred Revenue
    ,a.CREDIT_BILLING
    ,a.CREDIT_REDEEMED
    ,a.CREDIT_REFUND
    ,a.NET_UNREDEEMED_CREDIT_BILLING
    ,a.NET_UNREDEEMED_CREDIT_BILLING_PERCENT
    ,a.REFUND_AS_CREDIT_AMOUNT
    ,a.REFUND_CREDIT_REDEEMED_AMOUNT
    ,a.REFUND_AS_CREDIT_AMOUNT - a.REFUND_CREDIT_REDEEMED_AMOUNT as net_unredeemed_refund_credit
    ,net_unredeemed_refund_credit + a.NET_UNREDEEMED_CREDIT_BILLING as deferred_revenue
    ,a.BOP_VIPS
    ,a.CREDIT_BILLING_COUNT/a.BOP_VIPS as credit_billing_rate
    ,a.CREDIT_REDEEMED/a.CREDIT_BILLING as credit_redeemed_percent
    ,a.CREDIT_REFUND/a.CREDIT_BILLING as credit_cancel_percent
from REPORTING_PROD.GFB.gfb055_01_outlook_metrics a
where
    a.MONTH = $forecast_budget_month
    and a.DATE = $outlook_date;
