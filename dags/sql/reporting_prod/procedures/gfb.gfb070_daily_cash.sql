create or replace transient table REPORTING_PROD.GFB.gfb070_daily_cash as
select distinct
    (case
        when a.REPORT_MAPPING = 'JF-TREV-UK' then 'JF UK'
        when a.REPORT_MAPPING = 'JF-TREV-DK' then 'JF DK'
        when a.REPORT_MAPPING = 'JF-TREV-CA' then 'JF CA'
        when a.REPORT_MAPPING = 'SD-TREV-NA' then 'SD'
        when a.REPORT_MAPPING = 'JF-TREV-FR' then 'JF FR'
        when a.REPORT_MAPPING = 'JF-TREV-US' then 'JF US'
        when a.REPORT_MAPPING = 'JF-TREV-EU' then 'JF EU'
        when a.REPORT_MAPPING = 'JF-TREV-DE' then 'JF DE'
        when a.REPORT_MAPPING = 'JF-TREV-ES' then 'JF ES'
        when a.REPORT_MAPPING = 'JF-TREV-IT' then 'JF IT'
        when a.REPORT_MAPPING = 'JF-TREV-NA' then 'JF NA'
        when a.REPORT_MAPPING = 'JF-TREV-NL' then 'JF NL'
        when a.REPORT_MAPPING = 'JF-TREV-SE' then 'JF SE'
        when a.REPORT_MAPPING = 'FK-TREV-NA' then 'FK'
        when a.REPORT_MAPPING = 'JFSD-TREV-NA' then 'JFSH NA'
        when a.REPORT_MAPPING = 'JFB-TREV-NA' then 'JFB NA'
        when a.REPORT_MAPPING = 'JFB-TREV-Global' then 'JFB Global'
        end ) as store
    , a.report_mapping
    ,a.DATE

    ,a.PRODUCT_NET_REVENUE as total_product_net_revenue
    ,a.ACTIVATING_PRODUCT_NET_REVENUE
    ,a.NONACTIVATING_PRODUCT_NET_REVENUE repeat_product_net_revenue

    ,a.PRODUCT_GROSS_REVENUE_EXCL_SHIPPING as total_product_revenue_excl_shipping
    ,a.PRODUCT_ORDER_COUNT as total_product_order_count
    ,a.ACTIVATING_PRODUCT_GROSS_REVENUE_EXCL_SHIPPING as activating_product_revenue_excl_shipping
    ,a.ACTIVATING_PRODUCT_ORDER_COUNT
    ,a.NONACTIVATING_PRODUCT_GROSS_REVENUE_EXCL_SHIPPING as repeat_product_revenue_excl_shipping
    ,a.NONACTIVATING_PRODUCT_ORDER_COUNT as repeat_product_order_count

    ,a.UNIT_COUNT as total_product_unit_count
    ,a.ACTIVATING_UNIT_COUNT as activating_product_unit_count
    ,a.NONACTIVATING_UNIT_COUNT as repeat_product_unit_count

    ,a.PRODUCT_ORDER_PRODUCT_DISCOUNT_AMOUNT as total_product_discount_amount
    ,a.ACTIVATING_PRODUCT_ORDER_PRODUCT_DISCOUNT_AMOUNT as activating_product_discount_amount
    ,a.NONACTIVATING_PRODUCT_ORDER_PRODUCT_DISCOUNT_AMOUNT as repeat_product_discount_amount
    ,a.PRODUCT_ORDER_PRODUCT_SUBTOTAL_AMOUNT as total_product_subtotal_amount
    ,a.ACTIVATING_PRODUCT_ORDER_PRODUCT_SUBTOTAL_AMOUNT as activating_product_subtotal_amount
    ,a.NONACTIVATING_PRODUCT_ORDER_PRODUCT_SUBTOTAL_AMOUNT as repeat_product_subtotal

    ,a.PRODUCT_GROSS_PROFIT as total_product_gross_margin
    ,a.ACTIVATING_PRODUCT_GROSS_PROFIT as activating_product_gross_margin
    ,a.NONACTIVATING_PRODUCT_GROSS_PROFIT as repeat_product_gross_margin

    ,a.CASH_GROSS_PROFIT as total_cash_gross_margin
    ,a.NONACTIVATING_CASH_GROSS_PROFIT as repeat_cash_gross_margin
    ,a.ACTIVATING_PRODUCT_GROSS_PROFIT as activating_cash_gross_margin

    ,a.CASH_NET_REVENUE as total_cash_net_revenue
    ,a.ACTIVATING_CASH_NET_REVENUE
    ,a.NONACTIVATING_CASH_NET_REVENUE as repeat_cash_net_revenue

    ,a.BILLING_CASH_GROSS_REVENUE as credit_billing_amount
    ,a.BILLING_CASH_CHARGEBACK_AMOUNT + a.BILLING_CASH_REFUND_AMOUNT as credit_billing_cancelled_amount
    ,a.BILLED_CASH_CREDIT_REDEEMED_AMOUNT as credit_billing_redeemed_amount
    ,credit_billing_amount - credit_billing_cancelled_amount - credit_billing_redeemed_amount as credit_billing_net_amount
    ,a.BILLING_ORDER_TRANSACTION_COUNT as credit_billing_count
from EDW_PROD.REPORTING.DAILY_CASH_FINAL_OUTPUT a
where
    a.REPORT_MAPPING ILIKE any ('JF%', 'SD%', 'FK%')
    and a.REPORT_MAPPING like '%TREV%'
    and a.CURRENCY_TYPE = 'USD'
    and a.DATE_OBJECT = 'placed'
    and a.REPORT_MAPPING not in (
                                    'FK-TREV-CA-US'
                                    ,'JF-TREV-AT-DE'
                                    ,'JF-TREV-BE-FR'
                                    ,'JF-TREV-BE-NL'
                                    ,'SD-TREV-CA-US'
                                )
