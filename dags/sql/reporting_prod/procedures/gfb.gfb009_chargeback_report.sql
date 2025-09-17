set start_date = cast('2018-01-01' as date);
set end_date = current_date();


--final_order_info
create or replace temporary table _final_order_info as
SELECT
	ot.BUSINESS_UNIT
    ,ot.REGION
    ,ot.COUNTRY
	,date_trunc(month, ot.SHIP_DATE) as month_date
    ,ot.SHIP_DATE as date
	,(case
	    when ot.creditcard_type in ('Master Card', 'Ap_Mastercard') then 'Master Card'
	    when ot.creditcard_type in ('Ap_Visa', 'Visa') then 'Visa'
	    when ot.creditcard_type in ('Amex', 'Ap_Amex') then 'Amex'
	    when ot.creditcard_type in ('Paypal', 'Braintreepaypal') then 'Paypal'
	    when ot.creditcard_type in ('Discover', 'Ap_Discover') then 'Discover'
	    when ot.creditcard_type in ('Not Applicable') then 'Unknown'
	    else 'Others' end) as creditcard_type
	,(case
	    when fap.ACTIVATING_PAYMENT_METHOD = 'ppcc'
	    then 1 else 0 end) as ppcc_activating_flag
	,datediff(month, fap.FIRST_ACTIVATING_COHORT, date_trunc(month, ot.SHIP_DATE)) + 1 as tenure
	,fap.FIRST_ACTIVATING_COHORT
	,fap.MEMBERSHIP_PRICE

	--metrics
	--order count
	,count(distinct CASE
			WHEN ot.order_classification = 'product order'
			AND ot.ORDER_TYPE = 'vip activating' THEN ot.ORDER_ID
			END) AS activating_orders

	,count(distinct CASE
			WHEN ot.order_classification = 'product order'
			AND ot.ORDER_TYPE != 'vip activating'
			THEN ot.ORDER_ID END) AS repeat_ecomm_orders

	,count(distinct CASE
			WHEN ot.order_classification = 'credit billing'
			THEN ot.ORDER_ID END) AS credit_billing_transactions

	--order amount
	,SUM(CASE
			WHEN ot.order_classification = 'product order'
			AND ot.ORDER_TYPE = 'vip activating' THEN COALESCE(ot.TOTAL_PRODUCT_REVENUE - ot.TOTAL_NON_CASH_CREDIT_AMOUNT - ot.TOTAL_CASH_CREDIT_AMOUNT + ot.TOTAL_SHIPPING_REVENUE, 0)
			ELSE 0 END) AS activating_orders_cash_amount

	,SUM(CASE
			WHEN ot.order_classification = 'product order'
			AND ot.ORDER_TYPE != 'vip activating'
			THEN COALESCE(ot.TOTAL_PRODUCT_REVENUE - ot.TOTAL_NON_CASH_CREDIT_AMOUNT - ot.TOTAL_CASH_CREDIT_AMOUNT + ot.TOTAL_SHIPPING_REVENUE, 0)
			ELSE 0 END) AS repeat_ecomm_orders_cash_amount

	,SUM(CASE
			WHEN ot.order_classification = 'credit billing'
			THEN COALESCE(ot.TOTAL_PRODUCT_REVENUE - ot.TOTAL_NON_CASH_CREDIT_AMOUNT - ot.TOTAL_CASH_CREDIT_AMOUNT + ot.TOTAL_SHIPPING_REVENUE, 0)
			ELSE 0 END) AS credit_billing_transactions_cash_amount
FROM reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_SHIP_DATE ot
left join reporting_prod.GFB.GFB_DIM_VIP fap
    on fap.CUSTOMER_ID = ot.CUSTOMER_ID
WHERE
	ot.SHIP_DATE >= $start_date
	and ot.SHIP_DATE < $end_date
GROUP BY
	ot.BUSINESS_UNIT
    ,ot.REGION
    ,ot.COUNTRY
	,date_trunc(month, ot.SHIP_DATE)
    ,ot.SHIP_DATE
	,(case
	    when ot.creditcard_type in ('Master Card', 'Ap_Mastercard') then 'Master Card'
	    when ot.creditcard_type in ('Ap_Visa', 'Visa') then 'Visa'
	    when ot.creditcard_type in ('Amex', 'Ap_Amex') then 'Amex'
	    when ot.creditcard_type in ('Paypal', 'Braintreepaypal') then 'Paypal'
	    when ot.creditcard_type in ('Discover', 'Ap_Discover') then 'Discover'
	    when ot.creditcard_type in ('Not Applicable') then 'Unknown'
	    else 'Others' end)
	,(case
	    when fap.ACTIVATING_PAYMENT_METHOD = 'ppcc'
	    then 1 else 0 end)
	,datediff(month, fap.FIRST_ACTIVATING_COHORT, date_trunc(month, ot.SHIP_DATE)) + 1
    ,fap.FIRST_ACTIVATING_COHORT
	,fap.MEMBERSHIP_PRICE;


--final chargeback info
create or replace temporary table _final_chargeback_info as
SELECT
	cb.BUSINESS_UNIT
    ,cb.REGION
    ,cb.COUNTRY
	,date_trunc(month, cb.CHARGEBACK_DATE) as month_date
    ,cb.CHARGEBACK_DATE as date
	,(case
	    when cb.creditcard_type in ('Master Card', 'Ap_Mastercard') then 'Master Card'
	    when cb.creditcard_type in ('Ap_Visa', 'Visa') then 'Visa'
	    when cb.creditcard_type in ('Amex', 'Ap_Amex') then 'Amex'
	    when cb.creditcard_type in ('Paypal', 'Braintreepaypal') then 'Paypal'
	    when cb.creditcard_type in ('Discover', 'Ap_Discover') then 'Discover'
	    when cb.creditcard_type in ('Not Applicable') then 'Unknown'
	    else 'Others' end) as creditcard_type
	,(case
	    when fap.ACTIVATING_PAYMENT_METHOD = 'ppcc'
	    then 1 else 0 end) as ppcc_activating_flag
	,datediff(month, fap.FIRST_ACTIVATING_COHORT, date_trunc(month, cb.CHARGEBACK_DATE)) + 1 as tenure
	,fap.FIRST_ACTIVATING_COHORT
	,fap.MEMBERSHIP_PRICE

	--metrics
	--chargeback count
	,count(distinct CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE = 'vip activating' THEN cb.ORDER_ID
			END) AS chargeback_activating_orders

	,count(distinct CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE != 'vip activating'
			THEN cb.ORDER_ID END) AS chargeback_repeat_ecomm_orders

	,count(distinct CASE
			WHEN cb.order_classification = 'credit billing'
			THEN cb.ORDER_ID END) AS chargeback_credit_billing_transactions

	--chargeback amount
	,SUM(CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE = 'vip activating'
			THEN COALESCE(cb.TOTAL_CHARGEBACK_AMOUNT, 0)
			ELSE 0
			END) AS chargeback_activating_orders_cash_amount

	,SUM(CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE != 'vip activating'
			THEN COALESCE(cb.TOTAL_CHARGEBACK_AMOUNT, 0)
			ELSE 0
			END) AS chargeback_repeat_ecomm_orders_cash_amount

	,SUM(CASE
			WHEN cb.order_classification = 'credit billing'
			THEN COALESCE(cb.TOTAL_CHARGEBACK_AMOUNT, 0)
			ELSE 0
			END) AS chargeback_credit_billing_transactions_cash_amount
FROM reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_SHIP_DATE cb
left join reporting_prod.GFB.GFB_DIM_VIP fap
    on fap.CUSTOMER_ID = cb.CUSTOMER_ID
WHERE
	cb.CHARGEBACK_DATE >= $start_date
	and cb.CHARGEBACK_DATE < $end_date
GROUP BY
	cb.BUSINESS_UNIT
    ,cb.REGION
    ,cb.COUNTRY
	,date_trunc(month, cb.CHARGEBACK_DATE)
    ,cb.CHARGEBACK_DATE
	,(case
	    when cb.creditcard_type in ('Master Card', 'Ap_Mastercard') then 'Master Card'
	    when cb.creditcard_type in ('Ap_Visa', 'Visa') then 'Visa'
	    when cb.creditcard_type in ('Amex', 'Ap_Amex') then 'Amex'
	    when cb.creditcard_type in ('Paypal', 'Braintreepaypal') then 'Paypal'
	    when cb.creditcard_type in ('Discover', 'Ap_Discover') then 'Discover'
	    when cb.creditcard_type in ('Not Applicable') then 'Unknown'
	    else 'Others' end)
	,(case
	    when fap.ACTIVATING_PAYMENT_METHOD = 'ppcc'
	    then 1 else 0 end)
	,datediff(month, fap.FIRST_ACTIVATING_COHORT, date_trunc(month, cb.CHARGEBACK_DATE)) + 1
    ,fap.FIRST_ACTIVATING_COHORT
	,fap.MEMBERSHIP_PRICE;


--final refund info
create or replace temporary table _final_refund_info as
SELECT
	cb.BUSINESS_UNIT
    ,cb.REGION
    ,cb.COUNTRY
	,date_trunc(month, cb.REFUND_DATE) as month_date
    ,cb.REFUND_DATE as date
	,(case
	    when cb.creditcard_type in ('Master Card', 'Ap_Mastercard') then 'Master Card'
	    when cb.creditcard_type in ('Ap_Visa', 'Visa') then 'Visa'
	    when cb.creditcard_type in ('Amex', 'Ap_Amex') then 'Amex'
	    when cb.creditcard_type in ('Paypal', 'Braintreepaypal') then 'Paypal'
	    when cb.creditcard_type in ('Discover', 'Ap_Discover') then 'Discover'
	    when cb.creditcard_type in ('Not Applicable') then 'Unknown'
	    else 'Others' end) as creditcard_type
	,(case
	    when fap.ACTIVATING_PAYMENT_METHOD = 'ppcc'
	    then 1 else 0 end) as ppcc_activating_flag
	,datediff(month, fap.FIRST_ACTIVATING_COHORT, date_trunc(month, cb.REFUND_DATE)) + 1 as tenure
	,fap.FIRST_ACTIVATING_COHORT
	,fap.MEMBERSHIP_PRICE

	--metrics
	--refund count
	,count(distinct CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE = 'vip activating' THEN cb.ORDER_ID
			END) AS refund_activating_orders

	,count(distinct CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE != 'vip activating'
			THEN cb.ORDER_ID END) AS refund_repeat_ecomm_orders

	,count(distinct CASE
			WHEN cb.order_classification = 'credit billing'
			THEN cb.ORDER_ID END) AS refund_credit_billing_transactions

	--refund amount
	,SUM(CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE = 'vip activating'
			THEN COALESCE(cb.TOTAL_REFUND_CASH_AMOUNT, 0)
			ELSE 0
			END) AS refund_activating_orders_cash_amount

	,SUM(CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE != 'vip activating'
			THEN COALESCE(cb.TOTAL_REFUND_CASH_AMOUNT, 0)
			ELSE 0
			END) AS refund_repeat_ecomm_orders_cash_amount

	,SUM(CASE
			WHEN cb.order_classification = 'credit billing'
			THEN COALESCE(cb.TOTAL_REFUND_CASH_AMOUNT, 0)
			ELSE 0
			END) AS refund_credit_billing_transactions_cash_amount
    ,SUM(CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE = 'vip activating'
			THEN COALESCE(cb.TOTAL_REFUND_CREDIT_AMOUNT, 0)
			ELSE 0
			END) AS refund_activating_orders_credit_amount

	,SUM(CASE
			WHEN cb.order_classification = 'product order'
			AND cb.ORDER_TYPE != 'vip activating'
			THEN COALESCE(cb.TOTAL_REFUND_CREDIT_AMOUNT, 0)
			ELSE 0
			END) AS refund_repeat_ecomm_orders_credit_amount

	,SUM(CASE
			WHEN cb.order_classification = 'credit billing'
			THEN COALESCE(cb.TOTAL_REFUND_CREDIT_AMOUNT, 0)
			ELSE 0
			END) AS refund_credit_billing_transactions_credit_amount
FROM reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_SHIP_DATE cb
left join reporting_prod.GFB.GFB_DIM_VIP fap
    on fap.CUSTOMER_ID = cb.CUSTOMER_ID
WHERE
	cb.REFUND_DATE >= $start_date
	and cb.REFUND_DATE < $end_date
GROUP BY
	cb.BUSINESS_UNIT
    ,cb.REGION
    ,cb.COUNTRY
	,date_trunc(month, cb.REFUND_DATE)
    ,cb.REFUND_DATE
	,(case
	    when cb.creditcard_type in ('Master Card', 'Ap_Mastercard') then 'Master Card'
	    when cb.creditcard_type in ('Ap_Visa', 'Visa') then 'Visa'
	    when cb.creditcard_type in ('Amex', 'Ap_Amex') then 'Amex'
	    when cb.creditcard_type in ('Paypal', 'Braintreepaypal') then 'Paypal'
	    when cb.creditcard_type in ('Discover', 'Ap_Discover') then 'Discover'
	    when cb.creditcard_type in ('Not Applicable') then 'Unknown'
	    else 'Others' end)
	,(case
	    when fap.ACTIVATING_PAYMENT_METHOD = 'ppcc'
	    then 1 else 0 end)
	,datediff(month, fap.FIRST_ACTIVATING_COHORT, date_trunc(month, cb.REFUND_DATE)) + 1
    ,fap.FIRST_ACTIVATING_COHORT
	,fap.MEMBERSHIP_PRICE;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.GFB.gfb009_chargeback_report as
SELECT
	foi.BUSINESS_UNIT
    ,foi.REGION
    ,foi.COUNTRY
	,foi.month_date
	,'ALL' AS credit_card_type
	,foi.ppcc_activating_flag
	,foi.tenure
	,foi.date
	,foi.FIRST_ACTIVATING_COHORT
	,foi.MEMBERSHIP_PRICE

	--order count
	,SUM(foi.activating_orders) AS activating_orders
	,SUM(fci.chargeback_activating_orders) AS chargeback_activating_orders
	,SUM(foi.repeat_ecomm_orders) AS repeat_ecomm_orders
	,SUM(fci.chargeback_repeat_ecomm_orders) AS chargeback_repeat_ecomm_orders
	,SUM(foi.credit_billing_transactions) AS credit_billing_transactions
	,SUM(fci.chargeback_credit_billing_transactions) AS chargeback_credit_billing_transactions
	,sum(fri.refund_activating_orders) as refund_activating_orders
	,sum(fri.refund_repeat_ecomm_orders) as refund_repeat_ecomm_orders
	,sum(fri.refund_credit_billing_transactions) as refund_credit_billing_transactions

	--amount
	,SUM(foi.activating_orders_cash_amount) AS activating_orders_cash_amount
	,SUM(fci.chargeback_activating_orders_cash_amount) AS chargeback_activating_orders_cash_amount
	,SUM(foi.repeat_ecomm_orders_cash_amount) AS repeat_ecomm_orders_cash_amount
	,SUM(fci.chargeback_repeat_ecomm_orders_cash_amount) AS chargeback_repeat_ecomm_orders_cash_amount
	,SUM(foi.credit_billing_transactions_cash_amount) AS credit_billing_transactions_cash_amount
	,SUM(fci.chargeback_credit_billing_transactions_cash_amount) AS chargeback_credit_billing_transactions_cash_amount
    ,sum(fri.refund_activating_orders_cash_amount) as refund_activating_orders_cash_amount
	,sum(fri.refund_repeat_ecomm_orders_cash_amount) as refund_repeat_ecomm_orders_cash_amount
	,sum(fri.refund_credit_billing_transactions_cash_amount) as refund_credit_billing_transactions_cash_amount
    ,sum(fri.refund_activating_orders_credit_amount) as refund_activating_orders_credit_amount
	,sum(fri.refund_repeat_ecomm_orders_credit_amount) as refund_repeat_ecomm_orders_credit_amount
	,sum(fri.refund_credit_billing_transactions_credit_amount) as refund_credit_billing_transactions_credit_amount
FROM _final_order_info foi
LEFT JOIN _final_chargeback_info fci
	ON fci.BUSINESS_UNIT = foi.BUSINESS_UNIT
	and fci.REGION = foi.REGION
	and fci.COUNTRY = foi.COUNTRY
	AND fci.month_date = foi.month_date
	AND fci.creditcard_type = foi.creditcard_type
	and fci.ppcc_activating_flag = foi.ppcc_activating_flag
	and fci.tenure = foi.tenure
    and fci.date = foi.date
    and fci.FIRST_ACTIVATING_COHORT = foi.FIRST_ACTIVATING_COHORT
    and fci.MEMBERSHIP_PRICE = foi.MEMBERSHIP_PRICE
left join _final_refund_info fri
    on fri.BUSINESS_UNIT = foi.BUSINESS_UNIT
	and fri.REGION = foi.REGION
	and fri.COUNTRY = foi.COUNTRY
	AND fri.month_date = foi.month_date
	AND fri.creditcard_type = foi.creditcard_type
	and fri.ppcc_activating_flag = foi.ppcc_activating_flag
	and fri.tenure = foi.tenure
    and fri.date = foi.date
    and fri.FIRST_ACTIVATING_COHORT = foi.FIRST_ACTIVATING_COHORT
    and fri.MEMBERSHIP_PRICE = foi.MEMBERSHIP_PRICE
GROUP BY
	foi.BUSINESS_UNIT
    ,foi.REGION
    ,foi.COUNTRY
	,foi.month_date
	,foi.ppcc_activating_flag
	,foi.tenure
    ,foi.date
	,foi.FIRST_ACTIVATING_COHORT
	,foi.MEMBERSHIP_PRICE

UNION

SELECT
	foi.BUSINESS_UNIT
    ,foi.REGION
    ,foi.COUNTRY
	,foi.month_date
	,foi.creditcard_type as credit_card_type
	,foi.ppcc_activating_flag
	,foi.tenure
	,foi.date
	,foi.FIRST_ACTIVATING_COHORT
	,foi.MEMBERSHIP_PRICE

	--order count
	,SUM(foi.activating_orders) AS activating_orders
	,SUM(fci.chargeback_activating_orders) AS chargeback_activating_orders
	,SUM(foi.repeat_ecomm_orders) AS repeat_ecomm_orders
	,SUM(fci.chargeback_repeat_ecomm_orders) AS chargeback_repeat_ecomm_orders
	,SUM(foi.credit_billing_transactions) AS credit_billing_transactions
	,SUM(fci.chargeback_credit_billing_transactions) AS chargeback_credit_billing_transactions
	,sum(fri.refund_activating_orders) as refund_activating_orders
	,sum(fri.refund_repeat_ecomm_orders) as refund_repeat_ecomm_orders
	,sum(fri.refund_credit_billing_transactions) as refund_credit_billing_transactions

	--amount
	,SUM(foi.activating_orders_cash_amount) AS activating_orders_cash_amount
	,SUM(fci.chargeback_activating_orders_cash_amount) AS chargeback_activating_orders_cash_amount
	,SUM(foi.repeat_ecomm_orders_cash_amount) AS repeat_ecomm_orders_cash_amount
	,SUM(fci.chargeback_repeat_ecomm_orders_cash_amount) AS chargeback_repeat_ecomm_orders_cash_amount
	,SUM(foi.credit_billing_transactions_cash_amount) AS credit_billing_transactions_cash_amount
	,SUM(fci.chargeback_credit_billing_transactions_cash_amount) AS chargeback_credit_billing_transactions_cash_amount
    ,sum(fri.refund_activating_orders_cash_amount) as refund_activating_orders_cash_amount
	,sum(fri.refund_repeat_ecomm_orders_cash_amount) as refund_repeat_ecomm_orders_cash_amount
	,sum(fri.refund_credit_billing_transactions_cash_amount) as refund_credit_billing_transactions_cash_amount
    ,sum(fri.refund_activating_orders_credit_amount) as refund_activating_orders_credit_amount
	,sum(fri.refund_repeat_ecomm_orders_credit_amount) as refund_repeat_ecomm_orders_credit_amount
	,sum(fri.refund_credit_billing_transactions_credit_amount) as refund_credit_billing_transactions_credit_amount
FROM _final_order_info foi
LEFT JOIN _final_chargeback_info fci
	ON fci.BUSINESS_UNIT = foi.BUSINESS_UNIT
	and fci.REGION = foi.REGION
	and fci.COUNTRY = foi.COUNTRY
	AND fci.month_date = foi.month_date
	AND fci.creditcard_type = foi.creditcard_type
	and fci.ppcc_activating_flag = foi.ppcc_activating_flag
	and fci.tenure = foi.tenure
    and fci.date = foi.date
    and fci.FIRST_ACTIVATING_COHORT = foi.FIRST_ACTIVATING_COHORT
    and fci.MEMBERSHIP_PRICE = foi.MEMBERSHIP_PRICE
left join _final_refund_info fri
    on fri.BUSINESS_UNIT = foi.BUSINESS_UNIT
	and fri.REGION = foi.REGION
	and fri.COUNTRY = foi.COUNTRY
	AND fri.month_date = foi.month_date
	AND fri.creditcard_type = foi.creditcard_type
	and fri.ppcc_activating_flag = foi.ppcc_activating_flag
	and fri.tenure = foi.tenure
    and fri.date = foi.date
    and fri.date = foi.date
    and fri.FIRST_ACTIVATING_COHORT = foi.FIRST_ACTIVATING_COHORT
    and fri.MEMBERSHIP_PRICE = foi.MEMBERSHIP_PRICE
GROUP BY
	foi.BUSINESS_UNIT
    ,foi.REGION
    ,foi.COUNTRY
	,foi.month_date
	,foi.creditcard_type
    ,foi.ppcc_activating_flag
	,foi.tenure
    ,foi.date
	,foi.FIRST_ACTIVATING_COHORT
	,foi.MEMBERSHIP_PRICE
