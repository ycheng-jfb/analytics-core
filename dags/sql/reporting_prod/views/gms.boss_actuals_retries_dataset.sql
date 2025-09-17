create or replace view REPORTING_PROD.GMS.BOSS_ACTUALS_RETRIES_DATASET(
	"Current Date",
	"Billing Day",
	"billing hour",
	"Billing Month",
	"Total Credits Billed",
	"Billing Date",
	"Total Actuals",
	"Total Retries",
	"Storefront",
	"Curr_Datehour"
) as
select
cast(current_timestamp as date) as "Current Date",
day(o.datetime_added) as "Billing Day",
time_slice(o.datetime_added, 30, 'minutes') as "billing hour",
date_trunc('month',o.datetime_added) as "Billing Month",
count(o.order_id) as "Total Credits Billed",
o.datetime_added::date as "Billing Date",
count(o.order_id)-count(br.order_id) as "Total Actuals",
count(br.order_id) as "Total Retries",
CASE WHEN O.STORE_ID=26 THEN 'JF US'
     WHEN O.STORE_ID=38 THEN 'JF UK'
     WHEN O.STORE_ID=55 THEN 'SD US'
     WHEN O.STORE_ID=48 THEN 'JF FR'
     WHEN O.STORE_ID=36 THEN 'JF DE'
     WHEN O.STORE_ID=61 THEN 'JF DK'
     WHEN O.STORE_ID= 41 THEN 'JF CA'
     WHEN O.STORE_ID=71 THEN 'FL ES'
     WHEN O.STORE_ID=73 THEN 'FL NL'
     WHEN O.STORE_ID=63 THEN 'JF SE'
     WHEN O.STORE_ID=59 THEN 'JF NL'
     WHEN O.STORE_ID=65 THEN 'FL DE'
     WHEN O.STORE_ID=67 THEN 'FL UK'
     WHEN O.STORE_ID=69 THEN 'FL FR'
     WHEN O.STORE_ID=79 THEN 'FL CA'
     WHEN O.STORE_ID=52 THEN 'FL US'
     WHEN O.STORE_ID=50 THEN 'JF ES'
     WHEN O.STORE_ID=75 THEN 'FL SE'
     WHEN O.STORE_ID=77 THEN 'FL DK'
     WHEN O.STORE_ID=46 THEN 'FK US'
     WHEN O.STORE_ID=121 THEN 'SX US'
     WHEN O.STORE_ID=141 THEN 'SX US'
     WHEN O.STORE_ID=125 THEN 'SX FR'
     WHEN O.STORE_ID=127 THEN 'SX DE'
     WHEN O.STORE_ID=129 THEN 'SX DK'
     WHEN O.STORE_ID=131 THEN 'SX ES'
     WHEN O.STORE_ID=133 THEN 'SX UK'
     WHEN O.STORE_ID=135 THEN 'SX SE'
     WHEN O.STORE_ID=137 THEN 'SX EU'
     WHEN O.STORE_ID=139 THEN 'SX EU'
END AS "Storefront",
current_timestamp as "Curr_Datehour"
from lake_consolidated.ultra_merchant."ORDER" o
JOIN lake_consolidated.ultra_merchant.order_classification  oc  on o.order_id=oc.order_id
JOIN lake_consolidated.ultra_merchant.order_type ot on oc.order_type_id=ot.order_type_id
left join lake_consolidated.ultra_merchant.billing_retry_schedule_queue  br on o.order_id=br.order_id
where oc.order_type_id in (10,39) and o.payment_statuscode IN (2600,2650,2651) -- 10: Membership Credit, 39: Membership Token (for FL US)
and o.datetime_added>='2018-05-01'
and (br.order_id is null OR br.statuscode=4205)
and day(o.datetime_added) >= 6
group by "Current Date",
	"Billing Day",
	"billing hour",
	"Billing Month",
	"Billing Date",
	"Storefront";
