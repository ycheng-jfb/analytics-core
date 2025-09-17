create or replace view REPORTING_PROD.GMS.BOSS_ACTUALS_RETRIES_V2_DATASET(
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
with temp as (
select
    *
from reporting_prod.gms.boss_actuals_retries_dataset
where ("Billing Day" = 7 and "Billing Month" = '2020-06-01')
    or ("Billing Day" = 7 and "Billing Month" = '2020-10-01' and "Storefront" like '%SX%')
),
june_oct_2020_patch as (
select
    "Current Date",
    6 as "Billing Day",
    dateadd('day', -1, "billing hour") as "billing hour",
    "Billing Month",
    0 as "Total Credits Billed",
    dateadd('day', -1, "Billing Date") as "Billing Date",
    0 as "Total Actuals",
    0 as "Total Retries",
    "Storefront",
    "Curr_Datehour"
from temp
)
select * from june_oct_2020_patch
union
select * from reporting_prod.gms.boss_actuals_retries_dataset;
