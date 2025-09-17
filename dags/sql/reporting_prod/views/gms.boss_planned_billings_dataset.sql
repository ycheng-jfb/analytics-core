create or replace view REPORTING_PROD.GMS.BOSS_PLANNED_BILLINGS_DATASET(
	"Current Date",
	"Billing Day",
	"Storefront",
	"Total Planned",
	"Billing Month",
	"Billing Date",
	"billing hour",
	"Curr_Datehour"
) as
select
    to_date(current_date) as "Current Date",
    day(billing_date) as "Billing Day",
    case when store='Fabletics CA' then 'FL CA'
        when store='Fabletics DE' then 'FL DE'
        when store='Fabletics DK' then 'FL DK'
        when store='Fabletics ES' then 'FL ES'
        when store='Fabletics FR' then 'FL FR'
        when store='Fabletics NL' then 'FL NL'
        when store='Fabletics SE' then 'FL SE'
        when store='Fabletics UK' then 'FL UK'
        when store='JustFab DE' then 'JF DE'
        when store='JustFab DK' then 'JF DK'
        when store='JustFab ES' then 'JF ES'
        when store='JustFab FR' then 'JF FR'
        when store='JustFab SE' then 'JF SE'
        when store='JustFab UK' then 'JF UK'
        when store= 'JustFab NL' then 'JF NL'
        when store='Fabletics US' then 'FL US'
        when store='ShoeDazzle US' then 'SD US'
        when store='JustFab CA' then 'JF CA'
        when store='JustFab US' then 'JF US'
        when store='FabKids US' then 'FK US'
        when store='Savage X US' then 'SX US'
        when store='Savage X CA' then 'SX US'
        when store='Savage X FR' then 'SX FR'
        when store='Savage X DE' then 'SX DE'
        when store='Savage X DK' then 'SX DK'
        when store='Savage X ES' then 'SX ES'
        when store='Savage X UK' then 'SX UK'
        when store='Savage X SE' then 'SX SE'
        when store='Savage X NL' then 'SX EU'
        when store='Savage X EU' then 'SX EU'
    end as "Storefront",
    sum(planned_billings) as "Total Planned",
    billing_month as "Billing Month",
    billing_date as "Billing Date",
    dateadd(minute, datediff(minute, to_timestamp_ntz(0), datetime_start) / 30 * 30, to_timestamp_ntz(0)) as "billing hour", -- 26503560
    current_timestamp as "Curr_Datehour"
from reporting_prod.gms.planned_billings
where billing_month >= '2019-05-01'
group by
    billing_month,
    billing_date,
    "Billing Day",
    "billing hour",
    "Storefront";
