
set start_date = '2015-01-01';
set end_date = IFF(
        CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', CURRENT_TIMESTAMP())::DATE > CURRENT_DATE(),
        DATEADD(DAY, 1, CURRENT_DATE()),
        CURRENT_DATE()
    );
set report_date = dateadd(day, -1, $end_date);
set current_month_date = date_trunc(month, $report_date);
set last_month_date = dateadd(month, -1, $current_month_date);
set run_rate_multiplier = (select day(last_day($report_date)) / day($report_date));
set same_month_last_year_start = dateadd(month, -12, $current_month_date);


create table if not exists reporting_media_prod.attribution.cac_by_lead_channel_report_output (
    sequence           number(38, 0),
    store_name         varchar(100),
    store_abbreviation varchar(55),
    date               date,
    channel            varchar(75),
    primary_leads      number(20, 4),
    secondary_leads    number(20, 4),
    vips_from_leads_d1 number(20, 4),
    vips_from_leads_m1 number(20, 4),
    vips_from_reactivated_leads_d1 number(20, 4),
    vips_on_date_d1_reactivated_leads number(20, 4),
    vips_on_date_m1_reactivated_leads number(20, 4),
    total_vips_on_date number(20, 4),
    media_spend        number(20, 4),
    xcac               number(20, 4),
    cpl                number(20, 4),
    cross_brand_leads_per decimal(20,4),
    d1_lead_to_vip     number(20, 4),
    d1_vip_cac         number(20, 4),
    m1_lead_to_vip     number(20, 4),
    m1_vip_cac         number(20, 4),
    total_lead_to_vip  number(20, 4),
    total_vip_cac      number(20, 4),
    currency           varchar(25),
    source             varchar(25),
    version            varchar(500),
    channel_sequence   number(38, 0),
    report_month       date
);

--create table if not exists reporting_media_prod.snapshot.gcbc_output
create table if not exists reporting_media_prod.snapshot.gcbc_output_cac (
    sequence           number(38, 0),
    store_name         varchar(100),
    store_abbreviation varchar(55),
    date               date,
    channel            varchar(75),
    primary_leads      number(20, 4),
    secondary_leads    number(20, 4),
    vips_from_leads_d1 number(20, 4),
    vips_from_leads_m1 number(20, 4),
    vips_from_reactivated_leads_d1 number(20, 4),
    vips_on_date_d1_reactivated_leads number(20, 4),
    vips_on_date_m1_reactivated_leads number(20, 4),
    total_vips_on_date number(20, 4),
    media_spend        number(20, 4),
    xcac               number(20, 4),
    cpl                number(20, 4),
    cross_brand_leads_per decimal(20,4),
    d1_lead_to_vip     number(20, 4),
    d1_vip_cac         number(20, 4),
    m1_lead_to_vip     number(20, 4),
    m1_vip_cac         number(20, 4),
    total_lead_to_vip  number(20, 4),
    total_vip_cac      number(20, 4),
    currency           varchar(25),
    source             varchar(25),
    version            varchar(500),
    channel_sequence   number(38, 0),
    report_month       date,
    datetime_added     timestamp_ltz
);

delete
from reporting_media_prod.snapshot.gcbc_output_cac
where datetime_added < dateadd(day, -3, current_timestamp());

insert into reporting_media_prod.snapshot.gcbc_output_cac
select *,
       current_timestamp::timestamp_ltz
from reporting_media_prod.attribution.cac_by_lead_channel_report_output;

------------------------------------------------------------------------------------
-- daily actuals by segment

create or replace temporary table _cac_by_lead_daily_output_by_segment as
----- JFB WW-----
------ global roll-up
select
    'JFB-GLOBAL' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle','fabkids')
group by 1,2,3

union all
------ north america roll-up
select
    'JFB-NA' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle','fabkids')
    and lower(region) = 'na'
group by 1,2,3

union all
------ justfab + shoedazzle roll-up
select
    'JFSD-NA' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle')
    and lower(region) = 'na'
group by 1,2,3

union all
------ region roll-up
select
    case when region = 'NA' then store_brand_abbr || '-NA' else store_brand_abbr || '-EU' end as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle','fabkids')
group by 1,2,3

union all
------ country roll-up
select
    store_brand_abbr || '-' || country as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle','fabkids')
    and lower(country) in ('us','ca','de','dk','es','fr','uk','se','nl')
group by 1,2,3

union all
------ fabkids, no free trial
select
    'FK-NFT-NA' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('fabkids')
    and lower(region) = 'na'
    and is_fk_free_trial = false
group by 1,2,3

union all
----- SAVAGE X WW -----
------ global roll-up
select
    'SX-GLOBAL' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'savage x'
group by 1,2,3

union all

------ region roll-up
select
    case when region = 'NA' then 'SX-NA' else 'SX-EU' end as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'savage x'
group by 1,2,3

union all

------ region roll-up, online only
select
    'SX-O-NA' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'savage x'
    and lower(region) = 'na'
    and retail_customer = false
group by 1,2,3

union all

------ country roll-up
select
    'SX-' || country as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'savage x'
    and lower(country) in ('us','ca','de','es','fr','uk','eurem')
group by 1,2,3

union all

--- YITTY NA -----
---- region roll-up
select
    'YT-O-NA' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'yitty'
    and lower(region) = 'na'
group by 1,2,3

union all
--- FABLETICS CORP -----
---- global roll-up
select
    'FLC-GLOBAL' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('fabletics','yitty')
group by 1,2,3

union all

select
    'FL+SC-GLOBAL' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
group by 1,2,3

union all

------ north america roll-up
select
    'FLC-NA' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('fabletics','yitty')
    and lower(region) = 'na'
group by 1,2,3

union all

------ north america roll-up, online only
select
    'FLC-O-NA' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('fabletics','yitty')
    and lower(region) = 'na'
    and retail_customer = false
group by 1,2,3

union all

----- FABLETICS ACTIVEWEAR -----
------ fabletics region roll-up

select
    'FL-' || region as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) in ('na','eu')
    and is_fl_scrubs_customer = false
group by 1,2,3

union all

------ fabletics region roll-up by gender
select
    case when is_fl_mens_vip = true then 'FL-M-' || region else 'FL-W-' || region end as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) in ('na','eu')
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
------ country roll-up
select
    case when country = 'AT' then 'FL-DE' else 'FL-' || country end as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(country) in ('de','uk','dk','fr','es','nl','se','at','us','ca')
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
---- country roll-up by gender
select
    case when is_fl_mens_vip = true then 'FL-M-' || iff(country = 'AT', 'DE', country)
        else 'FL-W-' || iff(country = 'AT', 'DE', country) end as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(country) in ('de','uk','dk','fr','es','nl','se','at','us','ca')
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
------ region roll-up by gender, online only
select
    case when is_fl_mens_vip = true then 'FL-M-O-' || region
        else 'FL-W-O-' || region end as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) in ('na','eu')
    and retail_customer = false
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
------ country roll-up by gender, online only
select
    case when is_fl_mens_vip = true then 'FL-M-O-' || iff(country = 'AT', 'DE', country)
        else 'FL-W-O-' || iff(country = 'AT', 'DE', country) end as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(country) in ('de','uk','at')
    and retail_customer = false
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
----- FABLETICS & SCRUBS COMBINED -----
------ fabletics + scrubs region roll-up
select
    'FL+SC-NA' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) = 'na'
group by 1,2,3

union all

--- SCRUBS -----
------ scrubs region roll-up
select
    'SC-O-NA' as store_abbreviation,
    date,
    channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) = 'na'
    and is_fl_scrubs_customer = true
group by 1,2,3

union all

------------------------------------------
-- INCORPORATE ALL CHANNELS AGGREGATED BY SEGMENT --
------------------------------------------
----- JFB WW-----
------ global roll-up
select
    'JFB-GLOBAL' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle','fabkids')
group by 1,2,3

union all
------ north america roll-up
select
    'JFB-NA' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle','fabkids')
    and lower(region) = 'na'
group by 1,2,3

union all
------ justfab + shoedazzle roll-up
select
    'JFSD-NA' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle')
    and lower(region) = 'na'
group by 1,2,3

union all
------ region roll-up
select
    case when region = 'NA' then store_brand_abbr || '-NA' else store_brand_abbr || '-EU' end as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle','fabkids')
group by 1,2,3

union all
------ country roll-up
select
    store_brand_abbr || '-' || country as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('justfab','shoedazzle','fabkids')
    and lower(country) in ('us','ca','de','dk','es','fr','uk','se','nl')
group by 1,2,3

union all
------ fabkids, no free trial
select
    'FK-NFT-NA' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('fabkids')
    and lower(region) = 'na'
    and is_fk_free_trial = false
group by 1,2,3

union all
----- SAVAGE X WW -----
------ global roll-up
select
    'SX-GLOBAL' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'savage x'
group by 1,2,3

union all

------ region roll-up
select
    case when region = 'NA' then 'SX-NA' else 'SX-EU' end as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'savage x'
group by 1,2,3

union all

------ region roll-up, online only
select
    'SX-O-NA' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'savage x'
    and lower(region) = 'na'
    and retail_customer = false
group by 1,2,3

union all

------ country roll-up
select
    'SX-' || country as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'savage x'
    and lower(country) in ('us','ca','de','es','fr','uk','eurem')
group by 1,2,3

union all

--- YITTY NA -----
---- region roll-up
select
    'YT-O-NA' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'yitty'
    and lower(region) = 'na'
group by 1,2,3

union all
--- FABLETICS CORP -----
---- global roll-up
select
    'FLC-GLOBAL' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('fabletics','yitty')
group by 1,2,3

union all

select
    'FL+SC-GLOBAL' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
group by 1,2,3

union all

------ north america roll-up
select
    'FLC-NA' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('fabletics','yitty')
    and lower(region) = 'na'
group by 1,2,3

union all

------ north america roll-up, online only
select
    'FLC-O-NA' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) in ('fabletics','yitty')
    and lower(region) = 'na'
    and retail_customer = false
group by 1,2,3

union all

----- FABLETICS ACTIVEWEAR -----
------ fabletics region roll-up

select
    'FL-' || region as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) in ('na','eu')
    and is_fl_scrubs_customer = false
group by 1,2,3

union all

------ fabletics region roll-up by gender
select
    case when is_fl_mens_vip = true then 'FL-M-' || region else 'FL-W-' || region end as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) in ('na','eu')
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
------ country roll-up
select
    case when country = 'AT' then 'FL-DE' else 'FL-' || country end as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(country) in ('de','uk','dk','fr','es','nl','se','at','us','ca')
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
---- country roll-up by gender
select
    case when is_fl_mens_vip = true then 'FL-M-' || iff(country = 'AT', 'DE', country)
        else 'FL-W-' || iff(country = 'AT', 'DE', country) end as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(country) in ('de','uk','dk','fr','es','nl','se','at','us','ca')
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
------ region roll-up by gender, online only
select
    case when is_fl_mens_vip = true then 'FL-M-O-' || region
        else 'FL-W-O-' || region end as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) in ('na','eu')
    and retail_customer = false
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
------ country roll-up by gender, online only
select
    case when is_fl_mens_vip = true then 'FL-M-O-' || iff(country = 'AT', 'DE', country)
        else 'FL-W-O-' || iff(country = 'AT', 'DE', country) end as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(country) in ('de','uk','at')
    and retail_customer = false
    and is_fl_scrubs_customer = false
group by 1,2,3

union all
----- FABLETICS & SCRUBS COMBINED -----
------ fabletics + scrubs region roll-up
select
    'FL+SC-NA' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) = 'na'
group by 1,2,3

union all

--- SCRUBS -----
------ scrubs region roll-up
select
    'SC-O-NA' as store_abbreviation,
    date,
    'All' as channel,
    sum(ifnull(primary_leads, 0)) as primary_leads,
    sum(ifnull(secondary_leads, 0)) as secondary_leads,
    sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
    sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
    sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
    sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
    sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
    sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
    sum(ifnull(spend_local, 0)) as spend_local,
    sum(ifnull(spend_usd, 0)) as spend_usd,
    sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return_local,
    sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return_usd
from reporting_media_prod.attribution.cac_by_lead_channel_daily cac
where date < $end_date
    and lower(business_unit) = 'fabletics'
    and lower(region) = 'na'
    and is_fl_scrubs_customer = true
group by 1,2,3;


create or replace temporary table _tmp_gcbc_daily_output as
select b.tab_sequence as sequence,
       b.store_name_description as store_name,
       o.*
from reporting_media_prod.attribution.cac_tab_budget_store_lookup b
join _cac_by_lead_daily_output_by_segment o on o.store_abbreviation = b.store_tab_abbreviation
where o.date < $end_date;

------------------------------------------------------------------------------------
-- daily results

create or replace temporary table _daily_results as (
    select
           sequence,
           store_name,
           store_abbreviation,
           date as date,
           channel as channel,
           sum(ifnull(primary_leads, 0)) as primary_leads,
           sum(ifnull(secondary_leads, 0)) as secondary_leads,
           sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
           sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
           sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
           sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
           sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
           sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
           sum(ifnull(spend_local, 0)) as media_spend,
           sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return,
           'Local' currency,
           'DailyTM' as source
    from _tmp_gcbc_daily_output cac
    where date_trunc('month', cac.date) in ($current_month_date, $last_month_date, $same_month_last_year_start)
    group by 1,2,3,4,5

    union all

    select
           sequence,
           store_name,
           store_abbreviation,
           date as date,
           channel as channel,
           sum(ifnull(primary_leads, 0)) as primary_leads,
           sum(ifnull(secondary_leads, 0)) as secondary_leads,
           sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
           sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
           sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
           sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
           sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
           sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
           sum(ifnull(spend_usd, 0)) as media_spend,
           sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return,
           'USD' as currency,
           'DailyTM' as source
    from _tmp_gcbc_daily_output cac
    where date_trunc('month', cac.date) in ($current_month_date, $last_month_date, $same_month_last_year_start)
    group by 1,2,3,4,5
);

update _daily_results
set source = 'DailyLM'
where date_trunc('month', date) = $last_month_date;

update _daily_results
set source = 'DailyLY'
where date_trunc('month', date) = $same_month_last_year_start;

create or replace temporary table _daily_results_totals as
select
       sequence,
       store_name,
       store_abbreviation,
       dd.month_date as date,
       channel as channel,
       sum(ifnull(primary_leads, 0)) as primary_leads,
       sum(ifnull(secondary_leads, 0)) as secondary_leads,
       sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
       sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
       sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
       sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
       sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
       sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
       sum(ifnull(media_spend, 0)) as media_spend,
       sum(ifnull(first_guest_product_margin_pre_return, 0)) as first_guest_product_margin_pre_return,
       currency as currency,
       case
           when source = 'DailyTM' then 'MTD'
           when source = 'DailyLM' then 'DailyLMTotal'
           when source = 'DailyLY' then 'DailyLYTotal'
           end as source
from _daily_results d
join edw_prod.data_model.dim_date dd on dd.full_date = d.date
group by 1,2,3,4,5,16,17;


create or replace temporary table _daily_results_comp_totals as
select
       sequence,
       store_name,
       store_abbreviation,
       dd.month_date as date,
       channel as channel,
       sum(ifnull(primary_leads, 0)) as primary_leads,
       sum(ifnull(secondary_leads, 0)) as secondary_leads,
       sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
       sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
       sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
       sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
       sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
       sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
       sum(ifnull(media_spend, 0)) as media_spend,
       sum(ifnull(first_guest_product_margin_pre_return, 0)) as first_guest_product_margin_pre_return,
       currency as currency,
       case
           when date between $last_month_date and dateadd('month', -1, $report_date) then 'LMMTDComp'
           when date between $same_month_last_year_start and dateadd('year', -1, $report_date) then 'LYMTDComp'
           end as source
from _daily_results d
join edw_prod.data_model.dim_date dd on dd.full_date = d.date
where date between $last_month_date and dateadd('month', -1, $report_date)
   or date between $same_month_last_year_start and dateadd('year', -1, $report_date)
group by 1,2,3,4,5,16,17;

------------------------------------------------------------------------------------
-- monthly results

create or replace temporary table _monthly_results as
select
       sequence,
       store_name,
       store_abbreviation,
       date_trunc('month', date) as date,
       channel as channel,
       sum(ifnull(primary_leads, 0)) as primary_leads,
       sum(ifnull(secondary_leads, 0)) as secondary_leads,
       sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
       sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
       sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
       sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
       sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
       sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
       sum(ifnull(spend_local, 0)) as media_spend,
       sum(ifnull(first_guest_product_margin_pre_return_local, 0)) as first_guest_product_margin_pre_return,
       'Local' as currency,
       cast('Monthly' as varchar(25)) as source
from _tmp_gcbc_daily_output cac
where 1 = 1
  and cac.date >= $start_date
  and cac.date < $current_month_date
group by 1,2,3,4,5

union all

select
       sequence,
       store_name,
       store_abbreviation,
       date_trunc('month', date) as date,
       channel as channel,
       sum(ifnull(primary_leads, 0)) as primary_leads,
       sum(ifnull(secondary_leads, 0)) as secondary_leads,
       sum(ifnull(vips_from_leads_d1, 0)) as vips_from_leads_d1,
       sum(ifnull(vips_from_leads_m1, 0)) as vips_from_leads_m1,
       sum(ifnull(vips_from_reactivated_leads_d1, 0)) as vips_from_reactivated_leads_d1,
       sum(ifnull(vips_on_date_d1_reactivated_leads, 0)) as vips_on_date_d1_reactivated_leads,
       sum(ifnull(vips_on_date_m1_reactivated_leads, 0)) as vips_on_date_m1_reactivated_leads,
       sum(ifnull(total_vips_on_date, 0)) as total_vips_on_date,
       sum(ifnull(spend_usd, 0)) as media_spend,
       sum(ifnull(first_guest_product_margin_pre_return_usd, 0)) as first_guest_product_margin_pre_return,
       'USD' as currency,
       'Monthly' as source
FROM _tmp_gcbc_daily_output cac
WHERE 1 = 1
  AND cac.date >= $start_date
  AND cac.date < $current_month_date
group by 1,2,3,4,5;


------------------------------------------------------------------------------------
-- run rate

create or replace temporary table _run_rate as (
select
        sequence,
        store_name,
        store_abbreviation,
        date as date,
        channel as channel,
        primary_leads * $run_rate_multiplier as primary_leads,
        secondary_leads * $run_rate_multiplier as secondary_leads,
        vips_from_leads_d1 * $run_rate_multiplier as vips_from_leads_d1,
        vips_from_leads_m1 * $run_rate_multiplier as vips_from_leads_m1,
        vips_from_reactivated_leads_d1 * $run_rate_multiplier as vips_from_reactivated_leads_d1,
        vips_on_date_d1_reactivated_leads * $run_rate_multiplier as vips_on_date_d1_reactivated_leads,
        vips_on_date_m1_reactivated_leads * $run_rate_multiplier as vips_on_date_m1_reactivated_leads,
        total_vips_on_date * $run_rate_multiplier as total_vips_on_date,
        media_spend * $run_rate_multiplier as media_spend,
        first_guest_product_margin_pre_return * $run_rate_multiplier as first_guest_product_margin_pre_return,
        currency as currency,
        'RR' as source
from _daily_results_totals
where source = 'MTD'
);

------------------------------------------------------------------------------------
-- budget / forecast

create or replace temporary table _budget as (
select
        sequence,
        store_name,
        store_abbreviation,
        date as date,
        channel as channel,
        null as primary_leads,
        null as secondary_leads,
        null as vips_from_leads_d1,
        null as vips_from_leads_m1,
        null as vips_from_reactivated_leads_d1,
        null as vips_on_date_d1_reactivated_leads,
        null as vips_on_date_m1_reactivated_leads,
        null as total_vips_on_date,
        null as media_spend,
        null as first_guest_product_margin_pre_return,
        currency as currency,
        'Budget-BTFX' as source
from _daily_results_totals
where source = 'MTD'
union all
select
        sequence,
        store_name,
        store_abbreviation,
        date as date,
        channel as channel,
        null as primary_leads,
        null as secondary_leads,
        null as vips_from_leads_d1,
        null as vips_from_leads_m1,
        null as vips_from_reactivated_leads_d1,
        null as vips_on_date_d1_reactivated_leads,
        null as vips_on_date_m1_reactivated_leads,
        null as total_vips_on_date,
        null as media_spend,
        null as first_guest_product_margin_pre_return,
        currency as currency,
        'Budget-CURRFX' as source
from _daily_results_totals
where source = 'MTD'
);

create or replace temporary table _forecast as (
select
        sequence,
        store_name,
        store_abbreviation,
        date as date,
        channel as channel,
        null as primary_leads,
        null as secondary_leads,
        null as vips_from_leads_d1,
        null as vips_from_leads_m1,
        null as vips_from_reactivated_leads_d1,
        null as vips_on_date_d1_reactivated_leads,
        null as vips_on_date_m1_reactivated_leads,
        null as total_vips_on_date,
        null as media_spend,
        null as first_guest_product_margin_pre_return,
        currency as currency,
        'Forecast-BTFX' as source
from _daily_results_totals
where source = 'MTD'
union all
select
        sequence,
        store_name,
        store_abbreviation,
        date as date,
        channel as channel,
        null as primary_leads,
        null as secondary_leads,
        null as vips_from_leads_d1,
        null as vips_from_leads_m1,
        null as vips_from_reactivated_leads_d1,
        null as vips_on_date_d1_reactivated_leads,
        null as vips_on_date_m1_reactivated_leads,
        null as total_vips_on_date,
        null as media_spend,
        null as first_guest_product_margin_pre_return,
        currency as currency,
        'Forecast-CURRFX' as source
from _daily_results_totals
where source = 'MTD'
);

------------------------------------------------------------------------------------
-- consolidated final results

create or replace temporary table _final_output as (
    select *
    from _daily_results
    union all
    select *
    from _daily_results_totals
    union all
    select *
    from _daily_results_comp_totals
    union all
    select *
    from _monthly_results
    union all
    select *
    from _run_rate
    union all
    select *
    from _budget
    union all
    select *
    from _forecast
);

------------------------------------------------------------------------------------
-- incorporate budget / forecast targets

create or replace temporary table _budget_targets as
select store_tab_abbreviation,
       date,
       currency,
       channel,
       source,
       media_spend,
       leads,
       vips_from_leads_m1,
       total_vips_on_date
from reporting_media_prod.attribution.acquisition_budget_targets_cac;


update _final_output f
set media_spend = bt.media_spend,
    primary_leads = bt.leads,
    vips_from_leads_m1 = bt.vips_from_leads_m1,
    total_vips_on_date = bt.total_vips_on_date
from _budget_targets bt
where lower(f.store_abbreviation) = lower(bt.store_tab_abbreviation)
    and f.date = bt.date
    and lower(f.currency) = lower(bt.currency)
    and lower(f.source) = lower(bt.source)
    and lower(f.channel) = lower(bt.channel);

------------------------------------------------------------------------------------
-- create distinct report versions

-- report versions:
-- FL GLOBAL by Country - USD
-- FL GLOBAL by Region - USD
-- FLEU - Local
-- JFB GLOBAL - USD
-- JFEU - Local
-- SXF GLOBAL - USD
-- SXEU - Local

create or replace temporary table _global_cac_by_channel (
    sequence                    int,
    store_name                  varchar(100),
    store_abbreviation          varchar(25),
    date                        date,
    channel                     varchar(100),
    primary_leads               decimal(20,4),
    secondary_leads             decimal(20,4),
    vips_from_leads_d1          decimal(20,4),
    vips_from_leads_m1          decimal(20,4),
    vips_from_reactivated_leads_d1          decimal(20,4),
    vips_on_date_d1_reactivated_leads          decimal(20,4),
    vips_on_date_m1_reactivated_leads          decimal(20,4),
    total_vips_on_date          decimal(20,4),
    media_spend                 decimal(20,4),
    xcac                        number(20, 4),
    cpl                         decimal(20,4),
    cross_brand_leads_per       decimal(20,4),
    d1_lead_to_vip              decimal(20,4),
    d1_vip_cac                  decimal(20,4),
    m1_lead_to_vip              decimal(20,4),
    m1_vip_cac                  decimal(20,4),
    total_lead_to_vip           decimal(20,4),
    total_vip_cac               decimal(20,4),
    currency                    varchar(25),
    source                      varchar(25),
    version                     varchar(100)
);

create or replace temporary table _report_versions as
-- report versions excluding fabletics
select sequence,
       store_name,
       store_abbreviation,
       date,
       channel,
       primary_leads as primary_leads,
       secondary_leads as secondary_leads,
       vips_from_leads_d1 as vips_from_leads_d1,
       vips_from_leads_m1 as vips_from_leads_m1,
       vips_from_reactivated_leads_d1 as vips_from_reactivated_leads_d1,
       vips_on_date_d1_reactivated_leads as vips_on_date_d1_reactivated_leads,
       vips_on_date_m1_reactivated_leads as vips_on_date_m1_reactivated_leads,
       total_vips_on_date as total_vips_on_date,
       media_spend as media_spend,
       nvl((media_spend - nvl(first_guest_product_margin_pre_return,0))/nullif(total_vips_on_date, 0),0) as xcac,
       media_spend / nullif(primary_leads, 0) as cpl,
       (secondary_leads/nullif((primary_leads + secondary_leads),0)) as cross_brand_leads_per,
       vips_from_leads_d1 / nullif(primary_leads, 0) as d1_lead_to_vip,
       media_spend / nullif(vips_from_leads_d1, 0) as d1_vip_cac,
       vips_from_leads_m1 / nullif(primary_leads, 0) as m1_lead_to_vip,
       media_spend / nullif(vips_from_leads_m1, 0) as m1_vip_cac,
       total_vips_on_date / nullif(nvl(primary_leads,0),0) as total_lead_to_vip,
       media_spend / nullif(total_vips_on_date, 0) as total_vip_cac,
       currency as currency,
       source as source,
       case when store_name ilike '%savage%' and currency = 'USD' then 'SXF GLOBAL - USD'
            when store_name ilike '%savage%'
                     and not (store_abbreviation ilike any ('%-GLOBAL','%-NA','%-US','%-CA'))
                     and currency = 'Local' then 'SXEU - Local'

            when store_name ilike any ('%justfab%', '%shoedazzle%', '%fabkids%') and currency = 'USD' then 'JFB GLOBAL - USD'
            when store_name ilike any ('%justfab%', '%shoedazzle%', '%fabkids%')
                     and not (store_abbreviation ilike any ('%-GLOBAL','%-NA','%-US','%-CA'))
                     and currency = 'Local' then 'JFEU - Local'

            else 'No Report Reference'
            end as version
from _final_output
where store_name not ilike '%fabletics%'
    and store_name not ilike '%yitty%'

union all

-- fabletics global by region and country versions
select sequence,
       store_name,
       store_abbreviation,
       date,
       channel,
       primary_leads as primary_leads,
       secondary_leads as secondary_leads,
       vips_from_leads_d1 as vips_from_leads_d1,
       vips_from_leads_m1 as vips_from_leads_m1,
       vips_from_reactivated_leads_d1 as vips_from_reactivated_leads_d1,
       vips_on_date_d1_reactivated_leads as vips_on_date_d1_reactivated_leads,
       vips_on_date_m1_reactivated_leads as vips_on_date_m1_reactivated_leads,
       total_vips_on_date as total_vips_on_date,
       media_spend as media_spend,
       nvl((media_spend - nvl(first_guest_product_margin_pre_return,0))/nullif(total_vips_on_date, 0),0) as xcac,
       media_spend / nullif(primary_leads, 0) as cpl,
       (secondary_leads/nullif((primary_leads + secondary_leads),0)) as cross_brand_leads_per,
       vips_from_leads_d1 / nullif(primary_leads, 0) as d1_lead_to_vip,
       media_spend / nullif(vips_from_leads_d1, 0) as d1_vip_cac,
       vips_from_leads_m1 / nullif(primary_leads, 0) as m1_lead_to_vip,
       media_spend / nullif(vips_from_leads_m1, 0) as m1_vip_cac,
       total_vips_on_date / nullif(nvl(primary_leads,0),0) as total_lead_to_vip,
       media_spend / nullif(total_vips_on_date, 0) as total_vip_cac,
       currency as currency,
       source as source,
       case when store_abbreviation ilike any ('%-GLOBAL','%-NA','%-EU') then 'FL GLOBAL by Region - USD'
            else 'FL GLOBAL by Country - USD'
            end as version
from _final_output
where store_name ilike any ('%fabletics%', '%yitty%')
    and currency = 'USD'

union all

-- fabletics eu local version
select sequence,
       store_name,
       store_abbreviation,
       date,
       channel,
       primary_leads as primary_leads,
       secondary_leads as secondary_leads,
       vips_from_leads_d1 as vips_from_leads_d1,
       vips_from_leads_m1 as vips_from_leads_m1,
       vips_from_reactivated_leads_d1 as vips_from_reactivated_leads_d1,
       vips_on_date_d1_reactivated_leads as vips_on_date_d1_reactivated_leads,
       vips_on_date_m1_reactivated_leads as vips_on_date_m1_reactivated_leads,
       total_vips_on_date as total_vips_on_date,
       media_spend as media_spend,
       nvl((media_spend - nvl(first_guest_product_margin_pre_return,0))/nullif(total_vips_on_date, 0),0) as xcac,
       media_spend / nullif(primary_leads, 0) as cpl,
       (secondary_leads/nullif((primary_leads + secondary_leads),0)) as cross_brand_leads_per,
       vips_from_leads_d1 / nullif(primary_leads, 0) as d1_lead_to_vip,
       media_spend / nullif(vips_from_leads_d1, 0) as d1_vip_cac,
       vips_from_leads_m1 / nullif(primary_leads, 0) as m1_lead_to_vip,
       media_spend / nullif(vips_from_leads_m1, 0) as m1_vip_cac,
       total_vips_on_date / nullif(nvl(primary_leads,0),0) as total_lead_to_vip,
       media_spend / nullif(total_vips_on_date, 0) as total_vip_cac,
       currency as currency,
       source as source,
       'FLEU - Local' as version
from _final_output
where store_name ilike '%fabletics%'
    and (sequence = 4 or sequence >= 24)
    and currency = 'Local';


insert into _global_cac_by_channel
select * from _report_versions
where version != 'No Report Reference';


------------------------------------------------------------------------------------
-- budget /forecast run rates

insert into _global_cac_by_channel
select rr.sequence,
       rr.store_name,
       rr.store_abbreviation,
       rr.date,
       rr.channel,
       1.00 * (rr.primary_leads - b.primary_leads) / nullif(b.primary_leads, 0) as primary_leads,
       1.00 * (rr.secondary_leads - b.secondary_leads) / nullif(b.secondary_leads, 0) as secondary_leads,
       1.00 * (rr.vips_from_leads_d1 - b.vips_from_leads_d1) / nullif(b.vips_from_leads_d1, 0) as vips_from_leads_d1,
       1.00 * (rr.vips_from_leads_m1 - b.vips_from_leads_m1) / nullif(b.vips_from_leads_m1, 0) as vips_from_leads_m1,
       1.00 * (rr.vips_from_reactivated_leads_d1 - b.vips_from_reactivated_leads_d1) / nullif(b.vips_from_reactivated_leads_d1, 0) as vips_from_reactivated_leads_d1,
       1.00 * (rr.vips_on_date_d1_reactivated_leads - b.vips_on_date_d1_reactivated_leads) / nullif(b.vips_on_date_d1_reactivated_leads, 0) as vips_on_date_d1_reactivated_leads,
       1.00 * (rr.vips_on_date_m1_reactivated_leads - b.vips_on_date_m1_reactivated_leads) / nullif(b.vips_on_date_m1_reactivated_leads, 0) as vips_on_date_m1_reactivated_leads,
       1.00 * (rr.total_vips_on_date - b.total_vips_on_date) / nullif(b.total_vips_on_date, 0) as total_vips_on_date,
       1.00 * (rr.media_spend - b.media_spend) / nullif(b.media_spend, 0) as media_spend,
       1.00 * (rr.xcac - b.xcac) / nullif(b.xcac, 0) as xcac,
       1.00 * (rr.cpl - b.cpl) / nullif(b.cpl, 0) as cpl,
       1.00 * (rr.cross_brand_leads_per - b.cross_brand_leads_per) / nullif(b.cross_brand_leads_per, 0) as cross_brand_leads_per,
       null as d1_lead_to_vip,
       1.00 * (rr.d1_vip_cac - b.d1_vip_cac) / nullif(b.d1_vip_cac, 0) as d1_vip_cac,
       null as m1_lead_to_vip,
       1.00 * (rr.m1_vip_cac - b.m1_vip_cac) / nullif(b.m1_vip_cac, 0) as m1_vip_cac,
       null as total_lead_to_vip,
       1.00 * (rr.total_vip_cac - b.total_vip_cac) / nullif(b.total_vip_cac, 0) as total_vip_cac,
       rr.currency as currency,
       'RRLM' as source,
       rr.version as version
from _global_cac_by_channel rr
full join _global_cac_by_channel b on rr.store_name = b.store_name
    and rr.channel = b.channel
    and rr.currency = b.currency
    and rr.version = b.version
    and b.source = 'DailyLMTotal'
where rr.source = 'RR';

insert into _global_cac_by_channel
select rr.sequence,
       rr.store_name,
       rr.store_abbreviation,
       rr.date,
       rr.channel,
       1.00 * (rr.primary_leads - b.primary_leads) / nullif(b.primary_leads, 0) as primary_leads,
       1.00 * (rr.secondary_leads - b.secondary_leads) / nullif(b.secondary_leads, 0) as secondary_leads,
       1.00 * (rr.vips_from_leads_d1 - b.vips_from_leads_d1) / nullif(b.vips_from_leads_d1, 0) as vips_from_leads_d1,
       1.00 * (rr.vips_from_leads_m1 - b.vips_from_leads_m1) / nullif(b.vips_from_leads_m1, 0) as vips_from_leads_m1,
       1.00 * (rr.vips_from_reactivated_leads_d1 - b.vips_from_reactivated_leads_d1) / nullif(b.vips_from_reactivated_leads_d1, 0) as vips_from_reactivated_leads_d1,
       1.00 * (rr.vips_on_date_d1_reactivated_leads - b.vips_on_date_d1_reactivated_leads) / nullif(b.vips_on_date_d1_reactivated_leads, 0) as vips_on_date_d1_reactivated_leads,
       1.00 * (rr.vips_on_date_m1_reactivated_leads - b.vips_on_date_m1_reactivated_leads) / nullif(b.vips_on_date_m1_reactivated_leads, 0) as vips_on_date_m1_reactivated_leads,
       1.00 * (rr.total_vips_on_date - b.total_vips_on_date) / nullif(b.total_vips_on_date, 0) as total_vips_on_date,
       1.00 * (rr.media_spend - b.media_spend) / nullif(b.media_spend, 0) as media_spend,
       1.00 * (rr.xcac - b.xcac) / nullif(b.xcac, 0) as xcac,
       1.00 * (rr.cpl - b.cpl) / nullif(b.cpl, 0) as cpl,
       1.00 * (rr.cross_brand_leads_per - b.cross_brand_leads_per) / nullif(b.cross_brand_leads_per, 0) as cross_brand_leads_per,
       null as d1_lead_to_vip,
       1.00 * (rr.d1_vip_cac - b.d1_vip_cac) / nullif(b.d1_vip_cac, 0) as d1_vip_cac,
       null as m1_lead_to_vip,
       1.00 * (rr.m1_vip_cac - b.m1_vip_cac) / nullif(b.m1_vip_cac, 0) as m1_vip_cac,
       null as total_lead_to_vip,
       1.00 * (rr.total_vip_cac - b.total_vip_cac) / nullif(b.total_vip_cac, 0) as total_vip_cac,
       rr.currency as currency,
       'RRLY' as source,
       rr.version as version
from _global_cac_by_channel rr
full join _global_cac_by_channel b on rr.store_name = b.store_name
    and rr.channel = b.channel
    and rr.currency = b.currency
    and rr.version = b.version
    and b.source = 'DailyLYTotal'
where rr.source = 'RR';

insert into _global_cac_by_channel
select rr.sequence,
       rr.store_name,
       rr.store_abbreviation,
       rr.date,
       rr.channel,
       1.00 * (rr.primary_leads - b.primary_leads) / nullif(b.primary_leads, 0) as primary_leads,
       1.00 * (rr.secondary_leads - b.secondary_leads) / nullif(b.secondary_leads, 0) as secondary_leads,
       null as vips_from_leads_d1,
       1.00 * (rr.vips_from_leads_m1 - b.vips_from_leads_m1) / nullif(b.vips_from_leads_m1, 0) as vips_from_leads_m1,
       1.00 * (rr.vips_from_reactivated_leads_d1 - b.vips_from_reactivated_leads_d1) / nullif(b.vips_from_reactivated_leads_d1, 0) as vips_from_reactivated_leads_d1,
       1.00 * (rr.vips_on_date_d1_reactivated_leads - b.vips_on_date_d1_reactivated_leads) / nullif(b.vips_on_date_d1_reactivated_leads, 0) as vips_on_date_d1_reactivated_leads,
       1.00 * (rr.vips_on_date_m1_reactivated_leads - b.vips_on_date_m1_reactivated_leads) / nullif(b.vips_on_date_m1_reactivated_leads, 0) as vips_on_date_m1_reactivated_leads,
       1.00 * (rr.total_vips_on_date - b.total_vips_on_date) / nullif(b.total_vips_on_date, 0) as total_vips_on_date,
       1.00 * (rr.media_spend - b.media_spend) / nullif(b.media_spend, 0) as media_spend,
       1.00 * (rr.xcac - b.xcac) / nullif(b.xcac, 0) as xcac,
       1.00 * (rr.cpl - b.cpl) / nullif(b.cpl, 0) as cpl,
       1.00 * (rr.cross_brand_leads_per - b.cross_brand_leads_per) / nullif(b.cross_brand_leads_per, 0) as cross_brand_leads_per,
       null as d1_lead_to_vip,
       1.00 * (rr.d1_vip_cac - b.d1_vip_cac) / nullif(b.d1_vip_cac, 0) as d1_vip_cac,
       null as m1_lead_to_vip,
       1.00 * (rr.m1_vip_cac - b.m1_vip_cac) / nullif(b.m1_vip_cac, 0) as m1_vip_cac,
       null as total_lead_to_vip,
       1.00 * (rr.total_vip_cac - b.total_vip_cac) / nullif(b.total_vip_cac, 0) as total_vip_cac,
       rr.currency as currency,
       'RRBudget-BTFX' as source,
       rr.version as version
from _global_cac_by_channel rr
full join _global_cac_by_channel b on rr.store_name = b.store_name
    and rr.date = b.date
    and rr.channel = b.channel
    and rr.currency = b.currency
    and rr.version = b.version
    and b.source = 'Budget-BTFX'
where rr.source = 'RR';

insert into _global_cac_by_channel
select rr.sequence,
       rr.store_name,
       rr.store_abbreviation,
       rr.date,
       rr.channel,
       1.00 * (rr.primary_leads - b.primary_leads) / nullif(b.primary_leads, 0) as primary_leads,
       1.00 * (rr.secondary_leads - b.secondary_leads) / nullif(b.secondary_leads, 0) as secondary_leads,
       null as vips_from_leads_d1,
       1.00 * (rr.vips_from_leads_m1 - b.vips_from_leads_m1) / nullif(b.vips_from_leads_m1, 0) as vips_from_leads_m1,
       1.00 * (rr.vips_from_reactivated_leads_d1 - b.vips_from_reactivated_leads_d1) / nullif(b.vips_from_reactivated_leads_d1, 0) as vips_from_reactivated_leads_d1,
       1.00 * (rr.vips_on_date_d1_reactivated_leads - b.vips_on_date_d1_reactivated_leads) / nullif(b.vips_on_date_d1_reactivated_leads, 0) as vips_on_date_d1_reactivated_leads,
       1.00 * (rr.vips_on_date_m1_reactivated_leads - b.vips_on_date_m1_reactivated_leads) / nullif(b.vips_on_date_m1_reactivated_leads, 0) as vips_on_date_m1_reactivated_leads,
       1.00 * (rr.total_vips_on_date - b.total_vips_on_date) / nullif(b.total_vips_on_date, 0) as total_vips_on_date,
       1.00 * (rr.media_spend - b.media_spend) / nullif(b.media_spend, 0) as media_spend,
       1.00 * (rr.xcac - b.xcac) / nullif(b.xcac, 0) as xcac,
       1.00 * (rr.cpl - b.cpl) / nullif(b.cpl, 0) as cpl,
       1.00 * (rr.cross_brand_leads_per - b.cross_brand_leads_per) / nullif(b.cross_brand_leads_per, 0) as cross_brand_leads_per,
       null as d1_lead_to_vip,
       1.00 * (rr.d1_vip_cac - b.d1_vip_cac) / nullif(b.d1_vip_cac, 0) as d1_vip_cac,
       null as m1_lead_to_vip,
       1.00 * (rr.m1_vip_cac - b.m1_vip_cac) / nullif(b.m1_vip_cac, 0) as m1_vip_cac,
       null as total_lead_to_vip,
       1.00 * (rr.total_vip_cac - b.total_vip_cac) / nullif(b.total_vip_cac, 0) as total_vip_cac,
       rr.currency as currency,
       'RRBudget-CURRFX' as source,
       rr.version as version
from _global_cac_by_channel rr
full join _global_cac_by_channel b on rr.store_name = b.store_name
    and rr.date = b.date
    and rr.channel = b.channel
    and rr.currency = b.currency
    and rr.version = b.version
    and b.source = 'Budget-CURRFX'
where rr.source = 'RR';


insert into _global_cac_by_channel
select rr.sequence,
       rr.store_name,
       rr.store_abbreviation,
       rr.date,
       rr.channel,
       1.00 * (rr.primary_leads - b.primary_leads) / nullif(b.primary_leads, 0) as primary_leads,
       1.00 * (rr.secondary_leads - b.secondary_leads) / nullif(b.secondary_leads, 0) as secondary_leads,
       null as vips_from_leads_d1,
       1.00 * (rr.vips_from_leads_m1 - b.vips_from_leads_m1) / nullif(b.vips_from_leads_m1, 0) as vips_from_leads_m1,
       1.00 * (rr.vips_from_reactivated_leads_d1 - b.vips_from_reactivated_leads_d1) / nullif(b.vips_from_reactivated_leads_d1, 0) as vips_from_reactivated_leads_d1,
       1.00 * (rr.vips_on_date_d1_reactivated_leads - b.vips_on_date_d1_reactivated_leads) / nullif(b.vips_on_date_d1_reactivated_leads, 0) as vips_on_date_d1_reactivated_leads,
       1.00 * (rr.vips_on_date_m1_reactivated_leads - b.vips_on_date_m1_reactivated_leads) / nullif(b.vips_on_date_m1_reactivated_leads, 0) as vips_on_date_m1_reactivated_leads,
       1.00 * (rr.total_vips_on_date - b.total_vips_on_date) / nullif(b.total_vips_on_date, 0) as total_vips_on_date,
       1.00 * (rr.media_spend - b.media_spend) / nullif(b.media_spend, 0) as media_spend,
       1.00 * (rr.xcac - b.xcac) / nullif(b.xcac, 0) as xcac,
       1.00 * (rr.cpl - b.cpl) / nullif(b.cpl, 0) as cpl,
       1.00 * (rr.cross_brand_leads_per - b.cross_brand_leads_per) / nullif(b.cross_brand_leads_per, 0) as cross_brand_leads_per,
       null as d1_lead_to_vip,
       1.00 * (rr.d1_vip_cac - b.d1_vip_cac) / nullif(b.d1_vip_cac, 0) as d1_vip_cac,
       null as m1_lead_to_vip,
       1.00 * (rr.m1_vip_cac - b.m1_vip_cac) / nullif(b.m1_vip_cac, 0) as m1_vip_cac,
       null as total_lead_to_vip,
       1.00 * (rr.total_vip_cac - b.total_vip_cac) / nullif(b.total_vip_cac, 0) as total_vip_cac,
       rr.currency as currency,
       'RRForecast-BTFX' as source,
       rr.version as version
from _global_cac_by_channel rr
full join _global_cac_by_channel b on rr.store_name = b.store_name
    and rr.date = b.date
    and rr.channel = b.channel
    and rr.currency = b.currency
    and rr.version = b.version
    and b.source = 'Forecast-BTFX'
where rr.source = 'RR';


insert into _global_cac_by_channel
select rr.sequence,
       rr.store_name,
       rr.store_abbreviation,
       rr.date,
       rr.channel,
       1.00 * (rr.primary_leads - b.primary_leads) / nullif(b.primary_leads, 0) as primary_leads,
       1.00 * (rr.secondary_leads - b.secondary_leads) / nullif(b.secondary_leads, 0) as secondary_leads,
       null as vips_from_leads_d1,
       1.00 * (rr.vips_from_leads_m1 - b.vips_from_leads_m1) / nullif(b.vips_from_leads_m1, 0) as vips_from_leads_m1,
       1.00 * (rr.vips_from_reactivated_leads_d1 - b.vips_from_reactivated_leads_d1) / nullif(b.vips_from_reactivated_leads_d1, 0) as vips_from_reactivated_leads_d1,
       1.00 * (rr.vips_on_date_d1_reactivated_leads - b.vips_on_date_d1_reactivated_leads) / nullif(b.vips_on_date_d1_reactivated_leads, 0) as vips_on_date_d1_reactivated_leads,
       1.00 * (rr.vips_on_date_m1_reactivated_leads - b.vips_on_date_m1_reactivated_leads) / nullif(b.vips_on_date_m1_reactivated_leads, 0) as vips_on_date_m1_reactivated_leads,
       1.00 * (rr.total_vips_on_date - b.total_vips_on_date) / nullif(b.total_vips_on_date, 0) as total_vips_on_date,
       1.00 * (rr.media_spend - b.media_spend) / nullif(b.media_spend, 0) as media_spend,
       1.00 * (rr.xcac - b.xcac) / nullif(b.xcac, 0) as xcac,
       1.00 * (rr.cpl - b.cpl) / nullif(b.cpl, 0) as cpl,
       1.00 * (rr.cross_brand_leads_per - b.cross_brand_leads_per) / nullif(b.cross_brand_leads_per, 0) as cross_brand_leads_per,
       null as d1_lead_to_vip,
       1.00 * (rr.d1_vip_cac - b.d1_vip_cac) / nullif(b.d1_vip_cac, 0) as d1_vip_cac,
       null as m1_lead_to_vip,
       1.00 * (rr.m1_vip_cac - b.m1_vip_cac) / nullif(b.m1_vip_cac, 0) as m1_vip_cac,
       null as total_lead_to_vip,
       1.00 * (rr.total_vip_cac - b.total_vip_cac) / nullif(b.total_vip_cac, 0) as total_vip_cac,
       rr.currency as currency,
       'RRForecast-CURRFX' as source,
       rr.version as version
from _global_cac_by_channel rr
full join _global_cac_by_channel b on rr.store_name = b.store_name
    and rr.date = b.date
    and rr.channel = b.channel
    and rr.currency = b.currency
    and rr.version = b.version
    and b.source = 'Forecast-CURRFX'
where rr.source = 'RR';


---------------------------------- EU versions with metrics in USD currency

insert into _global_cac_by_channel
select
    sequence,
    store_name,
    store_abbreviation,
    date,
    channel,
    primary_leads,
    secondary_leads,
    vips_from_leads_d1,
    vips_from_leads_m1,
    vips_from_reactivated_leads_d1,
    vips_on_date_d1_reactivated_leads,
    vips_on_date_m1_reactivated_leads,
    total_vips_on_date,
    media_spend,
    xcac,
    cpl,
    cross_brand_leads_per,
    d1_lead_to_vip,
    d1_vip_cac,
    m1_lead_to_vip,
    m1_vip_cac,
    total_lead_to_vip,
    total_vip_cac,
    currency,
    source,
    case when version='SXF GLOBAL - USD' then 'SXEU - USD'
         when version='JFB GLOBAL - USD' then 'JFEU - USD'
         when version IN ('FL GLOBAL by Region - USD', 'FL GLOBAL by Country - USD') then 'FLEU - USD'
    end as version
from _global_cac_by_channel n
where version ilike '%- usd'
    and store_abbreviation in(
        select distinct store_abbreviation
        from _global_cac_by_channel
        where version ilike '%- local');

------------------------------------------------------------------------------------

delete from reporting_media_prod.attribution.cac_by_lead_channel_report_output
where report_month = $current_month_date;

insert into reporting_media_prod.attribution.cac_by_lead_channel_report_output (
     sequence,
     store_name,
     store_abbreviation,
     date,
     channel,
     primary_leads,
     secondary_leads,
     vips_from_leads_d1,
     vips_from_leads_m1,
     vips_from_reactivated_leads_d1,
     vips_on_date_d1_reactivated_leads,
     vips_on_date_m1_reactivated_leads,
     total_vips_on_date,
     media_spend,
     xcac,
     cpl,
     cross_brand_leads_per,
     d1_lead_to_vip,
     d1_vip_cac,
     m1_lead_to_vip,
     m1_vip_cac,
     total_lead_to_vip,
     total_vip_cac,
     currency,
     source,
     version,
     channel_sequence,
     report_month
 )
select c.sequence,
       store_name,
       store_abbreviation,
       date,
       coalesce(cs.channel_display_name, c.channel) as channel,
       primary_leads,
       secondary_leads,
       vips_from_leads_d1,
       vips_from_leads_m1,
       vips_from_reactivated_leads_d1,
       vips_on_date_d1_reactivated_leads,
       vips_on_date_m1_reactivated_leads,
       total_vips_on_date,
       media_spend,
       xcac,
       cpl,
       cross_brand_leads_per,
       d1_lead_to_vip,
       d1_vip_cac,
       m1_lead_to_vip,
       m1_vip_cac,
       total_lead_to_vip,
       total_vip_cac,
       currency,
       source,
       version,
       cs.sequence as channel_sequence,
       $current_month_date as report_month
from _global_cac_by_channel c
left join reporting_media_base_prod.lkp.channel_display_name cs on lower(c.channel) = lower(cs.channel_key);


delete from reporting_media_prod.attribution.cac_by_lead_channel_report_output
where channel is null;


update reporting_media_prod.attribution.cac_by_lead_channel_report_output
set sequence = (select max(sequence) + 1
                from reporting_media_prod.attribution.cac_by_lead_channel_report_output)
where store_abbreviation is null;


update reporting_media_prod.attribution.cac_by_lead_channel_report_output
set xcac = NULL
where xcac = 0;
