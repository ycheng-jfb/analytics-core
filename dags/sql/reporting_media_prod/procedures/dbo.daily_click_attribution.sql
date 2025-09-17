
-- tableau datasource: MEDDS16 - Daily Click Attribution
-- datasource includes individual session UTMs

------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

-- set variables for this year's start and end dates
set current_year_start_date = DATEADD(month, -6, CURRENT_DATE);
set current_year_end_date = current_date();
set last_year_start_date = dateadd('year', -1, $current_year_start_date);
set last_year_end_date = dateadd('year', -1, $current_year_end_date);

set session_data_last_updated = (select max(meta_update_datetime) from reporting_base_prod.shared.session);
set registration_data_last_updated = (select max(meta_update_datetime) from edw_prod.data_model.fact_registration);
set activation_data_last_updated = (select max(meta_update_datetime) from edw_prod.data_model.fact_activation);

create or replace temporary table _session_mapping as
select distinct
       s.store_id,
       session_id,
       s.utm_medium,
       s.utm_source,
       s.utm_campaign,
       s.utm_content,
       s.utm_term,
       channel_type,
       channel,
       subchannel,
       vendor
from reporting_base_prod.shared.session s
join reporting_base_prod.shared.media_source_channel_mapping m on m.media_source_hash = s.media_source_hash
join edw_prod.data_model.dim_store ds on ds.store_id = s.store_id
where session_id is not null
    and ((to_date(session_local_datetime) between dateadd(day,-2,$current_year_start_date) and $current_year_end_date)
            or (to_date(session_local_datetime) between dateadd(day,-2,$last_year_start_date) and $last_year_end_date));

update _session_mapping
set vendor = 'other'
where vendor is null or vendor = '' or vendor = 'none';

update _session_mapping
set vendor = 'unclassified'
where channel = 'unclassified' and subchannel = 'unclassified';

create or replace temporary table _sessions as
select distinct
       s.store_id,
       store_brand as store_brand_name,
       store_region,
       store_country,
       cast(case when is_male_session = true or is_male_gateway = true then true else false end as boolean) as is_male_customer,
       cast(case when is_scrubs_gateway = true then true else false end as boolean) as is_fl_scrubs_customer,
       to_date(session_local_datetime) as date,
       session_id,
       1 as sessions
from reporting_base_prod.shared.session s
join reporting_base_prod.shared.media_source_channel_mapping m on m.media_source_hash = s.media_source_hash
join edw_prod.data_model.dim_store ds on ds.store_id = s.store_id
where session_id is not null
    and ((to_date(session_local_datetime) between dateadd(day,-2,$current_year_start_date) and $current_year_end_date)
        or (to_date(session_local_datetime) between dateadd(day,-2,$last_year_start_date) and $last_year_end_date));

create or replace temporary table _leads as
select distinct
       l.store_id,
       store_brand as store_brand_name,
       store_region,
       store_country,
       cast(case when lower(gender) = 'm' and lower(store_brand_abbr) = 'fl' then true else false end as boolean) as is_male_customer,
       cast(is_scrubs_customer as boolean) as is_fl_scrubs_customer,
       to_date(l.registration_local_datetime) as date,
       session_id,
       1 as primary_leads
from edw_prod.data_model.fact_registration l
join edw_prod.data_model.dim_store ds on ds.store_id = l.store_id
join edw_prod.data_model.dim_customer c on c.customer_id = l.customer_id
where is_secondary_registration = false
    and is_retail_registration = false
    and is_fake_retail_registration = false
    and session_id is not null
    and ((to_date(l.registration_local_datetime) between $current_year_start_date and $current_year_end_date)
            or (to_date(l.registration_local_datetime) between $last_year_start_date and $last_year_end_date));

create or replace temporary table _from_leads as
select distinct
        l.store_id,
        store_brand as store_brand_name,
        store_region,
        store_country,
        cast(case when lower(gender) = 'm' and lower(store_brand_abbr) = 'fl' then true else false end as boolean) as is_male_customer,
        cast(is_scrubs_customer as boolean) as is_fl_scrubs_customer,
        to_date(l.registration_local_datetime) as date,
        l.session_id,
        1 as vips_from_leads_7d,
        unit_count,
        reporting_usd_conversion_rate,
        product_gross_revenue_local_amount,
        product_order_cash_margin_pre_return_local_amount as product_order_cash_margin
from edw_prod.data_model.fact_registration l
join edw_prod.data_model.dim_store ds on ds.store_id = l.store_id
join edw_prod.data_model.dim_customer c on c.customer_id = l.customer_id
join edw_prod.data_model.fact_activation a on a.customer_id = l.customer_id
    and datediff(day,l.registration_local_datetime,activation_local_datetime) < 7
    and datediff(day,l.registration_local_datetime,activation_local_datetime) >= 0
    and is_retail_vip = false
join edw_prod.data_model.fact_order o on a.order_id = o.order_id
where is_secondary_registration = false
    and is_retail_registration = false
    and is_fake_retail_registration = false
    and l.session_id is not null
    and ((to_date(l.registration_local_datetime) between $current_year_start_date and $current_year_end_date)
    or (to_date(l.registration_local_datetime) between $last_year_start_date and $last_year_end_date));

create or replace temporary table _same_session_vips as
select distinct
        a.store_id,
        store_brand as store_brand_name,
        store_region,
        store_country,
        cast(case when lower(gender) = 'm' and lower(store_brand_abbr) = 'fl' then true else false end as boolean) as is_male_customer,
        cast(is_scrubs_customer as boolean) as is_fl_scrubs_customer,
        a.session_id,
        to_date(activation_local_datetime) as date,
        1 as same_session_vips,
        unit_count,
        reporting_usd_conversion_rate,
        product_gross_revenue_local_amount,
        product_order_cash_margin_pre_return_local_amount as product_order_cash_margin
from edw_prod.data_model.fact_activation a
join edw_prod.data_model.dim_customer c on c.customer_id = a.customer_id
join edw_prod.data_model.fact_order o on a.order_id = o.order_id
join edw_prod.data_model.dim_store ds on ds.store_id = a.store_id
where is_retail_vip = false
    and a.session_id is not null
    and ((to_date(activation_local_datetime) between $current_year_start_date and $current_year_end_date)
            or (to_date(activation_local_datetime) between $last_year_start_date and $last_year_end_date));

create or replace temporary table _combos as
select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, date, session_id from _sessions
union select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, date, session_id from _leads
union select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, date, session_id from _from_leads
union select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, date, session_id from _same_session_vips;

create or replace temporary table _conversions_agg as
select c.*,
        utm_medium,
        utm_source,
        utm_campaign,
        utm_content,
        utm_term,
        channel_type,
        channel,
        subchannel,
        vendor,

        sessions as sessions,
        primary_leads as primary_leads,

        vips_from_leads_7d as vips_from_leads_7d,
        l.unit_count as unit_count_from_leads,
        l.product_gross_revenue_local_amount * l.reporting_usd_conversion_rate as product_gross_revenue_from_leads_usd,
        l.product_order_cash_margin * l.reporting_usd_conversion_rate as product_order_cash_margin_from_leads_usd,

        same_session_vips as same_session_vips,
        v.unit_count as unit_count_ss_vips,
        v.product_gross_revenue_local_amount * v.reporting_usd_conversion_rate as product_gross_revenue_ss_vips_usd,
        v.product_order_cash_margin * v.reporting_usd_conversion_rate as product_order_cash_margin_ss_vips_usd

from _combos c
join _session_mapping m on c.session_id = m.session_id
left join _sessions s on s.session_id = c.session_id
    and s.store_id = c.store_id
    and s.store_brand_name = c.store_brand_name
    and s.store_region = c.store_region
    and s.store_country = c.store_country
    and s.is_male_customer = c.is_male_customer
    and s.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and s.date = c.date
left join _leads r on r.session_id = c.session_id
    and r.store_id = c.store_id
    and r.store_brand_name = c.store_brand_name
    and r.store_region = c.store_region
    and r.store_country = c.store_country
    and r.is_male_customer = c.is_male_customer
    and r.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and r.date = c.date
left join _from_leads l on l.session_id = c.session_id
    and l.store_id = c.store_id
    and l.store_brand_name = c.store_brand_name
    and l.store_region = c.store_region
    and l.store_country = c.store_country
    and l.is_male_customer = c.is_male_customer
    and l.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and l.date = c.date
left join _same_session_vips v on v.session_id = c.session_id
    and v.store_id = c.store_id
    and v.store_brand_name = c.store_brand_name
    and v.store_region = c.store_region
    and v.store_country = c.store_country
    and v.is_male_customer = c.is_male_customer
    and v.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and v.date = c.date;

-- final output table
create or replace transient table reporting_media_prod.dbo.daily_click_attribution as
select  store_id,
        store_brand_name,
        store_region,
        store_country,
        is_male_customer,
        is_fl_scrubs_customer,
        utm_medium,
        utm_source,
        utm_campaign,
        utm_content,
        utm_term,
        channel_type,
        channel,
        subchannel,
        vendor,
        date,

        sum(sessions) as sessions,
        sum(primary_leads) as primary_leads,

        sum(vips_from_leads_7d) as vips_from_leads_7d,
        sum(unit_count_from_leads) as unit_count_from_leads,
        sum(product_gross_revenue_from_leads_usd) as product_gross_revenue_from_leads_usd,
        sum(product_order_cash_margin_from_leads_usd) as product_order_cash_margin_from_leads_usd,

        sum(same_session_vips) as same_session_vips,
        sum(unit_count_ss_vips) as unit_count_ss_vips,
        sum(product_gross_revenue_ss_vips_usd) as product_gross_revenue_ss_vips_usd,
        sum(product_order_cash_margin_ss_vips_usd) as product_order_cash_margin_ss_vips_usd,

        $session_data_last_updated as session_data_last_updated,
        $registration_data_last_updated as registration_data_last_updated,
        $activation_data_last_updated as activation_data_last_updated

from _conversions_agg
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;


------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

-- tableau datasource: MEDDS16b - Daily Click Attribution with Spend
-- dashboard:  MED98 - Daily Click Attribution
-- datasource includes media spend at the channel / subchannel grain

------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

create or replace temporary table _daily_click_attribution as
select  store_id,
        store_brand_name,
        store_region,
        store_country,
        is_male_customer,
        is_fl_scrubs_customer,
        channel_type,
        channel,
        subchannel,
        vendor,
        date,

        sum(sessions) as sessions,
        sum(primary_leads) as primary_leads,

        sum(vips_from_leads_7d) as vips_from_leads_7d,
        sum(unit_count_from_leads) as unit_count_from_leads,
        sum(product_gross_revenue_from_leads_usd) as product_gross_revenue_from_leads_usd,
        sum(product_order_cash_margin_from_leads_usd) as product_order_cash_margin_from_leads_usd,

        sum(same_session_vips) as same_session_vips,
        sum(unit_count_ss_vips) as unit_count_ss_vips,
        sum(product_gross_revenue_ss_vips_usd) as product_gross_revenue_ss_vips_usd,
        sum(product_order_cash_margin_ss_vips_usd) as product_order_cash_margin_ss_vips_usd

from reporting_media_prod.dbo.daily_click_attribution
group by 1,2,3,4,5,6,7,8,9,10,11;


create or replace temporary table _fmc as
select f.store_id,
       store_brand as store_brand_name,
       store_region,
       store_country,
       is_mens_flag as is_male_customer,
       is_scrubs_flag as is_fl_scrubs_customer,
       'Paid' as channel_type,
       channel,
       subchannel,
       vendor,
       media_cost_date::date as date,
       sum(cost) as spend_usd

from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where cost > 0
    and ((to_date(media_cost_date) between $current_year_start_date and $current_year_end_date)
            or (to_date(media_cost_date) between $last_year_start_date and $last_year_end_date))
group by 1,2,3,4,5,6,7,8,9,10,11;


create or replace temporary table _combos as
select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, channel_type, channel, subchannel, vendor, date from _daily_click_attribution
union
select distinct store_id, store_brand_name, store_region, store_country, is_male_customer, is_fl_scrubs_customer, channel_type, channel, subchannel, vendor, date from _fmc;


create or replace temporary table _metrics_combined as
select c.*,
       case when c.channel = 'programmatic-gdn' and c.subchannel in ('demandgen','discovery') then 'demandgen / discovery'
            when c.channel = 'print' and c.subchannel in ('ancc','insert') then c.subchannel || ' - ' || c.vendor
            when c.channel = 'print' and c.subchannel = 'other' then c.vendor
            when c.channel = 'tv+streaming' and c.subchannel in ('tv','amazon stv') then c.subchannel
            when c.channel = 'tv+streaming' and c.subchannel ilike '%streaming%' and c.vendor ilike '%blisspoint%' then 'streaming - blisspoint'
            when c.channel = 'tv+streaming' and c.subchannel ilike '%streaming%' then 'streaming' || ' - ' || c.vendor
            when c.channel = 'testing' and c.vendor = 'adquick' then 'adquick ooh'
            when c.channel = 'programmatic' and c.vendor in ('stackadapt','yumpu','tradedesk') then c.vendor
            when c.channel in ('youtube','snapchat','tiktok','fb+ig') then c.channel
            else c.subchannel
            end as subchannel_new,
       spend_usd,
       sessions,
       primary_leads,
       vips_from_leads_7d,
       unit_count_from_leads,
       product_gross_revenue_from_leads_usd,
       product_order_cash_margin_from_leads_usd,
       same_session_vips,
       unit_count_ss_vips,
       product_gross_revenue_ss_vips_usd,
       product_order_cash_margin_ss_vips_usd
from _combos c
left join _fmc f on f.store_id = c.store_id
    and f.store_brand_name = c.store_brand_name
    and f.store_region = c.store_region
    and f.store_country = c.store_country
    and f.is_male_customer = c.is_male_customer
    and f.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and f.channel_type = c.channel_type
    and f.channel = c.channel
    and f.subchannel = c.subchannel
    and f.vendor = c.vendor
    and f.date = c.date
left join _daily_click_attribution d on d.store_id = c.store_id
    and d.store_brand_name = c.store_brand_name
    and d.store_region = c.store_region
    and d.store_country = c.store_country
    and d.is_male_customer = c.is_male_customer
    and d.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and d.channel_type = c.channel_type
    and d.channel = c.channel
    and d.subchannel = c.subchannel
    and d.vendor = c.vendor
    and d.date = c.date;


create or replace transient table reporting_media_prod.dbo.daily_click_attribution_with_spend as
select store_id,
       store_brand_name,
       store_region,
       store_country,
       is_male_customer,
       is_fl_scrubs_customer,
       channel_type,
       channel,
       subchannel_new as subchannel,
       date,

       coalesce(sum(spend_usd),0) as spend_usd,
       coalesce(sum(sessions),0) as sessions,
       coalesce(sum(primary_leads),0) as primary_leads,
       coalesce(sum(vips_from_leads_7d),0) as vips_from_leads_7d,
       coalesce(sum(unit_count_from_leads),0) as unit_count_from_leads,
       coalesce(sum(product_gross_revenue_from_leads_usd),0) as product_gross_revenue_from_leads_usd,
       coalesce(sum(product_order_cash_margin_from_leads_usd),0) as product_order_cash_margin_from_leads_usd,
       coalesce(sum(same_session_vips),0) as same_session_vips,
       coalesce(sum(unit_count_ss_vips),0) as unit_count_ss_vips,
       coalesce(sum(product_gross_revenue_ss_vips_usd),0) as product_gross_revenue_ss_vips_usd,
       coalesce(sum(product_order_cash_margin_ss_vips_usd),0) as product_order_cash_margin_ss_vips_usd,

       $session_data_last_updated as session_data_last_updated,
       $registration_data_last_updated as registration_data_last_updated,
       $activation_data_last_updated as activation_data_last_updated

from _metrics_combined
group by 1,2,3,4,5,6,7,8,9,10;
