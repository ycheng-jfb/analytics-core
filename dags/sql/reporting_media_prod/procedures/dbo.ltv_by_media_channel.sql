
----------  1. Get all vips that became leads through channel, subchannel  ----------
--leads with channel mappings included
create or replace temporary table _leads as
select
    registration_channel as channel,
    registration_subchannel as subchannel,
    registration_channel_type as channel_type,
    l.customer_id,
    c.gender,
    c.is_scrubs_customer
from edw_prod.data_model.fact_registration l
join edw_prod.data_model.dim_customer c on c.customer_id = l.customer_id
where is_secondary_registration = false
    and is_retail_registration = false
    and is_fake_retail_registration = false
    and lower(channel) not in ('unclassified');

-- getting all vips from leads
-- store information from fact activation
create or replace temporary table _final_vips as
select
    case when lower(store_brand) = 'fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
         when lower(store_brand) = 'fabletics' and lower(gender) = 'm' then 'Fabletics Men'
         else store_brand end as store_brand_name,
    store_region as region,
    store_country as country,
    l.channel,
    l.subchannel,
    l.channel_type,
    l.customer_id,
    vip_cohort_month_date,
    cast(v.activation_local_datetime as date) as vip_date
from _leads l
join edw_prod.data_model.fact_activation v on v.customer_id = l.customer_id
join edw_prod.data_model.dim_store s on v.store_id = s.store_id
where vip_cohort_month_date < date_trunc(month,current_date())
    and activation_local_datetime::date >= '2021-01-01';


---------- 2. join final vips to ltv table  ----------
-- unique row ids: store, channel, cohort date, tenure
-- vip / ltv monthly / ltv ltd tables all have new vip cohort month for re-activated customers
-- one store ID per vip cohort activation month (if a customer activates through FL but then buys YTY, that revenue goes to FL)
create or replace transient table reporting_media_prod.dbo.ltv_by_media_channel as
select
    v.store_brand_name,
    v.channel,
    v.subchannel,
    v.channel_type,
    v.region,
    v.country,
    ltv.vip_cohort_month_date,
    ltv.month_date,
    'M' || cast(datediff(month,ltv.vip_cohort_month_date,ltv.month_date)+1 as varchar) as vip_tenure,

    count(ltv.customer_id) as initial_cohort_size,
    sum(cash_gross_profit) as cash_gross_profit,
        -- indicator of someone just being billed (includes member credits)
    sum(product_gross_profit) as product_gross_profit,
        -- shows that a customer is actively engaging with our brand and buying product
        -- (always lower than cash because of unredeemed credits)

    --getting AOV and activating unit count from ltv ltd table
    sum(activating_product_gross_revenue) as activating_product_gross_revenue,
    sum(activating_product_order_unit_count) as activating_product_order_unit_count,

    current_timestamp()::timestamp_ltz as meta_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime

from _final_vips v
join edw_prod.analytics_base.customer_lifetime_value_monthly ltv on v.customer_id = ltv.customer_id
    and v.vip_cohort_month_date = ltv.vip_cohort_month_date
left join (select distinct customer_id,
                           vip_cohort_month_date,
                           max(activating_product_gross_revenue) as activating_product_gross_revenue,
                           max(activating_product_order_unit_count) as activating_product_order_unit_count
                from edw_prod.analytics_base.customer_lifetime_value_ltd
                group by 1,2) ltd on ltd.customer_id = ltv.customer_id
                                         and ltd.vip_cohort_month_date = ltv.month_date
                                         and ltd.vip_cohort_month_date = ltv.vip_cohort_month_date
where ltv.vip_cohort_month_date >= '2021-01-01'
    and month_date < date_trunc(month,current_date())
    and datediff(month,ltv.vip_cohort_month_date,ltv.month_date)+1 < 13 -- limit to M12
group by 1,2,3,4,5,6,7,8,9;
