
set low_watermark_date = '2018-01-01';
set high_watermark_date = iff(
        convert_timezone('America/Los_Angeles', 'Europe/London', current_timestamp())::date > current_date(),
        dateadd(day, 1, current_date()),
        current_date()
    );

------------------------------------------------------------------------------------
-- all conversions from central source --

create or replace temporary table _all_conversions as
select event_store_id as store_id,
       ifnull(finance_specialty_store, 'none') as finance_specialty_store,

       cast(iff(is_cross_promo = true, 1, 0) as boolean) as is_fk_free_trial_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(customer_gender) = 'm', 1, 0) as boolean) as is_fl_mens_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(is_scrubs_customer) = true, 1, 0) as boolean) as is_fl_scrubs_customer,

       cast(iff(lower(store_brand) = 'yitty', 0, iff(is_retail_registration = true, 1, 0)) as boolean) as retail_lead,
       cast(iff(lower(store_brand) = 'yitty', 0, iff(is_retail_vip = true, 1, 0)) as boolean) as retail_vip,
       cast(iff(retail_lead = true or retail_vip = true, 1, 0) as boolean) as retail_customer,

       date,

        -- leads
        sum(primary_leads) as primary_leads,
        sum(secondary_leads) as secondary_leads,
        sum(reactivated_leads) as reactivated_leads,
        SUM(iff(retail_lead = true, primary_leads, 0))as retail_leads,

        -- vips from leads
        sum(vip_from_leads_30min) as vips_from_leads_30m,
        sum(vip_from_leads_2h) as vips_from_leads_2h,
        sum(vip_from_leads_24h) as vips_from_leads_24h,
        sum(vip_from_leads_d1) as vips_from_leads_d1,
        sum(vip_from_leads_d2) as vips_from_leads_d2,
        sum(vip_from_leads_d3) as vips_from_leads_d3,
        sum(vip_from_leads_d4) as vips_from_leads_d4,
        sum(vip_from_leads_d5) as vips_from_leads_d5,
        sum(vip_from_leads_d6) as vips_from_leads_d6,
        sum(vip_from_leads_d7) as vips_from_leads_d7,
        sum(vip_from_leads_d8_d30) as vips_from_leads_d8_d30,
        sum(vip_from_leads_m1) as vips_from_leads_m1,
        sum(vip_from_leads_m2) as vips_from_leads_m2,
        sum(vip_from_leads_m3) as vips_from_leads_m3,
        sum(vip_from_leads_m4) as vips_from_leads_m4,
        sum(vip_from_leads_m5) as vips_from_leads_m5,
        sum(vip_from_leads_m6) as vips_from_leads_m6,
        sum(vip_from_leads_m7) as vips_from_leads_m7,
        sum(vip_from_leads_m8) as vips_from_leads_m8,
        sum(vip_from_leads_m9) as vips_from_leads_m9,
        sum(vip_from_leads_m10) as vips_from_leads_m10,
        sum(vip_from_leads_m11) as vips_from_leads_m11,
        sum(vip_from_leads_m12) as vips_from_leads_m12,

        -- vips from reactivated leads
        sum(vips_from_reactivated_leads) as total_vips_from_reactivated_leads,
        sum(vip_from_reactivated_leads_30min) as vips_from_reactivated_leads_30m,
        sum(vip_from_reactivated_leads_2h) as vips_from_reactivated_leads_2h,
        sum(vip_from_reactivated_leads_24h) as vips_from_reactivated_leads_24h,
        sum(vips_from_reactivated_leads_m1) as vips_from_reactivated_leads_m1,

        -- vips on date
        sum(new_vips) as total_vips_on_date,
        sum(new_vips_d1) as vips_on_date_d1,
        sum(new_vips_d2) as vips_on_date_d2,
        sum(new_vips_d3) as vips_on_date_d3,
        sum(new_vips_d4) as vips_on_date_d4,
        sum(new_vips_d5) as vips_on_date_d5,
        sum(new_vips_d6) as vips_on_date_d6,
        sum(new_vips_d7) as vips_on_date_d7,
        sum(new_vips_d1_d7) as vips_on_date_d1_d7,
        sum(new_vips_d2_d7) as vips_on_date_d2_d7,
        sum(new_vips_d8_d30) as vips_on_date_d8_d30,
        sum(new_vips_d31_d89) as vips_on_date_d31_d89,
        sum(new_vips_d8_d30 + new_vips_d31_d89) as vips_on_date_d8_d90,
        sum(new_vips_d90plus) as vips_on_date_d90plus,
        sum(new_vips_m1) as vips_on_date_m1,
        sum(new_vips_m2) as vips_on_date_m2,
        sum(new_vips_m3) as vips_on_date_m3,
        sum(new_vips_m4) as vips_on_date_m4,
        sum(new_vips_m5) as vips_on_date_m5,
        sum(new_vips_m6) as vips_on_date_m6,
        sum(new_vips_m7) as vips_on_date_m7,
        sum(new_vips_m8) as vips_on_date_m8,
        sum(new_vips_m9) as vips_on_date_m9,
        sum(new_vips_m10) as vips_on_date_m10,
        sum(new_vips_m11) as vips_on_date_m11,
        sum(new_vips_m12) as vips_on_date_m12,
        sum(new_vips_m13plus) as vips_on_date_m13plus,

        sum(reactivated_vips) as reactivated_vips,
        sum(retail_vips) as retail_vips,
        sum(varsity_vips) as varsity_vips,

        -- vips on date excluding reactivated leads
        sum(new_vips_d1 - new_vips_d1_reactivated_leads) as vips_on_date_d1_excluding_reactivated_leads,

        -- vips on date from reactivated leads
        sum(vip_from_reactivated_leads_d1) as vips_from_reactivated_leads_d1,
        sum(new_vips_d1_reactivated_leads) as vips_on_date_d1_reactivated_leads,
        sum(new_vips_m1_reactivated_leads) as vips_on_date_m1_reactivated_leads,

        -- cancellations
        sum(cancels) as total_cancels_on_date,
        sum(passive_cancel) as passive_cancels_on_date,
        sum(d1_cancels) as cancels_on_date_d1,
        sum(m1_cancels) as cancels_on_date_m1,
        sum(m2_cancels) as cancels_on_date_m2,
        sum(m3_cancels) as cancels_on_date_m3,
        sum(m4_m12_cancels) as cancels_on_date_m4_m12,
        sum(m13plus_cancels) as cancels_on_date_m13plus,

        -- bop
        sum(bop_vips) as bop_vips

from edw_prod.analytics_base.acquisition_media_spend_daily_agg a
join edw_prod.data_model_jfb.dim_store ds on ds.store_id = a.event_store_id
where lower(currency_type) = 'usd'
    and lower(date_object) = 'placed'
    and date >= $low_watermark_date
group by 1,2,3,4,5,6,7,8,9;


------------------------------------------------------------------------------------
-- order metrics --

create or replace temporary table _order_metrics as
select
       fs.store_id,
       ifnull(finance_specialty_store, 'none') as finance_specialty_store,
       date,
       case when lower(fs.currency_object) = 'usd' then 'USD' else 'Local' end as currency,

       cast(iff(lower(ds.store_brand) = 'fabkids' and fs.is_cross_promo = true, 1, 0) as boolean) as is_fk_free_trial_flag,
       cast(iff(lower(ds.store_brand) = 'fabletics' and lower(fs.gender) = 'm', 1, 0) as boolean) as is_fl_mens_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(fs.is_scrubs_customer) = true, 1, 0) as boolean) as is_fl_scrubs_customer,


       cast(0 as boolean) as retail_lead,
       cast(iff(lower(ds.store_brand) = 'fabletics', is_retail_vip, 0) as boolean) as retail_vip,
       cast(iff(retail_vip = true, 1, 0)as boolean) as retail_customer,

       sum(iff(lower(membership_order_type_l3) = 'first guest', product_order_count, 0)) as first_guest_product_order_count,
       sum(iff(lower(membership_order_type_l3) = 'first guest', product_gross_revenue, 0)) as first_guest_product_gross_revenue,
       sum(iff(lower(membership_order_type_l3) = 'first guest', product_margin_pre_return, 0 )) as first_guest_product_margin_pre_return,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_order_count, 0)) as activating_vip_product_order_count,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_gross_revenue, 0)) as activating_vip_product_gross_revenue,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_margin_pre_return, 0)) as activating_vip_product_margin_pre_return

from edw_prod.analytics_base.finance_sales_ops fs
join edw_prod.data_model_jfb.dim_order_membership_classification c on c.order_membership_classification_key = fs.order_membership_classification_key
join edw_prod.data_model_jfb.dim_store ds on ds.store_id = fs.store_id
where date_object = 'placed'
    and lower(membership_order_type_l3) in ('first guest','activating vip')
    and lower(fs.currency_object) in ('local','usd')
    and ds.store_type <> 'Retail'
    and date >= $low_watermark_date
group by 1,2,3,4,5,6,7,8,9,10

union all

select
       fs.store_id,
       ifnull(finance_specialty_store, 'none') as finance_specialty_store,
       date,
       'Local' as currency,

       cast(iff(lower(ds.store_brand) = 'fabkids' and fs.is_cross_promo = true, 1, 0) as boolean) as is_fk_free_trial_flag,
       cast(iff(lower(ds.store_brand) = 'fabletics' and lower(fs.gender) = 'm', 1, 0) as boolean) as is_fl_mens_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(fs.is_scrubs_customer) = true, 1, 0) as boolean) as is_fl_scrubs_customer,

       cast(0 as boolean) as retail_lead,
       cast(iff(lower(ds.store_brand) = 'fabletics', is_retail_vip, 0) as boolean) as retail_vip,
       cast(iff(retail_vip = true, 1, 0)as boolean) as retail_customer,

       sum(iff(lower(membership_order_type_l3) = 'first guest', product_order_count, 0)) as first_guest_product_order_count,
       sum(iff(lower(membership_order_type_l3) = 'first guest', product_gross_revenue, 0)) as first_guest_product_gross_revenue,
       sum(iff(lower(membership_order_type_l3) = 'first guest', product_margin_pre_return, 0 )) as first_guest_product_margin_pre_return,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_order_count, 0)) as activating_vip_product_order_count,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_gross_revenue, 0)) as activating_vip_product_gross_revenue,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_margin_pre_return, 0)) as activating_vip_product_margin_pre_return

from edw_prod.analytics_base.finance_sales_ops fs
join edw_prod.data_model_jfb.dim_order_membership_classification c on c.order_membership_classification_key = fs.order_membership_classification_key
join edw_prod.data_model_jfb.dim_store ds on ds.store_id = fs.store_id
where date_object = 'placed'
    and lower(membership_order_type_l3) in ('first guest','activating vip')
    and lower(store_region) = 'na'
    and lower(fs.currency_object) in ('usd')
    and ds.store_type <> 'Retail'
    and date >= $low_watermark_date
group by 1,2,3,4,5,6,7,8,9;


------------------------------------------------------------------------------------
-- media spend --

create or replace temporary table _total_spend as
select
    st.store_id,
    ifnull(specialty_store, 'none') as finance_specialty_store,
    cast(media_cost_date as date) as date,

    cast(0 as boolean) as is_fk_free_trial_flag, -- hardcode all spend to the paid vips
    cast(iff(lower(store_brand) = 'fabletics' and is_mens_flag = 1, 1, 0) as boolean) as is_fl_mens_flag,
    cast(iff(lower(store_brand) = 'fabletics' and is_scrubs_flag = 1, 1, 0) as boolean) as is_fl_scrubs_customer,

    cast(0 as boolean) as retail_lead,
    cast(iff(lower(channel) = 'physical partnerships', 1, 0) as boolean) as retail_vip, -- only varsity or retail spend
    cast(iff(retail_vip = true, 1, 0) as boolean) as retail_customer,

    channel,
    fmc.local_store_conv_rate,
    fmc.spend_date_usd_conv_rate,

    sum(fmc.cost) as cost,
    sum(iff(lower(fmc.targeting) != 'lead retargeting', fmc.cost, 0)) as prospecting_cost

from reporting_media_prod.dbo.fact_media_cost_scrubs_split fmc
join edw_prod.data_model_jfb.dim_store st on st.store_id = fmc.store_id
where lower(fmc.targeting) not in ('vip retargeting','purchase retargeting','free alpha')
    and to_date(media_cost_date) >= $low_watermark_date
    and to_date(media_cost_date) < $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12;


create or replace temporary table _total_spend_currency as
select
    store_id,
    finance_specialty_store,
    date,
    'Local' as currency,
    is_fk_free_trial_flag,
    is_fl_mens_flag,
    is_fl_scrubs_customer,
    retail_lead,
    retail_vip,
    retail_customer,
    channel,
    cost * local_store_conv_rate as cost,
    prospecting_cost * local_store_conv_rate as prospecting_cost
from _total_spend

union all

select
    store_id,
    finance_specialty_store,
    date,
    'USD' as currency,
    is_fk_free_trial_flag,
    is_fl_mens_flag,
    is_fl_scrubs_customer,
    retail_lead,
    retail_vip,
    retail_customer,
    channel,
    cost * spend_date_usd_conv_rate as cost,
    prospecting_cost * spend_date_usd_conv_rate as prospecting_cost
from _total_spend;

create or replace temp table _channel_spend as
select
    store_id,
    finance_specialty_store,
    is_fk_free_trial_flag,
    is_fl_mens_flag,
    is_fl_scrubs_customer,
    retail_lead,
    retail_vip,
    retail_customer,
    date,
    currency,
    sum(ts.prospecting_cost) as prospecting_spend,
    sum(ts.cost) as total_spend,

    sum(case when lower(ts.channel) = 'meta' then cost else 0 end) as facebook_spend,
    sum(case when lower(ts.channel) = 'snapchat' then cost else 0 end) as snapchat_spend,
    sum(case when lower(ts.channel) = 'pinterest' then cost else 0 end) as pinterest_spend,
    sum(case when lower(ts.channel) = 'tiktok' then cost else 0 end) as tiktok_spend,
    sum(case when lower(ts.channel) = 'reddit' then cost else 0 end) as reddit_spend,
    sum(case when lower(ts.channel) = 'twitter' then cost else 0 end) as twitter_spend,

    sum(case when lower(ts.channel) = 'applovin' then cost else 0 end) as applovin_spend,
    sum(case when lower(ts.channel) = 'linkedin' then cost else 0 end) as linkedin_spend,
    sum(case when lower(ts.channel) = 'nift' then cost else 0 end) as nift_spend,
    sum(case when lower(ts.channel) = 'rokt' then cost else 0 end) as rokt_spend,
    sum(case when lower(ts.channel) = 'twitch' then cost else 0 end) as twitch_spend,

    sum(case when lower(ts.channel) = 'google other' then cost else 0 end) as google_other_spend,
    sum(case when lower(ts.channel) = 'ooh' then cost else 0 end) as ooh_spend,
    sum(case when lower(ts.channel) = 'celebrity' then cost else 0 end) as celebrity_spend,

    sum(case when lower(ts.channel) = 'non branded search' then cost else 0 end) as non_branded_search_spend,
    sum(case when lower(ts.channel) = 'branded search' then cost else 0 end) as branded_search_spend,
    sum(case when lower(ts.channel) = 'shopping' then cost else 0 end) as shopping_spend,
    sum(case when lower(ts.channel) = 'youtube'  then cost else 0 end) as youtube_spend,

    sum(case when lower(ts.channel) = 'programmatic'  then cost else 0 end) as programmatic_spend,

    sum(case when lower(ts.channel) = 'influencers' or lower(ts.channel) = 'ambassadors' then cost else 0 end) as influencers_spend,
    sum(case when lower(ts.channel) = 'tv+streaming' then cost else 0 end) as tv_and_streaming_spend,
    sum(case when lower(ts.channel) = 'print' then cost else 0 end) as print_spend,
    sum(case when lower(ts.channel) = 'radio/podcast' then cost else 0 end) as radio_podcast_spend,
    sum(case when lower(ts.channel) = 'affiliate' then cost else 0 end) as affiliate_spend,
    sum(case when lower(ts.channel) = 'testing'  then cost else 0 end) as testing_spend,
    sum(case when lower(ts.channel) = 'physical partnerships' then cost else 0 end) as physical_partnerships_spend

from _total_spend_currency ts
group by 1,2,3,4,5,6,7,8,9,10;


------------------------------------------------------------------------------------
-- create scaffold to house all combinations --

create or replace temporary table _scaffold as
select distinct store_id, finance_specialty_store, date, is_fk_free_trial_flag, is_fl_mens_flag, is_fl_scrubs_customer, retail_lead, retail_vip, retail_customer
from _all_conversions
union
select distinct store_id, finance_specialty_store, date, is_fk_free_trial_flag, is_fl_mens_flag, is_fl_scrubs_customer, retail_lead, retail_vip, retail_customer
from _channel_spend
union
select distinct store_id, finance_specialty_store, date, is_fk_free_trial_flag, is_fl_mens_flag, is_fl_scrubs_customer, retail_lead, retail_vip, retail_customer
from _order_metrics;

create or replace temporary table _currency
(currency varchar(55));
insert into _currency
values
('USD'),
('Local');

create or replace temporary table _scaffold_final as
select
    cs.*, c.*
from _scaffold cs
join _currency c;


------------------------------------------------------------------------------------
-- final output --

create or replace temporary table _attribution_daily_acquisition_metrics_temp as
select
    st.store_brand,
    iff(lower(s.finance_specialty_store) = 'none', st.store_country, s.finance_specialty_store) as store_country,
    st.store_region,
    s.store_id,
    s.date,
    s.is_fk_free_trial_flag,
    s.is_fl_mens_flag,
    s.is_fl_scrubs_customer,
    s.retail_lead,
    s.retail_vip,
    s.retail_customer,
    s.currency,

    -- leads
    coalesce(sum(c.primary_leads),0) as primary_leads,
    coalesce(sum(c.secondary_leads),0) as secondary_leads,
    coalesce(sum(c.reactivated_leads),0) as reactivated_leads,
    coalesce(sum(c.retail_leads),0) as retail_leads,

    -- vips from leads
    coalesce(sum(c.vips_from_leads_30m),0) as vips_from_leads_30m,
    coalesce(sum(c.vips_from_leads_2h),0) as vips_from_leads_2h,
    coalesce(sum(c.vips_from_leads_24h),0) as vips_from_leads_24h,
    coalesce(sum(c.vips_from_leads_d1),0) as vips_from_leads_d1,
    coalesce(sum(c.vips_from_leads_d2),0) as vips_from_leads_d2,
    coalesce(sum(c.vips_from_leads_d3),0) as vips_from_leads_d3,
    coalesce(sum(c.vips_from_leads_d4),0) as vips_from_leads_d4,
    coalesce(sum(c.vips_from_leads_d5),0) as vips_from_leads_d5,
    coalesce(sum(c.vips_from_leads_d6),0) as vips_from_leads_d6,
    coalesce(sum(c.vips_from_leads_d7),0) as vips_from_leads_d7,
    coalesce(sum(c.vips_from_leads_d8_d30),0) as vips_from_leads_d8_d30,

    coalesce(sum(c.vips_from_leads_m1),0) as vips_from_leads_m1,
    coalesce(sum(c.vips_from_leads_m2),0) as vips_from_leads_m2,
    coalesce(sum(c.vips_from_leads_m3),0) as vips_from_leads_m3,
    coalesce(sum(c.vips_from_leads_m4),0) as vips_from_leads_m4,
    coalesce(sum(c.vips_from_leads_m5),0) as vips_from_leads_m5,
    coalesce(sum(c.vips_from_leads_m6),0) as vips_from_leads_m6,
    coalesce(sum(c.vips_from_leads_m7),0) as vips_from_leads_m7,
    coalesce(sum(c.vips_from_leads_m8),0) as vips_from_leads_m8,
    coalesce(sum(c.vips_from_leads_m9),0) as vips_from_leads_m9,
    coalesce(sum(c.vips_from_leads_m10),0) as vips_from_leads_m10,
    coalesce(sum(c.vips_from_leads_m11),0) as vips_from_leads_m11,
    coalesce(sum(c.vips_from_leads_m12),0) as vips_from_leads_m12,

    -- vips from reactivated leads
    coalesce(sum(c.total_vips_from_reactivated_leads),0) as total_vips_from_reactivated_leads,
    coalesce(sum(c.vips_from_reactivated_leads_30m),0) as vips_from_reactivated_leads_30m,
    coalesce(sum(c.vips_from_reactivated_leads_2h),0) as vips_from_reactivated_leads_2h,
    coalesce(sum(c.vips_from_reactivated_leads_24h),0) as vips_from_reactivated_leads_24h,
    coalesce(sum(c.vips_from_reactivated_leads_m1),0) as vips_from_reactivated_leads_m1,


    -- vips on date
    coalesce(sum(c.total_vips_on_date),0) as total_vips_on_date,

    coalesce(sum(c.vips_on_date_d1),0) as vips_on_date_d1,
    coalesce(sum(c.vips_on_date_d2),0) as vips_on_date_d2,
    coalesce(sum(c.vips_on_date_d3),0) as vips_on_date_d3,
    coalesce(sum(c.vips_on_date_d4),0) as vips_on_date_d4,
    coalesce(sum(c.vips_on_date_d5),0) as vips_on_date_d5,
    coalesce(sum(c.vips_on_date_d6),0) as vips_on_date_d6,
    coalesce(sum(c.vips_on_date_d7),0) as vips_on_date_d7,
    coalesce(sum(c.vips_on_date_d1_d7),0) as vips_on_date_d1_d7,
    coalesce(sum(c.vips_on_date_d2_d7),0) as vips_on_date_d2_d7,
    coalesce(sum(c.vips_on_date_d8_d30),0) as vips_on_date_d8_d30,
    coalesce(sum(c.vips_on_date_d31_d89),0) as vips_on_date_d31_d89,
    coalesce(sum(c.vips_on_date_d8_d90),0) as vips_on_date_d8_d90,
    coalesce(sum(c.vips_on_date_d90plus),0) as vips_on_date_d90plus,

    coalesce(sum(c.vips_on_date_m1),0) as vips_on_date_m1,
    coalesce(sum(c.vips_on_date_m2),0) as vips_on_date_m2,
    coalesce(sum(c.vips_on_date_m3),0) as vips_on_date_m3,
    coalesce(sum(c.vips_on_date_m4),0) as vips_on_date_m4,
    coalesce(sum(c.vips_on_date_m5),0) as vips_on_date_m5,
    coalesce(sum(c.vips_on_date_m6),0) as vips_on_date_m6,
    coalesce(sum(c.vips_on_date_m7),0) as vips_on_date_m7,
    coalesce(sum(c.vips_on_date_m8),0) as vips_on_date_m8,
    coalesce(sum(c.vips_on_date_m9),0) as vips_on_date_m9,
    coalesce(sum(c.vips_on_date_m10),0) as vips_on_date_m10,
    coalesce(sum(c.vips_on_date_m11),0) as vips_on_date_m11,
    coalesce(sum(c.vips_on_date_m12),0) as vips_on_date_m12,
    coalesce(sum(c.vips_on_date_m13plus),0) as vips_on_date_m13plus,

    coalesce(sum(c.reactivated_vips),0) as reactivated_vips,
    coalesce(sum(c.retail_vips),0) as retail_vips,
    coalesce(sum(c.varsity_vips),0) as varsity_vips,

    -- vips on date excluding reactivated leads
    coalesce(sum(c.vips_on_date_d1_excluding_reactivated_leads),0) as vips_on_date_d1_excluding_reactivated_leads,

    -- vips on date from reactivated leads
    coalesce(sum(c.vips_from_reactivated_leads_d1),0) as vips_from_reactivated_leads_d1,
    coalesce(sum(c.vips_on_date_d1_reactivated_leads),0) as vips_on_date_d1_reactivated_leads,
    coalesce(sum(c.vips_on_date_m1_reactivated_leads),0) as vips_on_date_m1_reactivated_leads,

    -- cancels
    coalesce(sum(c.total_cancels_on_date),0) as total_cancels_on_date,
    coalesce(sum(c.passive_cancels_on_date),0) as passive_cancels_on_date,
    coalesce(sum(c.cancels_on_date_d1),0) as cancels_on_date_d1,
    coalesce(sum(c.cancels_on_date_m1),0) as cancels_on_date_m1,
    coalesce(sum(c.cancels_on_date_m2),0) as cancels_on_date_m2,
    coalesce(sum(c.cancels_on_date_m3),0) as cancels_on_date_m3,
    coalesce(sum(c.cancels_on_date_m4_m12),0) as cancels_on_date_m4_m12,
    coalesce(sum(c.cancels_on_date_m13plus),0) as cancels_on_date_m13plus,

    -- bop
    coalesce(sum(bop_vips),0) as bop_vips,

    -- order metrics
    coalesce(sum(o.first_guest_product_order_count),0) as first_guest_product_order_count,
    coalesce(sum(o.first_guest_product_gross_revenue),0) as first_guest_product_gross_revenue,
    coalesce(sum(o.first_guest_product_margin_pre_return),0) as first_guest_product_margin_pre_return,

    coalesce(sum(o.activating_vip_product_order_count),0) as activating_vip_product_order_count,
    coalesce(sum(o.activating_vip_product_gross_revenue),0) as activating_vip_product_gross_revenue,
    coalesce(sum(o.activating_vip_product_margin_pre_return),0) as activating_vip_product_margin_pre_return,

    -- spend
    coalesce(sum(cs.total_spend),0) as total_spend,
    coalesce(sum(cs.prospecting_spend),0) as prospecting_spend,

    -- spend by channel
    coalesce(sum(cs.facebook_spend),0) as facebook_spend,
    coalesce(sum(cs.snapchat_spend),0) as snapchat_spend,
    coalesce(sum(cs.pinterest_spend),0) as pinterest_spend,
    coalesce(sum(cs.tiktok_spend),0) as tiktok_spend,
    coalesce(sum(cs.reddit_spend),0) as reddit_spend,
    coalesce(sum(cs.twitter_spend),0) as twitter_spend,

    coalesce(sum(cs.applovin_spend),0) as applovin_spend,
    coalesce(sum(cs.linkedin_spend),0) as linkedin_spend,
    coalesce(sum(cs.nift_spend),0) as nift_spend,
    coalesce(sum(cs.rokt_spend),0) as rokt_spend,
    coalesce(sum(cs.twitch_spend),0) as twitch_spend,

    coalesce(sum(cs.google_other_spend),0) as google_other_spend,
    coalesce(sum(cs.ooh_spend),0) as ooh_spend,
    coalesce(sum(cs.celebrity_spend),0) as celebrity_spend,

    coalesce(sum(cs.non_branded_search_spend),0) as non_branded_search_spend,
    coalesce(sum(cs.branded_search_spend),0) as branded_search_spend,
    coalesce(sum(cs.shopping_spend),0) as shopping_spend,
    coalesce(sum(cs.youtube_spend),0) as youtube_spend,

    coalesce(sum(cs.programmatic_spend),0) as programmatic_spend,

    coalesce(sum(cs.tv_and_streaming_spend),0) as tv_and_streaming_spend,
    coalesce(sum(cs.influencers_spend),0) as influencers_spend,
    coalesce(sum(cs.testing_spend),0) as testing_spend,
    coalesce(sum(cs.print_spend),0) as print_spend,
    coalesce(sum(cs.radio_podcast_spend),0) as radio_podcast_spend,
    coalesce(sum(cs.affiliate_spend),0) as affiliate_spend,
    coalesce(sum(cs.physical_partnerships_spend),0) as physical_partnerships_spend,

    current_timestamp() as meta_create_datetime,
    current_timestamp() as meta_update_datetime

from _scaffold_final s
join edw_prod.data_model_jfb.dim_store st on st.store_id = s.store_id
left join _all_conversions c on c.date = s.date
    and c.store_id = s.store_id
    and c.finance_specialty_store = s.finance_specialty_store
    and c.is_fk_free_trial_flag = s.is_fk_free_trial_flag
    and c.is_fl_mens_flag = s.is_fl_mens_flag
    and c.is_fl_scrubs_customer = s.is_fl_scrubs_customer
    and c.retail_lead = s.retail_lead
    and c.retail_vip = s.retail_vip
    and c.retail_customer = s.retail_customer
left join _channel_spend cs on cs.date = s.date
    and cs.store_id = s.store_id
    and cs.finance_specialty_store = s.finance_specialty_store
    and cs.is_fk_free_trial_flag = s.is_fk_free_trial_flag
    and cs.is_fl_mens_flag = s.is_fl_mens_flag
    and cs.is_fl_scrubs_customer = s.is_fl_scrubs_customer
    and cs.retail_lead = s.retail_lead
    and cs.retail_vip = s.retail_vip
    and cs.retail_customer = s.retail_customer
    and cs.currency = s.currency
left join _order_metrics o on o.date = s.date
    and o.store_id = s.store_id
    and o.finance_specialty_store = s.finance_specialty_store
    and o.is_fk_free_trial_flag = s.is_fk_free_trial_flag
    and o.is_fl_mens_flag = s.is_fl_mens_flag
    and o.is_fl_scrubs_customer = s.is_fl_scrubs_customer
    and o.retail_lead = s.retail_lead
    and o.retail_vip = s.retail_vip
    and o.retail_customer = s.retail_customer
    and o.currency = s.currency
where s.date < $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12;


------------------------------------------------------------------------------------
-- final outputs --

create or replace transient table reporting_media_prod.attribution.acquisition_unattributed_total_cac as
select *
from _attribution_daily_acquisition_metrics_temp;


create or replace transient table reporting_media_prod.attribution.daily_acquisition_metrics_cac as
select *
from _attribution_daily_acquisition_metrics_temp;


create or replace transient table reporting_media_prod.attribution.total_cac_output_daily as
select date,
  st.store_brand_abbr || st.store_country as store,
  sum(total_spend) as media_spend,
  sum(primary_leads) as leads,
  sum(total_vips_on_date) as total_vips,
  nvl(media_spend / nullif(total_vips, 0), 0)  as total_vips_on_date_cac,
  nvl(sum(first_guest_product_margin_pre_return), 0) as first_e_commerce_gross_margin,
  nvl((media_spend - nvl(first_e_commerce_gross_margin, 0))/iff(nvl(total_vips, 0) = 0, 1, total_vips), 0) as xcac
from _attribution_daily_acquisition_metrics_temp a
join edw_prod.data_model_jfb.dim_store st on st.store_id = a.store_id
where lower(currency) = 'usd'
group by 1,2;


------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
-- top section of the total cac report in tableau online --
-- aggregate on the region, will not filter by country --
-- all data in USD --

set end_date = convert_timezone('America/Los_Angeles', current_timestamp)::date;
set report_date = dateadd(day, -1, $end_date);
set current_month_date = date_trunc(month, $report_date);
set last_month_date = dateadd(month,-1,date_trunc(month, $report_date));
set current_month_last_year_date = dateadd(year,-1,date_trunc(month, $report_date));
set run_rate_multiplier = (select day(last_day($report_date)) / day($report_date));


---- isolate retail budget / forecast -----
create or replace temporary table _total_vips as
select store_brand,
       store_region,
       case when lower(source) ilike 'forecast%' then 'Forecast'
         when lower(source) ilike 'budget%' then 'Budget' end as date_segment,
       date,
       cast(0 as boolean) as is_fk_free_trial_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(store_tab_abbreviation) ilike '%-M-%', 1, 0) as boolean) as is_fl_mens_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(store_tab_abbreviation) ilike 'SC-%', 1, 0) as boolean) as is_fl_scrubs_customer,

       leads as leads,
       0 as vips_from_leads_d1,
       vips_from_leads_m1,
       total_vips_on_date,
       media_spend as media_spend,
       media_spend as media_spend_excl_influencers,
       current_timestamp() as data_refresh_datetime
from reporting_media_prod.attribution.acquisition_budget_targets_cac
where source in ('Forecast-BTFX','Budget-BTFX')
    and store_tab_abbreviation in ('FL-W-NA','FL-M-NA','SX-NA');

create or replace temporary table _online_vips as
select store_brand,
       store_region,
       case when lower(source) ilike 'forecast%' then 'Forecast'
         when lower(source) ilike 'budget%' then 'Budget' end as date_segment,
       date,
       cast(0 as boolean) as is_fk_free_trial_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(store_tab_abbreviation) ilike '%-M-%', 1, 0) as boolean) as is_fl_mens_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(store_tab_abbreviation) ilike 'SC-%', 1, 0) as boolean) as is_fl_scrubs_customer,

       leads as leads,
       0 as vips_from_leads_d1,
       vips_from_leads_m1,
       total_vips_on_date,
       media_spend as media_spend,
       media_spend as media_spend_excl_influencers,
       current_timestamp() as data_refresh_datetime
from reporting_media_prod.attribution.acquisition_budget_targets_cac
where source in ('Forecast-BTFX','Budget-BTFX')
    and store_tab_abbreviation in ('FL-W-O-NA','FL-M-O-NA','SX-O-NA');

create or replace temporary table _retail_vips as
select t.store_brand,
       t.store_region,
       t.date_segment,
       t.date,
       t.is_fk_free_trial_flag,
       t.is_fl_mens_flag,
       t.is_fl_scrubs_customer,

       cast(0 as boolean) as retail_lead,
       cast(1 as boolean) as retail_vip,
       cast(1 as boolean) as retail_customer,

       t.leads - o.leads as leads,
       t.vips_from_leads_d1 - o.vips_from_leads_d1 as vips_from_leads_d1,
       t.vips_from_leads_m1 - o.vips_from_leads_m1 as vips_from_leads_m1,
       t.total_vips_on_date - o.total_vips_on_date as total_vips_on_date,
       t.media_spend - o.media_spend as media_spend,
       t.media_spend_excl_influencers - o.media_spend_excl_influencers as media_spend_excl_influencers
from _total_vips t
join _online_vips o on o.store_brand = t.store_brand
    and o.store_region = t.store_region
    and o.date_segment = t.date_segment
    and o.date = t.date
    and o.is_fk_free_trial_flag = t.is_fk_free_trial_flag
    and o.is_fl_mens_flag = t.is_fl_mens_flag
    and o.is_fl_scrubs_customer = t.is_fl_scrubs_customer;



create or replace temporary table _total_cac_budget_targets_tableau as
select
    store_brand,
    store_region,
    case when date_trunc('month',date) = $current_month_date then 'MTD'
         when date_trunc('month',date) = $last_month_date then 'Last Month'
         when date_trunc('month',date) = $current_month_last_year_date then 'Last Year'
    end as date_segment,
    date_trunc('month',date) as date,

    is_fk_free_trial_flag,
    is_fl_mens_flag,
    is_fl_scrubs_customer,

    retail_lead,
    retail_vip,
    retail_customer,

    sum(primary_leads) as leads,
    sum(vips_from_leads_d1) as vips_from_leads_d1,
    sum(vips_from_leads_m1) as vips_from_leads_m1,
    sum(total_vips_on_date) as total_vips_on_date,
    sum(total_spend) as media_spend,
    sum(total_spend - influencers_spend) as media_spend_excl_influencers,
    current_timestamp() as data_refresh_datetime
from _attribution_daily_acquisition_metrics_temp m
where date_trunc('month',date) in ($current_month_date, $last_month_date, $current_month_last_year_date)
    and lower(currency) = 'usd'
group by 1,2,3,4,5,6,7,8,9,10

union

select
    store_brand,
    store_region,
    'RunRate' as date_segment,
    date_trunc('month',date) as date,

    is_fk_free_trial_flag,
    is_fl_mens_flag,
    is_fl_scrubs_customer,

    retail_lead,
    retail_vip,
    retail_customer,

    sum(primary_leads) * $run_rate_multiplier as leads,
    sum(vips_from_leads_d1) * $run_rate_multiplier as vips_from_leads_d1,
    sum(vips_from_leads_m1) * $run_rate_multiplier as vips_from_leads_m1,
    sum(total_vips_on_date) * $run_rate_multiplier as total_vips_on_date,
    sum(total_spend) * $run_rate_multiplier as media_spend,
    sum(total_spend - influencers_spend) * $run_rate_multiplier as media_spend_excl_influencers,
    current_timestamp() as data_refresh_datetime
from _attribution_daily_acquisition_metrics_temp m
where date_trunc('month',date) = $current_month_date
    and lower(currency) = 'usd'
group by 1,2,3,4,5,6,7,8,9,10

union

-- online only --
select store_brand,
       store_region,
       case when lower(source) ilike 'forecast%' then 'Forecast'
         when lower(source) ilike 'budget%' then 'Budget' end as date_segment,
       date,
       cast(0 as boolean) as is_fk_free_trial_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(store_tab_abbreviation) ilike '%-M-%', 1, 0) as boolean) as is_fl_mens_flag,
       cast(iff(lower(store_brand) = 'fabletics' and lower(store_tab_abbreviation) ilike 'SC-%', 1, 0) as boolean) as is_fl_scrubs_customer,

       cast(0 as boolean) as retail_lead,
       cast(0 as boolean) as retail_vip,
       cast(0 as boolean) as retail_customer,

       leads as leads,
       0 as vips_from_leads_d1,
       vips_from_leads_m1,
       total_vips_on_date,
       media_spend as media_spend,
       media_spend as media_spend_excl_influencers,
       current_timestamp() as data_refresh_datetime
from reporting_media_prod.attribution.acquisition_budget_targets_cac
where source in ('Forecast-BTFX','Budget-BTFX')
    and store_tab_abbreviation in ('FK-NA','JF-EU','JF-NA','SD-NA','SX-EU','SX-O-NA','YT-O-NA',
                                  'FL-M-EU','FL-W-EU','SC-M-O-NA','SC-W-O-NA','FL-M-O-NA','FL-W-O-NA')

union

-- retail vips --
select *,
       current_timestamp() as data_refresh_datetime
from _retail_vips;


create or replace transient table reporting_media_prod.dbo.total_cac_budget_targets_tableau as
select store_brand,
       store_region,
       date_segment,
       date,
       is_fk_free_trial_flag,
       is_fl_mens_flag,
       is_fl_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       media_spend as total_spend,
       media_spend_excl_influencers,
       leads,
       vips_from_leads_d1,
       vips_from_leads_m1,
       total_vips_on_date,
       data_refresh_datetime
from _total_cac_budget_targets_tableau;
