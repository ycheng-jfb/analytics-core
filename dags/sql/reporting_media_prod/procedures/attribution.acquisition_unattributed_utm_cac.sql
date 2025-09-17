
set low_watermark_date = '2018-01-01':: timestamp_ltz;
set execution_time = current_timestamp()::timestamp_ltz;

create or replace transient table reporting_media_prod.attribution.acquisition_unattributed_utm_cac_back
    clone reporting_media_prod.attribution.acquisition_unattributed_utm_cac;

------------------------------------------------------------------------------------
-- all conversions from central source --

create or replace temporary table _conversions as
select *
from edw_prod.analytics_base.acquisition_media_spend_daily_agg a
where lower(currency_type) = 'usd'
    and lower(date_object) = 'placed'
    and date >= $low_watermark_date;

update _conversions v
set registration_channel = 'direct traffic', registration_subchannel = 'other'
from reporting_media_base_prod.lkp.channel_display_name c where lower(c.channel_key) = lower(v.registration_channel)
and lower(c.channel_type) = 'organic';

update _conversions v
set registration_channel = 'meta'
where lower(registration_channel) = 'facebook';

update _conversions
set how_did_you_hear = 'n/a'
where lower(how_did_you_hear) = 'unknown';

update _conversions
set how_did_you_hear_parent = 'n/a'
where lower(how_did_you_hear_parent) = 'unknown';

create or replace temporary table _all_conversions as
select event_store_id as store_id,
       ifnull(finance_specialty_store, 'none') as finance_specialty_store,

       date,

       cast(iff(lower(store_brand) = 'fabletics' and lower(customer_gender) = 'm', 1, 0) as boolean) as is_fl_mens_vip,
       cast(iff(lower(store_brand) = 'fabletics' and lower(is_scrubs_customer) = true, 1, 0) as boolean) as is_fl_scrubs_customer,

       cast(iff(lower(store_brand) = 'yitty', 0, iff(is_retail_registration = true, 1, 0)) as boolean) as retail_lead,
       cast(iff(lower(store_brand) = 'yitty', 0, iff(is_retail_vip = true, 1, 0)) as boolean) as retail_vip,
       cast(iff(retail_lead = true or retail_vip = true, 1, 0) as boolean) as retail_customer,

       cast(iff(is_cross_promo = true, 1, 0) as boolean) as is_fk_free_trial,

       lower(a.registration_channel) as channel,
       lower(a.registration_subchannel) as subchannel,
       lower(a.how_did_you_hear_parent) as howdidyouhear_parent,
       lower(a.how_did_you_hear) as howdidyouhear,

        -- leads
        sum(primary_leads) as primary_leads,
        sum(secondary_leads) as secondary_leads,
        sum(reactivated_leads) as reactivated_leads,

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
        sum(vip_from_leads_d1 + vip_from_leads_d2 + vip_from_leads_d3 + vip_from_leads_d4
                + vip_from_leads_d5 + vip_from_leads_d6 + vip_from_leads_d7) as vips_from_leads_7d,
        sum(vip_from_leads_d1 + vip_from_leads_d2 + vip_from_leads_d3 + vip_from_leads_d4
                + vip_from_leads_d5 + vip_from_leads_d6 + vip_from_leads_d7 + vip_from_leads_d8_d30) as vips_from_leads_30d,

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
        sum(m13plus_cancels) as cancels_on_date_m13plus

from _conversions a
join edw_prod.data_model_jfb.dim_store ds on ds.store_id = a.event_store_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;


------------------------------------------------------------------------------------
-- order metrics --

create or replace temporary table _order_metrics as
select
        fs.store_id,
        ifnull(finance_specialty_store, 'none') as finance_specialty_store,
        date,
        case when lower(fs.currency_object) = 'usd' then 'USD' else 'Local' end as currency,
        cast(iff(lower(ds.store_brand) = 'fabletics' and lower(fs.gender) = 'm', 1, 0) as boolean) as is_fl_mens_vip,
        cast(iff(lower(store_brand) = 'fabletics' and lower(fs.is_scrubs_customer) = true, 1, 0) as boolean) as is_fl_scrubs_customer,
        cast(0 as boolean) as retail_lead,
        cast(iff(lower(ds.store_brand) = 'fabletics', is_retail_vip, 0) as boolean) as retail_vip,
        cast(iff(retail_vip = true, 1, 0)as boolean) as retail_customer,
        cast(iff(lower(ds.store_brand) = 'fabkids' and fs.is_cross_promo = true, 1, 0) as boolean) as is_fk_free_trial,
        lower(ifnull(fg.registration_channel,'unclassified')) as channel,
        lower(iff(fg.registration_subchannel = '' or fg.registration_subchannel is null,'unclassified',fg.registration_subchannel)) as subchannel,
        lower(ifnull(fg.how_did_you_hear_parent,'unknown')) as howdidyouhear_parent,
        lower(ifnull(fg.how_did_you_hear,'unknown')) as howdidyouhear,


       sum(iff(lower(membership_order_type_l3) = 'first guest', product_order_count, 0)) as first_guest_product_order_count,
       sum(iff(lower(membership_order_type_l3) = 'first guest', product_gross_revenue, 0)) as first_guest_product_gross_revenue,
       sum(iff(lower(membership_order_type_l3) = 'first guest', product_margin_pre_return, 0 )) as first_guest_product_margin_pre_return,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_order_count, 0)) as activating_vip_product_order_count,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_gross_revenue, 0)) as activating_vip_product_gross_revenue,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_margin_pre_return, 0)) as activating_vip_product_margin_pre_return

from edw_prod.analytics_base.finance_sales_ops fs
join edw_prod.data_model_jfb.dim_order_membership_classification c on c.order_membership_classification_key = fs.order_membership_classification_key
join edw_prod.data_model_jfb.dim_store ds on ds.store_id = fs.store_id
join edw_prod.data_model_jfb.fact_registration fg on fg.customer_id = fs.customer_id and fg.store_id = fs.store_id
where date_object = 'placed'
    and lower(membership_order_type_l3) in ('first guest','activating vip')
    and lower(fs.currency_object) in ('local','usd')
    and ds.store_type <> 'Retail'
    and date >= $low_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

union all

select
       fs.store_id,
       ifnull(finance_specialty_store, 'none') as finance_specialty_store,
       date,
       'Local' as currency,

        cast(iff(lower(ds.store_brand) = 'fabletics' and lower(fs.gender) = 'm', 1, 0) as boolean) as is_fl_mens_vip,
        cast(iff(lower(store_brand) = 'fabletics' and lower(fs.is_scrubs_customer) = true, 1, 0) as boolean) as is_fl_scrubs_customer,
        cast(0 as boolean) as retail_lead,
        cast(iff(lower(ds.store_brand) = 'fabletics', is_retail_vip, 0) as boolean) as retail_vip,
        cast(iff(retail_vip = true, 1, 0)as boolean) as retail_customer,
        cast(iff(lower(ds.store_brand) = 'fabkids' and fs.is_cross_promo = true, 1, 0) as boolean) as is_fk_free_trial,
        lower(ifnull(fg.registration_channel,'unclassified')) as channel,
        lower(iff(fg.registration_subchannel = '' or fg.registration_subchannel is null,'unclassified',fg.registration_subchannel)) as subchannel,
        lower(ifnull(fg.how_did_you_hear_parent,'unknown')) as howdidyouhear_parent,
        lower(ifnull(fg.how_did_you_hear,'unknown')) as howdidyouhear,

       sum(iff(lower(membership_order_type_l3) = 'first guest', product_order_count, 0)) as first_guest_product_order_count,
       sum(iff(lower(membership_order_type_l3) = 'first guest', product_gross_revenue, 0)) as first_guest_product_gross_revenue,
       sum(iff(lower(membership_order_type_l3) = 'first guest', product_margin_pre_return, 0 )) as first_guest_product_margin_pre_return,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_order_count, 0)) as activating_vip_product_order_count,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_gross_revenue, 0)) as activating_vip_product_gross_revenue,
       sum(iff(lower(membership_order_type_l3) = 'activating vip', product_margin_pre_return, 0)) as activating_vip_product_margin_pre_return

from edw_prod.analytics_base.finance_sales_ops fs
join edw_prod.data_model_jfb.dim_order_membership_classification c on c.order_membership_classification_key = fs.order_membership_classification_key
join edw_prod.data_model_jfb.dim_store ds on ds.store_id = fs.store_id
join edw_prod.data_model_jfb.fact_registration fg on fg.customer_id = fs.customer_id and fg.store_id = fs.store_id
where date_object = 'placed'
    and lower(membership_order_type_l3) in ('first guest','activating vip')
    and lower(store_region) = 'na'
    and lower(fs.currency_object) in ('usd')
    and ds.store_type <> 'Retail'
    and date >= $low_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;

update _order_metrics v
set channel = 'direct traffic', subchannel = 'other'
from reporting_media_base_prod.lkp.channel_display_name c where c.channel_key = v.channel
and lower(c.channel_type) = 'organic';

update _order_metrics v
set channel = 'meta'
where channel = 'facebook';

update _order_metrics
set howdidyouhear = 'n/a'
where lower(howdidyouhear) = 'unknown';

update _order_metrics
set howdidyouhear_parent = 'n/a'
where lower(howdidyouhear_parent) = 'unknown';

create or replace temporary table _order_metrics_final as
select
    store_id,
    finance_specialty_store,
    date,
    is_fl_mens_vip,
    is_fl_scrubs_customer,
    retail_lead,
    retail_vip,
    retail_customer,
    is_fk_free_trial,
    channel,
    subchannel,
    howdidyouhear_parent,
    howdidyouhear,
    sum(distinct first_guest_product_order_count) as first_guest_product_order_count,
    sum(case when currency = 'Local' then first_guest_product_gross_revenue else 0 end)::decimal(18,4) as first_guest_product_gross_revenue_local,
    sum(case when currency = 'USD' then first_guest_product_gross_revenue else 0 end)::decimal(18,4) as first_guest_product_gross_revenue_usd,
    sum(case when currency = 'Local' then first_guest_product_margin_pre_return else 0 end)::decimal(18,4) as first_guest_product_margin_pre_return_local,
    sum(case when currency = 'USD' then first_guest_product_margin_pre_return else 0 end)::decimal(18,4) as first_guest_product_margin_pre_return_usd,

    sum(distinct activating_vip_product_order_count) as activating_vip_product_order_count,
    sum(case when currency = 'Local' then activating_vip_product_gross_revenue else 0 end)::decimal(18,4) as activating_vip_product_gross_revenue_local,
    sum(case when currency = 'USD' then activating_vip_product_gross_revenue else 0 end)::decimal(18,4) as activating_vip_product_gross_revenue_usd,
    sum(case when currency = 'Local' then activating_vip_product_margin_pre_return else 0 end)::decimal(18,4) as activating_vip_product_margin_pre_return_local,
    sum(case when currency = 'USD' then activating_vip_product_margin_pre_return else 0 end)::decimal(18,4) as activating_vip_product_margin_pre_return_usd,

from _order_metrics
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;

------------------------------------------------------------------------------------

create or replace temporary table _scaffold as
select distinct
    store_id,
    finance_specialty_store,
    date,
    is_fl_mens_vip,
    is_fl_scrubs_customer,
    retail_lead,
    retail_vip,
    retail_customer,
    is_fk_free_trial,
    channel,
    subchannel,
    howdidyouhear_parent,
    howdidyouhear
from _all_conversions
union
select distinct
    store_id,
    finance_specialty_store,
    date,
    is_fl_mens_vip,
    is_fl_scrubs_customer,
    retail_lead,
    retail_vip,
    retail_customer,
    is_fk_free_trial,
    channel,
    subchannel,
    howdidyouhear_parent,
    howdidyouhear
from _order_metrics_final;

create or replace temporary table _conversions_final as
select s.store_id,
       s.finance_specialty_store,
       s.date,
       s.is_fl_mens_vip,
       s.is_fl_scrubs_customer,
       s.retail_lead,
       s.retail_vip,
       s.retail_customer,
       s.is_fk_free_trial,

       s.channel,
       s.subchannel,
       s.howdidyouhear_parent,
       s.howdidyouhear,

        -- leads
        sum(primary_leads) as primary_leads,
        sum(secondary_leads) as secondary_leads,
        sum(reactivated_leads) as reactivated_leads,

        -- vips from leads
        sum(vips_from_leads_30m) as vips_from_leads_30m,
        sum(vips_from_leads_2h) as vips_from_leads_2h,
        sum(vips_from_leads_24h) as vips_from_leads_24h,
        sum(vips_from_leads_d1) as vips_from_leads_d1,
        sum(vips_from_leads_d2) as vips_from_leads_d2,
        sum(vips_from_leads_d3) as vips_from_leads_d3,
        sum(vips_from_leads_d4) as vips_from_leads_d4,
        sum(vips_from_leads_d5) as vips_from_leads_d5,
        sum(vips_from_leads_d6) as vips_from_leads_d6,
        sum(vips_from_leads_d7) as vips_from_leads_d7,
        sum(vips_from_leads_d8_d30) as vips_from_leads_d8_d30,
        sum(vips_from_leads_7d) as vips_from_leads_7d,
        sum(vips_from_leads_30d) as vips_from_leads_30d,

        sum(vips_from_leads_m1) as vips_from_leads_m1,
        sum(vips_from_leads_m2) as vips_from_leads_m2,
        sum(vips_from_leads_m3) as vips_from_leads_m3,
        sum(vips_from_leads_m4) as vips_from_leads_m4,
        sum(vips_from_leads_m5) as vips_from_leads_m5,
        sum(vips_from_leads_m6) as vips_from_leads_m6,
        sum(vips_from_leads_m7) as vips_from_leads_m7,
        sum(vips_from_leads_m8) as vips_from_leads_m8,
        sum(vips_from_leads_m9) as vips_from_leads_m9,
        sum(vips_from_leads_m10) as vips_from_leads_m10,
        sum(vips_from_leads_m11) as vips_from_leads_m11,
        sum(vips_from_leads_m12) as vips_from_leads_m12,

        -- vips from reactivated leads
        sum(total_vips_from_reactivated_leads) as total_vips_from_reactivated_leads,
        sum(vips_from_reactivated_leads_30m) as vips_from_reactivated_leads_30m,
        sum(vips_from_reactivated_leads_2h) as vips_from_reactivated_leads_2h,
        sum(vips_from_reactivated_leads_24h) as vips_from_reactivated_leads_24h,
        sum(vips_from_reactivated_leads_m1) as vips_from_reactivated_leads_m1,

        -- vips on date
        sum(total_vips_on_date) as total_vips_on_date,
        sum(vips_on_date_d1) as vips_on_date_d1,
        sum(vips_on_date_d2) as vips_on_date_d2,
        sum(vips_on_date_d3) as vips_on_date_d3,
        sum(vips_on_date_d4) as vips_on_date_d4,
        sum(vips_on_date_d5) as vips_on_date_d5,
        sum(vips_on_date_d6) as vips_on_date_d6,
        sum(vips_on_date_d7) as vips_on_date_d7,
        sum(vips_on_date_d1_d7) as vips_on_date_d1_d7,
        sum(vips_on_date_d2_d7) as vips_on_date_d2_d7,
        sum(vips_on_date_d8_d30) as vips_on_date_d8_d30,
        sum(vips_on_date_d31_d89) as vips_on_date_d31_d89,
        sum(vips_on_date_d8_d90) as vips_on_date_d8_d90,
        sum(vips_on_date_d90plus) as vips_on_date_d90plus,

        sum(vips_on_date_m1) as vips_on_date_m1,
        sum(vips_on_date_m2) as vips_on_date_m2,
        sum(vips_on_date_m3) as vips_on_date_m3,
        sum(vips_on_date_m4) as vips_on_date_m4,
        sum(vips_on_date_m5) as vips_on_date_m5,
        sum(vips_on_date_m6) as vips_on_date_m6,
        sum(vips_on_date_m7) as vips_on_date_m7,
        sum(vips_on_date_m8) as vips_on_date_m8,
        sum(vips_on_date_m9) as vips_on_date_m9,
        sum(vips_on_date_m10) as vips_on_date_m10,
        sum(vips_on_date_m11) as vips_on_date_m11,
        sum(vips_on_date_m12) as vips_on_date_m12,
        sum(vips_on_date_m13plus) as vips_on_date_m13plus,

        sum(reactivated_vips) as reactivated_vips,

        -- vips on date from reactivated leads
        sum(vips_from_reactivated_leads_d1) as vips_from_reactivated_leads_d1,
        sum(vips_on_date_d1_reactivated_leads) as vips_on_date_d1_reactivated_leads,
        sum(vips_on_date_m1_reactivated_leads) as vips_on_date_m1_reactivated_leads,

        -- cancellations
        sum(total_cancels_on_date) as total_cancels_on_date,
        sum(passive_cancels_on_date) as passive_cancels_on_date,
        sum(cancels_on_date_d1) as cancels_on_date_d1,
        sum(cancels_on_date_m1) as cancels_on_date_m1,
        sum(cancels_on_date_m2) as cancels_on_date_m2,
        sum(cancels_on_date_m3) as cancels_on_date_m3,
        sum(cancels_on_date_m4_m12) as cancels_on_date_m4_m12,
        sum(cancels_on_date_m13plus) as cancels_on_date_m13plus,

        --order metrics
        SUM(first_guest_product_order_count) as first_guest_product_order_count,
        SUM(first_guest_product_gross_revenue_local) as first_guest_product_gross_revenue_local,
        SUM(first_guest_product_gross_revenue_usd) as first_guest_product_gross_revenue_usd,
        SUM(first_guest_product_margin_pre_return_local) as first_guest_product_margin_pre_return_local,
        SUM(first_guest_product_margin_pre_return_usd) as first_guest_product_margin_pre_return_usd,
        SUM(activating_vip_product_order_count) as activating_vip_product_order_count,
        SUM(activating_vip_product_gross_revenue_local) as activating_vip_product_gross_revenue_local,
        SUM(activating_vip_product_gross_revenue_usd) as activating_vip_product_gross_revenue_usd,
        SUM(activating_vip_product_margin_pre_return_local) as activating_vip_product_margin_pre_return_local,
        SUM(activating_vip_product_margin_pre_return_usd) as activating_vip_product_margin_pre_return_usd,

from _scaffold s
left join _all_conversions c
    on  s.store_id = c.store_id
    and s.finance_specialty_store = c.finance_specialty_store
    and s.date = c.date
    and s.is_fl_mens_vip = c.is_fl_mens_vip
    and s.is_fl_scrubs_customer = c.is_fl_scrubs_customer
    and s.retail_lead = c.retail_lead
    and s.retail_vip = c.retail_vip
    and s.retail_customer = c.retail_customer
    and s.is_fk_free_trial = c.is_fk_free_trial
    and s.channel = c.channel
    and s.subchannel = c.subchannel
    and s.howdidyouhear_parent = c.howdidyouhear_parent
    and s.howdidyouhear = c.howdidyouhear
left join _order_metrics_final o
    on  s.store_id = o.store_id
    and s.finance_specialty_store = o.finance_specialty_store
    and s.date = o.date
    and s.is_fl_mens_vip = o.is_fl_mens_vip
    and s.is_fl_scrubs_customer = o.is_fl_scrubs_customer
    and s.retail_lead = o.retail_lead
    and s.retail_vip = o.retail_vip
    and s.retail_customer = o.retail_customer
    and s.is_fk_free_trial = o.is_fk_free_trial
    and s.channel = o.channel
    and s.subchannel = o.subchannel
    and s.howdidyouhear_parent = o.howdidyouhear_parent
    and s.howdidyouhear = o.howdidyouhear
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;

create or replace transient table reporting_media_prod.attribution.acquisition_unattributed_utm_cac as
select
        store_id,
        finance_specialty_store,
        date,
        is_fl_mens_vip,
        is_fl_scrubs_customer,
        retail_lead,
        retail_vip,
        retail_customer,
        is_fk_free_trial,
        channel,
        subchannel,
        howdidyouhear_parent,
        howdidyouhear,

        -- leads
        primary_leads,
        secondary_leads,
        reactivated_leads,

        -- vips from leads
        vips_from_leads_30m,
        vips_from_leads_2h,
        vips_from_leads_24h as vips_from_leads_24hr,
        vips_from_leads_d1,
        vips_from_leads_d2,
        vips_from_leads_d3,
        vips_from_leads_d4,
        vips_from_leads_d5,
        vips_from_leads_d6,
        vips_from_leads_d7,
        vips_from_leads_d8_d30,
        vips_from_leads_7d,
        vips_from_leads_30d,

        vips_from_leads_m1,
        vips_from_leads_m2,
        vips_from_leads_m3,
        vips_from_leads_m4,
        vips_from_leads_m5,
        vips_from_leads_m6,
        vips_from_leads_m7,
        vips_from_leads_m8,
        vips_from_leads_m9,
        vips_from_leads_m10,
        vips_from_leads_m11,
        vips_from_leads_m12,

        -- vips from reactivated leads
        total_vips_from_reactivated_leads,
        vips_from_reactivated_leads_30m,
        vips_from_reactivated_leads_2h,
        vips_from_reactivated_leads_24h,
        vips_from_reactivated_leads_m1,

        -- vips on date
        total_vips_on_date,
        vips_on_date_d1,
        vips_on_date_d2,
        vips_on_date_d3,
        vips_on_date_d4,
        vips_on_date_d5,
        vips_on_date_d6,
        vips_on_date_d7,
        vips_on_date_d1_d7,
        vips_on_date_d2_d7,
        vips_on_date_d8_d30,
        vips_on_date_d31_d89,
        vips_on_date_d8_d90,
        vips_on_date_d90plus,

        vips_on_date_m1,
        vips_on_date_m2,
        vips_on_date_m3,
        vips_on_date_m4,
        vips_on_date_m5,
        vips_on_date_m6,
        vips_on_date_m7,
        vips_on_date_m8,
        vips_on_date_m9,
        vips_on_date_m10,
        vips_on_date_m11,
        vips_on_date_m12,
        vips_on_date_m13plus,

        reactivated_vips,

        -- vips on date from reactivated leads
        vips_from_reactivated_leads_d1,
        vips_on_date_d1_reactivated_leads,
        vips_on_date_m1_reactivated_leads,

        -- cancellations
        total_cancels_on_date,
        passive_cancels_on_date,
        cancels_on_date_d1,
        cancels_on_date_m1,
        cancels_on_date_m2,
        cancels_on_date_m3,
        cancels_on_date_m4_m12,
        cancels_on_date_m13plus,

        -- order metrics
        first_guest_product_order_count,
        first_guest_product_gross_revenue_local,
        first_guest_product_gross_revenue_usd,
        first_guest_product_margin_pre_return_local,
        first_guest_product_margin_pre_return_usd,
        activating_vip_product_order_count,
        activating_vip_product_gross_revenue_local,
        activating_vip_product_gross_revenue_usd,
        activating_vip_product_margin_pre_return_local,
        activating_vip_product_margin_pre_return_usd,

        hash(store_id, finance_specialty_store, date, is_fl_mens_vip, retail_lead, retail_vip, retail_customer,
            is_fk_free_trial, channel, subchannel, howdidyouhear_parent, howdidyouhear) as meta_row_hash,
        $execution_time as meta_create_datetime,
        $execution_time as meta_update_datetime
from _conversions_final;
