create table if not exists reporting_media_prod.attribution.cac_by_lead_channel_daily (
       business_unit                varchar,
       store_brand_abbr             varchar,
       region                       varchar,
       country                      varchar,
       store                        varchar,
       is_fk_free_trial             boolean,
       is_fl_mens_vip               boolean,
       is_fl_scrubs_customer        boolean,
       retail_lead                  boolean,
       retail_vip                   boolean,
       retail_customer              boolean,
       date                         date,
       channel                      varchar,
       subchannel                   varchar,

       primary_leads                            number,
       secondary_leads                          number,
       reactivated_leads                        number,

        vips_from_leads_30m                     number,
        vips_from_leads_2h                      number,
        vips_from_leads_24hr                     number,
        vips_from_leads_d1                      number,
        vips_from_leads_d2                      number,
        vips_from_leads_d3                      number,
        vips_from_leads_d4                      number,
        vips_from_leads_d5                      number,
        vips_from_leads_d6                      number,
        vips_from_leads_d7                      number,
        vips_from_leads_d8_d30                  number,
        vips_from_leads_7d                      number,
        vips_from_leads_30d                     number,

        vips_from_leads_m1                      number,
        vips_from_leads_m2                      number,
        vips_from_leads_m3                      number,
        vips_from_leads_m4                      number,
        vips_from_leads_m5                      number,
        vips_from_leads_m6                      number,
        vips_from_leads_m7                      number,
        vips_from_leads_m8                      number,
        vips_from_leads_m9                      number,
        vips_from_leads_m10                     number,
        vips_from_leads_m11                     number,
        vips_from_leads_m12                     number,

        total_vips_from_reactivated_leads       number,
        vips_from_reactivated_leads_30m         number,
        vips_from_reactivated_leads_2h          number,
        vips_from_reactivated_leads_24h         number,
        vips_from_reactivated_leads_m1          number,

        total_vips_on_date                      number,
        vips_on_date_d1                         number,
        vips_on_date_d2                         number,
        vips_on_date_d3                         number,
        vips_on_date_d4                         number,
        vips_on_date_d5                         number,
        vips_on_date_d6                         number,
        vips_on_date_d7                         number,
        vips_on_date_d1_d7                      number,
        vips_on_date_d2_d7                      number,
        vips_on_date_d8_d30                     number,
        vips_on_date_d31_d89                    number,
        vips_on_date_d8_d90                     number,
        vips_on_date_d90plus                    number,

        vips_on_date_m1                         number,
        vips_on_date_m2                         number,
        vips_on_date_m3                         number,
        vips_on_date_m4                         number,
        vips_on_date_m5                         number,
        vips_on_date_m6                         number,
        vips_on_date_m7                         number,
        vips_on_date_m8                         number,
        vips_on_date_m9                         number,
        vips_on_date_m10                        number,
        vips_on_date_m11                        number,
        vips_on_date_m12                        number,
        vips_on_date_m13plus                    number,

        reactivated_vips                        number,

        vips_from_reactivated_leads_d1          number,
        vips_on_date_d1_reactivated_leads       number,
        vips_on_date_m1_reactivated_leads       number,

        total_cancels_on_date                   number,
        passive_cancels_on_date                 number,
        cancels_on_date_d1                      number,
        cancels_on_date_m1                      number,
        cancels_on_date_m2                      number,
        cancels_on_date_m3                      number,
        cancels_on_date_m4_m12                  number,
        cancels_on_date_m13plus                 number,

       spend_local                  number(18,4),
       spend_usd                    number(18,4),
       spend_excl_influencers_local number(18,4),
       spend_excl_influencers_usd   number(18,4),
       first_guest_product_margin_pre_return_local   number(18,4),
       first_guest_product_margin_pre_return_usd   number(18,4),
       first_guest_product_order_count  int,
       first_guest_product_gross_revenue_local   number(18,4),
       first_guest_product_gross_revenue_usd   number(18,4),
       activating_vip_product_order_count  int,
       activating_vip_product_gross_revenue_local   number(18,4),
       activating_vip_product_gross_revenue_usd   number(18,4),
       activating_vip_product_margin_pre_return_local   number(18,4),
       activating_vip_product_margin_pre_return_usd   number(18,4),
       channel_sequence_new         int,

       meta_create_datetime         timestamp_ltz,
       meta_update_datetime         timestamp_ltz
);

create table if not exists reporting_media_prod.snapshot.cac_by_lead_channel_daily (
       business_unit                varchar,
       store_brand_abbr             varchar,
       region                       varchar,
       country                      varchar,
       store                        varchar,
       is_fk_free_trial             boolean,
       is_fl_mens_vip               boolean,
       is_fl_scrubs_customer        boolean,
       retail_lead                  boolean,
       retail_vip                   boolean,
       retail_customer              boolean,
       date                         date,
       channel                      varchar,
       subchannel                   varchar,

       primary_leads                            number,
       secondary_leads                          number,
       reactivated_leads                        number,

        vips_from_leads_30m                     number,
        vips_from_leads_2h                      number,
        vips_from_leads_24hr                     number,
        vips_from_leads_d1                      number,
        vips_from_leads_d2                      number,
        vips_from_leads_d3                      number,
        vips_from_leads_d4                      number,
        vips_from_leads_d5                      number,
        vips_from_leads_d6                      number,
        vips_from_leads_d7                      number,
        vips_from_leads_d8_d30                  number,
        vips_from_leads_7d                      number,
        vips_from_leads_30d                     number,

        vips_from_leads_m1                      number,
        vips_from_leads_m2                      number,
        vips_from_leads_m3                      number,
        vips_from_leads_m4                      number,
        vips_from_leads_m5                      number,
        vips_from_leads_m6                      number,
        vips_from_leads_m7                      number,
        vips_from_leads_m8                      number,
        vips_from_leads_m9                      number,
        vips_from_leads_m10                     number,
        vips_from_leads_m11                     number,
        vips_from_leads_m12                     number,

        total_vips_from_reactivated_leads       number,
        vips_from_reactivated_leads_30m         number,
        vips_from_reactivated_leads_2h          number,
        vips_from_reactivated_leads_24h         number,
        vips_from_reactivated_leads_m1          number,

        total_vips_on_date                      number,
        vips_on_date_d1                         number,
        vips_on_date_d2                         number,
        vips_on_date_d3                         number,
        vips_on_date_d4                         number,
        vips_on_date_d5                         number,
        vips_on_date_d6                         number,
        vips_on_date_d7                         number,
        vips_on_date_d1_d7                      number,
        vips_on_date_d2_d7                      number,
        vips_on_date_d8_d30                     number,
        vips_on_date_d31_d89                    number,
        vips_on_date_d8_d90                     number,
        vips_on_date_d90plus                    number,

        vips_on_date_m1                         number,
        vips_on_date_m2                         number,
        vips_on_date_m3                         number,
        vips_on_date_m4                         number,
        vips_on_date_m5                         number,
        vips_on_date_m6                         number,
        vips_on_date_m7                         number,
        vips_on_date_m8                         number,
        vips_on_date_m9                         number,
        vips_on_date_m10                        number,
        vips_on_date_m11                        number,
        vips_on_date_m12                        number,
        vips_on_date_m13plus                    number,

        reactivated_vips                        number,

        vips_from_reactivated_leads_d1          number,
        vips_on_date_d1_reactivated_leads       number,
        vips_on_date_m1_reactivated_leads       number,

        total_cancels_on_date                   number,
        passive_cancels_on_date                 number,
        cancels_on_date_d1                      number,
        cancels_on_date_m1                      number,
        cancels_on_date_m2                      number,
        cancels_on_date_m3                      number,
        cancels_on_date_m4_m12                  number,
        cancels_on_date_m13plus                 number,

       spend_local                  number(18,4),
       spend_usd                    number(18,4),
       spend_excl_influencers_local number(18,4),
       spend_excl_influencers_usd   number(18,4),
       first_guest_product_margin_pre_return_local   number(18,4),
       first_guest_product_margin_pre_return_usd   number(18,4),
       first_guest_product_order_count  int,
       first_guest_product_gross_revenue_local   number(18,4),
       first_guest_product_gross_revenue_usd   number(18,4),
       activating_vip_product_order_count  int,
       activating_vip_product_gross_revenue_local   number(18,4),
       activating_vip_product_gross_revenue_usd   number(18,4),
       activating_vip_product_margin_pre_return_local   number(18,4),
       activating_vip_product_margin_pre_return_usd   number(18,4),
       channel_sequence_new         int,
       meta_create_datetime         timestamp_ltz,
       meta_update_datetime         timestamp_ltz,
       datetime_added               timestamp_ltz default current_timestamp::timestamp_ltz
);

------------------------------------------------------------------------------------
-- spend --

create or replace temporary table _all_spend as
select st.store_brand as business_unit,
       st.store_brand_abbr,
       iff(fmc.specialty_store is null, st.store_country, fmc.specialty_store) as country,
       st.store_region as region,
       concat(st.store_brand, ' ', country) as store,

       cast(0 as boolean) as is_fk_free_trial,
       cast(is_mens_flag as boolean) as is_fl_mens_vip,
       cast(is_scrubs_flag as boolean) as is_fl_scrubs_customer,

       cast(0 as boolean) as retail_lead,
       cast(iff(lower(channel) = 'physical partnerships', 1, 0) as boolean) as retail_vip,
       cast(iff(lower(channel) = 'physical partnerships', 1, 0) as boolean) as retail_customer,


       cast(media_cost_date as date) as date,
       case when lower(channel) = 'google display' then 'programmatic' else lower(channel) end as channel,
       lower(subchannel) as subchannel,
       st.store_currency as currency,
       sum(fmc.cost * coalesce(fmc.local_store_conv_rate,1))::decimal(18,4) as spend_local,
       sum(fmc.cost * coalesce(fmc.spend_date_usd_conv_rate,1))::decimal(18,4) as spend_usd
from reporting_media_prod.dbo.vw_fact_media_cost fmc
join edw_prod.data_model_jfb.dim_store st on st.store_id = fmc.store_id
where channel is not null
    and lower(fmc.targeting) not in ('vip retargeting','purchase retargeting','free alpha')
    and to_date(fmc.media_cost_date) >= '2018-01-01'
    and to_date(fmc.media_cost_date) < IFF(
                CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', CURRENT_TIMESTAMP())::DATE > CURRENT_DATE(),
                DATEADD(DAY, 1, CURRENT_DATE()),
                CURRENT_DATE()
            )
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;


create or replace temporary table _final_spend as
select
    business_unit,
    store_brand_abbr,
    country,
    region,
    date,
    store,
    is_fk_free_trial,
    is_fl_mens_vip,
    is_fl_scrubs_customer,
    retail_lead,
    retail_vip,
    retail_customer,
    channel,
    subchannel,
    sum(spend_local) as spend_local,
    sum(spend_usd) as spend_usd
from _all_spend
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;




-- conversions --

create or replace temporary table _conversions_unattributed as
select s.store_brand as business_unit,
       s.store_brand_abbr,
       s.store_region as region,
       iff(lower(au.finance_specialty_store) = 'none', s.store_country, au.finance_specialty_store) as country,
       concat(s.store_brand, ' ', country) as store,
       cast(au.is_fk_free_trial as boolean) as is_fk_free_trial,
       cast(au.is_fl_mens_vip as boolean) as is_fl_mens_vip,
       cast(au.is_fl_scrubs_customer as boolean) as is_fl_scrubs_customer,
       cast(au.retail_lead as boolean) as retail_lead,
       cast(au.retail_vip as boolean) as retail_vip,
       cast(au.retail_customer as boolean) as retail_customer,
       au.date,

       lower(au.channel) as click_channel,
       lower(au.subchannel) as click_subchannel,
       coalesce(lower(h.channel),'n/a') as reallocate_hdyh_channel,

        -- leads
        sum(primary_leads) as primary_leads,
        sum(secondary_leads) as secondary_leads,
        sum(reactivated_leads) as reactivated_leads,

        -- vips from leads
        sum(vips_from_leads_30m) as vips_from_leads_30m,
        sum(vips_from_leads_2h) as vips_from_leads_2h,
        sum(vips_from_leads_24hr) as vips_from_leads_24hr,
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
        SUM(first_guest_product_margin_pre_return_local) as first_guest_product_margin_pre_return_local,
        SUM(first_guest_product_margin_pre_return_usd) as first_guest_product_margin_pre_return_usd,
        SUM(first_guest_product_order_count) as first_guest_product_order_count,
        SUM(first_guest_product_gross_revenue_local) as first_guest_product_gross_revenue_local,
        SUM(first_guest_product_gross_revenue_usd) as first_guest_product_gross_revenue_usd,
        SUM(activating_vip_product_order_count) as activating_vip_product_order_count,
        SUM(activating_vip_product_gross_revenue_local) as activating_vip_product_gross_revenue_local,
        SUM(activating_vip_product_gross_revenue_usd) as activating_vip_product_gross_revenue_usd,
        SUM(activating_vip_product_margin_pre_return_local) as activating_vip_product_margin_pre_return_local,
        SUM(activating_vip_product_margin_pre_return_usd) as activating_vip_product_margin_pre_return_usd

from reporting_media_prod.attribution.acquisition_unattributed_utm_cac au
join edw_prod.data_model_jfb.dim_store s on s.store_id = au.store_id
left join reporting_media_base_prod.dbo.vw_med_hdyh_mapping_tsos h on h.hdyh_value = lower(replace(au.howdidyouhear,' ',''))
where lower(concat(s.store_brand, ' ', s.store_country)) != 'justfab nl'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;


update _conversions_unattributed
set click_channel = 'programmatic'
where lower(click_channel) = 'google display';

-- only include hdyh for paid channels, set all organic answers to n/a
update _conversions_unattributed
set reallocate_hdyh_channel = 'n/a'
where lower(reallocate_hdyh_channel) in ('ignore','friend','metanav','organic social','retail', 'wholesale');


------------------------------------------------------------------------------------
-- channel allocation --

-- if click comes from branded search, direct or organic with paid HDYH —> use HDYH channel
-- if click comes from branded search, direct or organic with no HDYH answer (or organic hdyh) —> goes to direct / branded search (click source)
-- if click comes from paid —> click channel

create or replace temporary table _final_attributed_conversions as
select business_unit,
       store_brand_abbr,
       region,
       country,
       store,
       is_fk_free_trial,
       is_fl_mens_vip,
       is_fl_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       date,

       case when lower(click_channel) in ('direct traffic','branded search') and lower(reallocate_hdyh_channel) != 'n/a' then reallocate_hdyh_channel
           else lower(click_channel)
           end as channel_attributed,

       case when lower(click_channel) in ('direct traffic','branded search') and lower(reallocate_hdyh_channel) != 'n/a' then 'hdyh'
           else lower(click_subchannel)
           end as subchannel_attributed,

        -- leads
        sum(primary_leads) as primary_leads,
        sum(secondary_leads) as secondary_leads,
        sum(reactivated_leads) as reactivated_leads,

        -- vips from leads
        sum(vips_from_leads_30m) as vips_from_leads_30m,
        sum(vips_from_leads_2h) as vips_from_leads_2h,
        sum(vips_from_leads_24hr) as vips_from_leads_24hr,
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
        SUM(first_guest_product_margin_pre_return_local) as first_guest_product_margin_pre_return_local,
        SUM(first_guest_product_margin_pre_return_usd) as first_guest_product_margin_pre_return_usd,
        SUM(first_guest_product_order_count) as first_guest_product_order_count,
        SUM(first_guest_product_gross_revenue_local) as first_guest_product_gross_revenue_local,
        SUM(first_guest_product_gross_revenue_usd) as first_guest_product_gross_revenue_usd,
        SUM(activating_vip_product_order_count) as activating_vip_product_order_count,
        SUM(activating_vip_product_gross_revenue_local) as activating_vip_product_gross_revenue_local,
        SUM(activating_vip_product_gross_revenue_usd) as activating_vip_product_gross_revenue_usd,
        SUM(activating_vip_product_margin_pre_return_local) as activating_vip_product_margin_pre_return_local,
        SUM(activating_vip_product_margin_pre_return_usd) as activating_vip_product_margin_pre_return_usd

from _conversions_unattributed
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;


------------------------------------------------------------------------------------
-- final spend + conversions --

create or replace temporary table _scaffold as
select distinct
       business_unit,
       store_brand_abbr,
       country,
       region,
       store,
       is_fk_free_trial,
       is_fl_mens_vip,
       is_fl_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       date,
       channel_attributed as channel,
       subchannel_attributed as subchannel
from _final_attributed_conversions
union
select distinct
       business_unit,
       store_brand_abbr,
       country,
       region,
       store,
       is_fk_free_trial,
       is_fl_mens_vip,
       is_fl_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       date,
       channel,
       subchannel
from _final_spend;

create or replace temporary table _global_output as
select
       a.business_unit,
       a.store_brand_abbr,
       a.region,
       a.country,
       a.store,
       a.is_fk_free_trial,
       a.is_fl_mens_vip,
       a.is_fl_scrubs_customer,
       a.retail_lead,
       a.retail_vip,
       a.retail_customer,
       a.date,
       a.channel,
       a.subchannel,

       sum(ifnull(spend_usd,0)) as spend_usd,
       sum(ifnull(spend_local,0)) as spend_local,
       sum(iff(a.channel = 'influencers', 0, ifnull(spend_usd,0))) as spend_excl_influencers_usd,
       sum(iff(a.channel = 'influencers', 0, ifnull(spend_local,0))) as spend_excl_influencers_local,

       sum(ifnull(first_guest_product_margin_pre_return_usd,0)) as first_guest_product_margin_pre_return_usd,
       sum(ifnull(first_guest_product_margin_pre_return_local,0)) as first_guest_product_margin_pre_return_local,
       sum(ifnull(first_guest_product_order_count,0)) as first_guest_product_order_count,
       sum(ifnull(first_guest_product_gross_revenue_local,0)) as first_guest_product_gross_revenue_local,
       sum(ifnull(first_guest_product_gross_revenue_usd,0)) as first_guest_product_gross_revenue_usd,
       SUM(ifnull(activating_vip_product_order_count,0)) as activating_vip_product_order_count,
       SUM(ifnull(activating_vip_product_gross_revenue_local,0)) as activating_vip_product_gross_revenue_local,
       SUM(ifnull(activating_vip_product_gross_revenue_usd,0)) as activating_vip_product_gross_revenue_usd,
       SUM(ifnull(activating_vip_product_margin_pre_return_local,0)) as activating_vip_product_margin_pre_return_local,
       SUM(ifnull(activating_vip_product_margin_pre_return_usd,0)) as activating_vip_product_margin_pre_return_usd,
        -- leads
        sum(ifnull(primary_leads,0)) as primary_leads,
        sum(ifnull(secondary_leads,0)) as secondary_leads,
        sum(ifnull(reactivated_leads,0)) as reactivated_leads,

        -- vips from leads
        sum(ifnull(vips_from_leads_30m,0)) as vips_from_leads_30m,
        sum(ifnull(vips_from_leads_2h,0)) as vips_from_leads_2h,
        sum(ifnull(vips_from_leads_24hr,0)) as vips_from_leads_24hr,
        sum(ifnull(vips_from_leads_d1,0)) as vips_from_leads_d1,
        sum(ifnull(vips_from_leads_d2,0)) as vips_from_leads_d2,
        sum(ifnull(vips_from_leads_d3,0)) as vips_from_leads_d3,
        sum(ifnull(vips_from_leads_d4,0)) as vips_from_leads_d4,
        sum(ifnull(vips_from_leads_d5,0)) as vips_from_leads_d5,
        sum(ifnull(vips_from_leads_d6,0)) as vips_from_leads_d6,
        sum(ifnull(vips_from_leads_d7,0)) as vips_from_leads_d7,
        sum(ifnull(vips_from_leads_d8_d30,0)) as vips_from_leads_d8_d30,
        sum(ifnull(vips_from_leads_7d,0)) as vips_from_leads_7d,
        sum(ifnull(vips_from_leads_30d,0)) as vips_from_leads_30d,

        sum(ifnull(vips_from_leads_m1,0)) as vips_from_leads_m1,
        sum(ifnull(vips_from_leads_m2,0)) as vips_from_leads_m2,
        sum(ifnull(vips_from_leads_m3,0)) as vips_from_leads_m3,
        sum(ifnull(vips_from_leads_m4,0)) as vips_from_leads_m4,
        sum(ifnull(vips_from_leads_m5,0)) as vips_from_leads_m5,
        sum(ifnull(vips_from_leads_m6,0)) as vips_from_leads_m6,
        sum(ifnull(vips_from_leads_m7,0)) as vips_from_leads_m7,
        sum(ifnull(vips_from_leads_m8,0)) as vips_from_leads_m8,
        sum(ifnull(vips_from_leads_m9,0)) as vips_from_leads_m9,
        sum(ifnull(vips_from_leads_m10,0)) as vips_from_leads_m10,
        sum(ifnull(vips_from_leads_m11,0)) as vips_from_leads_m11,
        sum(ifnull(vips_from_leads_m12,0)) as vips_from_leads_m12,

        -- vips from reactivated leads
        sum(ifnull(total_vips_from_reactivated_leads,0)) as total_vips_from_reactivated_leads,
        sum(ifnull(vips_from_reactivated_leads_30m,0)) as vips_from_reactivated_leads_30m,
        sum(ifnull(vips_from_reactivated_leads_2h,0)) as vips_from_reactivated_leads_2h,
        sum(ifnull(vips_from_reactivated_leads_24h,0)) as vips_from_reactivated_leads_24h,
        sum(ifnull(vips_from_reactivated_leads_m1,0)) as vips_from_reactivated_leads_m1,

        -- vips on date
        sum(ifnull(total_vips_on_date,0)) as total_vips_on_date,
        sum(ifnull(vips_on_date_d1,0)) as vips_on_date_d1,
        sum(ifnull(vips_on_date_d2,0)) as vips_on_date_d2,
        sum(ifnull(vips_on_date_d3,0)) as vips_on_date_d3,
        sum(ifnull(vips_on_date_d4,0)) as vips_on_date_d4,
        sum(ifnull(vips_on_date_d5,0)) as vips_on_date_d5,
        sum(ifnull(vips_on_date_d6,0)) as vips_on_date_d6,
        sum(ifnull(vips_on_date_d7,0)) as vips_on_date_d7,
        sum(ifnull(vips_on_date_d1_d7,0)) as vips_on_date_d1_d7,
        sum(ifnull(vips_on_date_d2_d7,0)) as vips_on_date_d2_d7,
        sum(ifnull(vips_on_date_d8_d30,0)) as vips_on_date_d8_d30,
        sum(ifnull(vips_on_date_d31_d89,0)) as vips_on_date_d31_d89,
        sum(ifnull(vips_on_date_d8_d90,0)) as vips_on_date_d8_d90,
        sum(ifnull(vips_on_date_d90plus,0)) as vips_on_date_d90plus,

        sum(ifnull(vips_on_date_m1,0)) as vips_on_date_m1,
        sum(ifnull(vips_on_date_m2,0)) as vips_on_date_m2,
        sum(ifnull(vips_on_date_m3,0)) as vips_on_date_m3,
        sum(ifnull(vips_on_date_m4,0)) as vips_on_date_m4,
        sum(ifnull(vips_on_date_m5,0)) as vips_on_date_m5,
        sum(ifnull(vips_on_date_m6,0)) as vips_on_date_m6,
        sum(ifnull(vips_on_date_m7,0)) as vips_on_date_m7,
        sum(ifnull(vips_on_date_m8,0)) as vips_on_date_m8,
        sum(ifnull(vips_on_date_m9,0)) as vips_on_date_m9,
        sum(ifnull(vips_on_date_m10,0)) as vips_on_date_m10,
        sum(ifnull(vips_on_date_m11,0)) as vips_on_date_m11,
        sum(ifnull(vips_on_date_m12,0)) as vips_on_date_m12,
        sum(ifnull(vips_on_date_m13plus,0)) as vips_on_date_m13plus,

        sum(ifnull(reactivated_vips,0)) as reactivated_vips,

        -- vips on date from reactivated leads
        sum(ifnull(vips_from_reactivated_leads_d1,0)) as vips_from_reactivated_leads_d1,
        sum(ifnull(vips_on_date_d1_reactivated_leads,0)) as vips_on_date_d1_reactivated_leads,
        sum(ifnull(vips_on_date_m1_reactivated_leads,0)) as vips_on_date_m1_reactivated_leads,

        -- cancellations
        sum(ifnull(total_cancels_on_date,0)) as total_cancels_on_date,
        sum(ifnull(passive_cancels_on_date,0)) as passive_cancels_on_date,
        sum(ifnull(cancels_on_date_d1,0)) as cancels_on_date_d1,
        sum(ifnull(cancels_on_date_m1,0)) as cancels_on_date_m1,
        sum(ifnull(cancels_on_date_m2,0)) as cancels_on_date_m2,
        sum(ifnull(cancels_on_date_m3,0)) as cancels_on_date_m3,
        sum(ifnull(cancels_on_date_m4_m12,0)) as cancels_on_date_m4_m12,
        sum(ifnull(cancels_on_date_m13plus,0)) as cancels_on_date_m13plus,

       current_timestamp() as meta_create_datetime,
       current_timestamp() as meta_update_datetime

from _scaffold a
left join _final_spend s on s.business_unit = a.business_unit
    and s.store_brand_abbr = a.store_brand_abbr
    and s.region = a.region
    and s.country = a.country
    and s.store = a.store
    and s.is_fk_free_trial = a.is_fk_free_trial
    and s.is_fl_mens_vip = a.is_fl_mens_vip
    and s.is_fl_scrubs_customer = a.is_fl_scrubs_customer
    and s.retail_lead = a.retail_lead
    and s.retail_vip = a.retail_vip
    and s.retail_customer = a.retail_customer
    and s.date = a.date
    and s.channel = a.channel
    and s.subchannel = a.subchannel
left join _final_attributed_conversions c on c.business_unit = a.business_unit
    and c.store_brand_abbr = a.store_brand_abbr
    and c.region = a.region
    and c.country = a.country
    and c.store = a.store
    and c.is_fk_free_trial = a.is_fk_free_trial
    and c.is_fl_mens_vip = a.is_fl_mens_vip
    and c.is_fl_scrubs_customer = a.is_fl_scrubs_customer
    and c.retail_lead = a.retail_lead
    and c.retail_vip = a.retail_vip
    and c.retail_customer = a.retail_customer
    and c.date = a.date
    and c.channel_attributed = a.channel
    and c.subchannel_attributed = a.subchannel
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;


------------------------------------------------------------------------------------
-- insert into final tables --

truncate table reporting_media_prod.attribution.cac_by_lead_channel_daily;

delete
from reporting_media_prod.snapshot.cac_by_lead_channel_daily
where meta_create_datetime < dateadd(day, -7, current_timestamp());

insert all
    into reporting_media_prod.attribution.cac_by_lead_channel_daily
    into reporting_media_prod.snapshot.cac_by_lead_channel_daily (
                    business_unit,
                    store_brand_abbr,
                    region,
                    country,
                    store,
                    is_fk_free_trial,
                    is_fl_mens_vip,
                    is_fl_scrubs_customer,
                    retail_lead,
                    retail_vip,
                    retail_customer,
                    date,
                    channel,
                    subchannel,

                    primary_leads,
                    secondary_leads,
                    reactivated_leads,

                    vips_from_leads_30m,
                    vips_from_leads_2h,
                    vips_from_leads_24hr,
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

                    total_vips_from_reactivated_leads,
                    vips_from_reactivated_leads_30m,
                    vips_from_reactivated_leads_2h,
                    vips_from_reactivated_leads_24h,
                    vips_from_reactivated_leads_m1,

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

                    vips_from_reactivated_leads_d1,
                    vips_on_date_d1_reactivated_leads,
                    vips_on_date_m1_reactivated_leads,

                    total_cancels_on_date,
                    passive_cancels_on_date,
                    cancels_on_date_d1,
                    cancels_on_date_m1,
                    cancels_on_date_m2,
                    cancels_on_date_m3,
                    cancels_on_date_m4_m12,
                    cancels_on_date_m13plus,

                    spend_local,
                    spend_usd,
                    spend_excl_influencers_local,
                    spend_excl_influencers_usd,
                    first_guest_product_margin_pre_return_local,
                    first_guest_product_margin_pre_return_usd,
                    first_guest_product_order_count,
                    first_guest_product_gross_revenue_local,
                    first_guest_product_gross_revenue_usd,
                    activating_vip_product_order_count,
                    activating_vip_product_gross_revenue_local,
                    activating_vip_product_gross_revenue_usd,
                    activating_vip_product_margin_pre_return_local,
                    activating_vip_product_margin_pre_return_usd,
                    channel_sequence_new,
                    meta_create_datetime,
                    meta_update_datetime
                    )
select
    business_unit,
    store_brand_abbr,
    region,
    country,
    store,
    is_fk_free_trial,
    is_fl_mens_vip,
    is_fl_scrubs_customer,
    retail_lead,
    retail_vip,
    retail_customer,
    date,
    lower(channel_name) as channel,
    subchannel,

        primary_leads,
        secondary_leads,
        reactivated_leads,

        vips_from_leads_30m,
        vips_from_leads_2h,
        vips_from_leads_24hr,
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

        total_vips_from_reactivated_leads,
        vips_from_reactivated_leads_30m,
        vips_from_reactivated_leads_2h,
        vips_from_reactivated_leads_24h,
        vips_from_reactivated_leads_m1,

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

        vips_from_reactivated_leads_d1,
        vips_on_date_d1_reactivated_leads,
        vips_on_date_m1_reactivated_leads,

        total_cancels_on_date,
        passive_cancels_on_date,
        cancels_on_date_d1,
        cancels_on_date_m1,
        cancels_on_date_m2,
        cancels_on_date_m3,
        cancels_on_date_m4_m12,
        cancels_on_date_m13plus,

    spend_local,
    spend_usd,
    spend_excl_influencers_local,
    spend_excl_influencers_usd,
    first_guest_product_margin_pre_return_local,
    first_guest_product_margin_pre_return_usd,
    first_guest_product_order_count,
    first_guest_product_gross_revenue_local,
    first_guest_product_gross_revenue_usd,
    activating_vip_product_order_count,
    activating_vip_product_gross_revenue_local,
    activating_vip_product_gross_revenue_usd,
    activating_vip_product_margin_pre_return_local,
    activating_vip_product_margin_pre_return_usd,
    case when lower(channel_name) = 'meta' then 1 else sequence end  as channel_sequence_new,
    meta_create_datetime,
    meta_update_datetime

from _global_output g
left join reporting_media_base_prod.lkp.channel_display_name c on lower(c.channel_key) = lower(g.channel);
