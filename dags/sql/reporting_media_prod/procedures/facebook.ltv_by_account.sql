
----------  1. Get all vips that became leads through desired UTMs  ----------

-- within 30 days
create or replace temporary table _vips_from_leads30d as
select l.customer_id,
       l.utm_medium,
       case when lower(l.utm_source) in ('paidinfluencer','facebook+instagram','agencywithin','tubescience','narrative')
           then replace(l.utm_campaign,'fb_campaign_id_','') end as fb_campaign_id,
       lower(l.utm_source) as utm_source,
       cast(activation_local_datetime as date) as vip_date,
       l.store_id
from edw_prod.data_model.fact_registration l
join edw_prod.data_model.fact_activation v
    on v.customer_id = l.customer_id
    and datediff(day,registration_local_datetime,activation_local_datetime) < 30
where l.utm_medium = 'paid_social_media'
    and (l.utm_campaign ilike '%fb_campaign_id%' or
         lower(l.utm_source) in ('paidinfluencer','facebook+instagram','agencywithin','tubescience','peoplehype','geistm', 'narrative'))
    and cast(registration_local_datetime as date) > '2020-01-01'
    and v.is_reactivated_vip = false
    and l.is_reactivated_lead = false;

-- same session
create or replace temporary table _same_session_vips AS
select distinct customer_id,
       utm_medium,
       case when lower(utm_source) in ('paidinfluencer','facebook+instagram','agencywithin','tubescience','narrative')
           then replace(utm_campaign,'fb_campaign_id_','') end as fb_campaign_id,
       lower(utm_source) as utm_source,
       cast(activation_local_datetime as date) as vip_date,
       store_id
from edw_prod.data_model.fact_activation
where utm_medium = 'paid_social_media'
    and lower(utm_source) in ('paidinfluencer','facebook+instagram','agencywithin','tubescience',
                                'peoplehype','geistm', 'narrative')
    and cast(activation_local_datetime as date) > '2020-01-01'
    and is_reactivated_vip = false;

-- combine vips from leads 30 days + same session (vips from leads will trump vips same session)
create or replace temporary table _all_vips
(
    customer_id varchar,
    utm_medium varchar,
    fb_campaign_id varchar,
    utm_source varchar,
    vip_date datetime,
    store_id varchar
);

insert into  _all_vips
    select l.*
    from _vips_from_leads30d l;
insert into  _all_vips
    select ss.*
    from _same_session_vips ss
    left join  _all_vips c on c.customer_id = ss.customer_id
    where c.customer_id is null;


----------  2. Get the account information for each customer (store brand + account)  ----------

-- get store brand + account from fb opt dataset on campaign id (only for the utm_sources below)
create or replace temporary table _vips_fb_opt as
select distinct

    --session data
    vips.customer_id,

    --facebook data
    opt.account_name,
    opt.store_brand_name

from _all_vips vips
join reporting_media_prod.facebook.facebook_optimization_dataset_country opt
    on vips.fb_campaign_id = cast(opt.campaign_id as varchar)
where opt.region = 'NA'
    and vips.utm_source not in ('geistm', 'peoplehype');

-- same logic as above, but different join for peolplehype/geistm (no utm information, so can't join on campaign id)
-- bring in store information from dim_store in this first temp table (session data doesn't have Fabletics Men)
create or replace temporary table _vips_fb_opt_gp as
select distinct

    --session data
    vips.*,

    -- change store brand name for fabletics men
    case when (st.store_brand = 'Fabletics' and c.gender = 'M')
        then 'Fabletics Men' else st.store_brand end as store_brand_name

from _all_vips vips
left join edw_prod.data_model.dim_customer c on c.customer_id = vips.customer_id
join edw_prod.data_model.dim_store  st on st.store_id = vips.store_id
where vips.utm_source in ('geistm', 'peoplehype')
and st.STORE_REGION = 'NA';

-- now that we have store (with Fabletics Men), we can join on that and the subchannel from fb opt
create or replace temporary table _vips_fb_opt_gp_2 as
select distinct
            gp.customer_id,
            opt.account_name,
            opt.store_brand_name

from _vips_fb_opt_gp gp
left join (select distinct
                account_name,
                subchannel,
                store_brand_name
            from reporting_media_prod.facebook.facebook_optimization_dataset_country
            where region = 'NA') opt on gp.utm_source = opt.subchannel
                    and gp.store_brand_name = opt.store_brand_name;

-- union the two temp tables above to create a final vips table
create or replace temporary table _final_vips as
select * from _vips_fb_opt
union
select * from _vips_fb_opt_gp_2;

---------- 3. join final vips to ltv table  ----------

-- unique row ids: store, account, cohort date, tenure
create or replace transient table reporting_media_prod.facebook.ltv_by_account as
select
    v.store_brand_name,
    v.account_name,
    ltv.vip_cohort_month_date,
    ltv.month_date,
    'M' || cast(datediff(month,ltv.vip_cohort_month_date,ltv.month_date)+1 as varchar) as vip_tenure,

    count(ltv.customer_id) as initial_cohort_size,
    sum(cash_gross_profit) as cash_gross_profit, -- indicator of someone just being billed
    sum(product_gross_profit) as product_gross_profit, -- shows that a customer is actively engaging with our brand and buying product

    --getting AOV and activating unit count from ltv ltd table
    sum(activating_product_gross_revenue) as activating_product_gross_revenue,
    sum(activating_product_order_unit_count) as activating_product_order_unit_count,

    current_timestamp()::timestamp_ltz as meta_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime

from _final_vips v
join edw_prod.analytics_base.customer_lifetime_value_monthly ltv on v.customer_id = ltv.customer_id
left join (select distinct customer_id,
                           max(activating_product_gross_revenue) as activating_product_gross_revenue,
                           max(activating_product_order_unit_count) as activating_product_order_unit_count
                from edw_prod.analytics_base.customer_lifetime_value_ltd
                where is_reactivated_vip = 0
                group by 1) ltd on ltd.customer_id = ltv.customer_id
    where vip_cohort_month_date >= '2020-01-01'
    and ltv.month_date < date_trunc(month,current_date())
    and datediff(month,ltv.vip_cohort_month_date,ltv.month_date)+1 < 13 -- limit to M12
    and is_reactivated_vip = 0
group by 1,2,3,4,5;
