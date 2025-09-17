set low_watermark_datetime = dateadd('month', -24, current_date());
set execution_time = to_timestamp_ntz(current_timestamp);

create or replace temporary table _order_line_ids as (
  select order_line_id,promo_id
   from lake_consolidated_view.ultra_merchant.order_line_discount
   where meta_update_datetime >= $low_watermark_datetime
      and promo_id is not null
  );

create or replace temporary table _final_order_line_ids as (
  select fol.order_line_id, old.promo_id, dpt.product_type_id
   from edw_prod.data_model.fact_order_line fol
   join edw_prod.data_model.dim_product_type dpt on dpt.product_type_key = fol.product_type_key
   left join lake_consolidated_view.ultra_merchant.order_line ol on ol.order_line_id = fol.order_line_id
   left join lake_consolidated_view.ultra_merchant.order_line ol2 on ol2.order_id = fol.order_id and ol2.group_key = ol.group_key
      and ol2.product_type_id = 14 -- join the bundle record
   join _order_line_ids old on old.order_line_id = coalesce(ol2.order_line_id, fol.order_line_id)
  );

create or replace temporary table _filtered_dim_promo_history as (
  select promo_id,
        first_promo_code,
        effective_start_datetime,
        effective_end_datetime
    from edw_prod.data_model.dim_promo_history
    where lower(first_promo_code) like any
          ('infl18%', 'infl2018%', 'infl2019%', 'infl2020%', 'infl2021%', 'infl2022%','infl2023%','infl2024%','seeding_%', 'influencer_%',
           '%flmen_ambassador_%','%flmen_promoter_%','%flmen_ugc_%','%flmen_giveaway_%',
           '%flmen_media_%','%flmen_gifting_%','%flmen_streamers_%', '%fl_ambassadors_%', '%fl_influencers%',
           '%flmen_membmod_%','%flmen_events_%', '%flmen_hydrow_%', '%flmen_socialshoot_%','%flmen_promoinfl%',
            '%flmen_streaminfl%','flscrubs_launchseeding_sep2022','fls_launchseeding_2022','scb_%','flmen_hydrowinfl_%',
           'fl_partnership%','flmen_influencers%', 'fl_socialsedding%','fl_prseeding%','fl_partnerseeding%','fl_collegeseeding%','fl_eventseeding%',
           'fl_grseeding%', 'fl_hydrowinfl%','fl_influencervd%', 'fl_stseeding%','fl_kkseeding%',
           'fl_miscseeding%', 'scb_eventseeding%', 'scb_miscseeding%', 'fl_retailevseeding%' )
         or lower(first_promo_code) in ('ambassador_flmen_nov',
                'influencers_flmen_janfeb_2',
                'influencer_flmens_11_2',
                'influencer_flmens_11_3',
                'influencer_flmens_12',
                'influencer_flmens_2',
                'ambassadors_flmen_mar2020',
                'ambassadors_flmen_feb_202',
               'flmen_hqpregame_event',
               'flmen_launch_unboxing')
         or lower(first_promo_code) in (select distinct lower(promo_code)
                                            from lake_view.sharepoint.influencer_promo_codes where lower(include_in_reporting) = 'x')
  );

create or replace temporary table _filtered_fact_order as (
  select order_id,
        store_id,
        customer_id,
        shipping_address_id,
        order_local_datetime,
        shipped_local_datetime,
        order_date_usd_conversion_rate,
        shipping_cost_local_amount
    from edw_prod.data_model.fact_order
    where order_local_datetime >= $low_watermark_datetime
  );

create or replace temporary table _influencer_promo_code_orders as (
  select dc.email as email,
      edw_prod.stg.udf_unconcat_brand(dc.customer_id) as customer_id,
      dc.first_name as firstname,
      dc.last_name as lastname,
      edw_prod.stg.udf_unconcat_brand(fo.order_id) as order_id,
      fo.order_local_datetime::date as date_placed,
      fo.shipped_local_datetime::date as date_shipped,
      dpr.first_promo_code as promotion_code,
      edw_prod.stg.udf_unconcat_brand(ol.order_line_id) as order_line_id,
      edw_prod.stg.udf_unconcat_brand(dp.product_id) as product_id,
      dp.sku as sku,
      dp.product_name as product_name,
      dp.department as department,
      dp.category as category,
      dp.subcategory as subcategory,
      case
          when lower(dpr.first_promo_code) ilike '%flmen%' then 'Fabletics Men'
          when (dpr.first_promo_code in ('FLScrubs_LaunchSeeding_Sep2022','FLS_LaunchSeeding_2022')
                    or lower(dpr.first_promo_code) ilike 'scb_%') then 'Fabletics Scrubs'
          else st.store_brand
          end as business_unit,
      st.store_country as country,
      (ol.subtotal_excl_tariff_local_amount + ol.tariff_revenue_local_amount)  * fo.order_date_usd_conversion_rate as subtotal,
      ol.product_discount_local_amount  * fo.order_date_usd_conversion_rate as discount,
      ol.tax_local_amount  * fo.order_date_usd_conversion_rate as tax,
      fo.shipping_cost_local_amount as shipping_cost,
      ol.estimated_landed_cost_local_amount * fo.order_date_usd_conversion_rate as product_cost,
      $execution_time as meta_create_datetime,
      $execution_time as meta_update_datetime,
      case when lower(mee.membership_state) = 'vip' then 1 else 0 end is_vip_flag,
      dp.image_url as image_url,
   da.address1 as ship_to_address,
   concat(da.firstname, ' ', da.lastname) as ship_to_name,
   da.city as ship_to_city,
   da.state as ship_to_state,
   da.zip as ship_to_zipcode,
   da.country_code as ship_to_country
   from _filtered_fact_order fo
   join edw_prod.data_model.fact_order_line ol  on ol.order_id = fo.order_id
   join _final_order_line_ids pu  on pu.order_line_id = ol.order_line_id
   join edw_prod.data_model.dim_product dp  on dp.product_id = ol.product_id
   join _filtered_dim_promo_history dpr  on dpr.promo_id= pu.promo_id
      and dpr.effective_start_datetime < fo.order_local_datetime
      and dpr.effective_end_datetime >= fo.order_local_datetime
   join edw_prod.data_model.dim_store st  on st.store_id = fo.store_id
   join edw_prod.data_model.dim_customer dc on dc.customer_id = fo.customer_id
   join edw_prod.data_model.fact_membership_event mee  on mee.customer_id = dc.customer_id
      and fo.order_local_datetime between mee.event_start_local_datetime and mee.event_end_local_datetime
   left join lake_consolidated_view.ultra_merchant.address da on fo.shipping_address_id = da.address_id
  );

create or replace temporary table _order_shipping_cost as (
  select order_id,order_line_id
   from
   (
   select order_id,
      order_line_id,
      row_number() over(partition by order_id order by order_line_id) as rno
   from _influencer_promo_code_orders
   ) as main
   where rno = 1
  );

update _influencer_promo_code_orders pco_upd
   set pco_upd.shipping_cost = 0
from _influencer_promo_code_orders pco
left join _order_shipping_cost osc on osc.order_id = pco.order_id
   and osc.order_line_id = pco.order_line_id
where osc.order_line_id is null
   and pco_upd.order_line_id = pco.order_line_id;

create or replace transient table reporting_media_prod.dbo.influencer_promo_code_orders as
select * from _influencer_promo_code_orders;
