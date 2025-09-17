
create or replace temporary table _spend_conv_combo as
select store_id, date, ad_id, age, gender
from reporting_media_base_prod.facebook.advertising_spend_metrics_daily_by_age_gender
union
select store_id, date, ad_id, age, gender
from reporting_media_base_prod.facebook.conversion_metrics_daily_by_age_gender;

create or replace transient table reporting_media_prod.facebook.facebook_optimization_dataset_age_gender as
select n.*,
       cc.date,
       cc.store_id,
       st.store_country as country,
       cc.age,
       cc.gender,

       spend_account_currency,
       spend_usd,
       spend_local,
       impressions,
       clicks,

       pixel_lead_click_1d,
       pixel_lead_view_1d,
       pixel_lead_click_7d,
       pixel_lead_view_7d,
       0 as pixel_lead_click_28d,
       0 as pixel_lead_view_28d,

       pixel_vip_click_1d,
       pixel_vip_view_1d,
       pixel_vip_click_7d,
       pixel_vip_view_7d,
       0 as pixel_vip_click_28d,
       0 as pixel_vip_view_28d,

       pixel_purchase_click_1d,
       pixel_purchase_view_1d,
       pixel_purchase_click_7d,
       pixel_purchase_view_7d,
       0 as pixel_purchase_click_28d,
       0 as pixel_purchase_view_28d,

       pixel_subscribe_click_1d,
       pixel_subscribe_view_1d,
       pixel_subscribe_click_7d,
       pixel_subscribe_view_7d,
       0 as pixel_subscribe_click_28d,
       0 as pixel_subscribe_view_28d,

       pixel_subscribe_revenue_click_1d,
       pixel_subscribe_revenue_view_1d,
       pixel_subscribe_revenue_click_7d,
       pixel_subscribe_revenue_view_7d,
       0 as pixel_subscribe_revenue_click_28d,
       0 as pixel_subscribe_revenue_view_28d,

       s.pixel_spend_meta_update_datetime,
       c.pixel_conversion_meta_update_datetime,
       current_timestamp()::timestamp_ltz as process_meta_update_datetime

from _spend_conv_combo cc
join edw_prod.data_model.dim_store st on st.store_id = cc.store_id
left join reporting_media_base_prod.facebook.advertising_spend_metrics_daily_by_age_gender s on s.store_id = cc.store_id
    and s.date = cc.date
    and s.ad_id = cc.ad_id
    and s.age = cc.age
    and s.gender = cc.gender
left join reporting_media_base_prod.facebook.conversion_metrics_daily_by_age_gender c on c.store_id = cc.store_id
    and c.date = cc.date
    and c.ad_id = cc.ad_id
    and c.age = cc.age
    and c.gender = cc.gender
left join reporting_media_base_prod.facebook.vw_facebook_naming_convention n on n.ad_id = cc.ad_id;
