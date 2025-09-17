
create or replace view reporting_media_base_prod.dbo.vw_med_hdyh_mapping as
with _hdyh_mapping as (
select hdyh_value_cleansed as hdyh_value,
       channel,
       'HDYH' as subchannel
from (
        select lower(replace(hdyh_value,' ','')) as hdyh_value_cleansed,
               channel,
               row_number() over (partition by hdyh_value_cleansed order by meta_create_datetime desc) as rn,
               meta_create_datetime
        from lake_view.sharepoint.med_hdyh_mapping
     )
where rn = 1)

, _supp_influencer_mapping as (
select lower(replace(hdyh,' ','')) as hdyh_value_cleansed,
       row_number() over (partition by hdyh_value_cleansed order by meta_create_datetime desc) as rn
from lake_view.sharepoint.med_selector_influencer_paid_spend_mappings)

, _final_influencer_mapping as (
select i.hdyh_value_cleansed as hdyh_value,
       'Influencers' as channel,
       'HDYH' as subchannel
from _supp_influencer_mapping i
left join _hdyh_mapping h on lower(h.hdyh_value) = lower(i.hdyh_value_cleansed)
where i.rn = 1
    and i.hdyh_value_cleansed is not null
    and i.hdyh_value_cleansed != ''
    and h.hdyh_value is null)

select * from _hdyh_mapping
union
select * from _final_influencer_mapping;
