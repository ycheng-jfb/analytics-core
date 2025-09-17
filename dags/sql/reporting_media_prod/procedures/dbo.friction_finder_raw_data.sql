
set session_start_date = dateadd(day, -45, current_date());
set session_end_date = current_date();

------------------------------------------
-- session data --

create or replace temporary table _session_details as
select distinct session_id, is_pdp_visit, is_atb_action
from reporting_base_prod.shared.session s
join edw_prod.data_model.dim_store ds on ds.store_id = s.store_id
where lower(membership_state) = 'prospect'
    and lower(store_country) = 'us'
    and to_date(session_local_datetime) >= $session_start_date
    and to_date(session_local_datetime) <= $session_end_date;

create or replace temporary table _sessions as
select case when isscrubsgateway = true or isscrubssession = true or isscrubscustomer = true then 'Fabletics Scrubs'
            when ismalecustomersessions = true then 'Fabletics Men'
            when isyittygateway = true then 'Yitty' else storebrand end as storebrand,
       storeregion,
       svm.store_id,
       membershipstate,
       sessionlocaldatetime,
       sessionlocaldate,
       svm.session_id,
       lower(channel_type) as channel_type,
       channel,
       1 as sessions,
       iff(quizstarts >= 1, 1, 0) as quizstarts,
       iff(quizleads >= 1, 1, 0) as quizleads,
       iff(quizskipleads >= 1, 1, 0) as quizskipleads,
       iff(speedyleads >= 1, 1, 0) as speedyleads,
       iff(leads >= 1, 1, 0) as leads,
       iff(leadsreactivated >= 1, 1, 0) as leadsreactivated,
       iff(activatingorders24hrfromleads >= 1, 1, 0) as activatingorders24hrfromleads,
       iff(activatingorders >= 1, 1, 0) as activatingorderssamesession,
       iff(is_pdp_visit = true, 1, 0) as sessions_w_pdp_view,
       iff(is_atb_action = true, 1, 0) as sessions_w_add_to_cart

from reporting_base_prod.shared.session_single_view_media svm
join _session_details s on s.session_id = svm.session_id
where lower(membershipstate) = 'prospect'
    and lower(storecountry) = 'us'
    and to_date(sessionlocaldatetime) >= $session_start_date
    and to_date(sessionlocaldatetime) <= $session_end_date;


create or replace temporary table _sessions_agg as
select storebrand,
       storeregion,

       membershipstate,
       sessionlocaldate as session_date,
       hour(sessionlocaldatetime) as session_hour,
       date_trunc('hour',sessionlocaldatetime) as session_datehour,
       channel_type,
       channel,

       sum(sessions) as sessions,
       sum(quizstarts) as quiz_starts,
       sum(quizleads) as quiz_complete_leads,
       sum(quizskipleads) as quiz_skip_leads,
       quiz_complete_leads + quiz_skip_leads as total_quiz_leads,
       sum(speedyleads) as speedy_leads,
       sum(leads) as total_leads,
       sum(leadsreactivated) as reactivated_leads,
       sum(sessions_w_pdp_view) as sessions_w_pdp_view,
       sum(sessions_w_add_to_cart) as sessions_w_add_to_cart,
       sum(activatingorderssamesession) as same_session_vips,
       sum(activatingorders24hrfromleads) as d1_vips_from_leads

from _sessions
group by 1,2,3,4,5,6,7,8;


create or replace temporary table _sessions_running as
select *,
       sum(sessions) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as sessions_running_sum,
       sum(quiz_starts) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as quiz_starts_running_sum,
       sum(quiz_complete_leads) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as quiz_complete_leads_running_sum,
       sum(quiz_skip_leads) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as quiz_skip_leads_running_sum,
       sum(total_quiz_leads) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as total_quiz_leads_running_sum,
       sum(speedy_leads) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as speedy_leads_running_sum,
       sum(total_leads) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as total_leads_running_sum,
       sum(reactivated_leads) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as reactivated_leads_running_sum,
       sum(sessions_w_pdp_view) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as sessions_w_pdp_view_running_sum,
       sum(sessions_w_add_to_cart) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as sessions_w_add_to_cart_running_sum,
       sum(same_session_vips) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as same_session_vips_running_sum,
       sum(d1_vips_from_leads) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as d1_vips_from_leads_running_sum
from _sessions_agg;

------------------------------------------
-- total vips at session datetime --

create or replace temporary table _vips_from_session as
select case when isscrubsgateway = true or isscrubssession = true or isscrubscustomer = true then 'Fabletics Scrubs'
            when ismalecustomersessions = true then 'Fabletics Men'
            when isyittygateway = true then 'Yitty' else storebrand end as storebrand,
       storeregion,
       svm.store_id,
       membershipstate,
       sessionlocaldatetime,
       sessionlocaldate,
       svm.session_id,
       lower(channel_type) as channel_type,
       channel,
       iff(activatingorders >= 1, 1, 0) as activatingorders

from reporting_base_prod.shared.session_single_view_media svm
where lower(storecountry) = 'us'
    and to_date(sessionlocaldatetime) >= $session_start_date
    and to_date(sessionlocaldatetime) <= $session_end_date;


create or replace temporary table _vips_agg as
select storebrand,
       storeregion,
       sessionlocaldate as session_date,
       hour(sessionlocaldatetime) as session_hour,
       date_trunc('hour',sessionlocaldatetime) as session_datehour,
       channel_type,
       channel,
       sum(activatingorders) as total_vips

from _vips_from_session
group by 1,2,3,4,5,6,7;


create or replace temporary table _vips_running as
select *,
       sum(total_vips) over (partition by storebrand, storeregion, session_date, channel order by session_hour) as total_vips_running_sum
from _vips_agg;



------------------------------------------
-- fb+ig spend --

create or replace temporary table _facebook_spend as
select case when is_scrubs_flag = 1 then 'Fabletics Scrubs'
            when mens_account_flag = 1 then 'Fabletics Men'
            else ds.store_brand end as store_brand,
       ds.store_region,
       a.store_id,
       case when adset_name ilike '%daba%' or ad_name ilike '%daba%'
           or contains(lower(adset_name),'_dpa_') then 'dynamic'
            else 'bau' end as facebook_segment,
       date as date,
       cast(iff(left(hourly_stats_aggregated_by_advertiser_time_zone,2) = 00, 0,
           iff(left(hourly_stats_aggregated_by_advertiser_time_zone,2) < 10,
               replace(left(hourly_stats_aggregated_by_advertiser_time_zone,2), 0, ''),
               left(hourly_stats_aggregated_by_advertiser_time_zone,2))) as int) as hour,
       cast(concat(date,' ', hour) as datetime) as date_hour,
       sum(spend) as spend,
       sum(impressions) as impressions
from lake_view.facebook.ad_insights_by_hour f
join lake_view.sharepoint.med_account_mapping_media a on f.account_id = a.source_id
    and lower(a.source) = 'facebook'
    and a.reference_column = 'account_id'
join edw_prod.data_model.dim_store ds on ds.store_id = a.store_id
where lower(store_country) = 'us'
    and account_name not ilike '%vipret%'
    and account_name not ilike '%organic%'
    and account_name not ilike '%leadret%'
    and adset_name not ilike '%leadret%'
    and adset_name not ilike '%vipret%'
    and date >= $session_start_date
    and date <= $session_end_date
group by 1,2,3,4,5,6,7;

create or replace temporary table _facebook_classify as
select store_brand,
       store_region,
       date,
       hour,
       date_hour,
       'paid' as channel_type,
       'fb+ig' as channel,
       sum(iff(facebook_segment = 'bau', spend, 0)) as fb_prospecting_spend,
       sum(iff(facebook_segment = 'bau', impressions, 0)) as fb_prospecting_impressions,
       sum(iff(facebook_segment = 'dynamic', spend, 0)) as fb_dynamic_spend,
       sum(iff(facebook_segment = 'dynamic', impressions, 0)) as fb_dynamic_impressions
from _facebook_spend
group by 1,2,3,4,5,6,7;

create or replace temporary table _facebook_spend_running as
select *,
       sum(fb_prospecting_spend) over (partition by store_brand, store_region, date order by hour) as fb_prospecting_spend_running_sum,
       sum(fb_prospecting_impressions) over (partition by store_brand, store_region, date order by hour) as fb_prospecting_imp_running_sum,
       sum(fb_dynamic_spend) over (partition by store_brand, store_region, date order by hour) as fb_dynamic_spend_running_sum,
       sum(fb_dynamic_impressions) over (partition by store_brand, store_region, date order by hour) as fb_dynamic_imp_running_sum
from _facebook_classify;


------------------------------------------
-- combine to single table --

create or replace temporary table _combos as
select distinct storebrand,
                storeregion,
                session_date,
                session_hour,
                session_datehour,
                channel_type,
                channel
from _sessions_running
union
select distinct storebrand,
                storeregion,
                session_date,
                session_hour,
                session_datehour,
                channel_type,
                channel
from _vips_running
union
select distinct store_brand as storebrand,
                store_region as storeregion,
                date as session_date,
                hour as session_hour,
                date_hour as session_datehour,
                channel_type,
                channel
from _facebook_spend_running;

create or replace temporary table _final_output_w_rates as
select c.storebrand,
       c.storeregion,
       c.session_date,
       c.session_hour,
       c.session_datehour,
       c.channel_type,
       c.channel,

       -- session
       sessions,
       quiz_starts,
       quiz_complete_leads,
       quiz_skip_leads,
       total_quiz_leads,
       speedy_leads,
       total_leads,
       reactivated_leads,
       sessions_w_pdp_view,
       sessions_w_add_to_cart,
       same_session_vips,
       d1_vips_from_leads,
       total_vips,

       sessions_running_sum,
       quiz_starts_running_sum,
       quiz_complete_leads_running_sum,
       quiz_skip_leads_running_sum,
       total_quiz_leads_running_sum,
       speedy_leads_running_sum,
       total_leads_running_sum,
       reactivated_leads_running_sum,
       sessions_w_pdp_view_running_sum,
       sessions_w_add_to_cart_running_sum,
       same_session_vips_running_sum,
       d1_vips_from_leads_running_sum,
       total_vips_running_sum,

       -- fb+ig spend
       fb_prospecting_spend,
       fb_prospecting_impressions,
       fb_dynamic_spend,
       fb_dynamic_impressions,

       fb_prospecting_spend_running_sum,
       fb_prospecting_imp_running_sum,
       fb_dynamic_spend_running_sum,
       fb_dynamic_imp_running_sum,

       -- rates
       fb_prospecting_impressions / iff(fb_prospecting_spend = 0, null, fb_prospecting_spend)  as rate_fb_pros_spend_to_imp,
       fb_dynamic_impressions / iff(fb_dynamic_spend = 0, null, fb_dynamic_spend)  as rate_fb_dynamic_spend_to_imp,

       sessions / iff((fb_prospecting_impressions + fb_dynamic_impressions) = 0, null,
                            (fb_prospecting_impressions + fb_dynamic_impressions)) as rate_total_fb_imp_to_session,
       quiz_starts / iff(fb_prospecting_impressions = 0, null, fb_prospecting_impressions)  as rate_fb_pros_imp_to_quiz_start,
       speedy_leads / iff(fb_dynamic_impressions = 0, null, fb_dynamic_impressions)  as rate_fb_dynamic_imp_to_speedy_lead,

       quiz_starts / iff(sessions = 0, null, sessions)  as rate_session_to_quiz_start,
       total_quiz_leads / iff(sessions = 0, null, sessions)  as rate_session_to_quiz_leads,
       total_quiz_leads / iff(quiz_starts = 0, null, quiz_starts)  as rate_quiz_start_to_quiz_leads,
       speedy_leads / iff(sessions = 0, null, sessions)  as rate_session_to_speedy_lead,
       total_leads / iff(sessions = 0, null, sessions)  as rate_session_to_total_leads,

       sessions_w_add_to_cart / iff(total_leads = 0, null, total_leads)  as rate_total_leads_to_add_to_cart,
       sessions_w_add_to_cart / iff(sessions_w_pdp_view = 0, null, sessions_w_pdp_view) as rate_pdp_view_to_add_to_cart,

       same_session_vips / iff(total_leads = 0, null, total_leads)  as rate_total_leads_to_same_session_vip,
       same_session_vips / iff(sessions_w_add_to_cart = 0, null, sessions_w_add_to_cart) as rate_add_to_cart_to_same_session_vip,
       d1_vips_from_leads / iff(sessions_w_add_to_cart = 0, null, sessions_w_add_to_cart) as rate_add_to_cart_to_d1_vip


from _combos c
left join _sessions_running s on s.storebrand = c.storebrand
    and s.storeregion = c.storeregion
    and s.session_date = c.session_date
    and s.session_hour = c.session_hour
    and s.session_datehour = c.session_datehour
    and s.channel_type = c.channel_type
    and s.channel = c.channel
left join _vips_running v on v.storebrand = c.storebrand
    and v.storeregion = c.storeregion
    and v.session_date = c.session_date
    and v.session_hour = c.session_hour
    and v.session_datehour = c.session_datehour
    and v.channel_type = c.channel_type
    and v.channel = c.channel
left join _facebook_spend_running f on f.store_brand = c.storebrand
    and f.store_region = c.storeregion
    and f.date = c.session_date
    and f.hour = c.session_hour
    and f.date_hour = c.session_datehour
    and f.channel_type = c.channel_type
    and f.channel = c.channel;


set max_session_hour_today = (select least(
                                    (select max(session_hour) from _sessions_running where session_date = $session_end_date),
                                    (select max(session_hour) from _vips_running where session_date = $session_end_date),
                                    (select max(hour) from _facebook_spend_running where date = $session_end_date)
                             ));

delete from _final_output_w_rates
where session_date = $session_end_date
    and session_hour >= $max_session_hour_today;

create or replace transient table reporting_media_prod.dbo.friction_finder_raw_data as
select *
from _final_output_w_rates;


------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
-- aggregate view --

set session_start_date = dateadd(day, -370, current_date());
set session_end_date = current_date();

------------------------------------------
-- session data --

create or replace temporary table _session_details as
select distinct session_id, is_pdp_visit, is_atb_action
from reporting_base_prod.shared.session s
join edw_prod.data_model.dim_store ds on ds.store_id = s.store_id
where lower(membership_state) = 'prospect'
    and to_date(session_local_datetime) >= $session_start_date
    and to_date(session_local_datetime) < $session_end_date
    and lower(store_country) = 'us';

create or replace temporary table _sessions as
select case when isscrubsgateway = true or isscrubssession = true or isscrubscustomer = true then 'Fabletics Scrubs'
            when ismalecustomersessions = true then 'Fabletics Men'
            when isyittygateway = true then 'Yitty' else storebrand end as storebrand,
       storeregion,
       svm.store_id,
       membershipstate,
       sessionlocaldatetime,
       sessionlocaldate,
       svm.session_id,
       lower(channel_type) as channel_type,
       channel,
       1 as sessions,
       iff(quizstarts >= 1, 1, 0) as quizstarts,
       iff(quizleads >= 1, 1, 0) as quizleads,
       iff(quizskipleads >= 1, 1, 0) as quizskipleads,
       iff(speedyleads >= 1, 1, 0) as speedyleads,
       iff(leads >= 1, 1, 0) as leads,
       iff(leadsreactivated >= 1, 1, 0) as leadsreactivated,
       iff(activatingorders24hrfromleads >= 1, 1, 0) as activatingorders24hrfromleads,
       iff(activatingorders >= 1, 1, 0) as activatingorderssamesession,
       iff(is_pdp_visit = true, 1, 0) as sessions_w_pdp_view,
       iff(is_atb_action = true, 1, 0) as sessions_w_add_to_cart

from reporting_base_prod.shared.session_single_view_media svm
join _session_details s on s.session_id = svm.session_id
where lower(membershipstate) = 'prospect'
    and lower(storecountry) = 'us'
    and to_date(sessionlocaldatetime) >= $session_start_date
    and to_date(sessionlocaldatetime) < $session_end_date;


create or replace temporary table _sessions_agg as
select storebrand,
       storeregion,
       sessionlocaldate as session_date,
       channel_type,
       channel,

       sum(sessions) as sessions,
       sum(quizstarts) as quiz_starts,
       sum(quizleads) as quiz_complete_leads,
       sum(quizskipleads) as quiz_skip_leads,
       quiz_complete_leads + quiz_skip_leads as total_quiz_leads,
       sum(speedyleads) as speedy_leads,
       sum(leads) as total_leads,
       sum(leadsreactivated) as reactivated_leads,
       sum(sessions_w_pdp_view) as sessions_w_pdp_view,
       sum(sessions_w_add_to_cart) as sessions_w_add_to_cart,
       sum(activatingorderssamesession) as same_session_vips,
       sum(activatingorders24hrfromleads) as d1_vips_from_leads

from _sessions
group by 1,2,3,4,5;

------------------------------------------
-- total vips at session datetime --

create or replace temporary table _vips_from_session as
select case when isscrubsgateway = true or isscrubssession = true or isscrubscustomer = true then 'Fabletics Scrubs'
            when ismalecustomersessions = true then 'Fabletics Men'
            when isyittygateway = true then 'Yitty' else storebrand end as storebrand,
       storeregion,
       svm.store_id,
       membershipstate,
       sessionlocaldatetime,
       sessionlocaldate,
       svm.session_id,
       lower(channel_type) as channel_type,
       channel,
       iff(activatingorders >= 1, 1, 0) as activatingorders

from reporting_base_prod.shared.session_single_view_media svm
where lower(storecountry) = 'us'
    and to_date(sessionlocaldatetime) >= $session_start_date
    and to_date(sessionlocaldatetime) <= $session_end_date;


create or replace temporary table _vips_agg as
select storebrand,
       storeregion,
       sessionlocaldate as session_date,
       channel_type,
       channel,
       sum(activatingorders) as total_vips

from _vips_from_session
group by 1,2,3,4,5;


------------------------------------------
-- fb+ig spend --

create or replace temporary table _facebook_spend as
select case when is_scrubs_flag = 1 then 'Fabletics Scrubs'
            when mens_account_flag = 1 then 'Fabletics Men'
            else ds.store_brand end as store_brand,
       ds.store_region,
       a.store_id,
       case when adset_name ilike '%daba%' or ad_name ilike '%daba%'
           or contains(lower(adset_name),'_dpa_') then 'dynamic'
            else 'bau' end as facebook_segment,
       date as date,
       sum(spend) as spend,
       sum(impressions) as impressions
from lake_view.facebook.ad_insights_by_hour f
join lake_view.sharepoint.med_account_mapping_media a ON f.account_id = a.source_id
    and lower(a.source) = 'facebook'
    and a.reference_column = 'account_id'
join edw_prod.data_model.dim_store ds on ds.store_id = a.store_id
where lower(store_country) = 'us'
    and account_name not ilike '%vipret%'
    and account_name not ilike '%organic%'
    and account_name not ilike '%leadret%'
    and adset_name not ilike '%leadret%'
    and adset_name not ilike '%vipret%'
    and date >= $session_start_date
    and date < $session_end_date
group by 1,2,3,4,5;


create or replace temporary table _facebook_classify as
select store_brand,
       store_region,
       date,
       'paid' as channel_type,
       'fb+ig' as channel,
       sum(iff(facebook_segment = 'bau', spend, 0)) as fb_prospecting_spend,
       sum(iff(facebook_segment = 'bau', impressions, 0)) as fb_prospecting_impressions,
       sum(iff(facebook_segment = 'dynamic', spend, 0)) as fb_dynamic_spend,
       sum(iff(facebook_segment = 'dynamic', impressions, 0)) as fb_dynamic_impressions
from _facebook_spend
group by 1,2,3,4,5;


------------------------------------------
-- combine to single table --

create or replace temporary table _combos as
select distinct storebrand,
                storeregion,
                session_date,
                channel_type,
                channel
from _sessions_agg
union
select distinct storebrand,
                storeregion,
                session_date,
                channel_type,
                channel
from _vips_agg
union
select distinct store_brand as storebrand,
                store_region as storeregion,
                date as session_date,
                channel_type,
                channel
from _facebook_classify;


create or replace temporary table _final_output_w_rates as
select c.storebrand,
       c.storeregion,
       c.session_date,
       c.channel_type,
       c.channel,

       -- session
       sessions,
       quiz_starts,
       quiz_complete_leads,
       quiz_skip_leads,
       total_quiz_leads,
       speedy_leads,
       total_leads,
       reactivated_leads,
       sessions_w_pdp_view,
       sessions_w_add_to_cart,
       same_session_vips,
       d1_vips_from_leads,
       total_vips,

       -- fb+ig spend
       fb_prospecting_spend,
       fb_prospecting_impressions,
       fb_dynamic_spend,
       fb_dynamic_impressions,

       -- rates
       fb_prospecting_impressions / iff(fb_prospecting_spend = 0, null, fb_prospecting_spend)  as rate_fb_pros_spend_to_imp,
       fb_dynamic_impressions / iff(fb_dynamic_spend = 0, null, fb_dynamic_spend)  as rate_fb_dynamic_spend_to_imp,

       sessions / iff((fb_prospecting_impressions + fb_dynamic_impressions) = 0, null,
                            (fb_prospecting_impressions + fb_dynamic_impressions)) as rate_total_fb_imp_to_session,
       quiz_starts / iff(fb_prospecting_impressions = 0, null, fb_prospecting_impressions)  as rate_fb_pros_imp_to_quiz_start,
       speedy_leads / iff(fb_dynamic_impressions = 0, null, fb_dynamic_impressions)  as rate_fb_dynamic_imp_to_speedy_lead,

       quiz_starts / iff(sessions = 0, null, sessions)  as rate_session_to_quiz_start,
       total_quiz_leads / iff(sessions = 0, null, sessions)  as rate_session_to_quiz_leads,
       total_quiz_leads / iff(quiz_starts = 0, null, quiz_starts)  as rate_quiz_start_to_quiz_leads,
       speedy_leads / iff(sessions = 0, null, sessions)  as rate_session_to_speedy_lead,
       total_leads / iff(sessions = 0, null, sessions)  as rate_session_to_total_leads,

       sessions_w_add_to_cart / iff(total_leads = 0, null, total_leads)  as rate_total_leads_to_add_to_cart,
       sessions_w_add_to_cart / iff(sessions_w_pdp_view = 0, null, sessions_w_pdp_view) as rate_pdp_view_to_add_to_cart,

       same_session_vips / iff(total_leads = 0, null, total_leads)  as rate_total_leads_to_same_session_vip,
       same_session_vips / iff(sessions_w_add_to_cart = 0, null, sessions_w_add_to_cart) as rate_add_to_cart_to_same_session_vip,
       d1_vips_from_leads / iff(sessions_w_add_to_cart = 0, null, sessions_w_add_to_cart) as rate_add_to_cart_to_d1_vip


from _combos c
left join _sessions_agg s on s.storebrand = c.storebrand
    and s.storeregion = c.storeregion
    and s.session_date = c.session_date
    and s.channel_type = c.channel_type
    and s.channel = c.channel
left join _vips_agg v on v.storebrand = c.storebrand
    and v.storeregion = c.storeregion
    and v.session_date = c.session_date
    and v.channel_type = c.channel_type
    and v.channel = c.channel
left join _facebook_classify f on f.store_brand = c.storebrand
    and f.store_region = c.storeregion
    and f.date = c.session_date
    and f.channel_type = c.channel_type
    and f.channel = c.channel;


create or replace transient table reporting_media_prod.dbo.friction_finder_raw_data_daily as
select * from _final_output_w_rates;

delete from reporting_media_prod.dbo.friction_finder_raw_data_daily
where lower(storebrand) = 'yitty' and session_date < '2022-04-12';
