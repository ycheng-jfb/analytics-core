set low_watermark_datetime = '2022-01-01';

create or replace temporary table _single_view_media as
select

    case when isscrubsgateway=true then 'Fabletics Scrubs'
        when ismalecustomersessions=true then 'Fabletics Men'
        when isyittygateway=true then 'Yitty' else storebrand
    end as store_brand_name,
    storeregion as region,
    storecountry as country,
    sessionlocaldate,

    channel,
    gatewayid,
    dmgcode,
    gatewaytype,
    gatewaysubtype,
    gatewayname,
    gatewayoffer,
    gatewaygender,
    gatewayftvorrtv,
    gatewayinfluencername,
    gatewayexperience,
    gatewaytestname,
    gatewaytestid,
    gatewayteststartlocaldatetime,
    gatewaytestendlocaldatetime,
    gatewaytestdescription,
    gatewaytesthypothesis,
    gatewaytestresults,
    lptestcellid,
    gatewaytestlptrafficsplit,
    gatewaytestlptype,
    gatewaytestlpclass,
    gatewaytestlplocation,
    lpname,
    lpid,
    lptype,
    isyittygateway,
    isscrubsgateway,
    ismalegateway,
    ismalesessionaction,
    ismalesession,
    ismalecustomer,
    ismalecustomersessions,
    leadregistrationtype,
    activatingordermembershiptype,

    sum(sessions) as sessions,
    sum(leads) as leads,
    sum(activatingorders) as activatingorders

from reporting_base_prod.shared.session_single_view_media svm
where svm.sessionlocaldate >= $low_watermark_datetime
and membershipstate = 'Prospect'
and region = 'NA'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39;

create or replace temporary table _friction_finder as
select

    to_date(datetime) as date,
    brand,

    -- total
    iff(rate_name = 'session_to_total_leads' and channel_group='total',rate_actual,0) as total_s2l_rate_actual,
    iff(rate_name = 'session_to_total_leads' and channel_group='total',rate_predicted,0) as total_s2l_rate_predicted,
    iff(rate_name = 'session_to_total_leads' and channel_group='total',rate_lower_bound,0) as total_s2l_rate_lower_bound,
    iff(rate_name = 'session_to_total_leads' and channel_group='total',rate_upper_bound,0) as total_s2l_rate_upper_bound,

    iff(rate_name = 'session_to_same_session_vip' and channel_group='total',rate_actual,0) as total_s2v_rate_actual,
    iff(rate_name = 'session_to_same_session_vip' and channel_group='total',rate_predicted,0) as total_s2v_rate_predicted,
    iff(rate_name = 'session_to_same_session_vip' and channel_group='total',rate_lower_bound,0) as total_s2v_rate_lower_bound,
    iff(rate_name = 'session_to_same_session_vip' and channel_group='total',rate_upper_bound,0) as total_s2v_rate_upper_bound,

    iff(rate_name = 'total_leads_to_same_session_vip' and channel_group='total',rate_actual,0) as total_l2v_rate_actual,
    iff(rate_name = 'total_leads_to_same_session_vip' and channel_group='total',rate_predicted,0) as total_l2v_rate_predicted,
    iff(rate_name = 'total_leads_to_same_session_vip' and channel_group='total',rate_lower_bound,0) as total_l2v_rate_lower_bound,
    iff(rate_name = 'total_leads_to_same_session_vip' and channel_group='total',rate_upper_bound,0) as total_l2v_rate_upper_bound,

    -- facebook
    iff(rate_name = 'session_to_total_leads' and channel_group='fb+ig',rate_actual,0) as fb_s2l_rate_actual,
    iff(rate_name = 'session_to_total_leads' and channel_group='fb+ig',rate_predicted,0) as fb_s2l_rate_predicted,
    iff(rate_name = 'session_to_total_leads' and channel_group='fb+ig',rate_lower_bound,0) as fb_s2l_rate_lower_bound,
    iff(rate_name = 'session_to_total_leads' and channel_group='fb+ig',rate_upper_bound,0) as fb_s2l_rate_upper_bound,

    iff(rate_name = 'session_to_same_session_vip' and channel_group='fb+ig',rate_actual,0) as fb_s2v_rate_actual,
    iff(rate_name = 'session_to_same_session_vip' and channel_group='fb+ig',rate_predicted,0) as fb_s2v_rate_predicted,
    iff(rate_name = 'session_to_same_session_vip' and channel_group='fb+ig',rate_lower_bound,0) as fb_s2v_rate_lower_bound,
    iff(rate_name = 'session_to_same_session_vip' and channel_group='fb+ig',rate_upper_bound,0) as fb_s2v_rate_upper_bound,

    iff(rate_name = 'total_leads_to_same_session_vip' and channel_group='fb+ig',rate_actual,0) as fb_l2v_rate_actual,
    iff(rate_name = 'total_leads_to_same_session_vip' and channel_group='fb+ig',rate_predicted,0) as fb_l2v_rate_predicted,
    iff(rate_name = 'total_leads_to_same_session_vip' and channel_group='fb+ig',rate_lower_bound,0) as fb_l2v_rate_lower_bound,
    iff(rate_name = 'total_leads_to_same_session_vip' and channel_group='fb+ig',rate_upper_bound,0) as fb_l2v_rate_upper_bound

from reporting_media_prod.dbo.friction_finder_daily_rates_and_preds
where channel_group in ('total','fb+ig')
and rate_name in ('sessions','total_leads','total_vips','session_to_total_leads','session_to_same_session_vip','total_leads_to_same_session_vip');

create or replace temporary table _friction_finder_agg as
select

    date as ff_date,
    brand as ff_brand,

    sum(total_s2l_rate_actual) as total_s2l_rate_actual_take_min,
    sum(total_s2l_rate_predicted) as total_s2l_rate_predicted_take_min,
    sum(total_s2l_rate_lower_bound) as total_s2l_rate_lower_bound_take_min,
    sum(total_s2l_rate_upper_bound) as total_s2l_rate_upper_bound_take_min,

    sum(total_s2v_rate_actual) as total_s2v_rate_actual_take_min,
    sum(total_s2v_rate_predicted) as total_s2v_rate_predicted_take_min,
    sum(total_s2v_rate_lower_bound) as total_s2v_rate_lower_bound_take_min,
    sum(total_s2v_rate_upper_bound) as total_s2v_rate_upper_bound_take_min,

    sum(total_l2v_rate_actual) as total_l2v_rate_actual_take_min,
    sum(total_l2v_rate_predicted) as total_l2v_rate_predicted_take_min,
    sum(total_l2v_rate_lower_bound) as total_l2v_rate_lower_bound_take_min,
    sum(total_l2v_rate_upper_bound) as total_l2v_rate_upper_bound_take_min,

    -- facebook
    sum(fb_s2l_rate_actual) as fb_s2l_rate_actual_take_min,
    sum(fb_s2l_rate_predicted) as fb_s2l_rate_predicted_take_min,
    sum(fb_s2l_rate_lower_bound) as fb_s2l_rate_lower_bound_take_min,
    sum(fb_s2l_rate_upper_bound) as fb_s2l_rate_upper_bound_take_min,

    sum(fb_s2v_rate_actual) as fb_s2v_rate_actual_take_min,
    sum(fb_s2v_rate_predicted) as fb_s2v_rate_predicted_take_min,
    sum(fb_s2v_rate_lower_bound) as fb_s2v_rate_lower_bound_take_min,
    sum(fb_s2v_rate_upper_bound) as fb_s2v_rate_upper_bound_take_min,

    sum(fb_l2v_rate_actual) as fb_l2v_rate_actual_take_min,
    sum(fb_l2v_rate_predicted) as fb_l2v_rate_predicted_take_min,
    sum(fb_l2v_rate_lower_bound) as fb_l2v_rate_lower_bound_take_min,
    sum(fb_l2v_rate_upper_bound) as fb_l2v_rate_upper_bound_take_min

from _friction_finder
group by 1,2;

create or replace transient table reporting_media_prod.dbo.single_view_media_friction_finder_baseline as
select
    svm.*,
    ff.*
from _single_view_media svm
left join _friction_finder_agg ff on svm.store_brand_name = ff.ff_brand and svm.sessionlocaldate = ff.ff_date;
