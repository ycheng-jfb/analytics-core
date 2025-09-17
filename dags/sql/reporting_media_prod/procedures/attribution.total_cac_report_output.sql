
/*
total cac currently connects to: reporting_media_prod.attribution.total_cac_report_output
we had created a parallel version, so maintaining this in the proc,
but will delete after stabilized: reporting_media_prod.attribution.total_cac_report_output
*/

set end_date = IFF(
        CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', CURRENT_TIMESTAMP())::DATE > CURRENT_DATE(),
        DATEADD(DAY, 1, CURRENT_DATE()),
        CURRENT_DATE()
    );
set report_date = dateadd(day, -1, $end_date);
set current_month_date = date_trunc(month, $report_date);

create table if not exists reporting_media_prod.attribution.total_cac_report_output (
    sequence                                 number(38, 0),
    store_name                               varchar,
    store_abbreviation                       varchar,
    date                                     date,
    primary_leads                            number(38, 4),
    secondary_leads                          number(38, 4),
    retail_leads                             number(38, 4),
    cross_brand_leads_per                    decimal(20,4),
    cpl                                      number(38, 4),
    media_spend                              number(38, 4),
    prospecting_cpl                          number(38, 4),
    d1_cac                                   number(38, 4),
    vips_from_leads_30m                      number(38, 4),
    vips_from_leads_30m_cac                  number(38, 4),
    vips_from_leads_1d                      number(38, 4),
    vips_from_leads_1d_cac                  number(38, 4),
    vips_from_leads_7d                       number(38, 4),
    vips_from_leads_24h                      number(38, 4),
    vips_from_leads_24h_cac                  number(38, 4),
	vips_from_leads_m1						 number(38, 4),
	vips_from_leads_m1_cac					 number(38, 4),
    vips_on_date_m1                          number(38, 4),
    vips_on_date_m1_cac                      number(38, 4),
    total_vips_from_reactivated_leads        number(38, 4),
    total_vips_on_date                       number(38, 4),
    total_vips_on_date_cac                   number(38, 4),
    vips_on_date_d1                          number(38, 4),
    vips_on_date_d2_d7                       number(38, 4),
    vips_on_date_d1_d7                       number(38, 4),
    vips_on_date_d8_d90                      number(38, 4),
    vips_on_date_d90plus                     number(38, 4),
    reactivated_vips                         number(38, 4),
    reactivated_vips_per                     number(38, 4),
    retail_vips                              number(38, 4),
    varsity_vips                             number(38, 4),
    vips_from_reactivated_leads_d1           number(38, 4),
    vips_on_date_d1_excluding_reactivated_leads     number(38, 4),
    vips_on_date_d1_reactivated_leads        number(38, 4),
    vips_on_date_m1_reactivated_leads        number(38, 4),
    reactivated_leads                        number(38, 4),
    vips_from_leads_30m_per                  number(38, 4),
    vips_from_leads_24h_per                  number(38, 4),
    vips_from_reactivated_leads_24h_per      number(38, 4),
    d1_vips_per                              number(38, 4),
    d1_vips_excluding_reactivated_leads_per  number(38, 4),
    d1_vips_reactivated_leads_per            number(38, 4),
    d2_d7_vips_per                           number(38, 4),
    d1_d7_vips_per                           number(38, 4),
    d8_d90_vips_per                          number(38, 4),
    d90plus_vips_per                         number(38, 4),
    reactivated_leads_per                    number(38, 4),
    prospecting_spend                        number(38, 4),
    programmatic_gdn_spend                   number(38, 4),
    facebook_spend                           number(38, 4),
    snapchat_spend                           number(38, 4),
    pinterest_spend                          number(38, 4),
    non_branded_search_spend                 number(38, 4),
    branded_search_spend                     number(38, 4),
    shopping_spend                           number(38, 4),
    programmatic_spend                       number(38, 4),
    youtube_spend                            number(38, 4),
    tv_and_streaming_spend                   number(38, 4),
    testing_spend                            number(38, 4),
    print_spend                              number(38, 4),
    radio_podcast_spend                      number(38, 4),
    influencers_spend                        number(38, 4),
    affiliate_spend                          number(38, 4),
    physical_partnerships_spend              number(38, 4),
    tiktok_spend                             number(38, 4),
    reddit_spend                             number(38, 4),
    twitter_spend                            number(38, 4),
    prospecting_spend_per                    number(38, 4),
    programmatic_gdn_spend_per               number(38, 4),
    facebook_spend_per                       number(38, 4),
    snapchat_spend_per                       number(38, 4),
    pinterest_spend_per                      number(38, 4),
    non_branded_search_spend_per             number(38, 4),
    branded_search_spend_per                 number(38, 4),
    shopping_spend_per                       number(38, 4),
    programmatic_spend_per                   number(38, 4),
    youtube_spend_per                        number(38, 4),
    tv_and_streaming_spend_per               number(38, 4),
    testing_spend_per                        number(38, 4),
    print_spend_per                          number(38, 4),
    radio_podcast_spend_per                  number(38, 4),
    influencers_spend_per                    number(38, 4),
    affiliate_spend_per                      number(38, 4),
    physical_partnerships_spend_per          number(38, 4),
    tiktok_spend_per                         number(38, 4),
    reddit_spend_per                         number(38, 4),
    twitter_spend_per                        number(38, 4),
    currency                                 varchar,
    source                                   varchar,
    version                                  varchar,
    channel_sequence                         number(38, 4),
    report_month                             date,
    activating_acquisition_margin            number(38, 4),
    activating_acquisition_margin_per_order  number(38, 4),
    cac_am                                   number(38, 4),
    pick_up_from_same_session_24hr           number(38, 4),
    pick_up_from_same_session_7d             number(38, 4),
    source_sequence                          number(38, 0),
    ecom_activating_shipped_gross_margin     number(20, 4),
    xcac                                     number(20, 4),
    bop_vips                                 number(20, 4),
    eop_vips                                 number(20, 4),
    cancels_on_date                          number(38,0),
    passive_cancels_on_date                  number(38,0),
	cpl_excl_influencer_spend 				 number(38, 4),
	vips_on_date_d1_excl_influencer_spend 	 number(38, 4),
	vips_from_leads_1d_excl_influencer_spend number(38, 4),
    vips_on_date_m1_excl_influencer_spend 	 number(38, 4),
    vips_from_leads_m1_excl_influencer_spend number(38, 4),
	total_vips_on_date_excl_influencer_spend number(38, 4)
);

CREATE TRANSIENT TABLE IF NOT EXISTS reporting_media_prod.snapshot.total_cac_report_output (
    sequence                                 number(38, 0),
    store_name                               varchar,
    store_abbreviation                       varchar,
    date                                     date,
    primary_leads                            number(38, 4),
    secondary_leads                          number(38, 4),
    retail_leads                             number(38, 4),
    cross_brand_leads_per                    decimal(20,4),
    cpl                                      number(38, 4),
    media_spend                              number(38, 4),
    prospecting_cpl                          number(38, 4),
    d1_cac                                   number(38, 4),
    vips_from_leads_30m                      number(38, 4),
    vips_from_leads_30m_cac                  number(38, 4),
    vips_from_leads_1d                      number(38, 4),
    vips_from_leads_1d_cac                  number(38, 4),
    vips_from_leads_7d                       number(38, 4),
    vips_from_leads_24h                      number(38, 4),
    vips_from_leads_24h_cac                  number(38, 4),
	vips_from_leads_m1						 number(38, 4),
	vips_from_leads_m1_cac					 number(38, 4),
    vips_on_date_m1                          number(38, 4),
    vips_on_date_m1_cac                      number(38, 4),
    total_vips_from_reactivated_leads        number(38, 4),
    total_vips_on_date                       number(38, 4),
    total_vips_on_date_cac                   number(38, 4),
    vips_on_date_d1                          number(38, 4),
    vips_on_date_d2_d7                       number(38, 4),
    vips_on_date_d1_d7                       number(38, 4),
    vips_on_date_d8_d90                      number(38, 4),
    vips_on_date_d90plus                     number(38, 4),
    reactivated_vips                         number(38, 4),
    reactivated_vips_per                     number(38, 4),
    retail_vips                              number(38, 4),
    varsity_vips                             number(38, 4),
    vips_from_reactivated_leads_d1           number(38, 4),
    vips_on_date_d1_excluding_reactivated_leads     number(38, 4),
    vips_on_date_d1_reactivated_leads        number(38, 4),
    vips_on_date_m1_reactivated_leads        number(38, 4),
    reactivated_leads                        number(38, 4),
    vips_from_leads_30m_per                  number(38, 4),
    vips_from_leads_24h_per                  number(38, 4),
    vips_from_reactivated_leads_24h_per      number(38, 4),
    d1_vips_per                              number(38, 4),
    d1_vips_excluding_reactivated_leads_per  number(38, 4),
    d1_vips_reactivated_leads_per            number(38, 4),
    d2_d7_vips_per                           number(38, 4),
    d1_d7_vips_per                           number(38, 4),
    d8_d90_vips_per                          number(38, 4),
    d90plus_vips_per                         number(38, 4),
    reactivated_leads_per                    number(38, 4),
    prospecting_spend                        number(38, 4),
    programmatic_gdn_spend                   number(38, 4),
    facebook_spend                           number(38, 4),
    snapchat_spend                           number(38, 4),
    pinterest_spend                          number(38, 4),
    non_branded_search_spend                 number(38, 4),
    branded_search_spend                     number(38, 4),
    shopping_spend                           number(38, 4),
    programmatic_spend                       number(38, 4),
    youtube_spend                            number(38, 4),
    tv_and_streaming_spend                   number(38, 4),
    testing_spend                            number(38, 4),
    print_spend                              number(38, 4),
    radio_podcast_spend                      number(38, 4),
    influencers_spend                        number(38, 4),
    affiliate_spend                          number(38, 4),
    physical_partnerships_spend              number(38, 4),
    tiktok_spend                             number(38, 4),
    reddit_spend                             number(38, 4),
    twitter_spend                            number(38, 4),
    prospecting_spend_per                    number(38, 4),
    programmatic_gdn_spend_per               number(38, 4),
    facebook_spend_per                       number(38, 4),
    snapchat_spend_per                       number(38, 4),
    pinterest_spend_per                      number(38, 4),
    non_branded_search_spend_per             number(38, 4),
    branded_search_spend_per                 number(38, 4),
    shopping_spend_per                       number(38, 4),
    programmatic_spend_per                   number(38, 4),
    youtube_spend_per                        number(38, 4),
    tv_and_streaming_spend_per               number(38, 4),
    testing_spend_per                        number(38, 4),
    print_spend_per                          number(38, 4),
    radio_podcast_spend_per                  number(38, 4),
    influencers_spend_per                    number(38, 4),
    affiliate_spend_per                      number(38, 4),
    physical_partnerships_spend_per          number(38, 4),
    tiktok_spend_per                         number(38, 4),
    reddit_spend_per                         number(38, 4),
    twitter_spend_per                        number(38, 4),
    currency                                 varchar,
    source                                   varchar,
    version                                  varchar,
    channel_sequence                         number(38, 4),
    report_month                             date,
    activating_acquisition_margin            number(38, 4),
    activating_acquisition_margin_per_order  number(38, 4),
    cac_am                                   number(38, 4),
    pick_up_from_same_session_24hr           number(38, 4),
    pick_up_from_same_session_7d             number(38, 4),
    source_sequence                          number(38, 0),
    ecom_activating_shipped_gross_margin     number(20, 4),
    xcac                                     number(20, 4),
    bop_vips                                 number(20, 4),
    eop_vips                                 number(20, 4),
    cancels_on_date                          number(38,0),
    passive_cancels_on_date                  number(38,0),
	cpl_excl_influencer_spend 				 number(38, 4),
	vips_on_date_d1_excl_influencer_spend 	 number(38, 4),
	vips_from_leads_1d_excl_influencer_spend number(38, 4),
    vips_on_date_m1_excl_influencer_spend 	 number(38, 4),
    vips_from_leads_m1_excl_influencer_spend number(38, 4),
	total_vips_on_date_excl_influencer_spend number(38, 4),
    datetime_added     timestamp_ltz
);


delete
from reporting_media_prod.snapshot.total_cac_report_output
where datetime_added < dateadd(day, -3, current_timestamp());

insert into reporting_media_prod.snapshot.total_cac_report_output
select sequence
	,store_name
	,store_abbreviation
	,date
	,primary_leads
	,secondary_leads
	,retail_leads
	,cross_brand_leads_per
	,cpl
	,media_spend
	,prospecting_cpl
	,d1_cac
	,vips_from_leads_30m
	,vips_from_leads_30m_cac
    ,vips_from_leads_1d
	,vips_from_leads_1d_cac
	,vips_from_leads_7d
	,vips_from_leads_24h
	,vips_from_leads_24h_cac
	,vips_from_leads_m1
    ,vips_from_leads_m1_cac
	,vips_on_date_m1
	,vips_on_date_m1_cac
	,total_vips_from_reactivated_leads
	,total_vips_on_date
	,total_vips_on_date_cac
	,vips_on_date_d1
	,vips_on_date_d2_d7
	,vips_on_date_d1_d7
	,vips_on_date_d8_d90
	,vips_on_date_d90plus
	,reactivated_vips
	,reactivated_vips_per
	,retail_vips
	,varsity_vips
    ,vips_from_reactivated_leads_d1
    ,vips_on_date_d1_excluding_reactivated_leads
    ,vips_on_date_d1_reactivated_leads
    ,vips_on_date_m1_reactivated_leads
	,reactivated_leads
	,vips_from_leads_30m_per
	,vips_from_leads_24h_per
	,vips_from_reactivated_leads_24h_per
	,d1_vips_per
    ,d1_vips_excluding_reactivated_leads_per
    ,d1_vips_reactivated_leads_per
	,d2_d7_vips_per
	,d1_d7_vips_per
	,d8_d90_vips_per
	,d90plus_vips_per
	,reactivated_leads_per
	,prospecting_spend
	,programmatic_gdn_spend
	,facebook_spend
	,snapchat_spend
	,pinterest_spend
	,non_branded_search_spend
	,branded_search_spend
	,shopping_spend
	,programmatic_spend
	,youtube_spend
	,tv_and_streaming_spend
	,testing_spend
	,print_spend
	,radio_podcast_spend
	,influencers_spend
	,affiliate_spend
	,physical_partnerships_spend
	,tiktok_spend
	,reddit_spend
	,twitter_spend
	,prospecting_spend_per
	,programmatic_gdn_spend_per
	,facebook_spend_per
	,snapchat_spend_per
	,pinterest_spend_per
	,non_branded_search_spend_per
	,branded_search_spend_per
	,shopping_spend_per
	,programmatic_spend_per
	,youtube_spend_per
	,tv_and_streaming_spend_per
	,testing_spend_per
	,print_spend_per
	,radio_podcast_spend_per
	,influencers_spend_per
	,affiliate_spend_per
	,physical_partnerships_spend_per
	,tiktok_spend_per
	,reddit_spend_per
	,twitter_spend_per
	,currency
	,source
	,version
	,channel_sequence
	,report_month
	,activating_acquisition_margin
	,activating_acquisition_margin_per_order
	,cac_am
	,pick_up_from_same_session_24hr
	,pick_up_from_same_session_7d
	,source_sequence
    ,ecom_activating_shipped_gross_margin
    ,xcac
    ,bop_vips
    ,eop_vips
    ,cancels_on_date
    ,passive_cancels_on_date
	,cpl_excl_influencer_spend
	,vips_on_date_d1_excl_influencer_spend
    ,vips_from_leads_1d_excl_influencer_spend
	,vips_on_date_m1_excl_influencer_spend
    ,vips_from_leads_m1_excl_influencer_spend
	,total_vips_on_date_excl_influencer_spend
    ,current_timestamp::timestamp_ltz
from reporting_media_prod.attribution.total_cac_report_output;

truncate table reporting_media_prod.attribution.total_cac_report_output;

insert into reporting_media_prod.attribution.total_cac_report_output (
     sequence
	,store_name
	,store_abbreviation
	,date
	,primary_leads
	,secondary_leads
	,retail_leads
	,cross_brand_leads_per
	,cpl
	,media_spend
	,prospecting_cpl
	,d1_cac
	,vips_from_leads_30m
	,vips_from_leads_30m_cac
    ,vips_from_leads_1d
	,vips_from_leads_1d_cac
	,vips_from_leads_7d
	,vips_from_leads_24h
	,vips_from_leads_24h_cac
	,vips_from_leads_m1
    ,vips_from_leads_m1_cac
	,vips_on_date_m1
	,vips_on_date_m1_cac
	,total_vips_from_reactivated_leads
	,total_vips_on_date
	,total_vips_on_date_cac
	,vips_on_date_d1
	,vips_on_date_d2_d7
	,vips_on_date_d1_d7
	,vips_on_date_d8_d90
	,vips_on_date_d90plus
	,reactivated_vips
	,reactivated_vips_per
	,retail_vips
	,varsity_vips
    ,vips_from_reactivated_leads_d1
    ,vips_on_date_d1_excluding_reactivated_leads
    ,vips_on_date_d1_reactivated_leads
    ,vips_on_date_m1_reactivated_leads
	,reactivated_leads
	,vips_from_leads_30m_per
	,vips_from_leads_24h_per
	,vips_from_reactivated_leads_24h_per
	,d1_vips_per
    ,d1_vips_excluding_reactivated_leads_per
    ,d1_vips_reactivated_leads_per
	,d2_d7_vips_per
	,d1_d7_vips_per
	,d8_d90_vips_per
	,d90plus_vips_per
	,reactivated_leads_per
	,prospecting_spend
	,programmatic_gdn_spend
	,facebook_spend
	,snapchat_spend
	,pinterest_spend
	,non_branded_search_spend
	,branded_search_spend
	,shopping_spend
	,programmatic_spend
	,youtube_spend
	,tv_and_streaming_spend
	,testing_spend
	,print_spend
	,radio_podcast_spend
	,influencers_spend
	,affiliate_spend
	,physical_partnerships_spend
	,tiktok_spend
	,reddit_spend
	,twitter_spend
	,prospecting_spend_per
	,programmatic_gdn_spend_per
	,facebook_spend_per
	,snapchat_spend_per
	,pinterest_spend_per
	,non_branded_search_spend_per
	,branded_search_spend_per
	,shopping_spend_per
	,programmatic_spend_per
	,youtube_spend_per
	,tv_and_streaming_spend_per
	,testing_spend_per
	,print_spend_per
	,radio_podcast_spend_per
	,influencers_spend_per
	,affiliate_spend_per
	,physical_partnerships_spend_per
	,tiktok_spend_per
	,reddit_spend_per
	,twitter_spend_per
	,currency
	,source
	,version
	,channel_sequence
	,report_month
	,activating_acquisition_margin
	,activating_acquisition_margin_per_order
	,cac_am
	,pick_up_from_same_session_24hr
	,pick_up_from_same_session_7d
	,source_sequence
    ,ecom_activating_shipped_gross_margin
    ,xcac
    ,bop_vips
    ,eop_vips
    ,cancels_on_date
    ,passive_cancels_on_date
	,cpl_excl_influencer_spend
	,vips_on_date_d1_excl_influencer_spend
    ,vips_from_leads_1d_excl_influencer_spend
	,vips_on_date_m1_excl_influencer_spend
    ,vips_from_leads_m1_excl_influencer_spend
	,total_vips_on_date_excl_influencer_spend
)
select sequence
	,store_name
	,store_abbreviation
	,date
	,primary_leads
	,secondary_leads
    ,retail_leads
	,cross_brand_leads_per
	,cpl
	,media_spend
	,prospecting_spend/nullif(primary_leads, 0) as prospecting_cpl
	,media_spend/nullif(vips_on_date_d1,0) as d1_cac
	,vips_from_leads_30m
	,vips_from_leads_30m_cac
    ,vips_from_leads_1d
	,vips_from_leads_1d_cac
	,vips_from_leads_7d
	,vips_from_leads_24h
	,vips_from_leads_24h_cac
    ,vips_from_leads_m1
    ,vips_from_leads_m1_cac
	,vips_on_date_m1
	,vips_on_date_m1_cac
	,total_vips_from_reactivated_leads
	,total_vips_on_date
	,total_vips_on_date_cac
	,vips_on_date_d1
	,vips_on_date_d2_d7
	,vips_on_date_d1_d7
	,vips_on_date_d8_d90
	,vips_on_date_d90plus
	,reactivated_vips
	,reactivated_vips_per
	,retail_vips
	,varsity_vips
    ,vips_from_reactivated_leads_d1
    ,vips_on_date_d1_excluding_reactivated_leads
    ,vips_on_date_d1_reactivated_leads
    ,vips_on_date_m1_reactivated_leads
	,reactivated_leads
	,vips_from_leads_30m_per
	,vips_from_leads_24h_per
	,vips_from_reactivated_leads_24h_per
	,d1_vips_per
	,d1_vips_excluding_reactivated_leads_per
    ,d1_vips_reactivated_leads_per
	,d2_d7_vips_per
	,d1_d7_vips_per
	,d8_d90_vips_per
	,d90plus_vips_per
	,reactivated_leads_per
	,prospecting_spend
	,programmatic_gdn_spend
	,facebook_spend
	,snapchat_spend
	,pinterest_spend
	,non_branded_search_spend
	,branded_search_spend
	,shopping_spend
	,programmatic_spend
	,youtube_spend
	,tv_and_streaming_spend
	,testing_spend
	,print_spend
	,radio_podcast_spend
	,influencers_spend
	,affiliate_spend
	,physical_partnerships_spend
	,tiktok_spend
	,reddit_spend
	,twitter_spend
	,prospecting_spend_per
	,programmatic_gdn_spend_per
	,facebook_spend_per
	,snapchat_spend_per
	,pinterest_spend_per
	,non_branded_search_spend_per
	,branded_search_spend_per
	,shopping_spend_per
	,programmatic_spend_per
	,youtube_spend_per
	,tv_and_streaming_spend_per
	,testing_spend_per
	,print_spend_per
	,radio_podcast_spend_per
	,influencers_spend_per
	,affiliate_spend_per
	,physical_partnerships_spend_per
	,tiktok_spend_per
	,reddit_spend_per
	,twitter_spend_per
	,currency
	,source
	,version
	,channel_sequence
	,report_month
	,activating_acquisition_margin
	,activating_acquisition_margin/iff(zeroifnull(product_order_count)=0,1,product_order_count) as activating_acquisition_margin_per_order
	,zeroifnull(total_vips_on_date_cac) - zeroifnull(activating_acquisition_margin_per_order) as cac_am
	,vips_from_leads_24h/nullif(primary_leads,0) - vips_from_leads_30m/nullif(primary_leads,0) as pick_up_from_same_session_24hr
	,vips_from_leads_7d/nullif(primary_leads,0) - vips_from_leads_30m/nullif(primary_leads,0) as pick_up_from_same_session_7d
	,case when source = 'MTD' then 1
            when source = 'RR' then 2
            when source = 'DailyLMTotal' then 3
            when source = 'RRLM' then 4
            when source = 'DailyLYTotal' then 5
            when source = 'RRLY' then 6
            when source = 'DailyTY' then 7
            when source = 'DailyLM' then 8
            when source = 'DailyLY' then 9
            when source = 'Monthly' then 10
            else 11 end as source_sequence
    ,first_guest_product_margin_pre_return
    ,nvl((media_spend - nvl(first_guest_product_margin_pre_return,0))/iff(nvl(total_vips_on_date,0)=0,1,total_vips_on_date),0) as xcac
    ,bop_vips
    ,bop_vips + total_vips_on_date - total_cancels_on_date as eop_vips
    ,cancels_on_date
    ,passive_cancels_on_date
	,(media_spend-coalesce(influencers_spend,0))/IFF(nvl(primary_leads,0)=0,1,primary_leads) cpl_excl_influencer_spend
    ,(media_spend-coalesce(influencers_spend,0))/IFF(nvl(vips_on_date_d1,0)=0,1,vips_on_date_d1) vips_on_date_d1_excl_influencer_spend
    ,(media_spend-coalesce(influencers_spend,0))/IFF(nvl(vips_from_leads_1d,0)=0,1,vips_from_leads_1d) vips_from_leads_1d_excl_influencer_spend
    ,(media_spend-coalesce(influencers_spend,0))/IFF(nvl(vips_on_date_m1,0)=0,1,vips_on_date_m1) vips_on_date_m1_excl_influencer_spend
    ,(media_spend-coalesce(influencers_spend,0))/IFF(nvl(vips_from_leads_m1,0)=0,1,vips_from_leads_m1) vips_from_leads_m1_excl_influencer_spend
    ,(media_spend-coalesce(influencers_spend,0))/IFF(nvl(total_vips_on_date,0)=0,1,total_vips_on_date) total_vips_on_date_excl_influencer_spend
from reporting_media_prod.attribution.total_cac_output
where report_month = $current_month_date;


delete
from reporting_media_prod.attribution.total_cac_report_output
where version in ('FL GLOBAL by Country - USD'
,'FL GLOBAL by Region - USD'
,'JFB GLOBAL - USD'
,'SXF GLOBAL - USD')
and date = CURRENT_DATE;
