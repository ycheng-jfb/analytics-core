create or replace temporary table _asknicely_json_transform as
select
    parse_json(replace(rd.ASKNICELY_RESPONSES_DATA, 'None', '"None"')) as json_data
    ,rd.*
from lake_view.ask_nicely.responses_data rd;


create or replace transient table reporting_prod.gfb.gfb_asknicely_response_data_set as
select
    ajt.BUSINESS_UNIT
    ,coalesce(ajt.COUNTRY, 'US') as COUNTRY
    ,ajt.RESPONSE_ID
    ,ajt.SENT_DATE
    ,ajt.RESPONSE_DATE
    ,ajt.COMMENT
    ,ajt.json_data:answer::int as answer
    ,(case
        when answer <= 6 then 'Detractor'
        when answer <= 8 then 'Passive'
        when answer <= 10 then 'Promoter'
        end) as answer_type
    ,upper(ajt.json_data:deliverymethod::varchar) as delivery_method
    ,upper(ajt.json_data:device_os_c::varchar) as device_os
    ,upper(ajt.json_data:email::varchar) as email
    ,upper(ajt.json_data:name::varchar) as customer_name
    ,upper(ajt.json_data:segment::varchar) as segment
    ,upper(ajt.json_data:status_c::varchar) as status
    ,upper(ajt.json_data:person_id::varchar) as person_id
    ,upper(ajt.json_data:data::varchar) as data
    ,upper(ajt.json_data:responded::varchar) as responded
    ,upper(ajt.json_data:lastemailed::varchar) as lastemailed
    ,upper(ajt.json_data:survey_template::varchar) as survey_template
    ,upper(ajt.json_data:theme::varchar) as theme
    ,upper(ajt.json_data:topic_c::varchar) as topic_c
    ,upper(ajt.json_data:email_token::varchar) as email_token
    ,upper(ajt.json_data:contmembership_csat_c::varchar) as contmembership_csat_c
    ,upper(ajt.json_data:personalstyle_ces_c::varchar) as personalstyle_ces_c
    ,upper(ajt.json_data:qualvalue_csat_c::varchar) as qualvalue_csat_c
    ,upper(ajt.json_data:sitenav_csat_c::varchar) as sitenav_csat_c
from _asknicely_json_transform ajt
