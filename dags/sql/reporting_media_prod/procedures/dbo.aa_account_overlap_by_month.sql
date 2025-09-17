-- Advanced Analytics Custom SQL Query

-- update to pull correct account name based on date
-- for example, Fabletics re-used PeopleHype account for Volt

-- put all data in temp table in order to make updates on account columns
create or replace temporary table _account_names as
select * from lake.facebook.advanced_analytics_overlap;

-- replace account ID with account name
update _account_names a
set account1 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account1 = n.account_id;

update _account_names a
set account2 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account2 = n.account_id;

update _account_names a
set account3 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account3 = n.account_id;

update _account_names a
set account4 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account4 = n.account_id;

update _account_names a
set account5 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account5 = n.account_id;

update _account_names a
set account6 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account6 = n.account_id;

update _account_names a
set account7 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account7 = n.account_id;

update _account_names a
set account8 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account8 = n.account_id;

update _account_names a
set account9 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account9 = n.account_id;

update _account_names a
set account10 = concat(cast(account_name as varchar),'|')
from reporting_media_base_prod.facebook.vw_facebook_naming_convention n
where a.account10 = n.account_id;

-- parse account name and replace null with '' so concat function will work
create or replace temporary table _final_account_names as
select
    brand_name,
    date_start,
    date_end,
    ifnull(split_part(account1,'_',-1),'') as account1,
    ifnull(split_part(account2,'_',-1),'') as account2,
    ifnull(split_part(account3,'_',-1),'') as account3,
    ifnull(split_part(account4,'_',-1),'') as account4,
    ifnull(split_part(account5,'_',-1),'') as account5,
    ifnull(split_part(account6,'_',-1),'') as account6,
    ifnull(split_part(account7,'_',-1),'') as account7,
    ifnull(split_part(account8,'_',-1),'') as account8,
    ifnull(split_part(account9,'_',-1),'') as account9,
    ifnull(split_part(account10,'_',-1),'') as account10,
    sum(reach) as reach,
    sum(total_impressions) as impressions,
    sum(total_spend_usd) as spend
from _account_names
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;

-- concat to create overlap column
create or replace temporary table _overlap as
select
    brand_name,
    date_start,
    date_end,
    rtrim(concat(account1,account2,account3,account4,account5,account6,account7,account8,account9,account10),'|') as overlap,
    dense_rank() over(partition by brand_name, date_start, date_end order by sum(reach) desc) as index,
    sum(reach) as reach,
    sum(impressions) as impressions,
    sum(spend) as spend
from _final_account_names
group by 1,2,3,4;

create or replace transient table reporting_media_prod.dbo.aa_account_overlap_by_month as
select
    brand_name,
    date_start,
    date_end,
    iff(((index <= 10 or overlap not like '%|%') and overlap not like ''),overlap,'Other Grouping') as overlap,
    sum(reach) as reach,
    sum(impressions) as impressions,
    sum(spend) as spend
from _overlap o
group by 1,2,3,4;
