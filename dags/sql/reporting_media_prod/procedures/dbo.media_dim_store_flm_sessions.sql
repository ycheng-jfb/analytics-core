create or replace temporary table _flm_media_store_mapping (
media_store_id int,
media_store_group_id int,
store_group varchar,
store_reporting_name varchar,
store_brand_name varchar,
store_brand_abbr varchar,
store_country_abbr varchar,
store_region_abbr varchar
);

insert into _flm_media_store_mapping
(media_store_id, media_store_group_id, store_group, store_reporting_name, store_brand_name, store_brand_abbr, store_country_abbr, store_region_abbr)
values
('52011','16011','Fabletics Men','Fabletics Men US','Fabletics Men','FLM','US','NA'),
('53011','16011','Fabletics Men','Fabletics Men US (DM)','Fabletics Men','FLM','US','NA'),
('79011','29011','Fabletics Men CA','Fabletics Men CA','Fabletics Men','FLM','CA','NA'),
('80011','29011','Fabletics Men CA','Fabletics Men CA (DM)','Fabletics Men','FLM','CA','NA'),
('5201011','-2011','Fabletics Men AU','Fabletics Men AU','Fabletics Men','FLM','AU','NA'),
('65011','22011','Fabletics Men DE','Fabletics Men DE','Fabletics Men','FLM','DE','EU'),
('66011','22011','Fabletics Men DE','Fabletics Men DE (DM)','Fabletics Men','FLM','DE','EU'),
('67011','23011','Fabletics Men UK','Fabletics Men UK','Fabletics Men','FLM','UK','EU'),
('68011','23011','Fabletics Men UK','Fabletics Men UK (DM)','Fabletics Men','FLM','UK','EU'),
('69011','24011','Fabletics Men FR','Fabletics Men FR','Fabletics Men','FLM','FR','EU'),
('71011','25011','Fabletics Men ES','Fabletics Men ES','Fabletics Men','FLM','ES','EU'),
('73011','26011','Fabletics Men NL','Fabletics Men NL','Fabletics Men','FLM','NL','EU'),
('75011','27011','Fabletics Men SE','Fabletics Men SE','Fabletics Men','FLM','SE','EU'),
('77011','28011','Fabletics Men DK','Fabletics Men DK','Fabletics Men','FLM','DK','EU'),
('6501011','-2011','Fabletics Men AT','Fabletics Men AT','Fabletics Men','FLM','AT','NA'),
-- scrubs
('52022','16022','Fabletics Scrubs','Fabletics Scrubs US','Fabletics Scrubs','SCB','US','NA'),
('53022','16022','Fabletics Scrubs','Fabletics Scrubs US (DM)','Fabletics Scrubs','SCB','US','NA'),
('79022','29022','Fabletics Scrubs CA','Fabletics Scrubs CA','Fabletics Scrubs','SCB','CA','NA'),
('80022','29022','Fabletics Scrubs CA','Fabletics Scrubs CA (DM)','Fabletics Scrubs','SCB','CA','NA'),
('5201022','-2022','Fabletics Scrubs AU','Fabletics Scrubs AU','Fabletics Scrubs','SCB','AU','NA'),
('65022','22022','Fabletics Scrubs DE','Fabletics Scrubs DE','Fabletics Scrubs','SCB','DE','EU'),
('66022','22022','Fabletics Scrubs DE','Fabletics Scrubs DE (DM)','Fabletics Scrubs','SCB','DE','EU'),
('67022','23022','Fabletics Scrubs UK','Fabletics Scrubs UK','Fabletics Scrubs','SCB','UK','EU'),
('68022','23022','Fabletics Scrubs UK','Fabletics Scrubs UK (DM)','Fabletics Scrubs','SCB','UK','EU'),
('69022','24022','Fabletics Scrubs FR','Fabletics Scrubs FR','Fabletics Scrubs','SCB','FR','EU'),
('71022','25022','Fabletics Scrubs ES','Fabletics Scrubs ES','Fabletics Scrubs','SCB','ES','EU'),
('73022','26022','Fabletics Scrubs NL','Fabletics Scrubs NL','Fabletics Scrubs','SCB','NL','EU'),
('75022','27022','Fabletics Scrubs SE','Fabletics Scrubs SE','Fabletics Scrubs','SCB','SE','EU'),
('77022','28022','Fabletics Scrubs DK','Fabletics Scrubs DK','Fabletics Scrubs','SCB','DK','EU'),
('6501022','-2022','Fabletics Scrubs AT','Fabletics Scrubs AT','Fabletics Scrubs','SCB','AT','NA');

create or replace transient table reporting_media_prod.dbo.media_dim_store_flm_sessions as
select store_id as media_store_id,
store_group_id as media_store_group_id,
store_group,
store_full_name as store_reporting_name,
store_brand as store_brand_name,
store_brand_abbr,
store_country as store_country_abbr,
store_region as store_region_abbr
from edw_prod.data_model_jfb.dim_store
union
select *
from _flm_media_store_mapping;
