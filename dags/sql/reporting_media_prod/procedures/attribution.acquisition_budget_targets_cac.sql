
/*

All Budget / Forecast targets are for consolidated brand/regions (NA + EU) in USD
Below creates all valid combinations

*/

------------------------------------------------------------------------------------
-- create lookup table with every tab combo (used for total cac and cac by lead) --

create or replace transient table reporting_media_prod.attribution.cac_tab_budget_store_lookup (

    tab_sequence int,
    store_tab_abbreviation varchar(55),
    store_name_description varchar(100),
    finance_report_mapping varchar(100),
    has_finance_budget_target boolean
);

insert into reporting_media_prod.attribution.cac_tab_budget_store_lookup
values
-- fabletics + yitty
(1, 'FLC-GLOBAL', 'Fabletics Global + Fabletics Scrubs NA + Yitty NA', 'FLC-TREV-Global', TRUE),
(2, 'FL+SC-GLOBAL', 'Fabletics Global + Fabletics Scrubs NA', 'FL+SC-TREV-Global', TRUE),
(3, 'FLC-NA', 'Fabletics NA + Fabletics Scrubs NA + Yitty NA', 'FLC-TREV-NA', TRUE),
(4, 'FL-EU', 'Fabletics EU', 'FL-TREV-EU', TRUE),
(5, 'FLC-O-NA', 'Fabletics NA + Fabletics Scrubs NA + Yitty NA - Online Registration and Activation', 'FLC-O-NA', TRUE),
(6, 'YT-O-NA', 'Yitty NA - Online Registration and Activation', 'YT-OREV-NA', TRUE),
(7, 'FL+SC-NA', 'Fabletics NA + Fabletics Scrubs NA', 'FL+SC-TREV-NA', TRUE),
(8, 'FL+SC-M-NA', 'Fabletics NA + Fabletics Scrubs NA - Mens Customer', 'FL+SC-M-TREV-NA', TRUE),
(9, 'FL+SC-W-NA', 'Fabletics NA + Fabletics Scrubs NA - Womens Customer', 'FL+SC-W-TREV-NA', TRUE),
(10, 'FL-NA', 'Fabletics NA', 'FL-TREV-NA', TRUE),
(11, 'FL-M-NA', 'Fabletics NA - Mens Customer', 'FL-M-TREV-NA', TRUE),
(12, 'FL-W-NA', 'Fabletics NA - Womens Customer', 'FL-W-TREV-NA', TRUE),
(13, 'FL-M-O-NA', 'Fabletics NA - Mens Customer - Online Registration and Activation', 'FL-M-OREV-NA', TRUE),
(14, 'FL-W-O-NA', 'Fabletics NA - Womens Customer - Online Registration and Activation', 'FL-W-OREV-NA', TRUE),
(15, 'SC-O-NA', 'Fabletics Scrubs NA - Online Registration and Activation', 'SC-OREV-NA', TRUE),
(16, 'SC-M-O-NA', 'Fabletics Scrubs NA - Mens Customer - Online Registration and Activation', 'SC-M-OREV-NA', TRUE),
(17, 'SC-W-O-NA', 'Fabletics Scrubs NA - Womens Customer - Online Registration and Activation', 'SC-W-OREV-NA', TRUE),
(18, 'FL-US', 'Fabletics US', NULL, FALSE),
(19, 'FL-M-US', 'Fabletics US - Mens Customers', NULL, FALSE),
(20, 'FL-W-US', 'Fabletics US - Womens Customers', NULL, FALSE),
(21, 'FL-CA', 'Fabletics CA', NULL, FALSE),
(22, 'FL-M-CA', 'Fabletics CA - Mens Customers', NULL, FALSE),
(23, 'FL-W-CA', 'Fabletics CA - Womens Customers', NULL, FALSE),
(24, 'FL-M-EU', 'Fabletics EU - Mens Customer', 'FL-M-TREV-EU', TRUE),
(25, 'FL-W-EU', 'Fabletics EU - Womens Customer', 'FL-W-TREV-EU', TRUE),
(26, 'FL-DE', 'Fabletics DE', NULL, FALSE),
(27, 'FL-M-DE', 'Fabletics DE - Mens Customer', NULL, FALSE),
(28, 'FL-W-DE', 'Fabletics DE - Womens Customer', NULL, FALSE),
(29, 'FL-M-O-DE', 'Fabletics DE - Mens Customer - Online Registration and Activation', NULL, FALSE),
(30, 'FL-W-O-DE', 'Fabletics DE - Womens Customer - Online Registration and Activation', NULL, FALSE),
(31, 'FL-UK', 'Fabletics UK', NULL, FALSE),
(32, 'FL-M-UK', 'Fabletics UK - Mens Customer', NULL, FALSE),
(33, 'FL-W-UK', 'Fabletics UK - Womens Customer', NULL, FALSE),
(34, 'FL-M-O-UK', 'Fabletics UK - Mens Customer - Online Registration and Activation', NULL, FALSE),
(35, 'FL-W-O-UK', 'Fabletics UK - Womens Customer - Online Registration and Activation', NULL, FALSE),
(36, 'FL-DK', 'Fabletics DK', NULL, FALSE),
(37, 'FL-M-DK', 'Fabletics DK - Mens Customer', NULL, FALSE),
(38, 'FL-W-DK', 'Fabletics DK - Womens Customer', NULL, FALSE),
(39, 'FL-FR', 'Fabletics FR', NULL, FALSE),
(40, 'FL-M-FR', 'Fabletics FR - Mens Customer', NULL, FALSE),
(41, 'FL-W-FR', 'Fabletics FR - Womens Customer', NULL, FALSE),
(42, 'FL-ES', 'Fabletics ES', NULL, FALSE),
(43, 'FL-M-ES', 'Fabletics ES - Mens Customer', NULL, FALSE),
(44, 'FL-W-ES', 'Fabletics ES - Womens Customer', NULL, FALSE),
(45, 'FL-NL', 'Fabletics NL', NULL, FALSE),
(46, 'FL-M-NL', 'Fabletics NL - Mens Customer', NULL, FALSE),
(47, 'FL-W-NL', 'Fabletics NL - Womens Customer', NULL, FALSE),
(48, 'FL-SE', 'Fabletics SE', NULL, FALSE),
(49, 'FL-M-SE', 'Fabletics SE - Mens Customer', NULL, FALSE),
(50, 'FL-W-SE', 'Fabletics SE - Womens Customer', NULL, FALSE),

-- jfb
(51, 'JFB-GLOBAL', 'JustFab Global + ShoeDazzle NA + FabKids NA', 'JFB-TREV-Global', TRUE),
(52, 'JFB-NA', 'JustFab NA + ShoeDazzle NA + FabKids NA', 'JFB-TREV-NA', TRUE),
(53, 'JFSD-NA', 'JustFab NA + ShoeDazzle NA', 'JFSD-TREV-NA', TRUE),
(54, 'JF-NA', 'JustFab NA', 'JF-TREV-NA', TRUE),
(55, 'JF-US', 'JustFab US', NULL, FALSE),
(56, 'JF-CA', 'JustFab CA', NULL, FALSE),
(57, 'SD-NA', 'ShoeDazzle NA', 'SD-TREV-NA', TRUE),
(58, 'SD-US', 'ShoeDazzle US', NULL, FALSE),
(59, 'FK-NA', 'FabKids NA', 'FK-TREV-NA', TRUE),
(60, 'FK-NFT-NA', 'FabKids NA - No Free Trial', NULL, FALSE),
(61, 'JF-EU', 'JustFab EU', 'JF-TREV-EU', TRUE),
(62, 'JF-DE', 'JustFab DE', NULL, FALSE),
(63, 'JF-DK', 'JustFab DK', NULL, FALSE),
(64, 'JF-ES', 'JustFab ES', NULL, FALSE),
(65, 'JF-FR', 'JustFab FR', NULL, FALSE),
(66, 'JF-NL', 'JustFab NL', NULL, FALSE),
(67, 'JF-SE', 'JustFab SE', NULL, FALSE),
(68, 'JF-UK', 'JustFab UK', NULL, FALSE),

-- savage x
(69, 'SX-GLOBAL', 'Savage X Global', 'SX-TREV-Global', TRUE),
(70, 'SX-NA', 'Savage X NA', 'SX-TREV-NA', TRUE),
(71, 'SX-O-NA', 'Savage X NA - Online Registration and Activation', 'SX-OREV-NA', TRUE),
(72, 'SX-US', 'Savage X US', NULL, FALSE),
(73, 'SX-CA', 'Savage X CA', NULL, FALSE),
(74, 'SX-EU', 'Savage X EU', 'SX-TREV-EU', TRUE),
(75, 'SX-DE', 'Savage X DE', NULL, FALSE),
(76, 'SX-ES', 'Savage X ES', NULL, FALSE),
(77, 'SX-FR', 'Savage X FR', NULL, FALSE),
(78, 'SX-UK', 'Savage X UK', NULL, FALSE),
(79, 'SX-EUREM', 'Savage X EU Rem', NULL, FALSE)
;

------------------------------------------------------------------------------------
-- create master table for budgets to use in excel reports --
-- all business units combos with valid finance mapping

create or replace temporary table _all_finance_bus as
select tab_sequence,
       case when l.store_tab_abbreviation ilike 'FLC%' then 'Fabletics Corp' else f.store_brand end as store_brand,
       case when l.store_tab_abbreviation ilike '%global%' then 'WW'
            when l.store_tab_abbreviation ilike '%-NA' then 'NA'
            else 'EU' end  as store_region,
       l.store_name_description,
       l.store_tab_abbreviation,
       f.financial_date as date,
       'All' as channel,
       'USD' as currency,
       case when lower(currency_type) = 'usdbtfx' and lower(version) = 'budget' then 'Budget-BTFX'
            when lower(currency_type) = 'usdbtfx' and lower(version) = 'forecast' then 'Forecast-BTFX'
            when lower(currency_type) = 'usdcurrfx' and lower(version) = 'budget' then 'Budget-CURRFX'
            when lower(currency_type) = 'usdcurrfx' and  lower(version) = 'forecast' then 'Forecast-CURRFX'
            end as source,
       media_spend,
       leads,
       0 as d1_vips,
       m1_vips,
       total_new_vips as total_vips_on_date,
       bom_vips as bop_vips,
       cancels as total_cancels_on_date
from reporting_media_prod.attribution.cac_tab_budget_store_lookup l
join (select distinct bu, replace(report_mapping,' ','') as report_mapping from edw_prod.reference.finance_bu_mapping) m
    on m.report_mapping = l.finance_report_mapping
join edw_prod.reference.finance_budget_forecast f on f.bu = m.bu
where l.has_finance_budget_target = true
    and lower(f.version) in ('budget','forecast')
    and lower(f.currency_type) in ('usdbtfx', 'usdcurrfx');


create or replace transient table reporting_media_prod.attribution.acquisition_budget_targets_cac as
select tab_sequence,
       store_brand,
       store_region,
       store_name_description,
       store_tab_abbreviation,
       date,
       channel,
       currency,
       source,
       zeroifnull(leads) as leads,
       zeroifnull(media_spend/nullif(leads,0)) as cpl,
       zeroifnull(m1_vips) as vips_from_leads_m1,
       zeroifnull(m1_vips/nullif(leads,0)) as m1_lead_to_vip,
       zeroifnull(media_spend/nullif(m1_vips,0)) as m1_vip_cac,
       zeroifnull(total_vips_on_date) as total_vips_on_date,
       zeroifnull(bop_vips) as bop_vips,
       zeroifnull(total_cancels_on_date) as total_cancels_on_date,
       zeroifnull(media_spend/nullif(total_vips_on_date,0)) as total_vip_cac,
       zeroifnull(total_vips_on_date/nullif(leads,0)) as total_lead_to_vip,
       zeroifnull(media_spend) as media_spend
from _all_finance_bus;


------------------------------------------------------------------------------------
-- create budget / forecast snapshot --

create or replace transient table reporting_media_prod.snapshot.acquisition_budget_targets_cac as
select *, current_timestamp() as snapshot_datetime
from reporting_media_prod.attribution.acquisition_budget_targets_cac;

-- remove snapshots older than 7 days
delete from reporting_media_prod.snapshot.acquisition_budget_targets_cac
where snapshot_datetime < dateadd(day, -7, current_date);
