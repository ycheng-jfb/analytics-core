
create or replace TEMPORARY table _gross_margin AS
select (case when IS_FL_MENS_FLAG=1 then 'Fabletics Men' else STORE_BRAND end) || ' ' || STORE_REGION as STORE_REGION,
       DATE,
       first_guest_product_margin_pre_return as ECOM_ACTIVATING_SHIPPED_GROSS_MARGIN
FROM reporting_media_prod.attribution.acquisition_unattributed_total_cac au
where retail_customer=0
    and IS_FK_FREE_TRIAL_FLAG=0
    and currency = 'USD'
    and DATE >= '2019-11-01'
    and au.store_id in (52,121,46,26,55);


create or replace TEMPORARY table _cac_by_lead AS
select
    (case when IS_FL_MENS_VIP=1 then 'Fabletics Men' else BUSINESS_UNIT end) || ' ' || REGION as STORE_REGION,
    DATE,
    sum(SPEND_USD) as spend,
    sum(TOTAL_VIPS_ON_DATE) as vips,
    sum(SPEND_USD)/iff(sum(TOTAL_VIPS_ON_DATE)>0,sum(TOTAL_VIPS_ON_DATE),null) as cpa_on_date
from reporting_media_prod.ATTRIBUTION.cac_by_lead_channel_daily
where date >= '2019-11-01'
    and REGION = 'NA'
    and retail_customer=0
    and DATE < CAST(CURRENT_TIMESTAMP() as DATE)
group by 1,2
order by 1,2;


create or replace transient table reporting_media_prod.dbo.media_forecasting_report_daily_xcpa_calc as
select cac.*, ECOM_ACTIVATING_SHIPPED_GROSS_MARGIN,
       nvl((spend - nvl(ECOM_ACTIVATING_SHIPPED_GROSS_MARGIN,0))/IFF(nvl(vips,0)=0,1,vips),0) as XCPA
from _cac_by_lead cac
left join _gross_margin gm on cac.DATE = gm.DATE
    and gm.STORE_REGION = cac.STORE_REGION;
