
create or replace transient table reporting_media_prod.attribution.total_cac_rolling_lead_cohorts as
with _leads as (
select store_brand,
       store_region,
       store_country,
       store_id,
       is_fl_mens_flag,
       is_fl_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       is_fk_free_trial_flag,
       date_trunc(month, date) as month,
       sum(primary_leads) as m1
from reporting_media_prod.attribution.daily_acquisition_metrics_cac
where date >= '2018-01-01'
    and currency = 'USD'
group by 1,2,3,4,5,6,7,8,9,10,11
)

select *,
       coalesce(lag(m1)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m2,
       coalesce(lag(m1, 2)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m3,
       coalesce(lag(m1, 3)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m4,
       coalesce(lag(m1, 4)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m5,
       coalesce(lag(m1, 5)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m6,
       coalesce(lag(m1, 6)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m7,
       coalesce(lag(m1, 7)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m8,
       coalesce(lag(m1, 8)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m9,
       coalesce(lag(m1, 9)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m10,
       coalesce(lag(m1, 10)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m11,
       coalesce(lag(m1, 11)
                    over (partition by store_brand, store_country, store_region, store_id, is_fl_mens_flag, is_fl_scrubs_customer,
                        retail_lead, retail_vip, retail_customer, is_fk_free_trial_flag
                        order by month), 0) as m12
from _leads;
