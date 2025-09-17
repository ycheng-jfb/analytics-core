create or replace transient table REPORTING_PROD.GFB.gfb035_merch_future_showroom_products as
select
    mdp.*
from REPORTING_PROD.GFB.MERCH_DIM_PRODUCT mdp
where
    mdp.LATEST_LAUNCH_DATE >= date_trunc(month, current_date())
