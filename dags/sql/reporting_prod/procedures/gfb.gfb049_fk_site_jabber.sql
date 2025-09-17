create or replace transient table REPORTING_PROD.GFB.gfb049_fk_site_jabber as
select distinct
    sj.EMAIL
    ,first_value(olp.ORDER_ID) over (partition by sj.EMAIL order by olp.ORDER_ID desc) as latest_successful_order_id
from LAKE_VIEW.SHAREPOINT.GFB_FK_SITE_JABBER_CUSTOMER_LIST sj
join EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER dc
    on lower(dc.EMAIL) = lower(sj.EMAIL)
join REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE olp
    on olp.CUSTOMER_ID = dc.CUSTOMER_ID
    and olp.BUSINESS_UNIT = 'FABKIDS'
    and olp.ORDER_CLASSIFICATION = 'product order'
join EDW_PROD.DATA_MODEL_JFB.FACT_ORDER fo
    on fo.ORDER_ID = olp.ORDER_ID
    and fo.ORDER_STATUS_KEY = 1 -- successful order
