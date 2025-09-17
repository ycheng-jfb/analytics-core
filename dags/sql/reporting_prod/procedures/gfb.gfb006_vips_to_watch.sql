create or replace temporary table _vip_activations as
select
    a.*
    ,rank() over (partition by a.CUSTOMER_ID order by a.vip_activation_date asc) as vip_activation_rank
from
(
    select distinct
        fol.CUSTOMER_ID
        ,(case
            when fol.BUSINESS_UNIT = 'FABKIDS' then 'FK'
            when fol.BUSINESS_UNIT = 'SHOEDAZZLE' then 'SD'
            when fol.BUSINESS_UNIT = 'JUSTFAB' then 'JF' || ' ' || fol.REGION
            end) as store
        ,fol.ORDER_DATE as vip_activation_date
        ,fol.ORDER_ID
        ,(case
            when fol.IS_PREPAID_CREDITCARD = 1 then 'PPCC'
            else 'Non-PPCC' end) as IS_PREPAID_CREDITCARD
    from reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE fol
    where
        fol.ORDER_CLASSIFICATION = 'product order'
        and fol.ORDER_TYPE = 'vip activating'
) a;

create or replace temporary table _vip_gamers_ppcc as
select
    ac.store
    ,ac.CUSTOMER_ID
    ,ac.vip_activation_date
    ,ac.IS_PREPAID_CREDITCARD
    ,(case
        when ac.vip_activation_date < '2019-06-01' then 'No Gamer Data'
        when gamers.CUSTOMER_ID is not null then 'Gamer'
        else 'Non-Gamer' end) as gamer_flag
    ,(case
        when ac.vip_activation_rank != 1 then 'reactivation'
        else 'first activation' end) as reactivation_flag
from _vip_activations ac
left join
(
    select distinct
        ac.CUSTOMER_ID
    from LAKE_JFB_VIEW.ULTRA_MERCHANT."ORDER" o
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.customer c
        on c.customer_id = o.customer_id
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.store st
        on c.store_id = st.store_id
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.order_classification oc
        on oc.order_id = o.order_id
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.order_detail od
        on o.order_id = od.order_id
        and od.name = 'gaming_prevention_log_id'
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.gaming_prevention_log gl
        on od.value = cast(gl.gaming_prevention_log_id as Varchar)
        and gl.gaming_prevention_action = 'void'
    join _vip_activations ac
        on ac.CUSTOMER_ID = c.CUSTOMER_ID
    where
        order_type_id = 23
        and o.date_placed >= cast('2019-06-01' as date)
) gamers on gamers.CUSTOMER_ID = ac.CUSTOMER_ID
where
    ac.vip_activation_date >= '2019-01-01';

create or replace transient table reporting_prod.GFB.gfb006_vips_to_watch as
select
    vgp.store
    ,vgp.vip_activation_date
    ,count(vgp.CUSTOMER_ID) as vip_activations
    ,count(case
            when vgp.gamer_flag = 'Gamer' then vgp.CUSTOMER_ID end) as vip_gamer_activations
    ,count(case
            when vgp.IS_PREPAID_CREDITCARD = 'PPCC' then vgp.CUSTOMER_ID end) as vip_ppcc_activations
from _vip_gamers_ppcc vgp
group by
    vgp.store
    ,vgp.vip_activation_date;
