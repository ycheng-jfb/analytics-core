create or replace temporary table _promo_classification as
select
    a.*
from
(
    select distinct
        p.PROMO_ID
        ,pc.LABEL as parent_promo_classification
        ,pc_1.LABEL as child_promo_classification
        ,pc.SORT
        ,rank() over (partition by p.PROMO_ID order by pc.SORT asc) as promo_class_rank
    from LAKE_JFB_VIEW.ULTRA_MERCHANT.promo p
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.PROMO_PROMO_CLASSFICATION ppc
        on ppc.PROMO_ID = p.PROMO_ID
    join LAKE_JFB_VIEW.ULTRA_MERCHANT.PROMO_CLASSIFICATION pc
        on pc.PROMO_CLASSIFICATION_ID = ppc.PROMO_CLASSIFICATION_ID
        and pc.PARENT_PROMO_CLASSIFICATION_ID is null
    left join
    (
        select distinct
            p.PROMO_ID
            ,pc_0.LABEL
        from LAKE_JFB_VIEW.ULTRA_MERCHANT.promo p
        join LAKE_JFB_VIEW.ULTRA_MERCHANT.PROMO_PROMO_CLASSFICATION ppc_0
            on ppc_0.PROMO_ID = p.PROMO_ID
        join LAKE_JFB_VIEW.ULTRA_MERCHANT.PROMO_CLASSIFICATION pc_0
            on pc_0.PROMO_CLASSIFICATION_ID = ppc_0.PROMO_CLASSIFICATION_ID
            and pc_0.PARENT_PROMO_CLASSIFICATION_ID is not null

    ) pc_1 on pc_1.PROMO_ID = p.PROMO_ID
) a
where
    a.promo_class_rank = 1
;


create or replace temporary table _cms_segments as
select
    a.PROMO_ID
    ,a.UI_PROMO_MANAGEMENT_PROMO_ID
    ,a.code
--     ,parse_json(a.TARGET_USER_JSON) as segmentation

    --1. Choose Your Segment
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_MEMBERCHOICE::string, '"', '') as choose_your_segment
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_USERLISTID::string, '"', '') as user_list
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_SAILTHRUID::string, '"', '') as sailthru_list
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_SIGNUP_DM_GATEWAY_ID::string, '"', '') as sign_up_gateway
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_CURRENT_DM_GATEWAY_ID::string, '"', '') as current_gateway

    --2. Add Filters
    --checkout options
    ,(case
        when parse_json(a.TARGET_USER_JSON):PROMOTARGET_VIPLEADS is not null then 1
        else 0 end) as checkout_option_leads_payg_flag
    ,(case
        when parse_json(a.TARGET_USER_JSON):PROMOTARGET_VIPPOSTREG is not null then 1
        else 0 end) as checkout_option_post_reg_lead_flag
    ,(case
        when parse_json(a.TARGET_USER_JSON):PROMOTARGET_VIPELITE is not null then 1
        else 0 end) as checkout_option_vip_flag
    --registration date
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_REGDATEOPTIONS::string, '"', '') as registration_date_option
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_REGEXACTDATE::string, '"', '') as registration_exact_date
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_REGSTARTDATE::string, '"', '') as registration_start_date
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_REGENDDATE::string, '"', '') as registration_end_date
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_REGDAYS::string, '"', '') as registration_day
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_REGDAYSTART::string, '"', '') as registration_day_start
    ,replace(parse_json(a.TARGET_USER_JSON):PROMOTARGET_REGDAYEND::string, '"', '') as registration_day_end

    --qualification
    ,coalesce(replace(parse_json(a.BENEFITS_JSON):PROMOBENEFIT_NUMBERAPPLYMINVALUE::string, '"', ''), '0') as required_purchase_product_num
from LAKE_JFB_VIEW.ULTRA_CMS.UI_PROMO_MANAGEMENT_PROMOS a;


create or replace temporary table _promo_tags as
select
    tag.PROMO_ID
    ,tag.campaign_tag
from
(
    select
        a.PROMO_ID
        ,ut.LABEL as campaign_tag
        ,rank() over (partition by a.PROMO_ID order by ut.DATETIME_ADDED desc) as promo_tag_rank
    from LAKE_JFB_VIEW.ULTRA_CMS.UI_PROMO_MANAGEMENT_PROMOS a
    join LAKE_JFB_VIEW.ULTRA_CMS.UI_TAGS_ASSOCIATION uta
        on uta.TABLE_ID = a.UI_PROMO_MANAGEMENT_PROMO_ID
    join LAKE_JFB_VIEW.ULTRA_CMS.UI_TAGS ut
        on ut.UI_TAGS_ID = uta.UI_TAGS_ID
) tag
where
    tag.promo_tag_rank = 1;


create or replace temporary table _dim_promo as
select distinct
    ds.STORE_GROUP
    ,upper(ds.STORE_BRAND) as business_unit
    ,upper(ds.STORE_REGION) as region
    ,upper(ds.STORE_COUNTRY) as country
    ,ds.STORE_COUNTRY
    ,p.PROMO_ID
    ,upper(p.LABEL) as PROMOTION_NAME
    ,clas.parent_promo_classification
    ,clas.child_promo_classification
    ,upper(dp.FIRST_PROMO_CODE) as PROMOTION_CODE
    ,sc.LABEL as status
    ,dp.PROMO_TYPE as PROMOTION_TYPE
    ,(case
        when coalesce(subd.PERCENTAGE,0) > 0 and coalesce(shipd.PERCENTAGE,0) > 0 then '% Off & Free Shipping'
        when coalesce(subd.rate,0) > 0 and coalesce(shipd.PERCENTAGE,0) > 0 then '$ Off & Free Shipping'
        when coalesce(subd.PERCENTAGE,0) > 0 then '% Off Only'
        when coalesce(subd.rate,0) > 0 then '$ Off Only'
        when coalesce(shipd.rate,0) > 0 then 'Free Shipping Only'
        end ) as PROMOTION_DISCOUNT_METHOD
    ,coalesce(subd.PERCENTAGE,0) as subtotal_discount_percentage
    ,coalesce(subd.rate,0) as subtotal_discount_amount
    ,coalesce(shipd.PERCENTAGE,0) as shipping_discount_percentage
    ,coalesce(shipd.rate,0) as shipping_discount_amount
    ,pc.LABEL as product_category_include
    ,fpl.LABEL as featured_product_list_include
    ,pc_exc.LABEL as product_category_exclude
    ,fpl_exc.LABEL as featured_product_list_exclude
    ,p.ALLOW_MEMBERSHIP_CREDITS
    ,p.ALLOW_PREPAID_CREDITCARD
    ,p.DATE_START
    ,p.DATE_END
    ,p.REFUNDS_ALLOWED
    ,p.EXCHANGES_ALLOWED
    ,p.DISPLAY_ON_PDP
    ,p.PDP_LABEL
    ,p.ALLOW_LIKE_PROMOS as allowed_with_other_promo
    ,p.MAX_USES_PER_CUSTOMER
    ,p.MIN_SUBTOTAL as min_cart_value

    --segments
    --1. Choose Your Segment
    ,cs.choose_your_segment
    ,cs.user_list
    ,cs.sailthru_list
    ,cs.sign_up_gateway
    ,cs.current_gateway

    --2. Add Filters
    --checkout options
    ,cs.checkout_option_leads_payg_flag
    ,cs.checkout_option_post_reg_lead_flag
    ,cs.checkout_option_vip_flag
    --registration date
    ,cs.registration_date_option
    ,cs.registration_exact_date
    ,cs.registration_start_date
    ,cs.registration_end_date
    ,cs.registration_day
    ,cs.registration_day_start
    ,cs.registration_day_end

    --3. Qualification
    ,cs.required_purchase_product_num

    --promo tag
    ,pt.campaign_tag
from EDW_PROD.DATA_MODEL_JFB.DIM_PROMO_HISTORY dp
join LAKE_JFB_VIEW.ULTRA_MERCHANT.PROMO p
    on p.PROMO_ID = dp.promo_id
join LAKE_JFB_VIEW.ULTRA_MERCHANT.STATUSCODE sc
    on sc.STATUSCODE = p.STATUSCODE
left join _promo_classification clas
    on clas.PROMO_ID = p.PROMO_ID
join reporting_prod.gfb.vw_store ds
    on ds.STORE_GROUP_ID = p.STORE_GROUP_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.DISCOUNT subd
    on subd.DISCOUNT_ID = p.SUBTOTAL_DISCOUNT_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.DISCOUNT shipd
    on shipd.DISCOUNT_ID = p.SHIPPING_DISCOUNT_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_CATEGORY pc
    on pc.PRODUCT_CATEGORY_ID = p.PRODUCT_CATEGORY_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.FEATURED_PRODUCT_LOCATION fpl
    on fpl.FEATURED_PRODUCT_LOCATION_ID = p.FEATURED_PRODUCT_LOCATION_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_CATEGORY pc_exc
    on pc_exc.PRODUCT_CATEGORY_ID = p.FILTERED_PRODUCT_CATEGORY_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.FEATURED_PRODUCT_LOCATION fpl_exc
    on fpl_exc.FEATURED_PRODUCT_LOCATION_ID = p.FILTERED_FEATURED_PRODUCT_LOCATION_ID
left join _cms_segments cs
    on iff(contains(p.CODE, 'REV_'), cast(cs.UI_PROMO_MANAGEMENT_PROMO_ID as varchar(100)), cs.PROMO_ID) = iff(contains(p.CODE, 'REV_'), split_part(p.CODE, '_', -1), p.PROMO_ID)
left join _promo_tags pt
    on pt.PROMO_ID = dp.promo_id
where
    dp.IS_CURRENT = 1
;


create or replace transient table Reporting_prod.gfb.dim_promo as
select
    a.*
    ,(case
        when a.REQUIRED_PURCHASE_PRODUCT_NUM = '2' then 'BOGO ' || cast(round(a.SUBTOTAL_DISCOUNT_AMOUNT, 0) as varchar(20))
        when a.REQUIRED_PURCHASE_PRODUCT_NUM = '3' then '3 for ' || cast(round(a.SUBTOTAL_DISCOUNT_AMOUNT, 0) as varchar(20))
        when a.PROMOTION_CODE in ('TEAM40', 'TEAM50') then 'Employee Discount'
        when a.CHILD_PROMO_CLASSIFICATION = 'Free Trial' then 'Free Trial'
        when a.SUBTOTAL_DISCOUNT_PERCENTAGE = 1 then 'BOGO'
        when a.PROMOTION_DISCOUNT_METHOD = '% Off Only' then cast(round(a.SUBTOTAL_DISCOUNT_PERCENTAGE*100,0) as varchar(20)) || '% Off'
        when a.PROMOTION_DISCOUNT_METHOD = '$ Off Only' and a.MIN_CART_VALUE = 0 then 'Price Bucket ' || cast(round(a.SUBTOTAL_DISCOUNT_AMOUNT,0) as varchar(20))
        when a.PROMOTION_DISCOUNT_METHOD = '$ Off Only' and round(a.MIN_CART_VALUE, 0) = round(a.SUBTOTAL_DISCOUNT_AMOUNT, 0) then cast(round(a.SUBTOTAL_DISCOUNT_AMOUNT,0) as varchar(20)) || ' Credit'
        when a.PROMOTION_DISCOUNT_METHOD = '$ Off Only' and a.MIN_CART_VALUE > a.SUBTOTAL_DISCOUNT_AMOUNT then cast(round(a.SUBTOTAL_DISCOUNT_AMOUNT, 0) as varchar(20)) || ' Off ' || cast(round(a.MIN_CART_VALUE,0) as varchar(20))
        when a.SHIPPING_DISCOUNT_PERCENTAGE = 1 and a.SUBTOTAL_DISCOUNT_PERCENTAGE = 0 then 'Price Bucket ' || cast(round(a.SUBTOTAL_DISCOUNT_AMOUNT,0) as varchar(20)) || ' + Free Shipping'
        when a.SHIPPING_DISCOUNT_PERCENTAGE = 1 then cast(round(a.SUBTOTAL_DISCOUNT_PERCENTAGE*100,0) as varchar(20)) || '% Off + Free Shipping'
        when a.SUBTOTAL_DISCOUNT_AMOUNT > 0 then 'Price Bucket ' || cast(round(a.SUBTOTAL_DISCOUNT_AMOUNT,0) as varchar(20))
        end) as offer
from _dim_promo a
;
