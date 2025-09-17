create or replace temporary table _main_brand as
SELECT id.product_sku                                                                          AS product_sku
     , FIRST_VALUE(main_brand) OVER (PARTITION BY id.product_sku ORDER BY inventory_date DESC) AS main_brand
FROM gfb.gfb_inventory_data_set id
         JOIN gfb.merch_dim_product mdp
              ON id.business_unit = mdp.business_unit
                  AND id.region = mdp.region
                  AND id.country = mdp.country
                  AND id.product_sku = mdp.product_sku
WHERE UPPER(id.business_unit) IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS')
  AND mdp.sub_brand IN ('JFB')
UNION
SELECT product_sku, sub_brand AS main_brand
FROM gfb.merch_dim_product
WHERE sub_brand NOT IN ('JFB');

create or replace temporary table _current_inventory as
select ids.main_brand,ids.region,ids.product_sku,sum(qty_available_to_sell) AS qty_avail
from gfb.gfb_inventory_data_set_current ids
join gfb.merch_dim_product mdp
    on ids.main_brand = mdp.business_unit
    and ids.region = mdp.region
    and ids.country = mdp.country
    and ids.product_sku = mdp.product_sku
group by ids.main_brand, ids.region, ids.product_sku;


create or replace transient table reporting_prod.gfb.gfb081_active_skus as
select distinct mdp.business_unit,
                mdp.sub_brand,
                mb.main_brand,
                mdp.region,
                mdp.department_detail,
                mdp.subcategory,
                mdp.subclass,
                mdp.product_sku,
                mdp.color,
                mdp.style_name,
                mdp.large_img_url,
                mdp.current_showroom,
                mdp.current_vip_retail,
                mdp.latest_launch_date,
                ci.qty_avail
from gfb.merch_dim_product mdp
left join _main_brand mb
    ON mdp.product_sku = mb.product_sku
LEFT JOIN _current_inventory ci
    ON mdp.business_unit = ci.main_brand
        AND mdp.region = ci.region
        AND mdp.product_sku = ci.product_sku
where current_status = 1;
