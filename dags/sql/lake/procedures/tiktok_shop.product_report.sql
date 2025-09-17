SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _product AS
SELECT
    *
FROM
    lake.tiktok_shop.product o
WHERE meta_update_datetime > $low_watermark_ltz;

CREATE OR REPLACE TEMP TABLE _product_final AS
SELECT
p.shop_name                                                                       shop_name,
id                                                                                product_id,
s.value:id                                                                        sku_id,
s.value:seller_sku                                                                seller_sku,
title                                                                             parent_title,
brand:name                                                                        brand,
FILTER(s.value:sales_attributes, a -> a:name = 'Color')[0]:value_name             color,
FILTER(s.value:sales_attributes, a -> a:name = 'Size')[0]:value_name              size,
category_chains[0].local_name::varchar(150)                                       department,
category_chains[1].local_name::varchar(150)                                       category,
category_chains[2].local_name::varchar(150)                                       subcategory,
main_images[0].thumb_urls[0]                                                      main_image_url,
FILTER(s.value:sales_attributes, a -> a:name = 'Color')[0]:sku_img:thumb_urls[0]  color_image_url,
s.value:price:sale_price                                                          unit_price,
FROM _product p,
LATERAL FLATTEN(INPUT => p.SKUS) s;

MERGE INTO lake.tiktok_shop.product_report as t
USING (
    SELECT *,
           hash(*) meta_row_hash,
           current_timestamp() meta_create_datetime,
           current_timestamp() meta_update_datetime
    FROM _product_final
    ) s
ON equal_null(t.product_id, s.product_id)
    AND equal_null(t.sku_id, s.sku_id)
    AND equal_null(t.shop_name, s.shop_name)
WHEN NOT MATCHED
    THEN INSERT (shop_name,product_id,sku_id,seller_sku,parent_title,brand,color,size,department,category,subcategory,
                 main_image_url,color_image_url,unit_price,meta_row_hash,meta_create_datetime,meta_update_datetime)
         VALUES (shop_name,product_id,sku_id,seller_sku,parent_title,brand,color,size,department,category,subcategory,
                 main_image_url,color_image_url,unit_price,meta_row_hash,meta_create_datetime,meta_update_datetime)
WHEN MATCHED AND t.meta_row_hash!=s.meta_row_hash
    THEN UPDATE
    SET
        t.seller_sku=s.seller_sku,
        t.parent_title=s.parent_title,
        t.brand=s.brand,
        t.color=s.color,
        t.size=s.size,
        t.department=s.department,
        t.category=s.category,
        t.subcategory=s.subcategory,
        t.main_image_url=s.main_image_url,
        t.color_image_url=s.color_image_url,
        t.unit_price=s.unit_price,
        t.meta_row_hash=s.meta_row_hash,
        t.meta_update_datetime=s.meta_update_datetime;
