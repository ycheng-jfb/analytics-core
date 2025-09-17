CREATE TABLE IF NOT EXISTS lake.amazon.catalog
(
    asin                 VARCHAR,
    parent_asin          VARCHAR,
    parent_title         VARCHAR,
    title                VARCHAR,
    brand                VARCHAR,
    color                VARCHAR,
    display_name         VARCHAR,
    size                 VARCHAR,
    product_type         VARCHAR,
    marketplace_id       VARCHAR,
    fit_type             VARCHAR,
    special_size         VARCHAR,
    item_name            VARCHAR,
    item_classification  VARCHAR,
    meta_row_hash        NUMBER(38, 0),
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ
);


CREATE OR REPLACE TEMP TABLE _catalog_items AS
SELECT DISTINCT asin,
                r.value:relationships[0]:parentAsins[0]::VARCHAR  parent_asin,
                sr.value:displayGroupRanks[0]:title::VARCHAR      title,
                s.value:brand::VARCHAR                            brand,
                s.value:color::VARCHAR                            color,
                s.value:browseClassification:displayName::VARCHAR display_name,
                s.value:size::VARCHAR                             size,
                pt.value:productType::VARCHAR                     product_type,
                s.value:marketplaceId::VARCHAR                    marketplace_id,
                at.this:fit_type[0]:value::VARCHAR                fit_type,
                at.this:special_size_type[0]:value::VARCHAR       special_size,
                at.this:item_name[0]:value::VARCHAR               item_name,
                s.value:itemClassification::VARCHAR               item_classification
FROM lake.amazon.amazon_catalog_items i,
     LATERAL FLATTEN(INPUT =>i.summaries) s,
     LATERAL FLATTEN(INPUT =>i.relationships) r,
     LATERAL FLATTEN(INPUT =>i.salesranks) sr,
     LATERAL FLATTEN(INPUT =>i.attributes) at,
     LATERAL FLATTEN(INPUT =>i.producttypes) pt;


CREATE OR REPLACE TEMP TABLE _catalog_items_final AS
select c.*,p.item_name as parent_title
from _catalog_items c
left join _catalog_items p on c.parent_asin = p.asin;


MERGE INTO lake.amazon.catalog as t
USING (
    SELECT *,
           hash(*) meta_row_hash,
           current_timestamp() meta_create_datetime,
           current_timestamp() meta_update_datetime
    FROM _catalog_items_final
    where item_classification = 'BASE_PRODUCT'
    ) s
ON equal_null(t.asin, s.asin)
WHEN NOT MATCHED
    THEN INSERT (asin,parent_asin,parent_title,title,brand,color,display_name,size,product_type,marketplace_id,fit_type,
                 special_size,item_name,item_classification,meta_row_hash,meta_create_datetime,meta_update_datetime)
         VALUES (asin,parent_asin,parent_title,title,brand,color,display_name,size,product_type,marketplace_id,fit_type,
                 special_size,item_name,item_classification,meta_row_hash,meta_create_datetime,meta_update_datetime)
WHEN MATCHED AND t.meta_row_hash!=s.meta_row_hash
    THEN UPDATE
    SET
        t.asin=s.asin,
        t.parent_asin=s.parent_asin,
        t.parent_title=s.parent_title,
        t.title=s.title,
        t.brand=s.brand,
        t.color=s.color,
        t.display_name=s.display_name,
        t.size=s.size,
        t.product_type=s.product_type,
        t.marketplace_id=s.marketplace_id,
        t.fit_type=s.fit_type,
        t.special_size=s.special_size,
        t.item_name=s.item_name,
        t.item_classification=s.item_classification,
        t.meta_row_hash=s.meta_row_hash,
        t.meta_update_datetime=s.meta_update_datetime;
