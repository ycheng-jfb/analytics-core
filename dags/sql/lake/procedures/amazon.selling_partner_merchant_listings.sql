USE lake;

ALTER SESSION SET QUERY_TAG='edm_inbound_amazon_selling_partner_reports,amazon_selling_partner_merchant_listings_to_snowflake';

CREATE TEMP TABLE _delta (
    market_place_id VARCHAR,
    item_name VARCHAR,
    item_description VARCHAR,
    listing_id VARCHAR,
    seller_sku VARCHAR,
    price NUMBER(19,4)   ,
    quantity INT,
    open_date VARCHAR,
    image_url VARCHAR,
    item_is_marketplace VARCHAR,
    product_id_type INT,
    zshop_shipping_fee NUMBER(19,4),
    item_note VARCHAR,
    item_condition VARCHAR,
    zshop_category1 VARCHAR,
    zshop_browse_path VARCHAR,
    zshop_storefront_feature VARCHAR,
    asin1 VARCHAR,
    asin2 VARCHAR,
    asin3 VARCHAR,
    will_ship_internationally VARCHAR,
    expedited_shipping VARCHAR,
    zshop_boldface VARCHAR,
    product_id VARCHAR,
    bid_for_featured_placement VARCHAR,
    add_delete VARCHAR,
    pending_quantity VARCHAR,
    fulfillment_channel VARCHAR,
    merchant_shipping_group VARCHAR,
    status VARCHAR,
    meta_from_datetime TIMESTAMP_LTZ,
    meta_type_1_hash INT,
    meta_type_2_hash INT,
    rn INT,
    meta_record_status VARCHAR(15)
);

BEGIN;

DELETE FROM lake.amazon.selling_partner_merchant_listings_stg;

COPY INTO lake.amazon.selling_partner_merchant_listings_stg (market_place_id, item_name, item_description, listing_id,
        seller_sku, price, quantity, open_date, image_url, item_is_marketplace, product_id_type, zshop_shipping_fee,
        item_note, item_condition, zshop_category1, zshop_browse_path, zshop_storefront_feature, asin1, asin2, asin3,
        will_ship_internationally, expedited_shipping, zshop_boldface, product_id, bid_for_featured_placement,
        add_delete, pending_quantity, fulfillment_channel, merchant_shipping_group, status)
FROM (
select SPLIT_PART(SPLIT_PART(metadata$filename, '/', 4), '_', 7), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
       $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29
from '@lake_stg.public.tsos_da_int_inbound/lake/amazon.selling_partner_merchant_listings/daily_v1'
)
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '\t',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = (''),
    REPLACE_INVALID_CHARACTERS = TRUE
)
ON_ERROR = 'SKIP_FILE_1%';


INSERT INTO _delta
SELECT s.*,
    CASE
    WHEN t.selling_partner_merchant_listings_key IS NULL THEN NULL -- null means new record
    WHEN s.meta_from_datetime < t.meta_from_datetime THEN 'ignore'
    WHEN s.meta_type_1_hash != t.meta_type_1_hash
        AND s.meta_type_2_hash != t.meta_type_2_hash THEN 'both'
    WHEN s.meta_type_1_hash != t.meta_type_1_hash THEN 'type 1'
    WHEN s.meta_type_2_hash != t.meta_type_2_hash THEN 'type 2'
    ELSE 'ignore' END :: VARCHAR(15) AS meta_record_status
FROM (
    SELECT *,
        current_timestamp AS meta_from_datetime,
        hash(NULL) :: INT AS meta_type_1_hash,
        hash(price) :: INT AS meta_type_2_hash,
        row_number() OVER (PARTITION BY market_place_id, seller_sku ORDER BY meta_from_datetime DESC) as rn
    FROM lake.amazon.selling_partner_merchant_listings_stg s
) s
LEFT JOIN lake.amazon.selling_partner_merchant_listings t on equal_null(t.market_place_id, s.market_place_id)
    AND equal_null(t.seller_sku, s.seller_sku)
    AND t.meta_is_current
WHERE rn = 1;


UPDATE lake.amazon.selling_partner_merchant_listings t
SET t.meta_to_datetime = s.meta_from_datetime,
    t.meta_is_current = FALSE,
    t.meta_update_datetime = current_timestamp
FROM _delta s
WHERE t.meta_is_current
    AND equal_null(t.market_place_id, s.market_place_id)
    AND equal_null(t.seller_sku, s.seller_sku)
    AND s.meta_record_status IN ('both', 'type 2');


INSERT INTO lake.amazon.selling_partner_merchant_listings (market_place_id, item_name, item_description, listing_id, seller_sku, price, quantity, open_date, image_url, item_is_marketplace, product_id_type, zshop_shipping_fee, item_note, item_condition, zshop_category1, zshop_browse_path, zshop_storefront_feature, asin1, asin2, asin3, will_ship_internationally, expedited_shipping, zshop_boldface, product_id, bid_for_featured_placement, add_delete, pending_quantity, fulfillment_channel, merchant_shipping_group, status, meta_type_1_hash, meta_type_2_hash, meta_from_datetime, meta_to_datetime, meta_is_current, meta_create_datetime, meta_update_datetime)
SELECT
    market_place_id,
	item_name,
	item_description,
	listing_id,
	seller_sku,
	price,
	quantity,
	open_date,
	image_url,
	item_is_marketplace,
	product_id_type,
	zshop_shipping_fee,
	item_note,
	item_condition,
	zshop_category1,
	zshop_browse_path,
	zshop_storefront_feature,
	asin1,
	asin2,
	asin3,
	will_ship_internationally,
	expedited_shipping,
	zshop_boldface,
	product_id,
	bid_for_featured_placement,
	add_delete,
	pending_quantity,
	fulfillment_channel,
	merchant_shipping_group,
	status,
    meta_type_1_hash,
    meta_type_2_hash,
    nvl2(
        s.meta_record_status,
        s.meta_from_datetime,
        convert_timezone('UTC', to_timestamp_ltz(0)) :: TIMESTAMP_LTZ
    ) AS meta_from_datetime,
    convert_timezone('UTC', to_timestamp_ltz(3e9)) :: TIMESTAMP_LTZ AS meta_to_datetime,
    TRUE AS meta_is_current,
    current_timestamp AS meta_create_datetime,
    current_timestamp AS meta_update_datetime
FROM _delta s
WHERE nvl(s.meta_record_status, 'new') IN ('new', 'type 2', 'both');

COMMIT;
