-- USE ROLE tfg_media_admin;
--
-- USE SCHEMA work.media;

SET ads_with_spend_months_back = -6;

SET leads_months_back = -6;

CREATE OR REPLACE TEMP TABLE _product_data_by_pid AS
(WITH _product_data_by_pid_raw AS (SELECT DISTINCT ds.store_group,
                                                   ds.store_country,
                                                   p.store_id,
                                                   edw_prod.stg.udf_unconcat_brand(iff(master_product_id = -1, product_id, master_product_id))                                                      AS mpid,
                                                   product_id,
                                                   product_sku,
                                                   is_active,
                                                   product_status,
                                                   replace(product_name, '* ', '')                                                                                                                  AS product_name,
                                                   product_category,
                                                   department,
                                                   category,
                                                   color,
                                                   image_url,
                                                   p.meta_update_datetime,
                                                   row_number() OVER (PARTITION BY product_id, store_group, store_country ORDER BY is_active DESC, product_status ASC, p.meta_update_datetime DESC) AS rn
                                   FROM edw_prod.data_model.dim_product p
                                        JOIN edw_prod.data_model.dim_store ds
                                             ON ds.store_id = p.store_id AND lower(store_brand_abbr) IN ('fl', 'yty')
                                   WHERE product_sku != 'Unknown'
    --keep all inactive products here in case they tagged a sku with an product that is now inactive
)
 SELECT DISTINCT store_group,
                 store_country,
                 store_id,
                 mpid,
                 is_active,
                 product_status,
                 product_id,
                 product_sku,
                 product_name,
                 product_category,
                 department,
                 category,
                 color,
                 image_url
 FROM _product_data_by_pid_raw
 WHERE rn = 1);

CREATE OR REPLACE TEMP TABLE _product_data_by_mpid AS
(WITH t AS (SELECT DISTINCT store_group,
                            store_country,
                            store_id,
                            mpid,
                            is_active,
                            product_status,
                            product_sku,
                            product_name,
                            product_category,
                            department,
                            category,
                            color,
                            image_url,
                            row_number() OVER (PARTITION BY product_sku, store_group, store_country ORDER BY is_active DESC, product_status ASC) AS rn
            FROM _product_data_by_pid)
 SELECT *
 FROM t
 WHERE rn = 1);

--matching mens womens counterpart items
CREATE OR REPLACE TEMP TABLE scrubs_corresponding_items AS
(WITH scrubs                 AS (SELECT DISTINCT product_sku,
                                                 product_category,
                                                 category,
                                                 department,
                                                 color,
                                                 product_status,
                                                 is_active,
                                                 replace(product_name, '*', '')                     AS cleansed_product_name,
                                                 concat(split_part(cleansed_product_name, ' ', -2), ' ',
                                                        split_part(cleansed_product_name, ' ', -1)) AS last_two_of_product_name,
                                                 iff(contains(cleansed_product_name, '-Pocket'),
                                                     Right(SPLIT(cleansed_product_name, '-Pocket')[0], 1),
                                                     NULL) AS pockets
                                 FROM _product_data_by_mpid
                                 WHERE contains(lower(department), 'scrubs')
                                 )
    , corresponding_products AS (SELECT DISTINCT a.product_sku                                                                       AS ad_sku,
                                                 a.product_category                                                                  AS ad_product_category,
                                                 a.category                                                                          AS ad_category,
                                                 a.department                                                                        AS ad_department,
                                                 a.color                                                                             AS ad_color,
                                                 a.cleansed_product_name                                                             AS ad_product_name,
                                                 a.product_status,
                                                 a.is_active,
                                                 b.product_sku                                                                       AS corresponding_sku,
                                                 b.product_category                                                                  AS corresponding_product_category,
                                                 b.category                                                                          AS corresponding_category,
                                                 b.department                                                                        AS corresponding_department,
                                                 b.color                                                                             AS corresponding_color,
                                                 b.cleansed_product_name                                                             AS corresponding_product_name,
                                                 iff(a.product_sku = b.product_sku, 3, 0)                                            AS sku_match,
                                                 iff(a.last_two_of_product_name = b.last_two_of_product_name, 2, 0)                  AS last_two_of_product_name_match,
                                                 iff(a.color = b.color, 2, 0)                                                        AS color_match,
                                                 iff(a.pockets = b.pockets, 1, 0)                                                    AS pockets_match,
                                                 last_two_of_product_name_match + pockets_match + color_match AS score,
                                                 row_number() OVER (PARTITION BY a.product_sku, sku_match ORDER BY score DESC) AS rnk
                                 FROM scrubs a
                                      LEFT JOIN scrubs b ON a.product_sku = b.product_sku -- keep rows for matching product sku
                                                         OR (a.product_category != b.product_category AND
                                                             a.category = b.category AND
                                                             (a.color = b.color OR b.color = 'Black')
                                                            )
                                 ORDER BY a.product_sku, rnk)
 SELECT ad_sku, corresponding_sku, ad_product_name, corresponding_product_name
 FROM corresponding_products
 WHERE rnk = 1 );

CREATE OR REPLACE TEMP TABLE _ad_skus AS
(WITH _all_channel                          AS (SELECT DISTINCT store_brand_name,
                                                                ad_id,
                                                                image_url                            AS ad_image,
                                                                store_id,
                                                                CASE WHEN sku1='x' then 'None'
                                                                    else ifnull(sku1, 'None') end as sku1,
                                                                case when sku1='x' then 'None'
                                                                    else ifnull(sku2, 'None') end as sku2,
                                                                case when sku1='x' then 'None'
                                                                    else ifnull(sku3, 'None') end as sku3,
                                                                case when sku1='x' then 'None'
                                                                    else ifnull(sku4, 'None') end as sku4,
                                                                case when sku1='x' then 'None'
                                                                    else ifnull(sku5, 'None') end as sku5,
                                                                max(date) OVER (PARTITION BY ad_id ) AS max_date
                                                FROM reporting_media_prod.dbo.all_channel_optimization
                                                WHERE lower(store_brand_name) IN
                                                      ('fabletics', 'fabletics men', 'fabletics scrubs', 'yitty')
                                                  AND date >= dateadd(MONTH, $ads_with_spend_months_back, current_date())
                                                  AND lower(channel) IN ('fb+ig')
                                                --  AND image_url IS NOT NULL
                                                )
    , _sku_pivot                            AS (SELECT *
                                                FROM _all_channel UNPIVOT (sku FOR ad_sku_rank IN (sku1, sku2, sku3, sku4, sku5)))
    , _final_sku_dataset                    AS (SELECT DISTINCT store_brand_name,
                                                                ad_id,
                                                                ad_image,
                                                                store_id,
                                                                max_date,
                                                                CASE WHEN replace(sku, ' ', '') NOT IN ('None', '-')
                                                                         THEN replace(ad_sku_rank, 'SKU', '') END AS ad_sku_rank,
                                                                replace(sku, ' ', '')                             AS ad_sku
                                                FROM _sku_pivot
                                                WHERE ((ad_sku_rank = 'SKU1') OR contains(sku, '-'))) -- AND sku = 'None'
    , _final_skus_with_corresponding_scrubs AS (SELECT ad_id,
                                                       coalesce(corresponding_sku, ad_sku) AS new_ad_sku,
                                                       min(ad_sku_rank)                    AS ad_sku_rank,
                                                       max(inferred_tagged_product)        AS inferred_tagged_product
                                                FROM (SELECT DISTINCT ad_id,
                                                                      f.ad_sku,
                                                                      s.corresponding_sku,
                                                                      CASE WHEN f.ad_sku = corresponding_sku OR corresponding_sku IS NULL
                                                                               THEN FALSE
                                                                           ELSE TRUE END                                         AS inferred_tagged_product,
                                                                      iff(inferred_tagged_product, ad_sku_rank + 5, ad_sku_rank) AS ad_sku_rank
                                                      FROM _final_sku_dataset f
                                                           LEFT JOIN scrubs_corresponding_items s
                                                                     ON s.ad_sku = f.ad_sku AND
                                                                        contains(lower(f.store_brand_name), 'scrubs')) a
                                                GROUP BY ad_id, new_ad_sku)

    , _with_product_data                    AS (SELECT DISTINCT b.store_id                                 AS ad_store_id,
                                                                store_brand_name                           AS ad_store,
                                                                a.ad_id,
                                                                max_date,
                                                                ad_image,
                                                                inferred_tagged_product,
                                                                ad_sku_rank,
                                                                iff(new_ad_sku = 'None', NULL, new_ad_sku) AS ad_sku,
                                                                iff(mpid IS NULL, -2, mpid)                AS ad_mpid,
                                                                store_group                                AS product_store_group,
                                                                store_country                              AS product_store_country,
                                                                department                                 AS product_department
                                                FROM _final_skus_with_corresponding_scrubs a
                                                     LEFT JOIN (SELECT DISTINCT store_id, store_brand_name, ad_id, max_date, ad_image
                                                                FROM _final_sku_dataset) b ON a.ad_id = b.ad_id
                                                     LEFT JOIN _product_data_by_mpid p
                                                               ON p.product_sku = a.new_ad_sku AND p.store_id =
                                                                                                   iff(b.store_id = 241, 52, b.store_id) --YITTY products are really fabletics products!!
    )
    , _similar_ads                          AS (SELECT DISTINCT orig.ad_id                                   AS orig_ad_id,
                                                                similar.ad_id                                AS similar_ad_id,
                                                                listagg(DISTINCT orig.ad_sku, ', ')
                                                                        WITHIN GROUP ( ORDER BY orig.ad_sku) AS matching_ad_skus
                                                FROM _with_product_data orig
                                                     JOIN _with_product_data similar ON orig.ad_sku = similar.ad_sku
                                                WHERE orig.inferred_tagged_product = 0
                                                  AND similar.inferred_tagged_product = 0
                                                GROUP BY orig_ad_id, similar_ad_id)

 SELECT a.*, s.similar_ad_id, s.matching_ad_skus
 FROM _with_product_data a
      LEFT JOIN _similar_ads s ON s.orig_ad_id = a.ad_id);

CREATE OR REPLACE TEMP TABLE _products_in_ads AS
(SELECT DISTINCT r.ad_store,
                 CASE WHEN r.ad_store IN ('Fabletics', 'Fabletics Men') THEN 'FL'
                      WHEN r.ad_store = 'Fabletics Scrubs' THEN 'SC'
                      WHEN r.ad_store = 'Yitty' THEN 'YT' END                                       AS sub_brand_prefix,
                 r.ad_id,
                 iff((min(ad_sku_rank) OVER (PARTITION BY r.ad_id)) IS NULL, 0, 1)                  AS tagged,
                 max(iff(contains(product_department, 'Womens'), 1, 0)) OVER (PARTITION BY r.ad_id) AS womens,
                 max(iff(contains(product_department, 'Mens'), 1, 0)) OVER (PARTITION BY r.ad_id)   AS mens,
                 iff(womens = 1 AND mens = 1, 1, 0)                                                 AS dual_gender_ad,
                 r.max_date,
                 r.ad_image,
                 r.inferred_tagged_product,
                 r.ad_sku,
                 r.ad_sku_rank,
                 iff(product_department != 'Scrubs Mens', ad_sku_rank, NULL)                        AS f_ad_sku_rank,
                 iff(product_department != 'Scrubs Womens', ad_sku_rank, NULL)                      AS m_ad_sku_rank,
                 r.ad_mpid                                                                          AS rec_mpid
 FROM _ad_skus r
 ORDER BY r.ad_id, r.ad_sku_rank);

CREATE OR REPLACE TEMP TABLE _ad_purchase_histories AS
(WITH _leads_from_ads    AS (SELECT DISTINCT to_date(r.registration_local_datetime) AS lead_date,
                                             r.customer_id,
                                             iff(c.gender = 'M', 'M', 'F')          AS gender,
                                             s.ad_id,
                                             s.ad_store                             AS ad_store,
                                             s.ad_image,
                                             s.ad_sku
                             FROM edw_prod.data_model.fact_registration r
                                  JOIN edw_prod.data_model.dim_customer c ON r.customer_id = c.customer_id
                                  JOIN _ad_skus s ON s.ad_id = replace(r.utm_term, 'fb_ad_id_', '')
                             WHERE to_date(r.registration_local_datetime) >=
                                   dateadd(MONTH, $leads_months_back, current_date())
                               AND is_secondary_registration = FALSE
                             ORDER BY ad_id, r.customer_id)

    , _vips_from_leads   AS (SELECT to_date(activation_local_datetime) AS vip_date, order_id, l.*
                             FROM _leads_from_ads l
                                  LEFT JOIN edw_prod.data_model.fact_activation a ON a.customer_id = l.customer_id AND
                                                                                     datediff(HOUR, lead_date, to_date(activation_local_datetime)) <
                                                                                     30)
    , _order_details     AS (SELECT DISTINCT ol.order_id,
                                             ol.product_id,
                                             p.product_sku AS purchase_product_sku, -- Get from product id to product sku
                                             pp.mpid       AS purchase_product_mpid --Then go get current MPID version of this sku (some old product ids were removed and redone)
                             FROM edw_prod.data_model.fact_order_line ol
                                  JOIN edw_prod.data_model.dim_product_type pt
                                       ON ol.product_type_key = pt.product_type_key
                                  JOIN _product_data_by_pid p ON ol.product_id = p.product_id
                                  JOIN _product_data_by_mpid pp
                                       ON p.product_sku = pp.product_sku AND ol.store_id = pp.store_id AND
                                          pp.is_active = TRUE
                             WHERE to_date(order_local_datetime) >= '2020-01-01'--dateadd(MONTH, $vip_months_back, current_date())
                               AND ol.store_id IN (52, 241)
                               AND lower(product_type_name) IN ('normal', 'bundle component')
                             ORDER BY order_id, purchase_product_sku)
    , _clicked_purchased AS (SELECT v.*,
                                    product_id AS exact_purchased_product_id,
                                    purchase_product_sku,
                                    purchase_product_mpid,
                                    similar_ad_id,
                                    matching_ad_skus
                             FROM _vips_from_leads v
                                  LEFT JOIN _order_details o ON o.order_id = v.order_id
                                  LEFT JOIN (SELECT DISTINCT ad_id, similar_ad_id, matching_ad_skus FROM _ad_skus) s
                                            ON v.ad_id = s.ad_id)

    , _from_ads          AS (SELECT vip_date,
                                    ad_id,
                                    purchase_product_mpid                             AS rec_mpid,
                                    count(DISTINCT iff(gender = 'F', order_id, NULL)) AS f_clicked_ad_purchased_product_orders,
                                    count(DISTINCT iff(gender = 'M', order_id, NULL)) AS m_clicked_ad_purchased_product_orders,
                                    count(DISTINCT order_id)                          AS t_clicked_ad_purchased_product_orders
                             FROM _clicked_purchased
                             WHERE exact_purchased_product_id IS NOT NULL
                             GROUP BY vip_date, ad_id, rec_mpid)
    , _from_similar_ads  AS (SELECT vip_date,
                                    similar_ad_id,
                                    purchase_product_mpid                             AS rec_mpid,
                                    count(DISTINCT iff(gender = 'F', order_id, NULL)) AS f_clicked_similar_ad_purchased_product_orders,
                                    count(DISTINCT iff(gender = 'M', order_id, NULL)) AS m_clicked_similar_ad_purchased_product_orders,
                                    count(DISTINCT order_id)                          AS t_clicked_similar_ad_purchased_product_orders
                             FROM _clicked_purchased
                             WHERE exact_purchased_product_id IS NOT NULL
                             GROUP BY vip_date, similar_ad_id, rec_mpid)
 SELECT coalesce(a.vip_date, s.vip_date)   AS vip_date,
        coalesce(a.ad_id, s.similar_ad_id) AS ad_id,
        coalesce(a.rec_mpid, s.rec_mpid)   AS rec_mpid,
        a.m_clicked_ad_purchased_product_orders,
        a.f_clicked_ad_purchased_product_orders,
        a.t_clicked_ad_purchased_product_orders,

        s.m_clicked_similar_ad_purchased_product_orders,
        s.f_clicked_similar_ad_purchased_product_orders,
        s.t_clicked_similar_ad_purchased_product_orders
 FROM _from_ads a
      FULL OUTER JOIN _from_similar_ads s
                      ON s.similar_ad_id = a.ad_id AND s.vip_date = a.vip_date AND s.rec_mpid = a.rec_mpid);

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.data_science.ad_id_source_data AS
-- CREATE OR REPLACE TABLE med_db_work.khirth.ad_id_source_data AS
(WITH ad_data               AS (SELECT DISTINCT ad_id,
                                                ad_store,
                                                ad_image,
                                                sub_brand_prefix,
                                                tagged,
                                                womens,
                                                mens,
                                                dual_gender_ad,
                                                max_date
                                FROM _products_in_ads)

    , todays_ad_products    AS (SELECT DISTINCT current_date() AS vip_date, ad_id, rec_mpid FROM _products_in_ads)
    , histories_with_todays AS (SELECT coalesce(h.vip_date, a.vip_date) AS date,
                                       coalesce(h.ad_id, a.ad_id)       AS ad_id,
                                       coalesce(h.rec_mpid, a.rec_mpid) AS rec_mpid,
                                       m_clicked_ad_purchased_product_orders,
                                       f_clicked_ad_purchased_product_orders,
                                       t_clicked_ad_purchased_product_orders,
                                       m_clicked_similar_ad_purchased_product_orders,
                                       f_clicked_similar_ad_purchased_product_orders,
                                       t_clicked_similar_ad_purchased_product_orders
                                FROM _ad_purchase_histories h
                                     FULL OUTER JOIN todays_ad_products a
                                                     ON a.ad_id = h.ad_id AND a.rec_mpid = h.rec_mpid AND
                                                        a.vip_date = h.vip_date)
    , add_ad_data           AS ( --need to fill in ad data for products that are not in replacement data
        SELECT a.*, ad_store, ad_image, sub_brand_prefix, tagged, womens, mens, dual_gender_ad, max_date
        FROM histories_with_todays a
             LEFT JOIN ad_data aa ON a.ad_id = aa.ad_id)

 SELECT DISTINCT h.ad_store,
                 h.sub_brand_prefix,
                 h.ad_id,
                 h.tagged,
                 h.womens,
                 h.mens,
                 h.dual_gender_ad,
                 h.max_date,
                 h.ad_image,
                 h.date,
                 h.rec_mpid,
                 m.image_url,
                 a.inferred_tagged_product,
                 ad_sku,
                 ad_sku_rank,
                 f_ad_sku_rank,
                 m_ad_sku_rank,
                 m_clicked_ad_purchased_product_orders,
                 f_clicked_ad_purchased_product_orders,
                 t_clicked_ad_purchased_product_orders,
                 m_clicked_similar_ad_purchased_product_orders,
                 f_clicked_similar_ad_purchased_product_orders,
                 t_clicked_similar_ad_purchased_product_orders,
                 current_date() AS last_updated
 FROM add_ad_data h
      LEFT JOIN _products_in_ads a ON h.ad_id = a.ad_id AND h.rec_mpid = a.rec_mpid
      LEFT JOIN _product_data_by_mpid m ON h.rec_mpid = m.mpid
 ORDER BY h.ad_id, date DESC, ad_sku_rank, t_clicked_ad_purchased_product_orders,
          t_clicked_similar_ad_purchased_product_orders);


-- THIS TABLE TACKS ON ALL TAGGED PRODUCTS TO TODAYS DATE ONLY IF THEY DO NOT HAVE PURCHASE HISTORY
-- SO IF THEY WERE NOT PURCHASED ON A PREVIOUS DATE, THEY WONT SHOW UP ON THOSE OTHER DATES
