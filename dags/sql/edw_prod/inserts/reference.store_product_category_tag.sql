BEGIN;

TRUNCATE TABLE reference.store_product_category_tag;

CREATE OR REPLACE TEMP TABLE _store_product_category_tag__stg
(
    store_id                          NUMBER(38, 0),
    meta_original_product_category_id NUMBER(38, 0),
    meta_original_tag_id              NUMBER(38, 0),
    level                             VARCHAR(20),
    label                             VARCHAR(20)
);

INSERT INTO _store_product_category_tag__stg (store_id, meta_original_product_category_id, meta_original_tag_id, level,
                                              label)
VALUES (26, 97, 1003, '1', 'Style'),
       (26, 97, 1010, '2', 'Color'),
       (26, 97, 1171, '3', 'Material'),
       (26, 98, 1103, '1', 'Style'),
       (26, 98, 1104, '3', 'Material'),
       (26, 98, 1106, '2', 'Color'),
       (26, 110, 1217, '1', 'Style'),
       (26, 110, 1218, '2', 'Color'),
       (26, 119, 1267, '1', 'Style'),
       (46, 0, 1324, '2', 'Color'),
       (52, 207, 1465, '2', 'Color'),
       (52, 208, 1486, '2', 'Color'),
       (52, 209, 1504, '2', 'Color'),
       (52, 210, 1567, '2', 'Color'),
       (52, 211, 1526, '2', 'Color'),
       (55, 258, 1648, '2', 'Color'),
       (55, 258, 1649, '3', 'Material'),
       (55, 258, 3230, '1', 'Style'),
       (55, 264, 1662, '2', 'Color'),
       (55, 264, 1663, '3', 'Material'),
       (55, 264, 3231, '1', 'Style'),
       (55, 265, 1673, '2', 'Color'),
       (55, 265, 3232, '1', 'Style');

INSERT INTO reference.store_product_category_tag(store_id,
                                                 product_category_id,
                                                 meta_original_product_category_id,
                                                 tag_id,
                                                 meta_original_tag_id,
                                                 level,
                                                 label)
SELECT s.store_id,
       CONCAT(s.meta_original_product_category_id, ds.company_id) AS product_category_id,
       meta_original_product_category_id,
       CONCAT(s.meta_original_tag_id, ds.company_id)              AS tag_id,
       meta_original_tag_id,
       level,
       s.label
FROM _store_product_category_tag__stg s
         JOIN lake_fl.reference.dim_store ds
              ON s.store_id = ds.store_id;


COMMIT;
