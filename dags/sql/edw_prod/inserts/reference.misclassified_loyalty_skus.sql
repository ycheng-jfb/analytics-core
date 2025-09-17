
/*  DA-29041 - There's an upstream issue where product available to loyalty redemptions are marked as loyalty
    but doesn't change to product_type_id 1 if the customer paid for it with cash/credit.  We are implementing a temporary fix until this is resolved upstream.
    If an order_line_id has product_type_id 11 and subtotal > 0 within a certain subset of SKUs, then are going to flag them "normal" instead of "loyalty"
 */

TRUNCATE TABLE reference.misclassified_loyalty_skus;

INSERT INTO reference.misclassified_loyalty_skus
(product_sku, meta_create_datetime, meta_update_datetime)
values
    ('UW2041886-0001', current_timestamp, current_timestamp),
    ('UW2041884-0001', current_timestamp, current_timestamp),
    ('UW2041886-8793', current_timestamp, current_timestamp),
    ('SC2252194-3570', current_timestamp, current_timestamp),
    ('UW2044696-4846', current_timestamp, current_timestamp),
    ('UW2044696-8793', current_timestamp, current_timestamp),
    ('SC2043060-8828', current_timestamp, current_timestamp),
    ('LG2045793-0001', current_timestamp, current_timestamp),
    ('BA2357067-0001', current_timestamp, current_timestamp),
    ('LG2457725-8813', current_timestamp, current_timestamp),
    ('BA2457731-8813', current_timestamp, current_timestamp),
    ('HT2355278-9229', current_timestamp, current_timestamp),
    ('HT2254324-3813', current_timestamp, current_timestamp),
    ('HT2354388-8709', current_timestamp, current_timestamp),
    ('HT2253641-6932', current_timestamp, current_timestamp),
    ('PT2356950-0001', current_timestamp, current_timestamp),
    ('BA2457741-8165', current_timestamp, current_timestamp),
    ('TK2357058-8402', current_timestamp, current_timestamp),
    ('LG2356977-8402', current_timestamp, current_timestamp),
    ('LE2147348-3813', current_timestamp, current_timestamp),
    ('AC1512790-0001', current_timestamp, current_timestamp),
    ('LE2457752-8864', current_timestamp, current_timestamp),
    ('SS2356914-1867', current_timestamp, current_timestamp),
    ('JT2457726-8811', current_timestamp, current_timestamp),
    ('ON2457739-0891', current_timestamp, current_timestamp),
    ('ON2457728-8811', current_timestamp, current_timestamp),
    ('ON2357055-8403', current_timestamp, current_timestamp),
    ('LS2355677-3813', current_timestamp, current_timestamp),
    ('JT2252972-4846', current_timestamp, current_timestamp),
    ('PT2457859-4846', current_timestamp, current_timestamp),
    ('JT2458242-9311', current_timestamp, current_timestamp),
    ('HW2252997-1855', current_timestamp, current_timestamp),
    ('HW2355333-6693', current_timestamp, current_timestamp),
    ('HW2252997-3560', current_timestamp, current_timestamp),
    ('TH2253016-4781', current_timestamp, current_timestamp),
    ('BB2250602-3560', current_timestamp, current_timestamp),
    ('MD2250597-5192', current_timestamp, current_timestamp),
    ('SO2250599-8389', current_timestamp, current_timestamp),
    ('BU2251651-5192', current_timestamp, current_timestamp),
    ('LG2357352-8391', current_timestamp, current_timestamp),
    ('SO2250599-5192', current_timestamp, current_timestamp),
    ('LG2354591-6351', current_timestamp, current_timestamp),
    ('BU2253015-4781', current_timestamp, current_timestamp),
    ('CN2357059-0687', current_timestamp, current_timestamp),
    ('LG2356976-4781', current_timestamp, current_timestamp),
    ('ON2357103-3560', current_timestamp, current_timestamp),
    ('MC2356985-3560', current_timestamp, current_timestamp);


