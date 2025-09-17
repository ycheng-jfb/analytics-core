CREATE OR REPLACE TRANSIENT TABLE validation.landed_cost_zero
(
    item_id              NUMBER(38, 0),
    warehouse_id         NUMBER(38, 0),
    brand                VARCHAR(255),
    region               VARCHAR(16777216),
    is_retail            BOOLEAN,
    sku                  VARCHAR(30),
    product_sku          VARCHAR(90),
    open_to_buy_quantity NUMBER(38, 0),
    intransit_quantity   NUMBER(38, 0),
    landed_cost_per_unit NUMBER(38, 10)
);
