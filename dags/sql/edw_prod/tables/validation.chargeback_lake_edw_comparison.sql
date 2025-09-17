CREATE OR REPLACE TRANSIENT TABLE validation.chargeback_lake_edw_comparison
(
    chargeback_month DATE,
    store_brand VARCHAR,
    store_country VARCHAR,
    lake_chargeback_amt NUMBER(38,2),
    edw_chargeback_amt NUMBER(38,2)
);
