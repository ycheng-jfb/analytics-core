CREATE OR REPLACE TRANSIENT TABLE shared.token_purchases_tax AS
SELECT st.store_brand,
       st.store_country,
       reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'country') AS shipping_country,
       reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'state')   AS shipping_state,
       zcs.county,
       COALESCE(zcs.city, a.city)                                                               AS city,
       st.store_brand || ' ' || st.store_country                                                AS brand,
       st.store_type,
       dcf.credit_id,
       dcf.credit_type,
       dcf.credit_reason,
       dcf.credit_issued_local_datetime::DATE                                                   AS issued_date,
       fce.credit_activity_type,
       fce.credit_activity_local_datetime::DATE                                                 AS activity_date,
       IFF(fce.credit_activity_type = 'Issued', -fce.credit_activity_local_amount,
           fce.credit_activity_local_amount)                                                    AS credit_activity_local_amount
FROM shared.dim_credit dcf
         JOIN shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN shared.fact_credit_event fce ON dcf.credit_key = fce.credit_key
    AND fce.credit_activity_type IN ('Issued', 'Redeemed', 'Cancelled', 'Expired')
    AND dcf.credit_type = 'Token'
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN lake_consolidated_view.ultra_merchant."ORDER" o
                   ON COALESCE(dcf2.credit_order_id, dcf.credit_order_id)
                       = o.order_id -- get the order where the credit was purchased
         LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs ON SUBSTRING(a.zip, 1, 5) = zcs.zip;

ALTER TABLE shared.token_purchases_tax
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

-- snapshot
INSERT INTO shared.token_purchases_tax_snapshot
SELECT store_brand,
       store_country,
       shipping_country,
       shipping_state,
       county,
       city,
       brand,
       store_type,
       credit_id,
       credit_type,
       credit_reason,
       issued_date,
       credit_activity_type,
       activity_date,
       credit_activity_local_amount,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM shared.token_purchases_tax;

DELETE
FROM shared.token_purchases_tax_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
