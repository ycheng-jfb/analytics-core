TRUNCATE TABLE reference.fake_retail_leads_with_activations;

INSERT INTO reference.fake_retail_leads_with_activations(customer_id)
SELECT
    DISTINCT fa.customer_id
FROM stg.fact_registration fr
JOIN stg.fact_activation fa
    ON fa.customer_id = fr.customer_id
WHERE fr.is_fake_retail_registration;
