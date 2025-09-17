CREATE OR REPLACE VIEW reporting.gms.planned_billings AS
SELECT  ds.store_brand || ' ' || ds.store_country AS store,
        CAST(p.date_period_start AS DATE) AS billing_month,
        CAST(t.date_billing AS DATE) AS billing_date,
        t.datetime_start,
        t.max_credits AS planned_billings,
        current_timestamp::timestamp_ltz AS datetime_added
FROM lake_view.ultra_merchant.membership_billing_credit_threshold t
JOIN lake_view.ultra_merchant.period p ON p.period_id = t.period_id AND p.type = 'monthly'
JOIN lake_view.ultra_merchant.membership_plan mp ON mp.membership_plan_id = t.membership_plan_id
JOIN edw.data_model.dim_store ds ON ds.store_id = mp.Store_Id;
