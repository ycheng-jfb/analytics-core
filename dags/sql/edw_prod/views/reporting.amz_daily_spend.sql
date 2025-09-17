CREATE OR REPLACE VIEW reporting.amz_daily_spend(
    ty_date,
    ly_date,
    spend_ty,
    spend_ly,
    meta_create_datetime,
    meta_update_datetime
)
AS
(
SELECT ty_date,
       ly_date,
       spend_ty,
       spend_ly,
       meta_create_datetime,
       meta_update_datetime
FROM dbt_edw_prod.reporting.amz_daily_spend
    );
