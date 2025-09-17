CREATE OR REPLACE VIEW data_model_sxf.dim_date
AS
SELECT DISTINCT
	dd.date_key,
	dd.full_date,
	dd.day_of_week,
	dd.day_name_of_week,
	dd.day_of_month,
	dd.day_of_year,
	dd.week_of_year,
	dd.month_name,
	dd.month_date,
	dd.calendar_quarter,
	dd.calendar_year,
	dd.calendar_year_quarter,
	pd.planning_year,
	pd.planning_month,
	pd.planning_week_of_month,
    	IFF(hd_us.full_date IS NULL, 0, 1) as is_us_holiday,
    	IFF(hd_eu.full_date IS NULL, 0, 1) as is_eu_holiday,
	--dd.effective_start_datetime,
	--dd.effective_end_datetime,
	--dd.is_current,
	dd.meta_create_datetime,
	dd.meta_update_datetime
FROM stg.dim_date dd
LEFT JOIN reference.planning_date pd ON dd.full_date = pd.full_date
LEFT JOIN reference.holiday_calendar hd_us ON dd.full_date = hd_us.full_date and hd_us.region = 'NA'
LEFT JOIN reference.holiday_calendar hd_eu ON dd.full_date = hd_eu.full_date and hd_eu.region = 'EU'
WHERE dd.is_current;
