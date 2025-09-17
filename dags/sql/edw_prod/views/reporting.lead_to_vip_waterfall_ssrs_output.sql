
CREATE OR REPLACE VIEW reporting.lead_to_vip_waterfall_ssrs_output(
	REPORT_MAPPING,
    BRAND_MAPPING,
	SEGMENT,
	REGISTRATION_COHORT,
	ACTIVATION_COHORT,
	TENURE_GROUP,
	SEQUENCE,
	TENURE,
	VIP_CUSTOMERS,
	LEAD_CUSTOMERS,
	LEADS,
	CUSTOMERS,
	DATE_UPDATED
) AS
SELECT report_mapping,
    brand_mapping,
    segment,
    registration_cohort,
    activation_cohort,
    tenure_group,
    CASE tenure_group WHEN 'M0' THEN 0
        WHEN 'M1' THEN 1
        WHEN 'M2' THEN 2
        WHEN 'M3' THEN 3
        WHEN 'M4' THEN 4
        WHEN 'M5' THEN 5
        WHEN 'M6' THEN 6
        WHEN 'M7' THEN 7
        WHEN 'M8-13' THEN 8
        WHEN 'M14+' THEN 9
        WHEN 'M14-25' THEN 9
        WHEN 'M26-37' THEN 10
        WHEN 'M38+' THEN 11
        ELSE 15 END AS sequence,
    tenure,
    IFF(segment = 'VIPs' AND SUM(customers) <> 0, SUM(customers), 0) AS vip_customers,
    IFF(segment = 'Leads' AND SUM(customers) <> 0, SUM(customers), 0) AS lead_customers,
    IFF(segment = 'Leads' AND tenure_group = 'M1', SUM(customers), 0) AS leads,
    SUM(customers) AS customers,
    CURRENT_TIMESTAMP() AS date_updated
FROM reporting.lead_to_vip_waterfall_final_output
WHERE report_mapping IS NOT NULL
    AND activation_cohort IS NOT NULL
GROUP BY report_mapping,
    brand_mapping,
    segment,
    registration_cohort,
    activation_cohort,
    tenure_group,
    CASE tenure_group WHEN 'M1' THEN 1
        WHEN 'M2' THEN 2
        WHEN 'M3' THEN 3
        WHEN 'M4' THEN 4
        WHEN 'M5' THEN 5
        WHEN 'M6' THEN 6
        WHEN 'M7' THEN 7
        WHEN 'M8-13' THEN 8
        WHEN 'M14+' THEN 9
        WHEN 'M14-25' THEN 9
        WHEN 'M26-37' THEN 10
        WHEN 'M38+' THEN 11
        ELSE 15 END,
    tenure;
