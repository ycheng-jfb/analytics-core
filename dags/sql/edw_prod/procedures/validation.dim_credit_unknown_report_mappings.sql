TRUNCATE TABLE validation.dim_credit_unknown_report_mappings;
INSERT INTO validation.dim_credit_unknown_report_mappings
SELECT 'reporting' AS db,
       original_credit_type,
       original_credit_reason,
       original_credit_tender,
       original_credit_match_reason,
       COUNT(*)    AS row_count
FROM reporting_base_prod.shared.dim_credit
WHERE credit_report_mapping = 'Unknown'
GROUP BY db, original_credit_type,
         original_credit_reason,
         original_credit_tender,
         original_credit_match_reason
HAVING COUNT(*)>500
UNION ALL
SELECT 'edw_prod' AS db,
       original_credit_type,
       original_credit_reason,
       original_credit_tender,
       original_credit_match_reason,
       COUNT(*)   AS row_count
FROM data_model.dim_credit
WHERE credit_report_mapping = 'Unknown'
GROUP BY db, original_credit_type,
         original_credit_reason,
         original_credit_tender,
         original_credit_match_reason
HAVING COUNT(*)>500;
