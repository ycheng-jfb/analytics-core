SELECT DISTINCT user_name
FROM lake_view.tableau.users_by_group
WHERE updated_at = (
    SELECT MAX(updated_at) FROM lake_view.tableau.users_by_group
)
AND lower(user_name) NOT IN (SELECT lower(email) FROM lake_view.workday.employees)
AND lower(user_name) NOT IN (SELECT COALESCE(lower(mail), 'NA') FROM lake_view.azure.user_details)
AND lower(user_name) NOT IN (SELECT COALESCE(lower(user_principal_name), 'NA') FROM lake_view.azure.user_details)
