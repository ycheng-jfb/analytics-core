SELECT * FROM (
    SELECT
    email,
    first_name,
    last_name,
    business_title,
    manager,
    status,
    termination_date,
    rank() over (PARTITION BY email ORDER BY termination_date ASC) AS r
    FROM (
        SELECT t.user_name as email, wd.first_name, wd.last_name, wd.business_title, wd.manager, wd.status, wd.termination_date
        FROM (
            SELECT DISTINCT user_name
            FROM lake_view.tableau.users_by_group
            WHERE updated_at = (
                SELECT MAX(updated_at) FROM lake_view.tableau.users_by_group
            )
        ) AS t
        JOIN (
            SELECT *
            FROM (
                SELECT
                    email,
                    first_name,
                    last_name,
                    business_title,
                    manager,
                    status,
                    termination_date,
                    rank() over (PARTITION BY employee_id ORDER BY Start_date DESC) AS r
                FROM lake_view.workday.employees
            )
            WHERE r = 1
        ) AS wd
        ON LOWER(t.user_name) = LOWER(wd.email)
        WHERE LOWER(status) = 'terminated'

        UNION

        SELECT t.user_name as email, az.first_name, az.last_name, az.business_title, az.manager, az.status, az.termination_date
        FROM (
            SELECT DISTINCT user_name
            FROM lake_view.tableau.users_by_group
            WHERE updated_at = (
                SELECT MAX(updated_at) FROM lake_view.tableau.users_by_group
            )
        ) AS t
        JOIN (
            SELECT
                mail as email,
                given_name AS first_name,
                surname AS last_name,
                job_title AS business_title,
                Null AS manager,
                'Terminated' AS status,
                Null AS termination_date
            FROM lake_view.azure.user_details
            WHERE account_enabled = 'false'
        ) AS az
        ON LOWER(t.user_name) = LOWER(az.email)

        UNION

        SELECT t.user_name AS email, az.first_name, az.last_name, az.business_title, az.manager, az.status, az.termination_date
        FROM (
            SELECT DISTINCT user_name
            FROM lake_view.tableau.users_by_group
            WHERE updated_at = (
                SELECT MAX(updated_at) FROM lake_view.tableau.users_by_group
            )
        ) AS t
        JOIN (
            SELECT
                user_principal_name as email,
                given_name AS first_name,
                surname AS last_name,
                job_title AS business_title,
                Null AS manager,
                'Terminated' AS status,
                Null AS termination_date
            FROM lake_view.azure.user_details
            WHERE account_enabled = 'false'
        ) AS az
        ON LOWER(t.user_name) = LOWER(az.email)
    )
)
WHERE r = 1
