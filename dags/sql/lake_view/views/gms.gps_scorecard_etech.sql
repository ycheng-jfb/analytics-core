CREATE OR REPLACE VIEW lake_view.gms.gps_scorecard_etech AS
SELECT
    bu,
    CASE WHEN LOWER(process) = 'digital services - chat' THEN 'Chat'
        WHEN LOWER(process) = 'digital services - email' THEN 'Email'
        ELSE process END AS process,
    location,
    date_of_contact,
    genesys_id,
    fashion_consultant,
    conversation_id,
    num_of_contacts_scored,
    overall_max_score,
    total_points_earned,
    num_of_max_alerts,
    meta_create_datetime,
    meta_update_datetime
FROM lake.excel.gps_scorecard;
