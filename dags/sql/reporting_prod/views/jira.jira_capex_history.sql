CREATE OR REPLACE VIEW reporting_prod.jira.jira_capex_history AS
SELECT DISTINCT
       i.id AS issue_id,
       i.key AS issue_key,
       i.summary AS issue_summary,
       i.project_description AS user_story,
       i.assignee AS issue_assignee,
       i.reporter AS issue_reporter,
       CASE WHEN i.component LIKE '%Data Analytics - Data Engineering%' THEN 'Data Engineering'
           WHEN i.component LIKE '%Data Analytics - GFB%' THEN 'JFB'
           WHEN i.component LIKE '%Rih-Creators%' THEN 'Rih-Creators'
           WHEN i.component LIKE '%Data Analytics- Global Applications%' THEN 'Global Apps'
           WHEN i.component LIKE '%Data Analytics - SXF%' THEN 'Savage X Fenty'
           WHEN i.component LIKE '%Console Squad%' THEN 'Console Squad'
           WHEN i.component LIKE '%Lingerie Legends%' THEN 'Lingerie Legends'
           WHEN i.component LIKE '%Data Analytics - FL%' THEN 'Fabletics'
           WHEN i.component LIKE '%Tech%' THEN 'Tech'
           WHEN i.component LIKE '%Data Analytics - Web Analytics%' THEN 'Web Analytics'
           WHEN i.component LIKE '%segment%'THEN 'segment'
           WHEN i.component LIKE '%DevOps%' THEN'DevOps'
           WHEN i.component LIKE '%GQL: Product (PDP)%' THEN 'GQL: Product PDP'
           WHEN i.component LIKE '%QA Automation%' THEN 'QA Automation'
           WHEN i.component LIKE '%GQL: Product (Grid)%' THEN 'GQL: Product Grid'
           WHEN i.component LIKE '%Merlin%' THEN 'Merlin'
           WHEN i.component LIKE '%Pricing Service%' THEN 'Pricing Service'
           WHEN i.component LIKE '%Automation%' THEN 'Automation'
           WHEN i.component LIKE '%AR%' THEN 'AR'
           WHEN i.component LIKE '%Centric PLM%' THEN 'Centric PLM'
           WHEN i.component LIKE '%CMS Squad%' THEN 'CMS Squad'
           WHEN i.component LIKE '%Data Analytics - Media Measurement%' THEN  'Media Measurement'
           WHEN i.component LIKE '%DB Split%' THEN 'DB Spilt'
           WHEN i.component LIKE '%Grids%' THEN 'Grids'
           WHEN i.component LIKE '%INV%' THEN 'INV'
           WHEN i.component LIKE '%POT%' THEN 'POT'
           WHEN i.component LIKE '%Priority Squad%' THEN  'Priority Squad'
           WHEN i.component LIKE '%Checkout%' THEN  'Checkout'
           WHEN i.component LIKE '%Data Analytics - Accounting%' THEN 'Accounting'
           WHEN i.component LIKE '%FIS%'THEN 'FIS-Rapid Response'
           WHEN i.component LIKE '%Other%' THEN 'Other'
           WHEN i.component LIKE '%Address%' THEN 'Address'
           WHEN i.component LIKE '%AP%' THEN 'AP'
           WHEN i.component LIKE '%API Packages%' THEN  'API Packages'
           WHEN i.component LIKE '%BlueCherry%' THEN  'BlueCherry'
           WHEN i.component LIKE '%builder%' THEN  'builder'
           WHEN i.component LIKE '%CF API%' THEN  'CF API'
           WHEN i.component LIKE '%Core%' THEN 'Core'
           WHEN i.component LIKE '%AB Test%' THEN 'AB Testing'
           WHEN i.component LIKE '%Media%' THEN 'Media'
           WHEN i.component LIKE '%Prod Support%' THEN  'Prod Support'
           WHEN i.component LIKE '%Database%' THEN 'Database'
           WHEN i.component LIKE '%Embroidery%' THEN 'Embroidery Service'
           WHEN i.component LIKE '%gms-api%' THEN 'gms-api'
           WHEN i.component LIKE '%GQL: Reviews%'THEN 'GQL: Reviews'
           WHEN i.component LIKE '%Homepage%' THEN 'Homepage'
           WHEN i.component LIKE '%My Account%' THEN 'My Account'
           WHEN i.component LIKE '%Node%' THEN  'Node API'
           WHEN i.component LIKE '%OmniTier%' THEN 'OmniTier-Service'
           WHEN i.component LIKE '%Order Details%' THEN 'Order Details'
           WHEN i.component LIKE '%PIX-E%' THEN 'PIX-E'
           WHEN i.component LIKE '%PO%' THEN  'PO'
           WHEN i.component LIKE '%Printing%' THEN  'Printing'
           WHEN i.component LIKE '%Resource%'THEN 'Resource Bundles'
           WHEN i.component LIKE '%Reviews REST%' THEN  'Reviews REST'
           WHEN i.component LIKE '%Tier 1%' THEN  'Tier 1 / BFF'
           WHEN i.component LIKE '%CMS Squad%' THEN 'CMS Squad'
           WHEN i.component IS NULL THEN 'No Component'
       ELSE i.component END AS component,
       p.name AS project_name,
       p.key AS project_key,
       i.priority,
       i.issue_type,
       //coalesce(to_date(f.first_closed_date),to_date(i.resolution_date)) as RESOLVED_DATE,
       min(to_date(ad.first_closed_date)) AS resolved_date,
       CASE WHEN i.capex = '14800' THEN 'Capital'
            WHEN i.capex = '14802' THEN 'Capital'
            WHEN i.capex = '10188' THEN 'Capital'
            WHEN i.capex = '10186' THEN 'Capital'
            WHEN i.capex = '14803' THEN 'Expense'
            WHEN i.capex = '14801' THEN 'Expense'
            WHEN i.capex = '10189' THEN 'Expense'
            WHEN i.capex = '10187' THEN 'Expense'
            WHEN i.capex = '18801' THEN NULL
       ELSE i.capex END AS capex,
       i.story_pts,
       i.shared_capex_pts,
       i.fl_capex_points,
       i.sxf_capex_points,
       i.jfb_capex_points,
       i.epic_link AS epic_key,
       e.epic_name AS epic_name,
       e.summary AS epic_summary,
       e.project_description AS epic_story,
       p.meta_create_datetime AS fivetran_synced
FROM reporting_prod.jira.jira_issues i
    LEFT JOIN reporting_prod.shared.jira_epics e ON
        i.epic_link = e.key
    LEFT JOIN lake_view.jira.project p ON
        i.project_id = p.id
    LEFT OUTER JOIN reporting_prod.jira.jira_first_close_date_mv2 ad ON
        i.id = ad.issue_id
WHERE i.project_id IN (10003, 10006, 10008, 10014, 10018, 10019, 10021, 10023, 10025, 10027, 10029, 10031, 10032, 10033, 10035, 10036,
       10038, 10039, 10043, 10045, 10050, 10054, 10056, 10058, 10060, 10065, 10068, 10074, 10120, 10124)
AND i.issue_type NOT IN ('Deployment', 'Sub-task','Epic','Translations')
AND ad.first_closed_date IS NOT NULL
GROUP BY ALL;
