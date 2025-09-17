SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _GENESYS_CONVERSATION AS
SELECT g.conversation_id,
      CASE
          WHEN c.case_id IS NULL THEN 'Y'
          ELSE 'N'
          END AS case_manually_created
FROM reporting_prod.gms.genesys_conversation g
LEFT JOIN lake_consolidated_view.ultra_merchant.case c
    ON g.conversation_id = TRIM(c.foreign_case_number)
WHERE g.conversation_start::DATE >= $low_watermark_ltz
 AND (
       g.address_from IS NULL OR
       (
           g.address_from NOT LIKE '%%stylist@e-mail.justfab.com%%' AND
           g.address_from NOT LIKE '%%Post_master@vtext.com%%' AND
           g.address_from NOT LIKE '%%jbullard@wcpss.net%%' AND
           g.address_from NOT LIKE '%%mail.ru%%' AND
           g.address_from NOT LIKE '%%bk.ru%%' AND
           g.address_from NOT LIKE '%%rambler.ru%%'
           ) OR
       g.participant_name NOT LIKE '%%AI Anna%%'
)
;

CREATE OR REPLACE TEMP TABLE _DISTINCT_LABELS AS
SELECT
    b.case_id,
    t.label,
    g.parent_group,
    b.datetime_added
FROM lake_consolidated.ultra_merchant.bond_user_action b
LEFT JOIN lake_consolidated.ultra_merchant.bond_user_action_type t
    ON b.bond_user_action_type_id = t.bond_user_action_type_id
LEFT JOIN lake_consolidated.ultra_merchant.bond_user_action_parameter p
    ON b.bond_user_action_id = p.bond_user_action_id
LEFT JOIN reporting_prod.gms.parent_grouping_case_auto_tagging g
    ON b.bond_user_action_type_id = g.bond_user_action_type_id
LEFT JOIN lake_consolidated_view.ultra_merchant.case c
    ON b.case_id = c.case_id
WHERE TO_DATE(b.datetime_added) >= $low_watermark_ltz
GROUP BY b.case_id,
    t.label,
    g.parent_group,
    b.bond_user_action_type_id,
    b.datetime_added;

CREATE OR REPLACE TEMP TABLE _TAGS AS
SELECT
    TRIM(c.foreign_case_number)  AS foreign_case_number,
    LISTAGG(d.label, ', ') WITHIN GROUP (ORDER BY d.datetime_added) AS concatenated_tags,
    COUNT(DISTINCT d.case_id) AS case_count
FROM _distinct_labels d
LEFT JOIN lake_consolidated.ultra_merchant.case c
    ON TRIM(c.case_id) = TRIM(d.case_id)
GROUP BY c.foreign_case_number
;

CREATE OR REPLACE TEMP TABLE _CASE_METADATA AS
SELECT
    TRIM(c.foreign_case_number) AS foreign_case_number,
    cx.customer_id,
    s.store_name_region,
    s.store_brand_abbr,
    s.store_country,
FROM lake_consolidated_view.ultra_merchant.case c
LEFT JOIN (SELECT *
            FROM lake_consolidated_view.ultra_merchant.case_customer
            WHERE customer_id NOT IN ('20208565010', '91992414020', '92211177030')) cc //excludes ghost CIDs
        ON c.case_id = cc.case_id
LEFT JOIN lake_consolidated_view.ultra_merchant.customer cx
    ON cc.customer_id = cx.customer_id
LEFT JOIN edw_prod.data_model.dim_store s
    ON s.store_id = cx.store_id
WHERE TO_DATE(c.datetime_added) >= $low_watermark_ltz
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.foreign_case_number ORDER BY cx.customer_id ASC) = 1 // returns 1 CID per foreign_case_number
;



insert into reporting_prod.gms.callminer_case_auto_tagging
(
conversation_id,
customer_id,
store_name_region,
store_brand_abbr,
store_country,
case_manually_created,
concatenated_tags,
meta_create_datetime,
meta_update_datetime
)
SELECT g.conversation_id,
    LEFT(c.customer_id, LENGTH(c.customer_id) - 2) AS customer_id,
    c.store_name_region,
    c.store_brand_abbr,
    c.store_country,
    g.case_manually_created,
    t.concatenated_tags,
    CURRENT_TIMESTAMP() AS meta_create_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime
FROM _genesys_conversation g
LEFT JOIN _tags t ON g.conversation_id = t.foreign_case_number
LEFT JOIN _case_metadata c ON g.conversation_id = c.foreign_case_number
GROUP BY g.conversation_id,
    c.customer_id,
    c.store_name_region,
    c.store_brand_abbr,
    c.store_country,
    g.case_manually_created,
    t.concatenated_tags;
