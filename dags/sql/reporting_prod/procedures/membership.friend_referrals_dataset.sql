
SET max_date_in_table = (SELECT MAX(date) FROM reporting_prod.membership.friend_referrals_dataset);
SET update_from_date = COALESCE(DATEADD(DAY,-7,$max_date_in_table),'2013-01-01');   --refresh last 7 days

CREATE TABLE IF NOT EXISTS reporting_prod.membership.friend_referrals_dataset
(
	division VARCHAR(25),
	region VARCHAR(25),
	business_unit VARCHAR(50),
	country VARCHAR(25),
	store VARCHAR(50),
	store_id NUMBER(19,4) NOT NULL,
	date DATE NOT NULL,
	customers_inviting NUMBER(19,4),
	invites_sent NUMBER(19,4),
	leads_from_invites NUMBER(19,4),
	vips_from_invites NUMBER(19,4)
);

CREATE OR REPLACE TEMP TABLE _tmp_invites AS
SELECT
    m.store_id AS store_id,
    CAST(mi.datetime_added AS DATE) AS date,
    COUNT(DISTINCT ml.membership_id) AS customers_inviting,
    COUNT(1) AS invites_sent
FROM lake_consolidated_view.ultra_merchant.membership_invitation mi
JOIN lake_consolidated_view.ultra_merchant.membership_invitation_link ml ON mi.membership_invitation_link_id = ml.membership_invitation_link_id
JOIN lake_consolidated_view.ultra_merchant.membership m ON m.membership_id = ml.membership_id
WHERE mi.datetime_added >= $update_from_date
GROUP BY
    CAST(mi.datetime_added AS DATE),
    m.store_id;

CREATE OR REPLACE TEMP TABLE _tmp_vips_first_activation AS
SELECT
    fme.store_id,
    fme.customer_id,
    fme.event_start_local_datetime,
    ROW_NUMBER() OVER(PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime) AS act_first
FROM edw_prod.stg.fact_membership_event fme
JOIN (SELECT customer_id FROM edw_prod.stg.fact_membership_event
      WHERE membership_event_type = 'Activation'
          AND event_start_local_datetime >= $update_from_date) base
    ON base.customer_id = fme.customer_id
WHERE fme.membership_event_type = 'Activation' and not fme.is_deleted;

CREATE OR REPLACE TEMP TABLE _tmp_vips AS
SELECT
    fa.store_id,
    CAST(fa.event_start_local_datetime AS DATE) AS date,
    COUNT(1) AS vips_from_invites
FROM _tmp_vips_first_activation fa
WHERE 1=1
    AND fa.act_first = 1
    AND fa.event_start_local_datetime >= $update_from_date
    AND EXISTS
        (
            SELECT 1
            FROM lake_consolidated_view.ultra_merchant.membership m
            JOIN lake_consolidated_view.ultra_merchant.membership_invitation mi ON m.membership_id = mi.invitee_membership_id
            WHERE m.customer_id = fa.customer_id
        )
GROUP BY
    fa.store_id,
    CAST(fa.event_start_local_datetime AS DATE);

CREATE OR REPLACE TEMP TABLE _tmp_leads AS
SELECT
    fme.store_id,
    CAST(fme.event_start_local_datetime AS DATE) AS date,
    COUNT(1) AS leads_from_invites
FROM edw_prod.stg.fact_membership_event fme
WHERE fme.membership_event_type = 'Registration'
    AND fme.event_start_local_datetime >= $update_from_date
    AND EXISTS
        (
            SELECT 1
            FROM lake_consolidated_view.ultra_merchant.membership m
            JOIN lake_consolidated_view.ultra_merchant.membership_invitation mi ON m.membership_id = mi.invitee_membership_id
            WHERE m.customer_id = fme.customer_id
        )
GROUP BY
    fme.store_id,
    CAST(fme.event_start_local_datetime AS DATE);


CREATE OR REPLACE TEMP TABLE _tmp_output AS
SELECT
    COALESCE(ti.store_id, tl.store_id, tv.store_id) AS store_id,
    COALESCE(ti.date, tl.date, tv.date) AS date,
    COALESCE(ti.customers_inviting, 0) AS customers_inviting,
    COALESCE(ti.invites_sent,0) AS invites_sent,
    COALESCE(tl.Leads_from_invites,0) AS leads_from_invites,
    COALESCE(tv.vips_from_invites,0) AS vips_from_invites
FROM _tmp_invites ti
FULL JOIN _tmp_leads tl ON tl.date = ti.date
    AND tl.store_id = ti.store_id
FULL JOIN _tmp_vips tv ON tv.date = ti.date
    AND tv.store_id = ti.store_id;

DELETE FROM reporting_prod.membership.friend_referrals_dataset
WHERE date >= $update_from_date;

INSERT INTO reporting_prod.membership.friend_referrals_dataset
SELECT
    IFF(s.store_name LIKE '%Fabletics%', 'Fabletics', 'Fast Fashion') AS division,
    s.store_region AS region,
    s.store_brand AS business_unit,
    s.store_country AS country,
    s.store_brand || ' ' || s.store_country AS store,
    o.store_id AS store_id,
    o.date AS date,
    SUM(o.customers_inviting) AS customers_inviting,
    SUM(o.invites_sent) AS invites_sent,
    SUM(o.leads_from_invites) AS leads_from_invites,
    SUM(o.VIPs_from_invites) AS vips_from_invites
FROM _tmp_output o
JOIN edw_prod.data_model.dim_store s ON o.store_id = s.store_id
GROUP BY
    IFF(s.store_name LIKE '%Fabletics%', 'Fabletics', 'Fast Fashion'),
    s.store_region,
    s.store_brand,
    s.store_brand || ' ' || s.store_country,
    s.store_country,
    o.store_id,
    o.date;
