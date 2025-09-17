/*
Tied to DA-28668
There was a source system bug that did not capture EU credit cancellations that occurred the same day as the credit's issuance.
These credits were refunded as a chargeback from the bank but were still listed as "active" in source and did not have a cancellation date.
On 10/17/2023, there was an upstream "fix" that cancelled these credits which caused them to all have a cancellation timestamp of 10/17/23.
This resulted in a significant spike in Finance Reporting for Oct-2023 credit cancellations.
This reference table stores a list of impacted credits/tokens and we will use the original_issuance_local_datetime as the new cancellation date.
There are 500+ cancelled credits that were incorrectly converted to token because they weren't listed as cancelled in our source system.
If we used the original issued date for these wrongfully converted tokens, the cancellation date will be before the issued date so we want
the cancellation to be tied to the original credit (reason for bottom half of UNION statement), and ignore incorrect token conversions (reason for is_ignore)
 */
CREATE OR REPLACE TABLE reference.credit_cancellation_datetime AS
select
    dc.credit_id,
    dc.meta_original_credit_id,
    IFF(aic.membership_token_id IS NOT NULL, 'Token', 'store_credit_id') AS source_credit_id_type,
    aic.store_id,
    dc.original_credit_issued_local_datetime::TIMESTAMP_NTZ AS new_credit_cancellation_local_datetime,
    dc.credit_issuance_reason,
    dc2.credit_id AS original_credit_id,
    dc2.source_credit_id_type AS original_source_credit_id_type,
    aic.amount::NUMBER(38,10) AS amount,
    aic.balance::NUMBER(38,10) AS balance,
    iff(dc.original_credit_match_reason = 'Already Original', TRUE, FALSE) AS is_original_credit,
    IFF(dc.credit_issuance_reason = 'Credit To Token Conversion' AND dc.original_credit_issued_local_datetime < dc.credit_issued_local_datetime, TRUE, FALSE) AS is_ignore
FROM work.dbo.ayden_impacted_credit AS aic
    JOIN stg.dim_credit AS dc
        ON dc.meta_original_credit_id = COALESCE(aic.store_credit_id, aic.membership_token_id)
        AND dc.source_credit_id_type = IFF(aic.membership_token_id IS NOT NULL, 'Token', 'store_credit_id')
    LEFT JOIN stg.dim_credit AS dc2
        ON dc2.credit_key = dc.original_credit_key

UNION ALL

SELECT
    dc2.credit_id,
    dc2.meta_original_credit_id,
    dc2.source_credit_id_type,
    dc2.store_id,
    dc2.original_credit_issued_local_datetime::TIMESTAMP_NTZ AS new_credit_cancellation_local_datetime,
    dc2.credit_issuance_reason,
    dc2.credit_id AS original_credit_id,
    dc2.source_credit_id_type AS original_source_credit_id_type,
    aic.amount::NUMBER(38,10) AS amount,
    aic.balance::NUMBER(38,10) AS balance,
    IFF(dc2.original_credit_match_reason = 'Already Original', TRUE, FALSE) AS is_original_credit,
    FALSE AS is_ignore
FROM work.dbo.ayden_impacted_credit AS aic
    JOIN stg.dim_credit AS dc
        ON dc.meta_original_credit_id = COALESCE(aic.store_credit_id, aic.membership_token_id)
        AND dc.source_credit_id_type = IFF(aic.membership_token_id IS NOT NULL, 'Token', 'store_credit_id')
    JOIN stg.dim_credit AS dc2
    ON dc2.credit_key = dc.original_credit_key
WHERE dc.original_credit_match_reason <> 'Already Original';
