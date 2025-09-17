SET process_to_date = DATE_TRUNC('month', CURRENT_DATE());

MERGE INTO month_end.loyalty_points_balance AS t
    USING (SELECT membership_reward_transaction_id                                    AS mrt_key,
                  lp.store_id,
                  lp.membership_id,
                  lp.membership_reward_plan_id,
                  lp.membership_reward_tier_id,
                  lp.membership_reward_tier,
                  lp.points,
                  lp.points                                                           AS fixed_points,
                  lp.membership_reward_transaction_type_id,
                  lp.datetime_added,
                  SUM(points) OVER (PARTITION BY membership_id ORDER BY mrt_key ASC)  AS raw_balance,
                  SUM(points) OVER (PARTITION BY membership_id ORDER BY mrt_key ASC)  AS fixed_balance,
                  ROW_NUMBER() OVER (PARTITION BY membership_id ORDER BY mrt_key ASC) AS rnk
           FROM month_end.loyalty_points lp
           WHERE date_added < $process_to_date) AS s
    ON s.mrt_key = t.mrt_key
    WHEN MATCHED AND (
                t.points <> s.points
            OR t.rnk <> s.rnk
        )
        THEN UPDATE
        SET t.store_id = s.store_id,
            t.membership_id = s.membership_id,
            t.membership_reward_plan_id = s.membership_reward_plan_id,
            t.membership_reward_tier_id = s.membership_reward_tier_id,
            t.membership_reward_tier = s.membership_reward_tier,
            t.points = s.points,
            t.fixed_points = s.fixed_points,
            t.membership_reward_transaction_type_id = s.membership_reward_transaction_type_id,
            t.datetime_added = s.datetime_added,
            t.raw_balance = s.raw_balance,
            t.fixed_balance = s.fixed_balance,
            t.rnk = s.rnk
    WHEN NOT MATCHED THEN
        INSERT (
                mrt_key,
                store_id,
                membership_id,
                membership_reward_plan_id,
                membership_reward_tier_id,
                membership_reward_tier,
                points,
                fixed_points,
                membership_reward_transaction_type_id,
                datetime_added,
                raw_balance,
                fixed_balance,
                rnk
            )
            VALUES (s.mrt_key,
                    s.store_id,
                    s.membership_id,
                    s.membership_reward_plan_id,
                    s.membership_reward_tier_id,
                    s.membership_reward_tier,
                    s.points,
                    s.fixed_points,
                    s.membership_reward_transaction_type_id,
                    s.datetime_added,
                    s.raw_balance,
                    s.fixed_balance,
                    s.rnk);

UPDATE month_end.loyalty_points_balance AS b
SET fixed_balance = a.new_balance
FROM (SELECT mrt_key,
             SUM(fixed_points) OVER (PARTITION BY membership_id ORDER BY mrt_key ASC) AS new_balance
      FROM month_end.loyalty_points_balance
      WHERE membership_id IN (SELECT DISTINCT membership_id
                              FROM month_end.loyalty_points_balance
                              WHERE points <> fixed_points)) AS a
WHERE a.mrt_key = b.mrt_key
  AND a.new_balance <> b.fixed_balance;

/*
Fix rolling balances that would be negative, by setting to zero instead

If recursive CTE fails, run this instead:
CALL accounting.month_end.loyalty_point_fixer_consolidated();
*/
CREATE OR REPLACE TEMP TABLE _LOYALTY_POINT_FIXER_DELTA AS (
WITH RECURSIVE _delta AS (
    SELECT DISTINCT membership_id
    FROM month_end.loyalty_points_balance
    WHERE fixed_balance < 0
),
_raw AS (
    SELECT
        row_number() OVER(PARTITION BY membership_id ORDER BY mrt_key ASC) AS rnk,
        mrt_key,
        membership_id,
        points
    FROM month_end.loyalty_points_balance
    WHERE membership_id IN (
        SELECT membership_id
        FROM _delta
        )
    ),
_rolling_points AS (
SELECT DISTINCT
    0 AS RNK,
    A.MEMBERSHIP_ID,
    0 AS mrt_key,
    0 AS POINTS,
    0 AS FIXED_POINTS,
    0 AS FIXED_BALANCE
FROM month_end.loyalty_points_balance AS A
WHERE membership_id IN (
        SELECT membership_id
        FROM _delta
        )

UNION ALL
SELECT
    A.RNK,
    A.MEMBERSHIP_ID,
    A.mrt_key,
    A.POINTS,
    CASE WHEN A.POINTS + B.FIXED_BALANCE < 0 THEN (0 - B.FIXED_BALANCE)
        ELSE A.POINTS END AS FIXED_POINTS,
    CASE WHEN A.POINTS + B.FIXED_BALANCE < 0 THEN 0
        ELSE A.POINTS + B.FIXED_BALANCE END AS FIXED_BALANCE
FROM _raw AS A
    INNER JOIN _rolling_points AS B
ON A.membership_id = B.membership_id
    AND A.RNK = B.RNK + 1
)

SELECT mrt_key, fixed_points, fixed_balance
FROM _rolling_points
WHERE rnk > 0
);

UPDATE month_end.loyalty_points_balance AS T
SET
    T.fixed_points = S.fixed_points,
    T.fixed_balance = S.fixed_balance
FROM _LOYALTY_POINT_FIXER_DELTA AS S
WHERE S.mrt_key = T.mrt_key
    AND (
        S.fixed_points <> T.fixed_points
        OR S.fixed_balance <> T.fixed_balance
        )
;
