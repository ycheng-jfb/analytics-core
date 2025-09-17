CREATE OR REPLACE PROCEDURE REPORTING_PROD.CREDIT.LOYALTY_POINT_FIXER_CONSOLIDATED()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '


var LOOP_COMMAND = ``;
var SQL_QUERY = ``;
var SQL_STMT = ``;
var FIX_LOOP = 0;

LOOP_COMMAND = `
      SELECT COALESCE(MIN(RNK), -1) as RNK
      FROM reporting_prod.credit.loyalty_points_balance
      WHERE fixed_balance < 0
    ;`;
   LOOP_COMMAND = LOOP_COMMAND.replace(''_FIX_LOOP_'', FIX_LOOP)
   SQL_QUERY = {sqlText: LOOP_COMMAND};
   SQL_STMT = snowflake.createStatement(SQL_QUERY);
   FIX_LOOP = SQL_STMT.execute();
   FIX_LOOP.next()
   FIX_LOOP = FIX_LOOP.getColumnValue(1);

while (FIX_LOOP != -1) {

    LOOP_COMMAND = `
        UPDATE reporting_prod.credit.loyalty_points_balance AS b
        SET fixed_points = a.fixed_points
        FROM (
            SELECT
                A.mrt_key,
                CASE WHEN A.fixed_balance < 0 AND A.points < 0 AND B.fixed_balance >= 0
                    THEN A.points - A.fixed_balance
                    ELSE 0 END
                    AS fixed_points
            FROM reporting_prod.credit.loyalty_points_balance AS A
                LEFT JOIN reporting_prod.credit.loyalty_points_balance AS B
                ON A.membership_id = B.membership_id
                AND A.rnk = B.rnk + 1
            WHERE (A.fixed_balance < 0 AND A.points < 0)
                AND A.rnk = _FIX_LOOP_
        ) AS A
        WHERE A.mrt_key = B.mrt_key
        ;`;
   LOOP_COMMAND = LOOP_COMMAND.replace(''_FIX_LOOP_'', FIX_LOOP)
   SQL_QUERY = {sqlText: LOOP_COMMAND};
   SQL_STMT = snowflake.createStatement(SQL_QUERY);
   SQL_STMT.execute();

   LOOP_COMMAND = `
        UPDATE reporting_prod.credit.loyalty_points_balance AS B
        SET fixed_balance = A.new_balance
        FROM (
            SELECT
                mrt_key,
                sum(fixed_points) over(partition by membership_id order by mrt_key ASC) as new_balance
            FROM reporting_prod.credit.loyalty_points_balance
            WHERE membership_id IN (
                SELECT DISTINCT membership_id
                FROM reporting_prod.credit.loyalty_points_balance
                WHERE rnk = _FIX_LOOP_
                    AND points <> fixed_points
                )
            ) AS A
        WHERE A.mrt_key = B.mrt_key
            AND A.new_balance <> B.fixed_balance
            AND B.rnk >= _FIX_LOOP_
        ;`;
   LOOP_COMMAND = LOOP_COMMAND.replace(''_FIX_LOOP_'', FIX_LOOP)
   LOOP_COMMAND = LOOP_COMMAND.replace(''_FIX_LOOP_'', FIX_LOOP)
   SQL_QUERY = {sqlText: LOOP_COMMAND};
   SQL_STMT = snowflake.createStatement(SQL_QUERY);
   SQL_STMT.execute();

   LOOP_COMMAND = `
      SELECT COALESCE(MIN(RNK), -1) as RNK
      FROM reporting_prod.credit.loyalty_points_balance
      WHERE fixed_balance < 0
    ;`;
   LOOP_COMMAND = LOOP_COMMAND.replace(''_FIX_LOOP_'', FIX_LOOP)
   SQL_QUERY = {sqlText: LOOP_COMMAND};
   SQL_STMT = snowflake.createStatement(SQL_QUERY);
   FIX_LOOP = SQL_STMT.execute();
   FIX_LOOP.next()
   FIX_LOOP = FIX_LOOP.getColumnValue(1);

   }
   return FIX_LOOP

';
