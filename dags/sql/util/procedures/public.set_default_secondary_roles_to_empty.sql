CREATE OR REPLACE PROCEDURE util.public.set_default_secondary_roles_to_empty()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
DECLARE sql_command STRING;
    cur CURSOR FOR
        select distinct NAME from SNOWFLAKE.ACCOUNT_USAGE.USERS where NAME not in (select name from util.public.secondary_roles_exception_users) and deleted_on is null and disabled = FALSE;

BEGIN
    FOR user_name IN cur DO
        -- Create the GRANT statement
        sql_command := 'alter user  "' || user_name.name || '" set default_secondary_roles = () ';
        EXECUTE IMMEDIATE sql_command;

    END FOR;

    RETURN 'Completed altering  all users to default_secondary_roles to empty.';
END;
