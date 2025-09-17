/*
Run Snowflake Procs through a DAG task.
*/

/* SET Default Roles for Users without a Default Role */
CALL UTIL.PUBLIC.SET_DEFAULT_ROLES();

/* SET Default Warehouses for Users without a Default Warehouse */
CALL UTIL.PUBLIC.SET_DEFAULT_WAREHOUSES();
