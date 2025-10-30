import os
import re
from include.airflow.hooks.snowflake import SnowflakeHook


def write_to_file(file_name, str):
    with open(file_name, "w") as file:
        file.write("---------Auto Generated Scripts-----------\n")
        file.write(str)


def get_table_list(cur, db, schema):
    sql = f"SELECT table_name FROM {db}.information_schema.tables WHERE table_schema = '{schema}' and is_transient = 'NO';"
    result = cur.execute(sql)
    return result.fetchall()


def convert_tables_to_transient(cur, config):
    for lst in config:
        db = lst["db_name"]
        schema = lst["schema"]
        if "tables" in lst.keys():
            tables = lst["tables"]
            sql_convert_to_transient = ""
            for t in tables:
                fq_table_name = f"{db}.{schema[0]}.{t}".lower()
                sql_formatted = sql_template_transient.format(table_name=fq_table_name)
                sql_convert_to_transient += sql_formatted + "\n\n"

            write_to_file(
                f"convert_to_transient_{db}_{schema[0]}.txt".lower(), sql_convert_to_transient
            )
            print(f"convert_to_transient_{db}_{schema[0]}.txt".lower())
        else:
            for sc in schema:
                sql_convert_to_transient = ""
                tables = get_table_list(cur, db, sc)
                for t in tables:
                    if sc == "STG" and t[0][-4:].lower() != '_stg':
                        continue
                    fq_table_name = f"{db}.{sc}.{t[0]}".lower()
                    sql_formatted = sql_template_transient.format(table_name=fq_table_name)
                    sql_convert_to_transient += sql_formatted + "\n\n"
                write_to_file(
                    f"convert_to_transient_{db}_{sc}.txt".lower(), sql_convert_to_transient
                )
                print(f"convert_to_transient_{db}_{sc}.txt".lower())


def modify_retention_days(cur, config):
    for lst in config:
        db = lst["db_name"]
        schema = lst["schema"]
        if "tables" in lst.keys():
            tables = lst["tables"]
            final_sql = ""
            for t in tables:
                fq_table_name = f"{db}.{schema[0]}.{t}".lower()
                sql_formatted = sql_template_retention.format(
                    table_name=fq_table_name, retention_days=lst["retention_days"]
                )
                final_sql += sql_formatted + "\n"

            write_to_file(f"modify_retention_days_{db}_{schema[0]}.txt".lower(), final_sql)
            print(f"modify_retention_days_{db}_{schema[0]}.txt".lower())
        else:
            for sc in schema:
                final_sql = ""
                tables = get_table_list(cur, db, sc)
                for t in tables:
                    if sc == "STG" and t[0][-4:].lower() == '_stg':
                        continue
                    fq_table_name = f"{db}.{sc}.{t[0]}".lower()
                    sql_formatted = sql_template_retention.format(
                        table_name=fq_table_name, retention_days=lst["days"]
                    )
                    final_sql += sql_formatted + "\n"
                write_to_file(f"modify_retention_days_{db}_{sc}.txt".lower(), final_sql)
                print(f"modify_retention_days_{db}_{sc}.txt".lower())


# if only few tables in a schema needs to considered, give them as separate dictionary.
transient_config = [
    {
        "db_name": "EDW_PROD",
        "schema": ["SNAPSHOT", "STG", "REPORTING"],
    },
    {
        "db_name": "EDW_PROD",
        "schema": ["REFERENCE"],
        "tables": ["FACT_ORDER_LINE_OPTIMIZED"],
    },
]

retention_config = [
    {
        "db_name": "EDW_PROD",
        "schema": ["REFERENCE"],
        "days": 4,
    },
    {
        "db_name": "EDW_PROD",
        "schema": ["STG"],
        "days": 3,
    },
]

sql_template_transient = """
CREATE TRANSIENT TABLE {table_name}_t LIKE {table_name};
INSERT INTO {table_name}_t SELECT * FROM  {table_name};
ALTER TABLE {table_name} RENAME TO {table_name}_old;
ALTER TABLE {table_name}_t RENAME TO {table_name};
DROP TABLE {table_name}_old;"""

sql_template_retention = (
    """ALTER TABLE {table_name} SET DATA_RETENTION_TIME_IN_DAYS = {retention_days};"""
)

snowflake_hook = SnowflakeHook(snowflake_conn_id="SNOWFLAKE_DEFAULT")
conn = snowflake_hook.get_conn()
cur = conn.cursor()

convert_tables_to_transient(cur, transient_config)
modify_retention_days(cur, retention_config)

cur.close()
