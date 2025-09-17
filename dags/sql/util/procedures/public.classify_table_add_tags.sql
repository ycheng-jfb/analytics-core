USE DATABASE UTIL;
USE SCHEMA PUBLIC;

CREATE OR REPLACE PROCEDURE UTIL.PUBLIC.CLASSIFY_TABLE_ADD_TAGS("DB_LIST" ARRAY, "INITIAL_RUN" BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'run'
EXECUTE AS CALLER
AS '
import json
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark import functions as F
from snowflake.snowpark.exceptions import SnowparkSQLException

def run(session: snowpark.Session, db_list: list, initial_run: bool):
    tag_name = ''PII_INFO_TAG''
    tag_value = ''PII''

    session.query_tag = ''classify_procedure''

    session.sql("""
        CREATE TABLE IF NOT EXISTS UTIL.PUBLIC.CLASSIFIED_TABLES (
            TABLE_TYPE      VARCHAR(255),
            TABLE_CATALOG   VARCHAR(255),
            TABLE_SCHEMA    VARCHAR(255),
            TABLE_NAME      VARCHAR(255),
            LAST_DDL        TIMESTAMP_LTZ(3)
    )""").collect()

    session.sql("""
        CREATE TABLE IF NOT EXISTS UTIL.PUBLIC.PII_COLUMNS_EXCLUSION (
            table_type      STRING,
            table_catalog   STRING,
            table_schema    STRING,
            table_name      STRING,
            column_name     STRING,
            reason          STRING,
            PRIMARY KEY (table_catalog, table_schema, table_name, column_name)
    )""").collect()

    session.sql("""
        CREATE TABLE IF NOT EXISTS UTIL.PUBLIC.PII_COLUMNS (
        TABLE_NAME VARCHAR,
        COLUMN_NAME VARCHAR,
        IS_MASKING_REQUIRED BOOLEAN,
        POLICY_NAME VARCHAR,
        DATETIME_ADDED TIMESTAMP_NTZ(9),
        COMMENTS VARCHAR
    )""").collect()

    session.sql("""
        CREATE TABLE IF NOT EXISTS UTIL.PUBLIC.CATEGORY_LOG (
            table_type          STRING,
            table_catalog       STRING,
            table_schema        STRING,
            table_name          STRING,
            column_name         STRING,
            privacy_category    STRING,
            semantic_category   STRING
    )""").collect()

    classified_table = session.table("UTIL.PUBLIC.CLASSIFIED_TABLES")

    # get all tables from specific schema
    load_all_table_sql = " union ".join(
        [f"""SELECT
            CASE
            WHEN IS_TRANSIENT = ''YES'' THEN ''TRANSIENT TABLE''
            ELSE ''TABLE''
            END AS TABLE_TYPE ,
        TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, LAST_DDL
        FROM {db_name}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE=''BASE TABLE''
                    AND IS_TEMPORARY = ''NO''
                    AND ROW_COUNT>25
                    AND TABLE_SCHEMA != ''INFORMATION_SCHEMA''
            """ for db_name in db_list])

    load_all_table = session.sql(load_all_table_sql)

    # exclude processed tables on previous run
    newly_changed_tables = load_all_table.except_(classified_table)

    if not initial_run:
        newly_changed_tables = newly_changed_tables.limit(30)
    newly_chaged_tables_df = newly_changed_tables.collect()

    columns_to_tag = []
    considered_tables = []

    # run classifier on each table
    for data in newly_chaged_tables_df:
        table_type = data[''TABLE_TYPE'']
        database = data[''TABLE_CATALOG'']
        schema = data[''TABLE_SCHEMA'']
        table = data[''TABLE_NAME'']
        absolute_table_name = f"{database}.{schema}.{table}"

        classify_table = f"""CALL SYSTEM$CLASSIFY(''{absolute_table_name}'', null)"""
        session.query_tag = absolute_table_name

        try:
            result = session.sql(classify_table).select(''SYSTEM$CLASSIFY'').collect()
            classification_result = result[0][0]
            columns = json.loads(classification_result)[''classification_result'']
            has_recommendation = False
            for column, value in columns.items():

                if recommendation := value.get("recommendation"):
                    has_recommendation = True
                    exclude_semantic_category_list = ["URL","GENDER","DATE_OF_BIRTH","AGE","YEAR_OF_BIRTH","administrative_area","POSTAL_CODE","CITY","COUNTRY"]
                    if recommendation.get(''semantic_category'') in exclude_semantic_category_list:
                        continue
                    columns_to_tag.append([table_type, database, schema, table, column,
                                           recommendation.get(''privacy_category''),
                                           recommendation.get(''semantic_category'')])
            if has_recommendation:
                considered_tables.append(data)
        except SnowparkSQLException as e:
            print(getattr(e, ''message'', repr(e)))
        except Exception as e:
            print(getattr(e, ''message'', repr(e)))

    session.query_tag = ''classify_procedure''
    considered_tables_schema = [''table_type'', ''table_catalog'', ''table_schema'', ''table_name'', ''last_ddl'']
    columns_to_tag_schema = [''table_type'', ''table_catalog'', ''table_schema'', ''table_name'', ''column_name'', ''privacy_category'', ''semantic_category'']
    if not (columns_to_tag and considered_tables):
        return "No Columns to TAG Success"
    considered_tables_df = session.create_dataframe(considered_tables, schema=considered_tables_schema)
    columns_to_tag_df = session.create_dataframe(columns_to_tag, schema=columns_to_tag_schema)

    columns_to_tag_df.write.mode("append").save_as_table("UTIL.PUBLIC.CATEGORY_LOG")

    columns_to_tag_df = columns_to_tag_df.select(''table_type'', ''table_catalog'', ''table_schema'', ''table_name'', ''column_name'')

    pii_columns_exclusion_list = session.table(''UTIL.PUBLIC.PII_COLUMNS_EXCLUSION'') \\
        .select(''table_type'', ''table_catalog'', ''table_schema'', ''table_name'', ''column_name'')
    columns_to_tag_except_excluded_columns = columns_to_tag_df.except_(pii_columns_exclusion_list)
    columns_to_be_tag = columns_to_tag_except_excluded_columns.with_column(''current_date'', F.current_date())

    pii_columns = session.table("UTIL.PUBLIC.PII_COLUMNS")
    pii_columns.merge(
        columns_to_be_tag,
        (pii_columns["TABLE_NAME"] == columns_to_be_tag["TABLE_NAME"]) &
        (pii_columns["COLUMN_NAME"] == columns_to_be_tag["COLUMN_NAME"]),
        [F.when_not_matched().insert({
                    "TABLE_NAME": F.concat(columns_to_be_tag["TABLE_CATALOG"], F.lit("."),
                                           columns_to_be_tag["TABLE_SCHEMA"], F.lit("."),
                                           columns_to_be_tag["TABLE_NAME"]),
                    "COLUMN_NAME": columns_to_be_tag["COLUMN_NAME"],
                    "IS_MASKING_REQUIRED": F.lit(True),
                    "POLICY_NAME": F.lit(None),
                    "DATETIME_ADDED": F.current_date(),
                    "COMMENTS": "ADDED BY procedure UTIL.PUBLIC.CAPTURE_NEW_TABLE_FOR_CLASSIFY "
        })])

    transformed_latest_table = session.sql(load_all_table_sql)

    latest_ddl_data = \\
        transformed_latest_table \\
            .join(newly_changed_tables,
                    on= (transformed_latest_table.TABLE_TYPE == newly_changed_tables.TABLE_TYPE) &
                        (transformed_latest_table.TABLE_CATALOG == newly_changed_tables.TABLE_CATALOG) &
                        (transformed_latest_table.TABLE_SCHEMA == newly_changed_tables.TABLE_SCHEMA) &
                        (transformed_latest_table.TABLE_NAME == newly_changed_tables.TABLE_NAME),
                    how=''inner'').select(transformed_latest_table[''TABLE_TYPE''].alias(''TABLE_TYPE''),
                                        transformed_latest_table[''TABLE_CATALOG''].alias(''TABLE_CATALOG''),
                                            transformed_latest_table[''TABLE_SCHEMA''].alias(''TABLE_SCHEMA''),
                                            transformed_latest_table[''TABLE_NAME''].alias(''TABLE_NAME''),
                                            transformed_latest_table[''LAST_DDL''].alias(''LAST_DDL''))

    classified_table.merge(
    latest_ddl_data,
    (classified_table["TABLE_CATALOG"] == latest_ddl_data["TABLE_CATALOG"]) &
    (classified_table["TABLE_SCHEMA"] == latest_ddl_data["TABLE_SCHEMA"]) &
    (classified_table["TABLE_NAME"] == latest_ddl_data["TABLE_NAME"])
    ,[F.when_matched().update({"LAST_DDL": latest_ddl_data["LAST_DDL"]}),
      F.when_not_matched().insert({
                "TABLE_TYPE": latest_ddl_data["TABLE_TYPE"],
                "TABLE_CATALOG": latest_ddl_data["TABLE_CATALOG"],
                "TABLE_SCHEMA": latest_ddl_data["TABLE_SCHEMA"],
                "TABLE_NAME": latest_ddl_data["TABLE_NAME"],
                "LAST_DDL": latest_ddl_data["LAST_DDL"]
            }) ])

    if not initial_run:
        df = columns_to_be_tag.to_pandas()

        # to=("psureshbhai@fabletics.com"),
        # subject="Recently added PII column tags",
        # body = f"We have applied tag {tag_name}:{tag_value} on below columns"
        # html_content = f"{body}<br><br>{df.to_html(index=False)}"
        # send_email_procedure = f"""CALL SYSTEM$SENDEMAIL("<INTEGRATION_NAME>", {to}, {subject}, {html_content}, "text/html")"""
        # session.sql(send_email_procedure).collect()

    return "SUCCESS"
';