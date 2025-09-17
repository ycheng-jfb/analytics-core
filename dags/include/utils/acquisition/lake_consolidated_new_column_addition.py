from include.airflow.hooks.snowflake import SnowflakeHook

column_list = [
    # 'ULTRA_MERCHANT.CREDITCARD_UPDATE_BATCH.DATETIME_MODIFIED',
    # 'ULTRA_MERCHANT.CREDITCARD_UPDATE_BATCH.REQUEST_FILE_CONTENTS',
    # 'ULTRA_MERCHANT.CREDITCARD_UPDATE_BATCH.RESPONSE_FILE_CONTENTS',
]

if len(column_list) == 0:
    print("Add the new columns details to column_list")
    exit(0)

table_dict = {}
for table_name in column_list:
    schema, table, column_name = table_name.split(".")
    if f"LAKE_CONSOLIDATED.{schema}.{table}" not in table_dict:
        table_dict[f"LAKE_CONSOLIDATED.{schema}.{table}"] = [column_name]
    else:
        table_dict[f"LAKE_CONSOLIDATED.{schema}.{table}"].append(column_name)

table_name_list = table_dict.keys()
lake_brand_db = "lake_fl"
snowflake_hook = SnowflakeHook(snowflake_conn_id="SNOWFLAKE_DEFAULT")
with snowflake_hook.get_conn() as conn:
    cur = conn.cursor()

    ## generate ALTER TABLE scripts
    output_file = "alter_table_lake_consolidated_scripts.sql"
    with open(output_file, "w") as file:
        file.write("-- Auto Generated ALTER TABLE ADD COLUMN scripts\n\n")
        for table_name in table_dict.keys():
            database, schema, table = table_name.split(".")
            query = f"select get_ddl('table', '{lake_brand_db}.{schema}.{table}');"

            cur.execute(query)
            result = cur.fetchone()[0]
            alter_query = (
                f"ALTER TABLE lake_consolidated.{schema}.{table} \n\tADD COLUMN"
            )
            table_config = ""
            for column_name in table_dict[table_name]:
                # generate alter table scripts
                column_name_start_loc = result.find(column_name)
                column_name_end_loc = result.find("\n\t", column_name_start_loc)
                column = result[
                    column_name_start_loc : column_name_end_loc - 1
                ].replace("\n", "")
                column_name = column[: column.find(" ")]
                column_type = column[column.find(" ") + 1 :]
                table_config += (
                    f"Column('{column_name.lower()}', '{column_type}'),\n\t\t"
                )
                alter_query += f" {column},"
            alter_query = alter_query[:-1] + ";"
            file.write(alter_query + "\n\n")

            # modify the lake_consolidated_table_config
            with open(
                f"../../../edm/acquisition/configs/lake_consolidated/{schema}/{table}.py"
            ) as fp:
                content = fp.read()
            insert_loc = content.find("],", content.find("column_list=["))
            content = content[:insert_loc] + table_config + content[insert_loc:]

            with open(
                f"../../../edm/acquisition/configs/lake_consolidated/{schema}/{table}.py",
                "w",
            ) as fp:
                fp.write(content)

    # generate lake_consolidated_view scripts
    output_file = "lake_consolidated_view_scripts.sql"
    with open(output_file, "w") as file:
        file.write("-- Auto Generated lake_consolidated_view scripts\n\n")
        for table in table_dict.keys():
            database, schema, table_name = table.split(".")
            query = f"select get_ddl('view', 'LAKE_CONSOLIDATED_VIEW.{schema}.{table_name}');"
            try:
                cur.execute(query)
            except Exception as e:
                print("### " + str(e))
                continue
            result = cur.fetchone()[0]
            column_name_start_loc = result.find("(")
            column_name_end_loc = result.find(") as")
            columns = result[column_name_start_loc + 1 : column_name_end_loc]
            hvr_is_deleted_start_loc = columns.find("HVR_IS_DELETED")
            from_start_loc = result.find("FROM ")
            new_cols = ""
            for col in table_dict[table]:
                new_cols = new_cols + col + ",\n\t"
            script_sql = f"CREATE OR REPLACE VIEW LAKE_CONSOLIDATED_VIEW.{schema}.{table_name}({columns[0:hvr_is_deleted_start_loc]}"
            script_sql = script_sql + new_cols

            script_sql = script_sql + columns[hvr_is_deleted_start_loc:] + ") AS"

            hvr_is_deleted_select_loc = result.find(
                "HVR_IS_DELETED", result.find("\nSELECT")
            )
            if hvr_is_deleted_select_loc == -1:
                hvr_is_deleted_select_loc = result.find(
                    "hvr_is_deleted", result.find("\nSELECT")
                )

            select_columns = result[
                result.find("\nSELECT") + len("\nSELECT") : hvr_is_deleted_select_loc
            ]

            select_script_sql = "\nSELECT "
            select_script_sql = select_script_sql + select_columns
            new_cols = ""
            for col in table_dict[table]:
                new_cols = new_cols + col + f" AS {col.lower()},\n\t"
            select_script_sql = select_script_sql + new_cols
            select_script_sql = select_script_sql + result[hvr_is_deleted_select_loc:]
            script_sql = script_sql + select_script_sql
            file.write(script_sql + "\n\n")

# Generate backfill scripts for newly adding columns
output_file = "lake_consolidated_backfill_scripts.sql"
final_script = "---- Auto Generated Backfill scripts------\n\n"
table_config = None
for table in table_dict.keys():
    database, schema, table_name = table.split(".")
    with open(
        f"../../../edm/acquisition/configs/{database}/{schema}/{table_name}.py", "r"
    ) as file:
        code = file.read()
        exec(code)
    id_column = []
    key_columns = []
    for col in table_config.column_list:
        if col.uniqueness and col.name.lower() == f"{table_name}_id".lower():
            id_column = col.name
        if col.key:
            key_columns.append(col.name)

    columns = table_dict[
        f"lake_consolidated.ultra_merchant.{table_config.table}".upper()
    ]
    set_string = ""
    where_string = ""
    col_list = ""
    for col in columns:
        set_string += f"t.{col}=s.{col},\n\t\t"
        where_string += f"{col.lower()} IS NOT NULL or "
        col_list += f"{col}, "
    set_string = set_string.removesuffix(",\n\t\t").lower()
    where_string = where_string.removesuffix(" or ")
    col_list = col_list.removesuffix(", ").lower()
    update_based_on_key = ""
    if schema == "ultra_merchant_history":
        for col in key_columns:
            update_based_on_key += f"s.{col} = t.{col} and "
        update_based_on_key = update_based_on_key[:-4]
    else:
        update_based_on_key = f"s.{id_column} = t.{id_column} "

    back_fill_script = f"""UPDATE {database}.{schema}.{table_config.table} AS t
    SET {set_string}
    FROM (SELECT concat(CAST({id_column} as VARCHAR), '20') as {id_column}, {col_list}
          FROM  lake_fl.ultra_merchant.{table_config.table}
          WHERE {where_string}
          UNION ALL
          SELECT concat(CAST({id_column} as VARCHAR), '10') as {id_column}, {col_list}
          FROM  lake_jfb.ultra_merchant.{table_config.table}
          WHERE {where_string}
          UNION ALL
          SELECT concat(CAST({id_column} as VARCHAR), '30') as {id_column}, {col_list}
          FROM  lake_sxf.ultra_merchant.{table_config.table}
          WHERE {where_string}) AS s
    WHERE {update_based_on_key};"""

    final_script += back_fill_script + "\n\n"

with open(output_file, "w") as file:
    file.write(final_script + "\n\n")
