from airflow.hooks.mysql_hook import MySqlHook


def rename_old_dag(metastore_conn_id, old_dag_id, new_dag_id):
    hook = MySqlHook(mysql_conn_id=metastore_conn_id)

    tables = hook.get_records(
        sql="""
    SELECT
        c.table_name
    FROM information_schema.columns c
    WHERE
        c.column_name = 'dag_id'
        AND c.table_name <> 'dag'
    """
    )

    sql_batch = "; ".join(
        [f"UPDATE {x[0]} SET dag_id = %(new)s WHERE dag_id = %(old)s" for x in tables]
    )
    hook.run(sql_batch, parameters={"old": old_dag_id, "new": new_dag_id})


def delete_old_dag(metastore_conn_id, dag_id, database="airflow"):
    hook = MySqlHook(mysql_conn_id=metastore_conn_id)
    tables = hook.get_records(
        sql=f"""
    SELECT
        c.table_name
    FROM information_schema.columns c
    WHERE
        c.column_name = 'dag_id'
        and c.table_schema = '{database}'
    """
    )
    sql_batch = "; ".join([f"DELETE FROM {x[0]} WHERE dag_id = %(dag_id)s" for x in tables])
    hook.run(sql_batch, parameters={"dag_id": dag_id})


def deactivate_dag(metastore_conn_id, dag_id):
    hook = MySqlHook(mysql_conn_id=metastore_conn_id)
    cmd = "UPDATE dag SET is_active = 0 WHERE dag_id = %(dag_id)s"
    hook.run(cmd, parameters={"dag_id": dag_id})
