from __future__ import annotations

from typing import Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.exceptions import AirflowSkipException

SNOWFLAKE_CONN_ID = "snowflake_default"

SOURCE_TARGET_MAPPING = [
    {
        "source_db": "QM27872_TECHSTYLE_LAKE_JFB_SHARE",
        "source_schema": "ULTRA_MERCHANT",
        "target_db": "LAKE_JFB",
        "target_schema": "ULTRA_MERCHANT",
    },
]

SOURCE_TABLE_LIST = [
    "membership_token",
    "membership_plan",
    "store_group",
    "customer_detail",
    "membership_detail",
    "membership_level_group_modification_log",
    "membership",
]

with DAG(
    dag_id="copy_share_tables",
    start_date=timezone.datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def build_confs(
        mapping: List[Dict],
        allow_list: List[str],
        snowflake_conn_id: str = SNOWFLAKE_CONN_ID,
    ) -> List[Dict]:
        """List existing target base tables and intersect with allow_list."""
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            allow_lower = {t.lower() for t in allow_list}
            confs: List[Dict] = []

            for st in mapping:
                tgt_db = st["target_db"]
                tgt_schema = st["target_schema"]

                cur.execute(
                    f"""
                    SELECT TABLE_NAME
                    FROM {tgt_db}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_TYPE = 'BASE TABLE'
                    """,
                    (tgt_schema,),
                )
                targets = [r[0] for r in cur.fetchall()]
                selected = [t for t in targets if t.lower() in allow_lower]

                for table in selected:
                    confs.append({**st, "table_name": table})

            return confs
        finally:
            cur.close()
            conn.close()

    @task(map_index_template="{{ task.op_kwargs['table_cfg']['table_name'] }}")
    def process_one(
        table_cfg: Dict, snowflake_conn_id: str = SNOWFLAKE_CONN_ID
    ) -> Dict:
        import logging

        src_db = table_cfg["source_db"]
        src_schema = table_cfg["source_schema"]
        tgt_db = table_cfg["target_db"]
        tgt_schema = table_cfg["target_schema"]
        table = table_cfg["table_name"]
        full = f"{tgt_db}.{tgt_schema}.{table}"

        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            logging.info(f"Processing table: {full}")

            # Ensure source table exists; skip task if not
            cur.execute(
                f"""
                SELECT 1
                FROM {src_db}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME   = %s
                """,
                (src_schema, table),
            )
            if not cur.fetchone():
                msg = f"Source missing: {src_db}.{src_schema}.{table}"
                logging.warning(msg)
                raise AirflowSkipException(msg)

            cur.execute("USE WAREHOUSE MEDIUM_WH")

            cur.execute(f'TRUNCATE TABLE "{tgt_db}"."{tgt_schema}"."{table}"')
            cur.execute(
                f"""
                INSERT INTO "{tgt_db}"."{tgt_schema}"."{table}"
                SELECT * FROM "{src_db}"."{src_schema}"."{table}"
                """
            )

            rows = cur.rowcount if isinstance(cur.rowcount, int) else 0
            logging.info("✓ %s loaded | rows inserted: %s", full, rows)

            return {"table": full, "status": "success", "rows": rows}

        finally:
            cur.close()
            conn.close()

    confs = build_confs(SOURCE_TARGET_MAPPING, SOURCE_TABLE_LIST)
    process_one.expand(table_cfg=confs)
