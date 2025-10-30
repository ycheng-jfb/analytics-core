# dags/snowflake_share_copy_factory.py
import json
import logging
from datetime import timedelta

import boto3
import pendulum
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import get_current_context

SNOWFLAKE_CONN_ID = "snowflake_default"
S3_BUCKET = "jfb.airflow.db"  # key = filename only (no prefix)

SOURCE_TARGET_MAPPING = [
    {"source_db": "QM27872_TECHSTYLE_LAKE_JFB_SHARE", "source_schema": "ULTRA_MERCHANT",         "target_db": "LAKE_JFB",      "target_schema": "ULTRA_MERCHANT"},
    {"source_db": "QM27872_TECHSTYLE_LAKE_JFB_SHARE", "source_schema": "ULTRA_MERCHANT_HISTORY", "target_db": "LAKE_JFB",      "target_schema": "ULTRA_MERCHANT_HISTORY"},
    {"source_db": "QM27872_TECHSTYLE_LAKE_JFB_SHARE", "source_schema": "ULTRA_CMS",              "target_db": "LAKE_JFB",      "target_schema": "ULTRA_CMS"},
    {"source_db": "QM27872_TECHSTYLE_LAKE_JFB_SHARE", "source_schema": "GDPR",                   "target_db": "LAKE_JFB",      "target_schema": "GDPR"},
    {"source_db": "QM27872_TECHSTYLE_LAKE_JFB_SHARE", "source_schema": "ULTRA_CART",             "target_db": "LAKE_JFB",      "target_schema": "ULTRA_CART"},
    {"source_db": "QM27872_TECHSTYLE_LAKE_JFB_SHARE", "source_schema": "ULTRA_ROLLUP",           "target_db": "LAKE_JFB",      "target_schema": "ULTRA_ROLLUP"},
    {"source_db": "QM27872_TECHSTYLE_LAKE_SEGMENT_GFB_SHARE", "source_schema": "SEGMENT_GFB",    "target_db": "LAKE",          "target_schema": "SEGMENT_GFB"},
    {"source_db": "QM27872_TECHSTYLE_REPORTING_PROD_GFB_SHARE","source_schema": "GFB",           "target_db": "REPORTING_PROD","target_schema": "GFB"},
    {"source_db": "QM27872_TECHSTYLE_JFB_SHARE_COMINGLED",     "source_schema": "MMM"},  # target derived from names
]

SPECIAL_TSOS = {
    "EDW_PROD.REFERENCE.CURRENCY_EXCHANGE_RATE_BY_DATE",
    "EDW_PROD.REFERENCE.RETURN_SHIPPING_COST",
    "EDW_PROD.REFERENCE.STORE_WAREHOUSE",
    "EDW_PROD.REPORTING.FAILED_BILLINGS_FINAL_OUTPUT",
    "EDW_PROD.REFERENCE.FINANCE_STORE_MAPPING",
    "EDW_PROD.REFERENCE.PRODUCT_COST_MARKDOWN_ADJUSTMENT",
    "EDW_PROD.REFERENCE.REDUCTION_FROM_RETURNS_COST",
    "LAKE_VIEW.SHAREPOINT.GFB_DAILY_SALE_STANDUP_ACTIVATING",
    "LAKE_VIEW.SHAREPOINT.GFB_MERCH_ATTRIBUTES",
    "LAKE_VIEW.SHAREPOINT.GFB_MERCH_BASE_SKU",
    "LAKE_VIEW.SHAREPOINT.GFB_MERCH_CLEARANCE_SKU_LIST",
    "EDW_PROD.REPORTING.DAILY_CASH_TABLEAU_COMPARISON",
    "LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TOKEN_REASON",
    "LAKE_VIEW.SHAREPOINT.GFB_COLLECTION_APPAREL_TRACKER",
    "LAKE_VIEW.SHAREPOINT.GFB_CUSTOMER_SEGMENT_PROMO_CODE",
    "LAKE_VIEW.SHAREPOINT.GFB_DAILY_SALE_STANDUP_REPEAT",
    "LAKE_VIEW.SHAREPOINT.GFB_FK_MERCH_ITEM_ATTRIBUTES",
    "LAKE_VIEW.SHAREPOINT.GFB_FK_MERCH_OUTFIT_ATTRIBUTES",
    "LAKE_VIEW.SHAREPOINT.GFB_LEAD_ONLY_PRODUCT",
    "LAKE_VIEW.SHAREPOINT.GFB_POST_REG_PRODUCT",
    "LAKE_VIEW.SHAREPOINT.GFB_PRODUCT_MARKETING_CAPSULE",
    "LAKE_VIEW.SHAREPOINT.GFB_PROMO_MAPPING",
    "LAKE_VIEW.SHAREPOINT.JFB_ADHOC_FORECAST_INPUT",
    "LAKE_VIEW.SHAREPOINT.MED_ACCOUNT_MAPPING_MEDIA",
    "REPORTING_PROD.GSC.PULSE_OUTFIT_DATASET",
    "REPORTING_BASE_PROD.SHARED.DIM_GATEWAY",
    "REPORTING_MEDIA_BASE_PROD.DBO.VW_MED_HDYH_MAPPING",
    "REPORTING_PROD.GSC.PO_DETAIL_DATASET",
    "REPORTING_PROD.GSC.OUTFIT_DATASET",
    "EDW_PROD.ANALYTICS_BASE.FINANCE_SALES_OPS",
    "EDW_PROD.REFERENCE.FINANCE_MONTHLY_BUDGET_FORECAST_ACTUAL",
}

default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5), "depends_on_past": False}

def _dag_id(m: dict) -> str:
    """Unique DAG id for each mapping."""
    sdb, ssc = m["source_db"], m["source_schema"]
    base = f"share_copy__{sdb}__{ssc}"
    return base.lower().replace(".", "_")

def build_dag(mapping: dict) -> DAG:
    """Build one DAG for one mapping."""
    dag = DAG(
        dag_id=_dag_id(mapping),
        description=f"Copy from {mapping['source_db']}.{mapping['source_schema']} "
                    + (f"to {mapping['target_db']}.{mapping['target_schema']}" if mapping.get("target_db") else "(comingled)"),
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule=None,
        catchup=False,
        default_args=default_args,
        max_active_runs=1,
        tags=["snowflake", "share", "copy", "factory"],
    )

    @task(dag=dag)
    def snowflake_load_data():
        """Single task per-DAG that runs this mapping. Supports dag_run.conf {\"force\": true}."""
        ctx = get_current_context()
        force = bool(((ctx.get("dag_run") and ctx["dag_run"].conf) or {}).get("force", False))

        sdb = mapping["source_db"]
        ssc = mapping["source_schema"]
        tdb = mapping.get("target_db")
        tsc = mapping.get("target_schema")

        log = logging.getLogger("airflow.task")
        s3 = boto3.client("s3")

        state_key = f'processed_{sdb}__{ssc}.json'
        processed = {}
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=state_key)
            processed = json.loads(obj["Body"].read().decode("utf-8"))
            log.info("Loaded state from s3://%s/%s (%d entries)", S3_BUCKET, state_key, len(processed))
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                log.info("No state file at s3://%s/%s; starting fresh", S3_BUCKET, state_key)
            else:
                log.warning("Could not load state file: %s", e)

        def save_state():
            s3.put_object(Bucket=S3_BUCKET, Key=state_key, Body=json.dumps(processed, indent=2).encode("utf-8"))
            log.info("Wrote state to s3://%s/%s", S3_BUCKET, state_key)

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            if sdb == "QM27872_TECHSTYLE_JFB_SHARE_COMINGLED":
                cur.execute("""
                    SELECT TABLE_NAME
                    FROM QM27872_TECHSTYLE_JFB_SHARE_COMINGLED.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'MMM' AND TABLE_TYPE = 'BASE TABLE'
                """)
                for (table_name,) in cur.fetchall():
                    tgt_db, tgt_schema, tgt_table = table_name.split("__", 2)
                    fqn = f"{tgt_db}.{tgt_schema}.{tgt_table}"

                    if fqn in SPECIAL_TSOS:
                        tgt_table = f"{tgt_table}_TSOS"
                        fqn = f"{tgt_db}.{tgt_schema}.{tgt_table}"
                        cur.execute(f"""
                            CREATE TABLE IF NOT EXISTS {fqn}
                            LIKE QM27872_TECHSTYLE_JFB_SHARE_COMINGLED.MMM.{table_name}
                        """)

                    if (not force) and processed.get(fqn):
                        continue

                    log.info("Processing %s (force=%s)", fqn, force)
                    cur.execute(f"""
                        SELECT 1 FROM {sdb}.INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = '{ssc}' AND TABLE_NAME = '{table_name}'
                    """)
                    if cur.fetchone() is None:
                        log.warning("Source not found: %s.%s.%s", sdb, ssc, table_name)
                        continue

                    try:
                        cur.execute(f'USE DATABASE "{tgt_db}"')
                        cur.execute(f'TRUNCATE TABLE "{tgt_db}"."{tgt_schema}"."{tgt_table}"')
                        cur.execute(f'''
                            INSERT INTO "{tgt_db}"."{tgt_schema}"."{tgt_table}"
                            SELECT * FROM "{sdb}"."{ssc}"."{table_name}"
                        ''')
                        processed[fqn] = True
                        save_state()
                        log.info("Finished %s", fqn)
                    except Exception as e:
                        log.error("Error processing %s: %s", fqn, e)

            else:
                tgt_db, tgt_schema = tdb, tsc
                cur.execute(f"""
                    SELECT TABLE_NAME
                    FROM {tgt_db}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{tgt_schema}' AND TABLE_TYPE = 'BASE TABLE'
                """)
                for (table_name,) in cur.fetchall():
                    fqn = f"{tgt_db}.{tgt_schema}.{table_name}"
                    if (not force) and processed.get(fqn):
                        continue

                    log.info("Processing %s (force=%s)", fqn, force)
                    cur.execute(f"""
                        SELECT 1 FROM {sdb}.INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = '{ssc}' AND TABLE_NAME = '{table_name}'
                    """)
                    if cur.fetchone() is None:
                        log.warning("Source not found: %s.%s.%s", sdb, ssc, table_name)
                        continue

                    try:
                        cur.execute(f'USE DATABASE "{tgt_db}"')
                        cur.execute(f'TRUNCATE TABLE "{tgt_db}"."{tgt_schema}"."{table_name}"')
                        cur.execute(f'''
                            INSERT INTO "{tgt_db}"."{tgt_schema}"."{table_name}"
                            SELECT * FROM "{sdb}"."{ssc}"."{table_name}"
                        ''')
                        processed[fqn] = True
                        save_state()
                        log.info("Finished %s", fqn)
                    except Exception as e:
                        log.error("Error processing %s: %s", fqn, e)
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    # build the task inside this DAG
    snowflake_load_data()
    return dag

# Register one DAG per mapping
for _m in SOURCE_TARGET_MAPPING:
    globals()[_dag_id(_m)] = build_dag(_m)
