from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import requests

# -------------------------
# 配置
# -------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

local_tz = pendulum.timezone("America/Los_Angeles")

# 表配置
TABLE_NAME_SHOEDAZZLE = "LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.ORDER"
THRESHOLD_SHOEDAZZLE = 150

TABLE_NAME_JUSTFAB = "LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.ORDER"
THRESHOLD_JUSTFAB = 650

# 飞书机器人 webhook
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/37f0c600-73fb-471d-b8b3-1ea78d47da15"

# -------------------------
# DAG 定义
# -------------------------
dag = DAG(
    'check_order_count_feishu',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # 每天0点
    start_date=datetime(2025, 10, 20, tzinfo=local_tz),
    catchup=False,
    tags=['snowflake', 'alert'],
)

# -------------------------
# Python 函数：检查条数并发送飞书告警
# -------------------------
def check_table_count_feishu(ti, table_name, threshold):
    order_count = ti.xcom_pull(task_ids=f'query_count_{table_name.replace(".", "_")}')[0]['ORDER_COUNT']
    today = (datetime.now(local_tz) - timedelta(days=1)).strftime("%Y-%m-%d")
    now_str = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")

    print(f"[check_order_count_feishu] 表 {table_name} {today} 条数: {order_count}")

    if order_count < threshold:
        msg = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": "⚠️ 表数据告警",
                        "content": [
                            [
                                {"tag": "text", "text": f"检测时间: {now_str}\n"},
                                {"tag": "text", "text": f"表名: {table_name}\n"},
                                {"tag": "text", "text": f"日期: {today}\n"},
                                {"tag": "text", "text": f"条数: {order_count}\n"},
                                {"tag": "text", "text": f"阈值: {threshold}\n"},
                                {"tag": "text", "text": f"异常原因: 条数低于阈值\n"},
                                {"tag": "text", "text": f"建议: 检查数据源和 DAG 运行状态\n"},
                                {"tag": "at", "user_id": "ou_af76734765cef067d97e2ff45a9dd6f2"},  # tea
                                {"tag": "at", "user_id": "ou_9a014ffee4c8a2797a50cd0b2436efcd"},  # Sarah.Wu
                                {"tag": "at", "user_id": "ou_9cc0edf7582dc2c85480cc1fc4935c31"},  # Roger.Huang
                                {"tag": "at", "user_id": "ou_cd65744a8e619bf8a419e01f507314e3"},  # Goslly.Gao
                                {"tag": "at", "user_id": "ou_c5f88e949551df4ad0830b44977fe72d"},  # Yuan Cheng
                                {"tag": "at", "user_id": "ou_8626ecb01959a9b45bb189b8f5effac3"}   # cary tai
                                
                            ]
                        ]
                    }
                }
            }
        }
        requests.post(FEISHU_WEBHOOK, json=msg, headers={"Content-Type": "application/json;charset=utf-8"})

# -------------------------
# Shoedazzle 查询任务
# -------------------------

query_task_shoedazzle = SnowflakeOperator(
    task_id='query_count_LAKE_MMOS_SHOPIFY_SHOEDAZZLE_PROD_ORDER',
    sql=f"""
        SELECT COUNT(*) AS ORDER_COUNT
        FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o
        WHERE TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) = (CURRENT_DATE() - INTERVAL '1 day');
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag,
    do_xcom_push=True,
)

# -------------------------
# 检查条数并发送告警
# -------------------------
check_task_shoedazzle = PythonOperator(
    task_id='check_count_LAKE_MMOS_SHOPIFY_SHOEDAZZLE_PROD_ORDER',
    python_callable=check_table_count_feishu,
    op_kwargs={
        "table_name": TABLE_NAME_SHOEDAZZLE,
        "threshold": THRESHOLD_SHOEDAZZLE
    },
    dag=dag,
)



# JustFab 查询任务
# -------------------------
query_task_justfab = SnowflakeOperator(
    task_id='query_count_LAKE_MMOS_SHOPIFY_JUSTFAB_PROD_ORDER',
    sql=f"""
        SELECT COUNT(*) AS ORDER_COUNT
        FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o
        WHERE TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) = (CURRENT_DATE() - INTERVAL '1 day');
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag,
    do_xcom_push=True,
)

check_task_justfab = PythonOperator(
    task_id='check_count_LAKE_MMOS_SHOPIFY_JUSTFAB_PROD_ORDER',
    python_callable=check_table_count_feishu,
    op_kwargs={
        "table_name": TABLE_NAME_JUSTFAB,
        "threshold": THRESHOLD_JUSTFAB
    },
    dag=dag,
)

query_task_shoedazzle >> check_task_shoedazzle
query_task_justfab >> check_task_justfab
