from airflow import DAG
from datetime import datetime  # 必须导入 datetime 类
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
# from airflow.sensors.external_task import ExternalTaskSensor
import pendulum 

default_args = {
    'owner': 'airflow',   
}

local_tz = pendulum.timezone("America/Los_Angeles")

dag = DAG(
    'dim_promo_history',   # DAG任务名   建议python 脚本名字，DAG任务名，sql脚本名三者统一
    default_args=default_args,
    schedule_interval="0 2 * * *",  # 启用每日调度
    start_date=datetime(2025, 10, 27, tzinfo=local_tz),
    catchup=False,
    tags=['snowflake', 'sql'],
)

# 等待 dim_bundle_component_history 成功
# wait_for_dim_bundle_component_history = ExternalTaskSensor(
#     task_id="wait_for_dim_bundle_component_history",
#     external_dag_id="dim_bundle_component_history",  # 上游 DAG 名称
#     external_task_id=None,       # None = 等整个 DAG 成功
#     allowed_states=["success"],  # 必须是成功状态
#     poke_interval=60,            # 每 60 秒检查一次
#     timeout=60*60,               # 最多等 1 小时
#     mode="reschedule",           # 节省 worker slot
# )


run_dim_promo_history = SnowflakeOperator(
    task_id='dim_promo_history',
    sql='sql/dim_promo_history.sql',  # 执行脚本的相对路径
    snowflake_conn_id='snowflake_default',     # 配置的snowflake连接地址名字
    dag=dag,
)

run_dim_promo_history
