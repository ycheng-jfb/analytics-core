from airflow import DAG
from datetime import datetime  # 必须导入 datetime 类
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',   
}

dag = DAG(
    'jfb_sxf_membership_credit_breakage_report',   # DAG任务名   建议python 脚本名字，DAG任务名，sql脚本名三者统一
    default_args=default_args,
    schedule_interval="@daily",  # 启用每日调度
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['snowflake', 'sql'],
)

run_sql_file = SnowflakeOperator(
    task_id='jfb_sxf_membership_credit_breakage_report',
    sql='sql/jfb_sxf_membership_credit_breakage_report.sql',  # 执行脚本的相对路径
    snowflake_conn_id='snowflake_default',     # 配置的snowflake连接地址名字
    dag=dag,
)