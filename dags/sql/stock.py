from airflow import DAG
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'stock',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['snowflake', 'sql'],
)

# 定义13个SQL文件的任务
sql_files = [
    'sql/mmos/dim_lpn.sql',
    'sql/mmos/dim_shipment_carrier_milestone.sql',
    'sql/mmos/dim_store.sql',
    'sql/mmos/dim_warehouse.sql',
    'sql/mmos/fact_ebs_bulk_shipment.sql',
    'sql/mmos/fact_inventory.sql',
    'sql/mmos/fact_inventory_in_transit_from.sql',
    'sql/mmos/fact_inventory_in_transit.sql',
    'sql/mmos/fact_order_shipment.sql',
    # 包含history的文件放在后面
    'sql/mmos/fact_inventory_history.sql',
    'sql/mmos/fact_inventory_in_transit_from_history.sql',
    'sql/mmos/fact_inventory_in_transit_history.sql',
    'sql/mmos/fact_inventory_system_date_history.sql',
]


tasks = []
for i, sql_file in enumerate(sql_files):
    table_name = sql_file.split('/')[-1].replace('.sql', '')
    task = SnowflakeOperator(
        task_id=f'{table_name}',
        sql=sql_file,
        snowflake_conn_id='snowflake_default',
        dag=dag,
    )
    tasks.append(task)

# 添加任务依赖关系
for i in range(1, len(tasks)):
    tasks[i-1] >> tasks[i]
