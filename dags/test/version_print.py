from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_cryptography_version():
    import cryptography

    print(f"cryptography version: {cryptography.__version__}")


with DAG(
    dag_id="print_cryptography_version",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    print_version_task = PythonOperator(
        task_id="print_cryptography_version", python_callable=print_cryptography_version
    )

    print_version_task
