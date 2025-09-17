from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from include.config import owners

default_args = {
    "owner": owners.data_integrations,
}

# from airflow.
dag = DAG(
    dag_id="astronomer_test_dag",
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2020, 1, 1),
    catchup=False,
)


def test():
    raise Exception("Oops.")


test = PythonOperator(task_id="wow", dag=dag, python_callable=test)
