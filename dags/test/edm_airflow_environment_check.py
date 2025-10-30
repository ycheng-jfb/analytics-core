from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from include.config import email_lists, owners
import pathlib
from airflow.operators.bash import BashOperator
import os
import platform

import pyodbc


ariflowroot = pathlib.Path("/usr/local/airflow")
ariflowroot = pathlib.Path("/usr")

default_args = {
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
}

# from airflow.
dag = DAG(
    dag_id='edm_airflow_environment_check',
    default_args=default_args,
    schedule='@once',
    start_date=datetime(2020, 1, 1),
    catchup=False,
)


def test():
    print('edit performed in UI test was successful')
    print("====== start list(ariflowroot.rglob(*)=====", ariflowroot)
    # for item in ariflowroot.rglob("*"):
    #     print(item)
    print("====== end list(ariflowroot.rglob(*)=====", ariflowroot)

    f = open("/usr/local/airflow/airflow.cfg", "r")
    print(f.read())

    print("pyodbc.drivers()")
    l = pyodbc.drivers()
    print(l)

    print("Name of the operating system:", os.name)
    print("Name of the OS system:", platform.system())
    print("Version of the operating system:", platform.release())

    print("os.environ start {0}: {1}".format(os.name, platform.release()))
    for name, value in os.environ.items():
        print("os.environ var {0}: {1}".format(name, value))
    print("os.environ end {0}: {1}".format(os.name, platform.release()))

    return ""


def pip_list():
    pip_list = os.popen('pip list').readlines()
    print(''.join(pip_list))


test = PythonOperator(task_id='wow', dag=dag, python_callable=test)
pip_list = PythonOperator(task_id='pip_list', dag=dag, python_callable=pip_list)
# test2 = BashOperator(task_id='wow2', dag=dag, bash_command="cat /usr/local/airflow/airflow.cfg")
# test3 = BashOperator(task_id='wow3', dag=dag, bash_command="ping relay.smtp.justfab.net  2> output.log & disown; sleep 5; cat ./output.log")
# test4 = BashOperator(task_id='wow4', dag=dag, bash_command="ping -i 0.2 relay.smtp.justfab.net")
# test5 = BashOperator(task_id='wow5', dag=dag, bash_command="ping -i 0.4 relay.smtp.justfab.net")
