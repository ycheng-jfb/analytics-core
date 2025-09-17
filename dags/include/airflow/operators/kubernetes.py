import os

from airflow.utils.context import Context
from airflow.hooks.base import BaseHook

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_env_vars,
)

IS_PRODUCTION = "production" in os.environ.get("ENVIRONMENT_STAGE", "").lower()


class KubernetesPythonOperator(KubernetesPodOperator):
    def __init__(
        self, python_script, conn_id_list=None, env_variables: dict = None, **kwargs
    ):
        self.conn_id_list = conn_id_list
        self.env_variables = env_variables

        if IS_PRODUCTION:
            namespace = "duploservices-da-int"
            node_selector = {"tenantname": "duploservices-da-int"}
            image = "157194816704.dkr.ecr.us-west-2.amazonaws.com/airflow-constructor:latest"
            config_file = "/usr/local/airflow/dags/kube/kube_config_prod.yaml"
            name = "mwaa-pod-prod"
        else:
            namespace = "duploservices-da-int-dev"
            node_selector = {"tenantname": "duploservices-da-int-dev"}
            image = "294468937448.dkr.ecr.us-west-2.amazonaws.com/airflow-constructor:latest"
            config_file = "/usr/local/airflow/dags/kube/kube_config_test.yaml"
            name = "mwaa-pod-test"

        super().__init__(
            **kwargs,
            namespace=namespace,
            node_selector=node_selector,
            image=image,
            name=name,
            get_logs=True,
            is_delete_operator_pod=False,
            config_file=config_file,
            in_cluster=False,
            cmds=["python3", "-c"],
            arguments=[python_script],
        )

    def get_env_vars(self, **kwargs):
        env_variables = self.env_variables
        for conn_id in self.conn_id_list:
            conn = BaseHook.get_connection(conn_id)
            env_variables[f"{conn_id}_conn_type"] = str(conn.conn_type)
            env_variables[f"{conn_id}_host"] = str(conn.host)
            env_variables[f"{conn_id}_login"] = str(conn.login)
            env_variables[f"{conn_id}_password"] = str(conn.password)
            env_variables[f"{conn_id}_schema"] = str(conn.schema)
            env_variables[f"{conn_id}_port"] = str(conn.port)
            env_variables[f"{conn_id}_extra"] = str(conn.extra)

        return env_variables

    def execute(self, context: Context):
        self.env_vars = convert_env_vars(self.get_env_vars())
        if self.deferrable:
            self.execute_async(context)
        else:
            return self.execute_sync(context)
