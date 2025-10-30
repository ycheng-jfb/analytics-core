from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import pendulum
from airflow import DAG
from airflow.models import BaseOperator
from functools import cached_property

from include import YAML_DAGS_DIR
from include.airflow.callbacks import slack
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners
from include.pre_commit_hooks.check_yaml_dag import load_yaml_file

DEFAULT_OWNER = "data_integrations"


@dataclass
class YamlTask:
    config: dict
    task: BaseOperator


def sql_kwargs_map(procedure: str, **kwargs):
    database, proc_name = procedure.split('.', maxsplit=1)
    return dict(database=database, procedure=proc_name, **kwargs)


class DagBuilder:
    """
    Builds tasks and DAG from config and set tasks dependencies within a DAG

    Args:
        dag_name: name of the dag
        dag_yaml_dict: dag configuration values from a YAML config file
        owner: owner of the dag
    """

    def __init__(self, dag_name: str, dag_yaml_dict: Dict[str, Any], owner: str) -> None:
        self.dag_name: str = dag_name
        self.dag_yaml_dict: Dict[str, Any] = dag_yaml_dict
        self.owner: str = owner

    @property
    def email_list(self):
        email_list_mapping = {
            owners.data_integrations: email_lists.data_integration_support,
            owners.media_analytics: email_lists.media_measurement,
            owners.global_apps_analytics: email_lists.global_applications,
        }
        if self.owner in email_list_mapping:
            return email_list_mapping[self.owner]
        else:
            return getattr(email_lists, f"{self.owner}_reporting_support")

    @property
    def on_failure_callback(self):
        try:
            return getattr(slack, f"slack_failure_{self.owner}")
        except AttributeError:
            return getattr(slack, "slack_failure_edm")

    @property
    def default_args(self) -> Dict[str, Any]:
        timezone = self.dag_yaml_dict.get('timezone', 'America/Los_Angeles')
        return {
            'start_date': pendulum.datetime(2020, 1, 1, tz=timezone),
            'owner': self.owner,
            'email': self.email_list,
            'retries': 0,
            'on_failure_callback': self.on_failure_callback,
        }

    @property
    def dag_kwargs(self) -> Dict[str, Any]:
        dag_id = "yaml_dag"
        return {
            "dag_id": dag_id,
            "schedule": self.dag_yaml_dict["schedule"],
            "max_active_tasks": 1000,
            "catchup": False,
            "max_active_runs": 1,
            "default_args": self.default_args,
        }

    @staticmethod
    def build_task(task_conf: Dict[str, Any]):
        """
        Generate task params dictionaly for a given task_conf from YAML config file
        Args:
            task_conf: dict of task params in a tasks of a DAG
        """
        task_type = task_conf['type'].lower()

        task_type_map = {
            'sql': SnowflakeProcedureOperator,
            'tableau': TableauRefreshOperator,
        }

        def map_kwargs(task_type, kwargs):
            kwargs_map_dict = {'sql': sql_kwargs_map}
            kwargs_map = kwargs_map_dict.get(task_type)
            return kwargs_map(**kwargs) if kwargs_map else kwargs

        op_cls = task_type_map.get(task_type)

        if not op_cls:
            raise Exception(f"unsupported task type provided: {task_type}")

        excluded_kwargs = {'upstream_tasks', 'type'}
        op_kwargs = {k: v for k, v in task_conf.items() if k not in excluded_kwargs}
        return op_cls(**map_kwargs(task_type, op_kwargs))  # type: ignore

    def build_dag(self) -> DAG:
        """
        Create a dag from DAG parameters and config values.
        Returns Dictionary with dag_id and DAG object.
        """
        dag: DAG = DAG(**self.dag_kwargs)
        dag.is_yaml_auto_generated = True

        tasks_dict: Dict[str, YamlTask] = {}

        with dag:
            for task_config in self.dag_yaml_dict["tasks"]:
                task = self.build_task(task_config)
                if task.task_id in tasks_dict:
                    raise Exception(
                        f"bad yaml file; dag already has task with task_id {task.task_id}"
                    )
                tasks_dict[task.task_id] = YamlTask(task_config, task)

        for task_id, yt in tasks_dict.items():
            deps = yt.config.get("upstream_tasks")
            if deps:
                for dep in deps:
                    tasks_dict[dep].task >> tasks_dict[task_id].task

        return dag


class YamlFile:
    """
    Takes a YAML config and generates DAGs.

    Args:
        path: absolute path of yaml configuration file
        owner: Owner of the dag. Example: fabletics, savage_x
    """

    def __init__(self, path: Path):
        self.path = path

    @property
    def owner(self):
        yaml_dir = YAML_DAGS_DIR
        rel_path = self.path.relative_to(yaml_dir)
        base_folder = rel_path.as_posix().split('/', 1)[0]
        try:
            return getattr(owners, base_folder)
        except AttributeError:
            return DEFAULT_OWNER

    @cached_property
    def yaml_dict(self) -> Dict[str, Any]:
        return load_yaml_file(self.path)

    def generate_dags(self, dag_globals: Dict[str, Any]):
        """
        Generate dags from YAML config files

        Args:
            dag_globals: The globals() from the file used to generate DAGs. The dag_id
            must be passed into globals() for Airflow to import.
        """

        for dag_name, dag_config in self.yaml_dict.items():
            dag_builder = DagBuilder(dag_name=dag_name, dag_yaml_dict=dag_config, owner=self.owner)
            dag = dag_builder.build_dag()
            dag_globals[f"{dag.dag_id}_dag"] = dag
