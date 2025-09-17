from pathlib import Path

from airflow import DAG  # noqa: F401

from include import YAML_DAGS_DIR
from include.utils.dag_generator import YamlFile

for filename in Path(YAML_DAGS_DIR).rglob("*.yml"):
    dag_factory = YamlFile(filename)
    dag_factory.generate_dags(globals())
