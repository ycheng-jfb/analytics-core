from pathlib import Path

INCLUDE_DIR = Path(__file__).parent
DAGS_DIR = INCLUDE_DIR.parent
ROOT_DIR = DAGS_DIR.parent
SQL_DIR = DAGS_DIR / "sql"  # type: Path
REPO_SQL_URL = (
    "https://github.com/TechstyleOS/analytics-core/tree/master/airflow/dags/sql"
)
YAML_DAGS_DIR = DAGS_DIR / "yaml_dags"
CONFIG_DIR = DAGS_DIR / "include/config"
TASK_CONFIGS_DIR = DAGS_DIR / "task_configs"
GSHEET_CONFIGS_DIR = TASK_CONFIGS_DIR / "gsheet"
