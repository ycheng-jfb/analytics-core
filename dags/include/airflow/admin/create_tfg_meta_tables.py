from sqlalchemy.sql.ddl import CreateSchema

from include.airflow.models.process_state import ProcessState
from include.airflow.models.task_state import TaskState
from include.airflow.utils.db import provide_session


@provide_session
def create_tfg_meta_tables(session=None):
    print("ensuring tfg meta tables")
    bind = session.get_bind()
    if hasattr(bind.dialect, "has_schema") and not bind.dialect.has_schema(
        bind, "tfg_meta"
    ):
        print("creating schema")
        bind.execute(CreateSchema("tfg_meta"))
    ProcessState.metadata.create_all(
        bind, tables=[ProcessState.__table__, TaskState.__table__]
    )


if __name__ == "__main__":
    create_tfg_meta_tables()
