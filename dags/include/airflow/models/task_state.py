import json

import pendulum
from airflow.utils.sqlalchemy import UtcDateTime
from sqlalchemy import Column, PrimaryKeyConstraint, String, and_

from include.airflow.models.base import Base
from include.airflow.utils.db import create_session


class TaskState(Base):
    """
    Sqlalchemy model for task_state table.

    This table is meant to be used to persist task state to the database.
    """

    __tablename__ = "task_state"

    dag_id = Column(String(250), primary_key=True)
    task_id = Column(String(250), primary_key=True)
    value = Column(String, nullable=True)
    timestamp = Column(UtcDateTime, default=pendulum.DateTime.utcnow())

    __table_args__ = (PrimaryKeyConstraint("dag_id", "task_id"),)

    def __repr__(self):
        return (
            f"TaskState(\n"
            f"    '{self.dag_id}',\n"
            f"    '{self.task_id}',\n"
            f"    '{self.value}',\n"
            f"    '{self.timestamp}',\n"
            f")"
        )

    @classmethod
    def set_value(cls, dag_id, task_id, value):
        """
        Store a value representing dag state.

        :return: None
        """
        new_task_state = TaskState(
            value=json.dumps(value),
            task_id=task_id,
            dag_id=dag_id,
            timestamp=pendulum.DateTime.utcnow(),
        )

        with create_session() as session:
            session.merge(new_task_state)

        return new_task_state

    @classmethod
    def get_value(cls, dag_id, task_id):
        """
        Retrieve latest dag state

        :return: Watermark value
        """
        filters = [cls.dag_id == dag_id, cls.task_id == task_id]
        with create_session() as session:
            query = session.query(TaskState.value).filter(and_(*filters))
            result = query.first()
        if result:
            return json.loads(result[0])
        else:
            return None

    @classmethod
    def get_object(cls, dag_id, task_id):
        """
        Retrieve latest dag state

        :return: Watermark value
        """
        filters = [cls.dag_id == dag_id, cls.task_id == task_id]
        with create_session() as session:
            query = session.query(TaskState).filter(and_(*filters))
            result = query.first()
        return result
