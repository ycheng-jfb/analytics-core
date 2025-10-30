import json

import pendulum
from airflow.utils.sqlalchemy import UtcDateTime
from sqlalchemy import Column, PrimaryKeyConstraint, String, and_

from include.airflow.models.base import Base
from include.airflow.utils.db import create_session


class ProcessState(Base):
    """
    Sqlalchemy model for ProcessState table.

    It is meant to provide a mechanism for state persistence of arbitrary processes.

    There is no association with dag_id, task_id, or data_interval_start -- it is entirely generic.

    Primary key is ``namespace``, 'process_name', and ``timestamp``.
    """

    __tablename__ = "process_state"

    namespace = Column(String(512), primary_key=True, default='default')
    process_name = Column(String(512), primary_key=True)
    value = Column(String, nullable=True)
    timestamp = Column(UtcDateTime, default=pendulum.DateTime.utcnow())

    __table_args__ = (PrimaryKeyConstraint('namespace', 'process_name'),)

    def __repr__(self):
        return (
            f"ProcessState(\n"
            f"    '{self.namespace}',\n"
            f"    '{self.process_name}',\n"
            f"    '{self.value}',\n"
            f"    '{self.timestamp}',\n"
            f")"
        )

    @classmethod
    def set_value(cls, namespace, process_name, value):
        """
        Store a value representing dag state.

        :return: None
        """
        new_task_state = ProcessState(
            value=json.dumps(value),
            namespace=namespace,
            process_name=process_name,
            timestamp=pendulum.DateTime.utcnow(),
        )

        with create_session() as session:
            session.merge(new_task_state)

        return new_task_state

    @classmethod
    def get_value(cls, namespace, process_name):
        """
        Retrieve latest dag state

        :return: Watermark value
        """
        filters = [cls.namespace == namespace, cls.process_name == process_name]
        with create_session() as session:
            query = session.query(ProcessState.value).filter(and_(*filters))
            result = query.first()
        if result:
            return json.loads(result[0])
        else:
            return None

    @classmethod
    def get_object(cls, namespace, process_name):
        """
        Retrieve latest dag state

        :return: Watermark value
        """
        filters = [cls.namespace == namespace, cls.process_name == process_name]
        with create_session() as session:
            query = session.query(ProcessState).filter(and_(*filters))
            result = query.first()
        return result
