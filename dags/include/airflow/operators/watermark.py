from abc import abstractmethod

from airflow.models import BaseOperator

from include.airflow.models.process_state import ProcessState
from include.airflow.models.task_state import TaskState


class BaseWatermarkOperator(BaseOperator):
    """
    Wraps method :meth:`~.BaseWatermarkOperator.watermark_execute` with pre- and post-execute
    methods that handle watermarking for incremental processes.

    Args:
        initial_load_value: on first run, the value that should be used for low watermark
    """

    def __init__(
        self, initial_load_value: str = "1900-01-01T00:00:00+00:00", *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.initial_load_value = initial_load_value
        self.low_watermark = None
        self.new_high_watermark = None

    @abstractmethod
    def get_low_watermark(self):
        """
        Grab last high watermark from database.
        """
        pass

    @abstractmethod
    def get_high_watermark(self) -> str:
        """
        Override with logic to return new high watermark.
        Must be json-serializable.
        """
        pass

    @abstractmethod
    def update_progress(self, value):
        """
        Update database watermark with value.
        """
        pass

    def set_high_watermark(self):
        """
        Update database with :attr:`new_high_watermark`.
        """
        return self.update_progress(value=self.new_high_watermark)

    def watermark_pre_execute(self):
        """
        1. Get last high watermark (or initial load value) and store as :attr:`low_watermark`.
        2. Get new high watermark and store as :attr:`new_high_watermark`.
        """
        print("running watermark pre-execute")

        self.low_watermark = self.get_low_watermark()
        print(f"low_watermark: {self.low_watermark}")

        self.new_high_watermark = self.get_high_watermark()
        print(f"new_high_watermark: {self.new_high_watermark}")

    def watermark_post_execute(self):
        """
        Update database with new high watermark
        """
        print("running watermark post-execute")
        self.set_high_watermark()

    @abstractmethod
    def watermark_execute(self, context=None):
        """
        Override this with your main logic, whatever you would normally put in
        :meth:`~.BaseWatermarkOperator.execute`.

        By the time this method is called, low watermark will be available in attribute :attr:`low_watermark`.
        After this method completes successfully, new high watermark will be stored in database.
        """
        pass

    def execute(self, context=None):
        """
        Generally speaking, do not override this method.
        Instead, override :meth:`~.BaseWatermarkOperator.watermark_execute`.
        """
        self.watermark_pre_execute()

        self.watermark_execute(context=context)

        self.watermark_post_execute()


class DeprecatedBaseTaskWatermarkOperator(BaseWatermarkOperator):
    """
    Implementation of :class:`~.BaseWatermarkOperator` that persists state to TaskState table.

    Primary key of TaskState is ``dag_id`` + ``task_id``.
    """

    def get_low_watermark(self):
        """
        Grab last high watermark from database.
        """
        task_state_val = TaskState.get_value(dag_id=self.dag_id, task_id=self.task_id)
        return task_state_val or self.initial_load_value

    def update_progress(self, value):
        """
        Update database watermark with value.
        """
        if value:
            return TaskState.set_value(
                dag_id=self.dag_id, task_id=self.task_id, value=value
            )


class BaseProcessWatermarkOperator(BaseWatermarkOperator):
    """
    Implementation of :class:`~.BaseWatermarkOperator` that persists state to ProcessState table.

    Values stored in ProcessState table are identified with the required parameters ``namespace``
    and ``process_name``.

    Args:
        process_name: name of the process e.g. "thisdb.thisschema.this_table"
        namespace: group name for process.  e.g. "acquisition_copy_paste"
        initial_load_value: on first run, the value that should be used for low watermark
    """

    def __init__(
        self,
        process_name,
        namespace,
        initial_load_value: str = "1900-01-01T00:00:00+00:00",
        **kwargs,
    ):
        super().__init__(initial_load_value=initial_load_value, **kwargs)
        self.process_name = process_name
        self.namespace = namespace

    def get_low_watermark(self):
        """
        Grab last high watermark from database.
        """
        task_state_val = ProcessState.get_value(
            namespace=self.namespace, process_name=self.process_name
        )
        return task_state_val or self.initial_load_value

    def update_progress(self, value):
        """
        Update database watermark
        """
        if value:
            return ProcessState.set_value(
                namespace=self.namespace, process_name=self.process_name, value=value
            )


class BaseTaskWatermarkOperator(BaseWatermarkOperator):
    def get_low_watermark(self):
        """
        Grab last high watermark from database.
        """
        task_state_val = ProcessState.get_value(
            namespace=self.dag_id, process_name=self.task_id
        )
        return task_state_val or self.initial_load_value

    def update_progress(self, value):
        """
        Update database watermark
        """
        if value:
            return ProcessState.set_value(
                namespace=self.dag_id, process_name=self.task_id, value=value
            )
