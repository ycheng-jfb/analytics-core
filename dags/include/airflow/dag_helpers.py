from typing import List, Union

from airflow.models import BaseOperator


class TFGControlOperator(BaseOperator):
    """
    Operator that does literally nothing. It can be used to group tasks in a
    DAG.

    The task is evaluated by the scheduler but never processed by the executor.
    """

    ui_color = "#e8f7e4"
    inherits_from_dummy_operator = True

    def __init__(self, **kwargs) -> None:
        super().__init__(task_id="support_control_task", **kwargs)

    def execute(self, context):
        pass


def chain_tasks(*elements: Union[BaseOperator, List[BaseOperator]]):
    """
    Helper to simplify task dependency definition.

    E.g.: suppose you want precedence like so::

            ╭─op2─╮ ╭─op4─╮
        op1─┤     ├─┤     ├─op6
            ╰-op3─╯ ╰-op5─╯

    Then you can accomplish like so::

        chain_tasks(
            op1,
            [op2, op3],
            [op4, op5],
            op6
        )

    Args:
        elements: a list of operators / lists of operators
    """

    prev_elem = None
    for curr_elem in elements:
        if prev_elem is not None:
            for task in prev_elem:
                task >> curr_elem
        prev_elem = curr_elem if isinstance(curr_elem, list) else [curr_elem]
