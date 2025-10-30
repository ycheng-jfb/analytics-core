import warnings
from typing import List

from airflow.models import BaseOperator
from airflow.utils.email import send_email

from include.airflow.hooks.windows_exec import WindowsExecutableHook


class WindowsExecutableOperator(BaseOperator):
    """
    This operator will run an executable on windows server

    Args:
        executable_file_loc: executable file .exe location path
        executable_arguments: parameters to the executable command
        remote_conn_id: remote server connection
        warning_return_codes: return_codes_allowed [1,2,3] as warning and any other nonzero
            code will raise exception
    """

    def __init__(
        self,
        executable_file_loc,
        executable_arguments,
        remote_conn_id,
        warning_return_codes: List[int],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.executable_file_loc = executable_file_loc
        self.executable_arguments = executable_arguments
        self.remote_conn_id = remote_conn_id
        self.warning_return_codes = warning_return_codes

    def execute(self, context):
        hook = WindowsExecutableHook(remote_conn_id=self.remote_conn_id)
        with hook.get_conn() as client:
            stdout, stderr, rc = client.run_executable(
                executable=self.executable_file_loc,
                arguments=f"\"{self.executable_arguments}\"",
                username=f"{hook.domain}\\{hook.username}",
                password=hook.password,
            )
            exception_msg = f'Task {self.task_id} failed with exit code {rc}'
            if rc == 0:
                return
            elif rc in self.warning_return_codes or '0xc0000121' in exception_msg:
                warnings.warn(message=exception_msg)
                send_email(
                    to=self.email,
                    subject=f'Warning:  {self.task_id} exited nonzero',
                    html_content=exception_msg,
                )
            else:
                raise Exception(exception_msg)
