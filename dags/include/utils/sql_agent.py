import os
import time

import pyodbc


def start_job(name, cnx: pyodbc.Connection):
    cmd = "EXEC msdb.dbo.sp_start_job @job_name = '{}'".format(name)
    cnx.execute(cmd)
    cnx.commit()
    cnx.close()


def run_sqlagent_job_and_await_finish_file(
    job_name,
    cnx: pyodbc.Connection,
    staging_path,
    initial_wait_interval_minutes,
    retry_wait_interval_minutes,
    max_total_wait_time_minutes,
):
    start_job(
        name=job_name,
        cnx=cnx,
    )

    print("waiting {} minutes...".format(initial_wait_interval_minutes))
    time.sleep(initial_wait_interval_minutes * 60)

    finish_file_path = staging_path + ".finish"
    error_file_path = staging_path + ".error"
    current_wait_time_minutes = initial_wait_interval_minutes
    attempt_count = 1
    while current_wait_time_minutes <= max_total_wait_time_minutes:
        print("attempt {}".format(attempt_count))
        if os.path.exists(error_file_path):
            raise Exception("SSIS job failed; .error file found.")
        elif os.path.exists(finish_file_path):
            return
        else:
            print("Did not find finish file on attempt {}.".format(attempt_count))
            print("Waiting {} minutes before checking again...".format(retry_wait_interval_minutes))
            attempt_count += 1
            time.sleep(retry_wait_interval_minutes * 60)
            current_wait_time_minutes += retry_wait_interval_minutes
    raise Exception("SSIS job did not complete in specified wait period")
