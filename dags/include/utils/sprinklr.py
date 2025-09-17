import json
import re

import pandas as pd

from global_apps.global_apps_inbound_sprinklr import SprinklrConfig
from include import INCLUDE_DIR
from datetime import timedelta
from include.airflow.operators.sprinklr import PAYLOADS, SprinklrToS3Operator
from include.utils.snowflake import Column
from include.utils.string import camel_to_snake


def pandas_type_to_sql(panda_type):
    pandas_sql_map = {'object': 'string', 'float64': 'float', 'int64': 'bigint', 'bool': 'boolean'}
    return pandas_sql_map[panda_type]


def generate_column_config_from_df(
    df, delta_cols=[], unique_cols=[], datetime_columns=[], convert_camel_to_snake=False
):
    col_list = []
    for col in df.columns:
        col_name = camel_to_snake(col) if convert_camel_to_snake else col.lower()
        col_type = (
            'TIMESTAMP_LTZ(7)'
            if col in datetime_columns
            else pandas_type_to_sql(df.dtypes[col].__str__())
        )
        unique = True if col in unique_cols else False
        col_obj = Column(col_name, type=col_type, uniqueness=unique, source_name=col)
        if col in delta_cols:
            col_obj.delta_column = delta_cols.index(col)
        col_list.append(col_obj)

    return col_list


def modify_report_payload(report_payload):
    """
    This function can be used to modify the payload extract from the RWP Extraction tool:
    https://developer.sprinklr.com/docs/read/api_tutorials/Reporting_Widget_Payload_Extraction_Tool

    This modified version ensures that each heading is unique and sets certain values as defaults.
    """
    if type(report_payload) == str:
        report_payload = json.loads(report_payload)
    count_of_headings = {}
    if len(report_payload) == 1:
        modified_payload = report_payload[0]
        modified_payload['pageSize'] = 1000
        modified_payload['page'] = 0
        modified_payload['startTime'] = 0
        modified_payload['endTime'] = 0
        modified_payload['timeZone'] = 'America/Los_Angeles'
        for i, group_by in enumerate(modified_payload['groupBys']):
            # Modify the heading if it's a repeat
            heading = group_by['heading']
            if heading in count_of_headings:
                modified_heading = heading + str(count_of_headings[heading])
                count_of_headings[heading] += 1
            else:
                modified_heading = heading
                count_of_headings[heading] = 1

            # Set the name
            modified_payload['groupBys'][i]['heading'] = modified_heading
    else:
        raise Exception('Len of report_payload is not equal to 1. Need new implementation')
    return modified_payload


def add_report_payload(file_path, report_name):
    """
    This will modify or append a report payload to the PAYLOADS variable at the bottom of the
    operators/sprinklr.py file. You can download raw payloads at
    https://prod-customdev-service.sprinklr.com/public/ui/reporting?module=REPORTING

    Args:
        file_path: File path location of the payload downloaded from Sprinklr
        report_name: Custom name to assign to the report. This will be the key in PAYLOADS
    """
    d = json.load(open(file_path))
    modified_d = modify_report_payload(d)
    payloads = PAYLOADS
    payloads[report_name] = modified_d
    py_file_path = INCLUDE_DIR / 'airflow' / 'operators' / 'sprinklr.py'
    with open(py_file_path) as f:
        py_file = f.read()
    regex = r"^PAYLOADS = {(.|\n)*"
    new_py_file = re.sub(regex, f"PAYLOADS = {payloads}", py_file)
    with open(py_file_path, 'w') as f:
        f.write(new_py_file)


def create_sprinklr_config(report_name, extract_case_id=False, **context):
    """
    This will print out the SprinklrConfig for a report once its payload exists in the PAYLOADS
    variable within operators/sprinklr.py

    Args:
        report_name: Name of the report. Should correspond to the key in PAYLOADS.
        extract_case_id: Optional. If the report requires extraction of the CASE_ID, include it here
    """
    payload = PAYLOADS[report_name]

    s = SprinklrToS3Operator(
        task_id='s',
        key='',
        bucket='',
        endpoint=report_name,
        extract_case_id=extract_case_id,
        from_date=(context["macros"].datetime.today() - timedelta(hours=4)).isoformat(),
        to_date=context["macros"].datetime.today().isoformat(),
    )

    df = pd.DataFrame(s.get_rows(payload))
    column_list = generate_column_config_from_df(
        df, unique_cols=['CASE_ID'], delta_cols=['load_time'], datetime_columns=['load_time']
    )
    print(SprinklrConfig(endpoint=report_name, column_list=column_list))


def add_report_payload_and_column_config(file_path, report_name, extract_case_id=False):
    """
    This will run both `add_report_payload` and `create_sprinklr_config`

    Args:
        file_path: File path location of the payload downloaded from Sprinklr
        report_name: Custom name to assign to the report. This will be the key in PAYLOADS
        extract_case_id: Optional. If the report requires extraction of the CASE_ID, include it here
    """
    add_report_payload(file_path, report_name)
    create_sprinklr_config(report_name, extract_case_id)
