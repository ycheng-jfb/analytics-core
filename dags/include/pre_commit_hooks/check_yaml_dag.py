#!/usr/bin/env python

import argparse
import json
from typing import Any, Dict, Optional, Sequence

import yaml
from cerberus import Validator

task_schema_common = {
    'type': {'required': True, 'type': 'string'},
    'upstream_tasks': {'required': False, 'type': 'list', 'schema': {'type': 'string'}},
}

task_schemas = {
    'tableau': {
        'task_id': {'type': 'string'},
        'data_source_id': {'required': True, 'type': 'string'},
    },
    'sql': {
        'procedure': {'type': 'string'},
        'watermark_tables': {
            'required': False,
            'type': 'list',
            'schema': {
                'type': 'dict',
                'schema': {
                    'table_name': {
                        'type': 'string',
                        'required': True,
                        'regex': r'^\w+\.\w+\.\w+$',
                    },
                    'column_name': {'type': 'string'},
                },
            },
        },
    },
}

for task_type, schema in task_schemas.items():
    schema.update(task_schema_common)

dag_schema = {
    'schedule': {'required': True, 'type': 'string'},
    'tasks': {
        'type': 'list',
        'required': True,
    },
}


def load_yaml_file(filename) -> Dict[str, Any]:
    with open(filename, "r") as file:
        config: Dict[str, Any] = yaml.load(stream=file, Loader=yaml.FullLoader)

    v = Validator(dag_schema)
    for dag_id, dag_config in config.items():
        if not v.validate(dag_config) is True:
            message = f"error in yaml file `{filename}`\n"
            message += f"dag `{dag_id}` has bad schema: {v.errors}\n"
            message += f"expected schema: {dag_schema}\n"
            raise Exception(message)
        for task_config in dag_config['tasks']:
            task_type = task_config['type']
            task_schema = task_schemas[task_type]
            v = Validator(task_schema)
            if not v.validate(task_config) is True:
                message = f"error in yaml file `{filename}`\n"
                message += f"task in dag `{dag_id}` has bad schema\n"
                message += f"bad task: {json.dumps(task_config, indent=2)}\n"
                message += f"expected schema: {json.dumps(task_schema, indent=2)}\n"
                raise Exception(message)
    return config


def main(argv=None):  # type: (Optional[Sequence[str]]) -> int
    parser = argparse.ArgumentParser(description='Ensure filenames are lowercase.')
    parser.add_argument('filenames', nargs='*', help='Filenames to check')
    args = parser.parse_args(argv)

    retv = 0

    for filename in args.filenames:
        load_yaml_file(filename)
    return retv


if __name__ == '__main__':
    exit(main())
