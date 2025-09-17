import re

from functools import cached_property

from edm.mart.configs import get_all_mart_operators
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.utils.snowflake import ForeignKey


class SyncPrimaryAndForeignKeysToSourceCode:
    """
    This will sync snowflake tables' primary keys and foreign keys to our source code
    NOTE: Only foreign keys are currently implemented. This is also only implemented for EDW.
    """

    def __init__(self, *args, **kwargs):
        pass

    @cached_property
    def hook(self):
        return SnowflakeHook()

    @staticmethod
    def get_src_code_operators():
        # EDW operators
        table_ops_map = {}
        operators = get_all_mart_operators()
        for op in operators:
            if not isinstance(op, SnowflakeProcedureOperator):
                full_table_name = f"edw.{op.schema}.{op.table}"
                table_ops_map[full_table_name] = op
        return table_ops_map

    def compare_src_keys_to_db_keys(self, ops):
        for table_name, op in ops.items():
            # Check primary keys
            src_keys = self.get_src_op_primary_keys(op)
            db_keys = self.get_db_table_primary_keys(table_name)
            if db_keys != src_keys:
                print(f'DIDNT MATCH PRIMARY KEYS FOR {table_name}')
                raise NotImplementedError

            # Check foreign keys
            db_ddl = self.hook.get_first(f"SELECT get_ddl('table', '{table_name}')")[0]
            src_f_keys = self.get_foreign_keys_from_ddl(op.create_base_table())
            db_f_keys = self.get_foreign_keys_from_ddl(db_ddl)
            if src_f_keys != db_f_keys:
                print(f'DIDNT MATCH FOREIGN KEYS FOR {table_name}')
                yield from self.create_alter_sql_for_foreign_keys(table_name, src_f_keys, db_f_keys)

    @staticmethod
    def create_alter_sql_for_foreign_keys(table_name, src_f_keys, db_f_keys):
        # Create statements for any missing keys in db
        for fk_col, fk_obj in src_f_keys.items():
            if fk_col not in db_f_keys:
                ref = f"{fk_obj.database}.{fk_obj.schema}.{fk_obj.table}({fk_obj.field})"
                yield f"ALTER TABLE {table_name} ADD FOREIGN KEY ({fk_col}) REFERENCES {ref};"

        # Create statements for any deleted keys in src
        for fk_col, fk_obj in db_f_keys.items():
            if fk_col not in src_f_keys:
                yield f"ALTER TABLE {table_name} DROP FOREIGN KEY ({fk_col});"

    @staticmethod
    def get_src_op_primary_keys(op):
        return [pk.lower() for pk in op.column_list.unique_col_names]

    def get_db_table_primary_keys(self, table_name):
        table_def = self.hook.get_pandas_df(f"DESC table {table_name}")
        primary_keys = table_def.loc[table_def['primary key'] == 'Y', 'name'].to_list()
        return [pk.lower() for pk in primary_keys]

    @staticmethod
    def get_foreign_keys_from_ddl(ddl):
        fks = {}
        regex = r'foreign key \((\w+)\) references (\w+\.\w+\.\w+)\s?\((\w+)\)'
        matches = re.findall(regex, ddl.lower())
        for match in matches:
            col_name = match[0]
            db, schema, table = match[1].split('.')
            fks[col_name] = ForeignKey(table, database=db, schema=schema, field=match[2])
        return fks

    def execute(self, dry_run=False):
        ops = self.get_src_code_operators()
        alter_statements = '\n'.join(self.compare_src_keys_to_db_keys(ops))
        if alter_statements:
            if dry_run:
                print(alter_statements)
            else:
                self.hook.run(alter_statements, autocommit=True)
