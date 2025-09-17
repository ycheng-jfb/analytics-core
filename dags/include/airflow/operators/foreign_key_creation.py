from functools import cached_property

import pandas as pd
from airflow.models import BaseOperator
from include.airflow.hooks.snowflake import SnowflakeHook
from include.utils.context_managers import ConnClosing
from include.utils.snowflake import generate_query_tag_cmd


class ForeignKeyRelationshipOperator(BaseOperator):
    def __init__(
        self,
        database="edw",
        schema="reference",
        table="object_relationships",
        dry_run=False,
        **kwargs,
    ):
        self.database = database
        self.schema = schema
        self.table = table
        super().__init__(**kwargs)

        if dry_run:
            self.database = "edw_dev"

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook(database=self.database)

    def get_foreign_keys(self):
        sql_cmd = f"SHOW IMPORTED KEYS IN DATABASE {self.database};"

        with ConnClosing(self.snowflake_hook.get_conn()) as conn, conn.cursor() as cur:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            result = pd.read_sql(sql_cmd, con=conn)

        foreign_keys = []
        for index, row in result.iterrows():
            foreign_keys.append(
                {
                    "database": row["fk_database_name"],
                    "schema": row["fk_schema_name"],
                    "table": row["fk_table_name"],
                    "column": row["fk_column_name"],
                }
            )
        return foreign_keys

    def drop_foreign_keys(self, foreign_keys):
        for foreign_key in foreign_keys:
            self.snowflake_hook.execute_multiple(
                f"ALTER TABLE {foreign_key['database']}.{foreign_key['schema']}.{foreign_key['table']}"
                f" DROP FOREIGN KEY ({foreign_key['column']});"
            )

    def create_foreign_keys(self) -> None:
        with ConnClosing(self.snowflake_hook.get_conn()) as conn, conn.cursor() as cur:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            sql_cmd = f"""SELECT DISTINCT fk_schema_name, fk_table_name, fk_column_name,
                pk_schema_name, pk_table_name, pk_column_name
                FROM {self.database}.{self.schema}.{self.table} WHERE object_type = 'table'"""
            result = pd.read_sql(sql_cmd, con=conn)

        for index, row in result.iterrows():
            sql_cmd = f"""ALTER TABLE {self.database}.{row['FK_SCHEMA_NAME']}.{row['FK_TABLE_NAME']}
            ADD FOREIGN KEY ({row['FK_COLUMN_NAME']})
            REFERENCES
            {self.database}.{row['PK_SCHEMA_NAME']}.{row['PK_TABLE_NAME']}({row['PK_COLUMN_NAME']})
            NOT ENFORCED;"""

            self.snowflake_hook.execute_multiple(sql=sql_cmd)

    def execute(self):
        self.drop_foreign_keys(self.get_foreign_keys())
        self.create_foreign_keys()
