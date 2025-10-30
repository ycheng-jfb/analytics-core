from functools import cached_property

import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from include.airflow.hooks.atlan import AtlanHook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.config.api_request_bodies import atlan_get_assets
from include.utils.context_managers import ConnClosing
from include.utils.snowflake import generate_query_tag_cmd


class AtlanRelationshipOperator(BaseOperator):
    """
    This will create foreign key relationships for tables or views within a schema for Snowflake

    Args:
        database: name of database in snowflake
        schema: name of schema within the database
        table_type: 'table' or 'view'.
        base_api_url: base API url from Atlan
        integration: integration name from Atlan
        foreign_schema_override: pass a different schema name here to override the name of the
            schema in foreign key references
    """

    def __init__(
        self,
        database: str,
        base_api_url: str = 'https://techstyle.atlan.com/api/metadata/atlas/tenants/default',
        integration: str = 'snowflake/techstyle',
        *args,
        **kwargs,
    ):
        self.database = database
        self.base_api_url = base_api_url
        self.integration = integration
        super().__init__(*args, **kwargs)

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook(database=self.database)

    @cached_property
    def headers(self):
        conn = BaseHook.get_connection('atlan_default')
        return {'APIKEY': conn.password}

    def make_request(self, method, endpoint, body=None):
        url = f'{self.base_api_url}{endpoint}'
        try:
            r = requests.request(method, url, headers=self.headers, json=body)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if r.status_code == 409:  # This code comes up when a relationship already exists
                print(r.text)
            else:
                print(r.text)
                raise e
        return r

    @staticmethod
    def get_outer_relationship_type(relation_type):
        # Atlan has a relationship type for each type of object
        relation_types_to_outer_type = {
            "AtlanTable": "atlan_table_table_fact_to_dim",
            "AtlanColumn": "atlan_column_column_foreign_key_to_primary_key",
        }
        if relation_type not in relation_types_to_outer_type:
            raise ValueError(
                f"{relation_type} is not an accepted relation_type. Value must be "
                f"one of {list(relation_types_to_outer_type.keys())}"
            )
        return relation_types_to_outer_type[relation_type]

    def body_for_relationship(self, relation_type, end1, end2):
        """
        Creates JSON body for creating a relationship for the Atlan API

        Args:
            relation_type: 'AtlanTable' or 'AtlanColumn'
            end1: ie. edw/dbo/fact_order (Table) or edw/dbo/fact_order/order_id (Column)
            end2: ie. edw/dbo/fact_order (Table) or edw/dbo/fact_order/order_id (Column)
        """
        outer_type_name = self.get_outer_relationship_type(relation_type)
        return {
            "end1": {
                "uniqueAttributes": {"qualifiedName": f"{self.integration}/{end1}"},
                "typeName": relation_type,
            },
            "end2": {
                "uniqueAttributes": {"qualifiedName": f"{self.integration}/{end2}"},
                "typeName": relation_type,
            },
            "typeName": outer_type_name,
        }

    def create_atlan_relationship(self, body):
        return self.make_request("POST", '/relationship', body=body)

    def update_atlan_relationships(self):
        with ConnClosing(self.snowflake_hook.get_conn()) as conn, conn.cursor() as cur:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)

            # Create table to table relationship
            sql_cmd = """SELECT DISTINCT pk_schema_name, pk_table_name, fk_schema_name, fk_table_name
                          FROM edw.reference.object_relationships"""
            result = pd.read_sql(sql_cmd, con=conn)
            for index, row in result.iterrows():
                current_table = f"""edw/{row['FK_SCHEMA_NAME']}/{row['FK_TABLE_NAME']}"""
                ref_table = f"""edw/{row['PK_SCHEMA_NAME']}/{row['PK_TABLE_NAME']}"""
                api_body = self.body_for_relationship('AtlanTable', current_table, ref_table)
                print("Creating:", api_body)
                r = self.create_atlan_relationship(api_body)
                if r.status_code == 200:
                    print(
                        f"Successfully created new relationship for {current_table} to {ref_table}"
                    )

            # Create column to column relationships
            sql_cmd = """SELECT DISTINCT pk_schema_name, pk_table_name, pk_column_name,
                                          fk_schema_name, fk_table_name, fk_column_name
                          FROM edw.reference.object_relationships"""
            result = pd.read_sql(sql_cmd, con=conn)

            for index, row in result.iterrows():
                current_col = f"""edw/{row['FK_SCHEMA_NAME']}/{row['FK_TABLE_NAME']}/{row['FK_COLUMN_NAME']}"""
                ref_col = f"""edw/{row['PK_SCHEMA_NAME']}/{row['PK_TABLE_NAME']}/{row['PK_COLUMN_NAME']}"""
                api_body = self.body_for_relationship('AtlanColumn', current_col, ref_col)
                print("Creating:", api_body)
                r = self.create_atlan_relationship(api_body)
                if r.status_code == 200:
                    print(f"Successfully created new relationship for {current_col} to {ref_col}")

    def execute(self, context=None):
        self.update_atlan_relationships()


class AtlanBaseOperator(BaseOperator):
    """
    Base operator to use when working with Atlan API

    Args:
        base_api_url: base API url from Atlan
    """

    def __init__(
        self,
        hook_conn_id=None,
        **kwargs,
    ):
        self.hook_conn_id = hook_conn_id
        super().__init__(**kwargs)

    @cached_property
    def atlan_hook(self):
        return AtlanHook(conn_id=self.hook_conn_id) if self.hook_conn_id else AtlanHook()


class AtlanUpdateStatusOperator(AtlanBaseOperator):
    """
    This will update the Last Update Date metadata for stg tables and data_model views in Atlan.
    """

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook()

    def get_table_names(self):
        # Get watermark values
        with ConnClosing(self.snowflake_hook.get_conn()) as conn, conn.cursor() as cur:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)

            sql = (
                "select TABLE_NAME, HIGH_WATERMARK_DATETIME "
                "from edw.stg.meta_table_dependency_watermark "
                "where dependent_table_name is null"
            )
            df = pd.read_sql(sql, con=conn)
            df['TABLE_NAME'] = df['TABLE_NAME'].str.split('.').str[1].str.upper()
            return df

    def update_status(self, df):
        table_names = list(df['TABLE_NAME'])
        # Get guid values from Atlan
        resp = self.atlan_hook.make_request('POST', '/search/basic', json=atlan_get_assets)
        entities_stg = resp.json()['entities']

        # Update the metadata in Atlan
        for entity in entities_stg:
            if entity['attributes']['name'].upper() in table_names:
                row = df[df.TABLE_NAME.eq(entity['attributes']['name'].upper())]
                timestamp = row.iloc[0]['HIGH_WATERMARK_DATETIME']
                epoch_timestamp = int(timestamp.timestamp() * 1000)

                body_update = {'ETL Details': {'Last Update Date': epoch_timestamp}}
                self.atlan_hook.make_request(
                    'POST',
                    f'/entity/guid/{entity["guid"]}/' 'businessmetadata?isOverwrite=true',
                    json=body_update,
                )

    def execute(self, context=None):
        df = self.get_table_names()
        self.update_status(df)
