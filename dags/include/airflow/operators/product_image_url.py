import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Union

import pandas as pd
import requests
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.snowflake import (
    BaseSnowflakeOperator,
    get_effective_database,
)
from include.config import conn_ids
from include.utils import snowflake
from include.utils.context_managers import ConnClosing
from snowflake.connector.pandas_tools import write_pandas


class SavageXImageUrlOperator(BaseSnowflakeOperator):
    """
    Finds image URLs for products or sets and outputs the results to a Snowflake table

    Args:
        output_table: Snowflake table to upload the results to. Will be a truncate and load.
        sku_type: Indicate if the sku_type is 'Product' or 'Set'
        sku_query: Query to pull in SKUs from Snowflake
        base_product_url: Common base path used for product URLs
        break_count: Specify how many records to find URLS for. Defaults to 100,000.
        *args:
        **kwargs:
    """

    def __init__(
        self,
        sku_query_or_path: Union[str, Path],
        sku_type: str = "Product",
        output_table: str = None,
        base_product_url="https://cdn.savagex.com/media/images/products",
        break_count=100000,
        task_id=None,
        **kwargs,
    ):
        self.base_product_url = base_product_url
        if sku_type not in ["Product", "Set"]:
            raise Exception(
                "The only acceptable sku types are one of ['Product', 'Set']"
            )
        self.sku_query_or_path = sku_query_or_path
        self.sku_type = sku_type
        output_table = (
            output_table
            if output_table
            else f"edw.reference.{sku_type.lower()}_image_url_sxf"
        )
        database, self.schema, self.table = output_table.split(".")
        self.database = database
        self.break_count = break_count
        self.output_table = output_table
        super().__init__(
            task_id=task_id or f"{output_table}_load",
            database=database,
            schema=self.schema,
            **kwargs,
        )

    def get_sql_cmd(self, sql_or_path):
        try:
            if Path(sql_or_path).exists() or isinstance(sql_or_path, Path):
                with open(sql_or_path.as_posix(), "rt") as f:
                    sql = f.read()
            else:
                sql = sql_or_path
        except OSError as e:
            if e.strerror == "File name too long":
                sql = sql_or_path
            else:
                raise e
        return sql

    @property
    def sku_query(self):
        sku_query = self.get_sql_cmd(sql_or_path=self.sku_query_or_path)
        return sku_query

    @staticmethod
    def check_url(url):
        try:
            response = requests.get(
                url,
                stream=True,
                headers={
                    "User-Agent": "python-requests/2.31.0 tfg_bypass_bot",
                    "Accept-Encoding": "gzip, deflate",
                    "Accept": "*/*",
                    "Connection": "keep-alive",
                },
            )
            response.raise_for_status()
            return response.url
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            return False

    def create_original_url(self, product_sku, product_name):
        clean_product_name = product_name.replace(
            product_name.strip(), product_name.strip().replace(" ", "-")
        )
        return (
            f"{self.base_product_url}/{product_sku}/"
            f"{clean_product_name}-{product_sku}-1-600x800.jpg"
        )

    def create_url(self, product_sku, permalink):
        return f"{self.base_product_url}/{product_sku}/{permalink}-1-600x800.jpg"

    def create_laydown(self, product_sku, permalink):
        return f"{self.base_product_url}/{product_sku}/{permalink}-LAYDOWN-600x800.jpg"

    def product_url_program(self, sku, product_type, df_skus):
        # First try to make the url with the self.create_original_url...
        translation_table = str.maketrans("éàèùâêîôûç", "eaeuaeiouc")
        name_list = df_skus[df_skus.product_sku == sku].product_name.unique().tolist()
        name_list_sec = df_skus[df_skus.product_sku == sku].label.unique().tolist()
        found_working_url = False
        for name in name_list:
            if product_type == "Product":
                name = re.sub(r" \(.*\)", "", name)
                name = re.sub(r"&", "and", name)
            else:
                name = re.sub(r"&", "and", name)
                name = re.sub(r"'", "", name)
            if self.check_url(self.create_original_url(sku, name)):
                url = self.create_original_url(sku, name)
                found_working_url = True
                print(f"SKU: {sku}. Traditional = {url}")
                break

        # if labels in ultra_merchant.product does not work,
        # check with labels in ultra_merchant_history.product
        if not found_working_url and product_type == "Product":
            for name in name_list_sec:
                name = re.sub(r" \(.*\)", "", name)
                name = re.sub(r"&", "and", name)
                name = re.sub(r"NEW ", "", name)
                name = re.sub(r"[\',.!?;]", "", name)
                name = re.sub(r"/", "-", name)
                name = re.sub(r"\+", "and", name)
                name = name.translate(translation_table)
                if self.check_url(self.create_original_url(sku, name)):
                    url = self.create_original_url(sku, name)
                    found_working_url = True
                    print(f"SKU: {sku}. Alternate = {url}")
                    break
                elif self.check_url(self.create_laydown(sku, f"{name}-{sku}")):
                    url = self.create_laydown(sku, f"{name}-{sku}")
                    found_working_url = True
                    print(f"SKU: {sku}. Alternate laydown = {url}")
                    break

        # Now make it with second call if it didn't work with first...
        if not found_working_url and product_type == "Product":
            mpid_list = (
                df_skus[df_skus.product_sku == sku].master_product_id.unique().tolist()
            )
            name = df_skus[df_skus.product_sku == sku].product_name.reset_index(
                drop=True
            )[0]
            for i, mpid in enumerate(mpid_list):
                redirect = self.check_url(f"https://www.savagex.com/products/{mpid}")
                print(f"SKU: {sku}. MPid {i + 1} of {len(mpid_list)}. MPid = {mpid}")
                if redirect:
                    # Fix up the thingy and then send it to the other..
                    permalink = re.search(
                        r"(?<=https://www.savagex.com/shop/).*(?=-\d*\Z)", redirect
                    )
                    if permalink:
                        permalink = permalink[0]

                    # Ignoring checking urls with permalink as None to avoid 404 errors.
                    if permalink is None:
                        print(
                            f"Permalink is None for SKU: {sku}, mpid: {mpid}. Skipping....."
                        )
                        continue

                    url = self.create_url(sku, permalink)
                    print(mpid_list)
                    print(f"SKU: {sku}. Redirect = {redirect}")
                    if self.check_url(url):
                        found_working_url = True
                        break
                    url = self.create_laydown(sku, permalink)
                    if self.check_url(url):
                        found_working_url = True
                        break

        if found_working_url:
            link_exists = 1
        else:
            link_exists = 0
            url = "https://i.imgur.com/lHE8O17.jpg"
            print(f"{sku} {name} will use default url {url}")
        return [sku, link_exists, product_type, url]

    def find_working_urls(self, df_skus, product_type, break_count):
        rows = []
        dead_link_count = 0
        df_skus["product_sku"] = df_skus.groupby(
            df_skus["product_sku"].str.lower().str.strip()
        )["product_sku"].transform("first")
        sku_list = list(df_skus.product_sku.unique())
        sku_list.reverse()

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    self.product_url_program,
                    sku=sku,
                    product_type=product_type,
                    df_skus=df_skus,
                )
                for sku in sku_list
            ]
            for count, future in enumerate(as_completed(futures), 1):
                print(
                    f"SKU {count} completed with running percent of {dead_link_count / count}"
                )
                result = future.result()
                if not result[1]:
                    dead_link_count += 1
                rows.append(result)
                if count == break_count:
                    break

        df = pd.DataFrame(
            rows,
            columns=[
                "product_sku",
                "returns_product_image",
                "product_type",
                "image_url",
            ],
        )
        total_links = df_skus.product_sku.unique().shape[0]
        print(
            f"There were {dead_link_count} of {total_links} links not working i.e. "
            f"{dead_link_count / total_links}"
        )
        return df

    def execute(self, context=None):
        self.database = get_effective_database(self.database, self)
        self.output_table = f"{self.database}.{self.schema}.{self.table}"
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id=conn_ids.Snowflake.default,
            database=self.database,
            schema=self.schema,
        )
        query_tag = snowflake.generate_query_tag_cmd(self.dag_id, self.task_id)
        with ConnClosing(snowflake_hook.get_conn()) as conn, conn.cursor() as cursor:
            cursor.execute(query_tag)
            df = pd.read_sql(self.sku_query, con=conn)
            df_skus = df.rename(columns=str.lower)
            df_urls = self.find_working_urls(df_skus, self.sku_type, self.break_count)
            sql_cmd = f"truncate table {self.database}.{self.schema}.{self.table}"
            cursor.execute(sql_cmd)
            write_pandas(
                conn=conn,
                df=df_urls,
                table_name=self.table,
                schema=self.schema,
                database=self.database,
                quote_identifiers=False,
            )
