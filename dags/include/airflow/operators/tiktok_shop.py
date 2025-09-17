from functools import cached_property

import pendulum
from airflow.models import BaseOperator
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.hooks.tiktok_shop import TiktokShopHook
from include.airflow.operators.rows_to_s3 import (
    BaseRowsToS3CsvOperator,
    BaseRowsToS3CsvWatermarkOperator,
)
from include.config import conn_ids


class TikTokShopBaseOperator(BaseOperator):
    """ """

    def __init__(
        self,
        tiktok_shop_conn_id: str = conn_ids.Tiktok.shop_default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.tiktok_conn_id = tiktok_shop_conn_id
        self.url = "https://open-api.tiktokglobalshop.com"
        self.page_size = 10

    @cached_property
    def hook(self):
        return TiktokShopHook(tiktok_conn_id=self.tiktok_conn_id)

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook()

    def get_authorized_shops(self):
        response = self.hook.make_request(
            url=self.url, path="/authorization/202309/shops", method="GET"
        )
        shops = response["shops"]

        if len(shops) > 1:
            raise Exception(
                "Multiple shops found, current implementation only supports one shop"
            )
        return shops[0]

    def list_objects(self, path, params, data=None):
        next_token = None
        while True:
            if next_token:
                params.update({"page_token": next_token})

            response = self.hook.make_request(
                url=self.url, path=path, params=params, data=data, method="POST"
            )

            yield response
            if not response.get("next_page_token"):
                break
            next_token = response["next_page_token"]


class TiktokShopEndpointToS3Operator(
    BaseRowsToS3CsvWatermarkOperator, TikTokShopBaseOperator
):
    def __init__(
        self,
        endpoint: str,
        response_obj: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.response_obj = response_obj
        self.page_size = 50

    def get_rows(self):
        shop = self.get_authorized_shops()
        shop_cipher = shop["cipher"]

        params = {
            "shop_cipher": shop_cipher,
            "sort_field": "update_time",
            "page_size": self.page_size,
        }

        body = (
            {"update_time_ge": int(self.low_watermark)}
            if self.low_watermark != "None"
            else {}
        )

        for obj in self.list_objects(self.endpoint, params, body):
            if obj.get(self.response_obj):
                for data in obj[self.response_obj]:
                    yield {**data, "shop_name": shop["name"]}

    def get_high_watermark(self) -> str:
        return str(int(pendulum.DateTime.utcnow().timestamp()))


class TiktokShopProductToS3Operator(
    BaseRowsToS3CsvWatermarkOperator, TikTokShopBaseOperator
):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.page_size = 50

    def get_rows(self):
        shop = self.get_authorized_shops()
        shop_cipher = shop["cipher"]

        params = {
            "shop_cipher": shop_cipher,
            "sort_field": "update_time",
            "page_size": self.page_size,
        }

        body = (
            {"update_time_ge": int(self.low_watermark)}
            if self.low_watermark != "None"
            else {}
        )

        for products_obj in self.list_objects(
            "/product/202312/products/search", params, body
        ):
            if products_obj.get("products"):
                for product in products_obj["products"]:
                    response = self.hook.make_request(
                        url=self.url,
                        path=f"/product/202309/products/{product['id']}",
                        method="GET",
                        params={"shop_cipher": shop["cipher"]},
                        data=None,
                    )
                    yield {**response, "shop_name": shop["name"]}

    def get_high_watermark(self) -> str:
        return str(int(pendulum.DateTime.utcnow().timestamp()))


class TiktokShopInventoryToS3Operator(TikTokShopBaseOperator, BaseRowsToS3CsvOperator):
    def get_rows(self):
        shop = self.get_authorized_shops()
        shop_cipher = shop["cipher"]

        params = {
            "shop_cipher": shop_cipher,
            "sort_field": "update_time",
            "page_size": self.page_size,
        }

        for product in self.list_objects(
            "/product/202312/products/search", params, None
        ):
            if product.get("products"):
                product_ids = list(map(lambda x: x["id"], product["products"]))

                inventory_params = {"shop_cipher": shop_cipher}
                inventory_data = {"product_ids": product_ids}

                response = self.hook.make_request(
                    url=self.url,
                    path="/product/202309/inventory/search",
                    method="POST",
                    params=inventory_params,
                    data=inventory_data,
                )

                for inventory in response["inventory"]:
                    yield {**inventory, "shop_name": shop["name"]}
