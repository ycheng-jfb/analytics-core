import json
import pandas as pd
from functools import cached_property

from include.airflow.hooks.tiktok import TiktokHook
from include.airflow.operators.snowflake import SnowflakeWatermarkSqlOperator
from include.utils.snowflake import generate_query_tag_cmd


class TiktokConversionsExportOperator(SnowflakeWatermarkSqlOperator):
    """
    Operator for posting converstions to tiktok
    """

    def __init__(self, pixel_code, store, *args, **kwargs):
        self.pixel_code = pixel_code
        self.store = store
        super().__init__(sql_or_path="", *args, **kwargs)

    @cached_property
    def tiktok_hook(self):
        return TiktokHook()

    def prepare_batch(self):
        log_query = f"""
                INSERT INTO reporting_media_base_prod.tiktok.conversions_history_audit
                SELECT
                    event_id,
                    timestamp,
                    click_id as call_back,
                    external_id,
                    IFF(len(email) <64,NULL,email) email,
                    currency,
                    revenue,
                    user_agent,
                    ip,
                    store,
                    '{self.pixel_code}' pixel_code,
                    meta_row_hash,
                    meta_create_datetime,
                    meta_update_datetime,
                    CASE WHEN event ='complete_registration' then 'CompleteRegistration'
                        WHEN event = 'product_added' then 'AddToCart'
                        WHEN event = 'product_viewed' then 'ViewContent'
                    ELSE 'CompletePayment' END AS event,
                    store_id,
                    customer_gender,
                    phone,
                    contents,
                    current_timestamp as event_post_datetime
                FROM reporting_media_base_prod.dbo.conversions
                WHERE store_brand_abbr||store_country='{self.store}'
                    AND timestamp > DATEADD(DAY, -8, CURRENT_TIMESTAMP())
                    AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
                    AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
                    AND meta_update_datetime > '{self.low_watermark}'
                    AND event != 'non_activating_order';
                """
        print(log_query)
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        cur.execute(log_query)
        print(cur.fetchall())

        query = f"""
            SELECT
                event_id,
                timestamp,
                click_id as call_back,
                external_id,
                IFF(len(email) <64,NULL,email) email,
                currency,
                revenue,
                user_agent,
                ip,
                store,
                '{self.pixel_code}' pixel_code,
                meta_row_hash,
                meta_create_datetime,
                meta_update_datetime,
                CASE WHEN event ='complete_registration' then 'CompleteRegistration'
                    WHEN event = 'product_added' then 'AddToCart'
                    WHEN event = 'product_viewed' then 'ViewContent'
                ELSE 'CompletePayment' END AS event,
                store_id,
                customer_gender,
                phone,
                contents
            FROM reporting_media_base_prod.dbo.conversions
            WHERE store_brand_abbr||store_country='{self.store}'
                AND timestamp > DATEADD(DAY, -8, CURRENT_TIMESTAMP())
                AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
                AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
                AND meta_update_datetime > '{self.low_watermark}'
                AND event != 'non_activating_order';
        """
        print(query)
        cur.execute(query)
        batch_count = 0
        while True:
            batch_count += 1
            print(f"fetching rows: batch {batch_count}")
            rows = cur.fetchmany(1000)
            if rows:
                yield pd.DataFrame(rows, columns=[x[0] for x in cur.description])
            else:
                break

    def del_none(self, d):
        """
        Delete keys with the value ``None`` in a dictionary, recursively.
        This alters the input so you may wish to ``copy`` the dict first.
        """
        for key, value in list(d.items()):
            if value is None:
                del d[key]
            elif isinstance(value, dict):
                self.del_none(value)
        return d

    def prepare_json(self, df):
        json_data = []
        for index, row in df.iterrows():
            json_obj = {
                "type": "track",
                "event": row.EVENT,
                "event_id": row.EVENT_ID,
                "timestamp": str(row.TIMESTAMP),
                "context": {
                    "ad": {"callback": row.CALL_BACK},
                    "user": {
                        "external_id": row.EXTERNAL_ID,
                        "email": row.EMAIL,
                        "phone_number": row.PHONE,
                    },
                    "user_agent": row.USER_AGENT,
                    "ip": row.IP,
                },
                "properties": {
                    "contents": row.CONTENTS if row.CONTENTS else None,
                    "currency": row.CURRENCY,
                    "value": row.REVENUE,
                },
            }
            json_data.append(self.del_none(json_obj))
        return json_data

    def post_data(self, json_data):
        payload = json.dumps(
            {
                "pixel_code": self.pixel_code,
                # "test_event_code": "TEST83428",
                "batch": json_data,
            }
        )
        response = self.tiktok_hook.make_request(
            method="POST", path="open_api/v1.2/pixel/batch/", data=payload
        )

        response_raw = response.json()
        if response_raw["code"] != 0:
            raise ValueError(f"POST request failed with('{response_raw}').")
        print(response_raw)

    def watermark_execute(self, context=None):
        for df in self.prepare_batch():
            df["REVENUE"] = df["REVENUE"].fillna(0)
            json_data = self.prepare_json(df)
            self.post_data(json_data)
