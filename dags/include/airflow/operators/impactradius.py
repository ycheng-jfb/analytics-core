import json
from functools import cached_property
from typing import Iterable, List
from include.airflow.hooks.impactradius import ImpactRadiusHook
from include.airflow.operators.snowflake import SnowflakeWatermarkSqlOperator
from include.utils.snowflake import generate_query_tag_cmd


class ImpactRadiusConversionsExportOperator(SnowflakeWatermarkSqlOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(sql_or_path="", *args, **kwargs)

    @cached_property
    def impactradius_hook(self):
        return ImpactRadiusHook()

    def get_user_list_chunks(self, low_watermark: str) -> Iterable[List]:
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        # cur.execute(query_tag)
        query = f"""
            SELECT *
            FROM reporting_media_base_prod.dbo.impact_radius_ltk_all_events
            WHERE META_UPDATE_DATETIME > '{low_watermark}';
            """
        print(query)
        cur.execute(query)
        batch_count = 0
        while True:
            batch_count += 1
            print(f"fetching rows: batch {batch_count}")
            rows = cur.fetchmany(10000)
            if rows:
                yield rows
            else:
                break

    def post_data(self, users):
        for user in users:
            payload = json.dumps(
                {
                    "CampaignId": user[0],
                    "ActionTrackerId": user[1],
                    "EventDate": user[2].isoformat(),
                    "ClickId": user[3],
                    "OrderId": user[4],
                    "CustomerId": user[5],
                    "CustomerStatus": user[6],
                }
            )
            response = self.impactradius_hook.make_request(
                method="POST", path="/Conversions", data=payload
            )
            if response.status_code != 200:
                raise ValueError(f"POST request failed with('{response.text}').")
            print(f"posted {user[5]} successfully")

    def watermark_execute(self, context=None):
        print("in watermark_execute")
        for users in self.get_user_list_chunks(low_watermark=self.low_watermark):
            self.post_data(users)
