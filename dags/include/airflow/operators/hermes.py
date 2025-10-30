import pendulum
import requests
from functools import cached_property

from include.airflow.hooks.hermes import HermesHook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3JsonWatermarkOperator
from include.utils.snowflake import generate_query_tag_cmd
import pandas as pd


class HermesTrackingEvents(BaseRowsToS3JsonWatermarkOperator):
    def __init__(
        self,
        biz_unit,
        **kwargs,
    ):
        super().__init__(
            **kwargs,
        )
        self.biz_unit = biz_unit

    @cached_property
    def hermes_hook(self):
        return HermesHook(self.hook_conn_id)

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook()

    def get_high_watermark(self):
        pass

    def get_tracking_ids(self, data_shipped_cutoff):
        sql = f"""
                select
                ib.tracking_number
                from
                lake_view.ultra_warehouse.invoice_box ib
                inner join lake_view.ultra_warehouse.invoice i
                on ib.invoice_id = i.invoice_id
                inner join lake_view.ultra_warehouse.company bu
                on i.company_id = bu.company_id
                inner join lake_view.ultra_warehouse.company_carrier_service ccs
                on ib.company_carrier_service_id = ccs.company_carrier_service_id
                inner join lake_view.ultra_warehouse.carrier_service cs
                on ccs.carrier_service_id = cs.carrier_service_id
                inner join lake_view.ultra_warehouse.carrier c
                on cs.carrier_id = c.carrier_id
                where
                c.label like 'Hermes%'
                and ib.date_shipped > '{data_shipped_cutoff}'
                and ib.tracking_number not in
                (select TRACKING_ID from REPORTING_BASE_PROD.GSC.HERMES_DELIVERED_EVENTS)
                and bu.label = '{self.biz_unit}'
            """
        conn = self.snowflake_hook.get_conn()
        cur = conn.cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        cur.execute(sql)
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=['TRACKING_NUMBER'])
        return df.TRACKING_NUMBER

    def get_rows(self):
        now = pendulum.DateTime.utcnow()
        updated_at = {"updated_at": str(now)}
        low_watermark = pendulum.parse(self.get_low_watermark())
        data_shipped_cutoff = str(low_watermark.add(days=-28).date())

        tracking_ids = self.get_tracking_ids(data_shipped_cutoff)
        for track_id in tracking_ids:
            try:
                endpoint = f'events?barcode={track_id}&descriptionType=CLIENT'
                r = self.hermes_hook.make_request('GET', endpoint)
            except requests.exceptions.HTTPError:
                continue
            for event in r.json():
                event_formatted = {
                    'tracking_id': track_id,
                    'date_time': event.get('dateTime'),
                    'location_latitude': (
                        event['location'].get('latitude') if event.get('location') else None
                    ),
                    'location_longitude': (
                        event['location']['longitude'] if event.get('location') else None
                    ),
                    'links': event.get('links'),
                    'tracking_point_description': (
                        event['trackingPoint'].get('description')
                        if event.get('trackingPoint')
                        else None
                    ),
                    'tracking_point_tracking_point_id': (
                        event['trackingPoint'].get('trackingPointId')
                        if event.get('trackingPoint')
                        else None
                    ),
                }
                yield {**event_formatted, **updated_at}

        self.new_high_watermark = str(now)
