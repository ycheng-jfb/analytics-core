import json
from typing import Dict, Iterable, List

import pandas as pd
import requests
from facebook_business import FacebookAdsApi
from facebook_business.adobjects.serverside.custom_data import CustomData
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.user_data import UserData

from include.airflow.hooks.facebook import FacebookAdsHook
from include.airflow.operators.snowflake import SnowflakeWatermarkSqlOperator
from include.config import conn_ids
from include.utils.data_structures import chunk_df, chunk_list
from include.utils.snowflake import generate_query_tag_cmd
from include.airflow.callbacks.slack import send_slack_message, send_slack_alert


class SegmentFacebookConversionsExportOperator(SnowflakeWatermarkSqlOperator):
    def __init__(self, pixel_id, store, country, *args, **kwargs):
        super().__init__(sql_or_path="", *args, **kwargs)
        self._hooks: Dict[str, FacebookAdsHook] = {}
        self.pixel_id = pixel_id
        self.store = store
        self.country = country
        self.failed_batch = []
        self.failed_records = []
        self.total_records = 0

    def get_user_list_chunks(self, low_watermark: str) -> Iterable[List]:
        alert_query = f"""
            SELECT event
            FROM reporting_media_base_prod.dbo.conversions
            WHERE timestamp > dateadd('hour',-4,current_timestamp())
                AND (event_id != '' AND event_id IS NOT NULL)
                AND store_brand_abbr =  '{self.store}'
                AND store_country IN ({','.join(self.country)})
              group by store_brand_abbr,store_country,event
              having count(*) = 0;
        """
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        cur.execute(alert_query)
        events = [row[0] for row in cur.fetchall()]
        if events:
            message = f""":no_entry: Dag *{self.task_id}* has no events in last 4 hours for pixel_id {self.pixel_id} with event_type - {events}. <!here>
*DAG*: {self.dag_id}
        """
            send_slack_message(message=message, conn_id=conn_ids.SlackAlert.media_p1)

        log_query = f"""
        insert into reporting_media_base_prod.facebook.conversions_history_audit
             SELECT
               date_part(epoch_second, timestamp::TIMESTAMP) as event_time_unix,
               timestamp event_time,
               event_id,
               IFF(len(email) <64,NULL,email) email,
               {self.pixel_id}::number(38,0) PIXEL_ID,
               store_region region,
               CASE WHEN event ='order_completed' then 'Purchase'
                    WHEN event = 'non_activating_order' then 'Subscribe'
                    WHEN event = 'product_added' then 'AddToCart'
                    WHEN event = 'product_viewed' then 'ViewContent'
                ELSE 'CompleteRegistration' END AS event_name,
               parse_json('{{' || '"' || 'currency' || '"' || ':' || '"' || ifnull(currency,'')  || '"'
                    || ', ' || '"' || 'value' || '"'|| ':' || revenue || '}}') AS custom_data,
               ip,
               user_agent,
               external_id subscription_id,
               'fb.1.' || date_part(epoch_millisecond, timestamp::TIMESTAMP) || '.' || click_id AS fbc,
               IFF(db is null,db::varchar,sha2(db)) db_sha,
               db,
               sha2(zp) zp_sha,
               zp,
               IFF(country is null or country='',country::varchar,sha2(country)) country_sha,
               country,
               external_id,
               sha2(lower(first_name)) first_name_sha,
               first_name,
               sha2(lower(last_name)) last_name_sha,
               last_name,
               sha2(phone) phone_sha,
               phone,
               current_timestamp as event_post_datetime,
               CASE WHEN event = 'product_added' AND store_brand_abbr = 'SX' AND store_region = 'NA'
                    THEN 'internal_capi' END AS partner_agent
            FROM reporting_media_base_prod.dbo.conversions
            WHERE meta_update_datetime > '{low_watermark}'
                AND external_id is not null
                AND (event in ('complete_registration', 'non_activating_order') or custom_data is not null)
                AND (event_id != '' AND event_id IS NOT NULL)
                AND timestamp > dateadd(day, -7, CONVERT_TIMEZONE('UTC', current_timestamp()))::timestamp
                AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
                AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
                AND store_country IN ({','.join(self.country)})
                AND case when 'FL' = '{self.store}' and  store_country = 'US' then ((store_brand_abbr = 'YTY'  and event = 'order_completed') or store_brand_abbr = 'FL')
                    else store_brand_abbr = '{self.store}' end
            UNION
            SELECT
               date_part(epoch_second, timestamp::TIMESTAMP) as event_time_unix,
               timestamp event_time,
               event_id,
               IFF(len(email) <64,NULL,email) email,
               {self.pixel_id}::number(38,0) PIXEL_ID,
               store_region region,
               'Subscribe' event_name,
               parse_json('{{' || '"' || 'currency' || '"' || ':' || '"' || ifnull(currency,'')  || '"'
                    || ', ' || '"' || 'value' || '"'|| ':' || revenue || '}}') AS custom_data,
               ip,
               user_agent,
               external_id subscription_id,
               'fb.1.' || date_part(epoch_millisecond, timestamp::TIMESTAMP) || '.' || click_id AS fbc,
               IFF(db is null,db::varchar,sha2(db)) db_sha,
               db,
               sha2(zp) zp_sha,
               zp,
               IFF(country is null or country='',country::varchar,sha2(country)) country_sha,
               country,
               external_id,
               sha2(lower(first_name)) first_name_sha,
               first_name,
               sha2(lower(last_name)) last_name_sha,
               last_name,
               sha2(phone) phone_sha,
               phone,
               current_timestamp as event_post_datetime,
               CASE WHEN event = 'product_added' AND store_brand_abbr = 'SX' AND store_region = 'NA'
                    THEN 'internal_capi' END AS partner_agent
            FROM reporting_media_base_prod.dbo.conversions
            WHERE meta_update_datetime > '{low_watermark}'
                AND event ='order_completed'
                AND external_id is not null
                AND (event_id != '' AND event_id IS NOT NULL)
                AND timestamp > dateadd(day, -7, CONVERT_TIMEZONE('UTC', current_timestamp()))::timestamp
                AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
                AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
                AND store_country IN ({','.join(self.country)})
                AND store_brand_abbr = '{self.store}';
        """
        print(log_query)
        cur.execute(log_query)
        print(cur.fetchall())

        query = f"""
            SELECT
               date_part(epoch_second, timestamp::TIMESTAMP) as event_time,
               event_id,
               IFF(len(email) <64,NULL,email) email,
               {self.pixel_id}::number(38,0),
               store_region region,
               CASE WHEN event ='order_completed' then 'Purchase'
                    WHEN event = 'non_activating_order' then 'Subscribe'
                    WHEN event = 'product_added' then 'AddToCart'
                    WHEN event = 'product_viewed' then 'ViewContent'
                ELSE 'CompleteRegistration' END AS event_name,
               parse_json('{{' || '"' || 'currency' || '"' || ':' || '"' || ifnull(currency,'') || '"'
                    || ', ' || '"' || 'value' || '"'|| ':' || revenue || '}}') AS custom_data,
               ip,
               user_agent,
               external_id subscription_id,
               'fb.1.' || date_part(epoch_millisecond, timestamp::TIMESTAMP) || '.' || click_id AS fbc,
               IFF(db is null,db::varchar,sha2(db)),
               sha2(zp),
               IFF(country is null or country='',country::varchar,sha2(country)),
               external_id,
               sha2(lower(first_name)),
               sha2(lower(last_name)),
               phone,
               contents,
               CASE WHEN event = 'product_added' AND store_brand_abbr = 'SX' AND store_region = 'NA'
                    THEN 'internal_capi' END AS partner_agent
            FROM reporting_media_base_prod.dbo.conversions
            WHERE meta_update_datetime > '{low_watermark}'
                AND timestamp <= CONVERT_TIMEZONE('UTC', current_timestamp())::timestamp_ntz
                AND (event in ('complete_registration', 'non_activating_order') or custom_data is not null)
                AND (external_id != '' AND external_id is not null)
                AND (event_id != '' AND event_id IS NOT NULL)
                AND timestamp > dateadd(day, -7, CONVERT_TIMEZONE('UTC', current_timestamp()))::timestamp
                AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
                AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
                AND store_country IN ({','.join(self.country)})
                AND case when 'FL' = '{self.store}' and  store_country = 'US' then ((store_brand_abbr = 'YTY'  and event = 'order_completed') or store_brand_abbr = 'FL')
                    else store_brand_abbr = '{self.store}' end
            UNION
            SELECT
               date_part(epoch_second, timestamp::TIMESTAMP) as event_time,
               event_id,
               IFF(len(email) <64,NULL,email) email,
               {self.pixel_id}::number(38,0),
               store_region region,
               'Subscribe' event_name,
               parse_json('{{' || '"' || 'currency' || '"' || ':' || '"' || ifnull(currency,'') || '"'
                    || ', ' || '"' || 'value' || '"'|| ':' || revenue || '}}') AS custom_data,
               ip,
               user_agent,
               external_id subscription_id,
               'fb.1.' || date_part(epoch_millisecond, timestamp::TIMESTAMP) || '.' || click_id AS fbc,
               IFF(db is null,db::varchar,sha2(db)),
               sha2(zp),
               IFF(country is null or country='',country::varchar,sha2(country)),
               external_id,
               sha2(lower(first_name)),
               sha2(lower(last_name)),
               phone,
               contents,
               CASE WHEN event = 'product_added' AND store_brand_abbr = 'SX' AND store_region = 'NA'
                    THEN 'internal_capi' END AS partner_agent
            FROM reporting_media_base_prod.dbo.conversions
            WHERE meta_update_datetime > '{low_watermark}'
                AND timestamp <= CONVERT_TIMEZONE('UTC', current_timestamp())::timestamp_ntz
                AND event ='order_completed'
                AND (external_id != '' AND external_id is not null)
                AND (event_id != '' AND event_id IS NOT NULL)
                AND timestamp > dateadd(day, -7, CONVERT_TIMEZONE('UTC', current_timestamp()))::timestamp
                AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
                AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
                AND store_country IN ({','.join(self.country)})
                AND store_brand_abbr = '{self.store}'
            ORDER BY event_time, event_name;
        """
        print(query)
        cur.execute(query)
        self.total_records = cur.rowcount
        batch_count = 0
        while True:
            batch_count += 1
            print(f"fetching rows: batch {batch_count}")
            rows = cur.fetchmany(100000)
            if rows:
                yield rows
            else:
                break

    def post_conversions(self, pixel_df, pixel_id):
        print(f"Posting conversions on pixel id {pixel_id}")
        batch_num = 0
        for df_chunk in chunk_df(df=pixel_df, chunk_size=1000):
            batch_num += 1
            print(f"batch_num: {batch_num}")
            records = list(df_chunk.values.tolist())
            events = []
            for record in records:

                if record[3] in ['Purchase', 'CancelSubscription', 'AddToCart', 'ViewContent']:
                    user_data = UserData(
                        email=record[2],
                        zip_code=record[9],
                        date_of_birth=record[10],
                        country_code=record[11],
                        external_id=record[12],
                        first_name=record[13],
                        last_name=record[14],
                        phone=record[15],
                        # phone=record[5],
                        client_ip_address=record[5],
                        client_user_agent=record[6],
                        subscription_id=record[7],
                        fbc=record[8],
                    )
                    cd = json.loads(record[4])
                    content_ids = [con['content_id'] for con in json.loads(record[16])]
                    custom_data = CustomData(
                        currency=cd['currency'],
                        value=float(cd['value']),
                        order_id=record[1] if record[3] == 'Purchase' else None,
                        content_ids=content_ids,
                        content_type="product_group",
                        # custom_properties={"partner_agent": "internal_capi"} if record[3] == 'AddToCart' else None
                        custom_properties={"partner_agent": record[17]},
                    )
                    event = Event(
                        event_name=record[3],
                        event_time=record[0],
                        user_data=user_data,
                        event_id=record[1],
                        custom_data=custom_data,
                    )
                else:
                    user_data = UserData(
                        email=record[2],
                        zip_code=record[9],
                        date_of_birth=record[10],
                        country_code=record[11],
                        external_id=record[12],
                        first_name=record[13],
                        last_name=record[14],
                        phone=record[15],
                        # phone=record[5],
                        client_ip_address=record[5],
                        client_user_agent=record[6],
                        fbc=record[8],
                    )

                    event = Event(
                        event_name=record[3],
                        event_time=record[0],
                        user_data=user_data,
                        event_id=record[1],
                        # custom_data=CustomData(custom_properties={"partner_agent":"internal_capi"})
                    )
                events.append(event)
            event_request = EventRequest(events=events, pixel_id=pixel_id)
            try:
                event_response = event_request.execute()
                self.log.info(f"events posted status:{event_response}")
            except Exception as e:
                self.failed_batch.extend(events)
                self.log.info("Failed with the error:", e)

    def post_failed_batch(self, prev_batch_size):
        failed_records = self.failed_batch
        self.failed_batch = []
        batch_size = max(1, len(failed_records) // 10)
        if batch_size == prev_batch_size:
            batch_size = batch_size // 2
        elif batch_size > prev_batch_size != 0:
            batch_size = max(1, prev_batch_size // 2)
        for chunk in chunk_list(failed_records, size=batch_size):
            event_request = EventRequest(events=chunk, pixel_id=self.pixel_id)
            try:
                event_response = event_request.execute()
                self.log.info(f"events posted status:{event_response}")
            except Exception as e:
                self.log.info("Failed with the error:", e)
                if len(chunk) == 1:
                    self.failed_records.extend(chunk)
                    chunk[0].user_data.fbc = None
                    event_req = EventRequest(events=chunk, pixel_id=self.pixel_id)
                    self.log.info(f"Failed events posted status:{event_req.execute()}")
                else:
                    self.failed_batch.extend(chunk)
        if self.failed_batch:
            self.post_failed_batch(batch_size)

    def initialize_api(self, region):
        facebook_conn_id = "facebook_" + region.lower()
        if facebook_conn_id not in self._hooks:
            self._hooks[facebook_conn_id] = FacebookAdsHook(facebook_ads_conn_id=facebook_conn_id)
        FacebookAdsApi.init(
            access_token=self._hooks[facebook_conn_id].access_token,
            api_version=self._hooks[facebook_conn_id].api_version,
        )

    def watermark_execute(self, context=None):
        print("in watermark_execute")
        for users in self.get_user_list_chunks(low_watermark=self.low_watermark):
            df = pd.DataFrame(
                users,
                columns=[
                    'EVENT_TIME',
                    'EVENT_ID',
                    'EMAIL',
                    'PIXEL_ID',
                    'REGION',
                    'EVENT_NAME',
                    'CUSTOM_DATA',
                    # 'PHONE',
                    'IP',
                    'USER_AGENT',
                    'SUBSCRIPTION_ID',
                    'FBC',
                    'ZP',
                    'DB',
                    'COUNTRY',
                    'EXTERNAL_ID',
                    'FIRST_NAME',
                    'LAST_NAME',
                    'PHONE',
                    'CONTENTS',
                    'PARTNER_AGENT',
                ],
            )
            df.set_index(["REGION"], inplace=True)
            for region in df.index.unique():
                print(f"region: {region}")
                self.initialize_api(region=region)
                region_df = df.loc[[region]]
                region_df.set_index(["PIXEL_ID"], inplace=True)
                for pixel_id in region_df.index.unique():
                    print(f"pixel_id: {pixel_id}")
                    pixel_df = region_df.loc[[pixel_id]]
                    self.post_conversions(pixel_df=pixel_df, pixel_id=pixel_id)
        if self.failed_batch:
            self.post_failed_batch(prev_batch_size=0)
            print(
                "Posted all the records in failed batch to CAPI by replacing click_ids with empty string"
            )
        if self.failed_records:
            cur = self.snowflake_hook.get_cursor()
            query = "insert into reporting_media_base_prod.facebook.capi_exceptions(failed_record) VALUES (%s)"
            for record in self.failed_records:
                cur.execute(query, json.dumps(record.to_dict()))

            message = f"""
                        :no_entry: Task *{self.task_id}* <!here>
*DAG*: {self.dag_id}
*There are {len(self.failed_records)} out of {self.total_records} records with discrepancies in FB Click id*
_Query the following table to get more details_:`reporting_media_base_prod.facebook.capi_exceptions`
                        """
            send_slack_message(message=message, conn_id=conn_ids.SlackAlert.media_p1)
            if len(self.failed_records) / self.total_records > 0.1:
                raise Exception(
                    f"More than 10% of click ids has issues i.e. {len(self.failed_records)} records has issues out of {self.total_records} records."
                )


class FacebookOfflineConversionsExportOperator(SnowflakeWatermarkSqlOperator):
    def __init__(self, pixel_id, store, country, *args, **kwargs):
        super().__init__(sql_or_path="", *args, **kwargs)
        self._hooks: Dict[str, FacebookAdsHook] = {}
        self.pixel_id = pixel_id
        self.store = store
        self.country = country

    def get_user_list_chunks(self, low_watermark: str) -> Iterable[List]:

        cur = self.snowflake_hook.get_cursor()

        query = f"""
        select event_name,
            date_part(epoch_second, event_time::TIMESTAMP) as event_time,
            sha2(lower(email)) as em,
            sha2(phone_number) as ph,
            iff(lower(gender) in ('m','f'), sha2(lower(gender)), sha2('f')) as gen,
            sha2(lower(last_name)) as ln,
            sha2(lower(first_name)) as fn,
            sha2(lower(city)) as ct,
            iff(lower(state) in ('unknown','[removed]', NULL), NULL, sha2(lower(state))) as st,
            iff(lower(default_postal_code) = 'unknown', NULL,
                    sha2(left(lower(REGEXP_REPLACE(default_postal_code, ' ', '')),5))) as zip,
            sha2(lower(country)) as country,
            customer_id as external_id,
            lower(currency) as currency,
            value,
            order_id,
            {self.pixel_id}::number(38,0) as pixel_id,
            IFF( lower(country) in ('us','ca'), 'NA' , 'EU' ) as REGION
        from reporting_media_prod.dbo.facebook_offline_conversion
            WHERE meta_update_datetime > '{low_watermark}'
                AND store_brand_abbr =  '{self.store}'
                AND country IN ({','.join(self.country)})
;
        """
        print(query)
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        cur.execute(query)
        batch_count = 0
        while True:
            batch_count += 1
            print(f"fetching rows: batch {batch_count}")
            rows = cur.fetchmany(100000)
            if rows:
                yield rows
            else:
                break

    def post_conversions(self, pixel_df, pixel_id, conn_id):
        print(f"Posting conversions on pixel id {pixel_id}")
        batch_num = 0
        conn = FacebookAdsHook(facebook_ads_conn_id=conn_id)
        for df_chunk in chunk_df(df=pixel_df, chunk_size=1000):
            batch_num += 1
            print(f"batch_num: {batch_num}")
            records = list(df_chunk.values.tolist())
            events = []
            for record in records:
                event = {
                    "event_name": record[0],
                    "event_time": record[1],
                    "user_data": {
                        "em": [record[2]],
                        "ph": [record[3]],
                        "gen": record[4],
                        "ln": record[5],
                        "fn": record[6],
                        "ct": record[7],
                        "st": record[8],
                        "zip": record[9],
                        "country": record[10],
                        "external_id": record[11],
                    },
                    "custom_data": {
                        "currency": record[12],
                        "value": float(record[13]),
                        "order_id": record[14],
                    },
                    "action_source": "physical_store",
                }
                if event['user_data']['st'] is None:
                    del event['user_data']['st']
                if event['user_data']['zip'] is None:
                    del event['user_data']['zip']
                events.append(event)

            payload = {
                "data": str(events),
                "access_token": conn.access_token,
            }
            response = requests.request(
                "POST",
                f"https://graph.facebook.com/{conn.api_version}/{pixel_id}/events",
                headers={},
                data=payload,
                files=[],
            )
            print(response.json())
            response.raise_for_status()
            self.log.info(f"events posted status:{response.json()}")

    def watermark_execute(self, context=None):
        print("in watermark_execute")
        for users in self.get_user_list_chunks(low_watermark=self.low_watermark):
            df = pd.DataFrame(
                users,
                columns=[
                    'EVENT_TIME',
                    'EVENT_ID',
                    'EM',
                    'PH',
                    'GEN',
                    'LN',
                    'FN',
                    'CT',
                    'ST',
                    'ZIP',
                    'COUNTRY',
                    'EXTERNAL_ID',
                    'CURRENCY',
                    'VALUE',
                    'ORDER_ID',
                    'PIXEL_ID',
                    'REGION',
                ],
            )
            df.set_index(["REGION"], inplace=True)
            for region in df.index.unique():
                print(f"region: {region}")
                region_df = df.loc[[region]]
                conn_id = "facebook_" + region.lower()
                self.post_conversions(pixel_df=region_df, pixel_id=self.pixel_id, conn_id=conn_id)
