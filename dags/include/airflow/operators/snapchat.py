import pandas as pd
from functools import cached_property

from include.airflow.hooks.snapchat import SnapchatAdsHook
from include.airflow.operators.snowflake import SnowflakeWatermarkSqlOperator
from include.utils.snowflake import generate_query_tag_cmd
from include.config import conn_ids


class SnapchatConversionsExportOperator(SnowflakeWatermarkSqlOperator):
    """
    Operator for posting converstions to Snapchat
    """

    def __init__(self, pixel_code, store, *args, **kwargs):
        self.pixel_code = pixel_code
        self.store = store
        super().__init__(sql_or_path="", *args, **kwargs)

    @cached_property
    def snapchat_hook(self):
        return SnapchatAdsHook()

    def prepare_batch(self):
        log_query = f"""
        INSERT INTO reporting_media_base_prod.snapchat.conversions_history_audit
        SELECT '{self.pixel_code}' pixel_id,
           CASE WHEN event ='order_completed' then 'PURCHASE' ELSE 'SIGN_UP' END AS event_type,
           'WEB' event_conversion_type,
           date_part(epoch_second, timestamp) timestamp,
           timestamp original_timestamp,
           email hashed_email,
           phone hashed_phone_number,
           user_agent,
           sha2(ip) hashed_ip_address,
           ip,
           price,
           currency,
           event_id transaction_id,
           event_id client_dedup_id,
           click_id,
           page_url,
           customer_gender gender,
           store_id,
           store,
           current_timestamp as event_post_datetime,
           first_name,
           last_name,
           zp
        FROM reporting_media_base_prod.dbo.conversions
        WHERE meta_update_datetime > '{self.low_watermark}'
            AND timestamp > dateadd(day, -37, current_timestamp())
            AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
            AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
            AND store_brand_abbr||store_country='{self.store}'
            AND event in ('complete_registration','order_completed')

        UNION ALL

        SELECT '{self.pixel_code}',
            CASE WHEN lower(customer_gender) = 'f' and store='FLUS' and event='complete_registration' then 'CUSTOM_EVENT_1'
            when lower(customer_gender) = 'f' and store='FLUS' and event='order_completed' then 'CUSTOM_EVENT_2'
            when lower(customer_gender) = 'm' and store='FLUS' and event='complete_registration' then 'CUSTOM_EVENT_3'
            when lower(customer_gender) = 'm' and store='FLUS' and event='order_completed' then 'CUSTOM_EVENT_4'
            END as event_type,
           'WEB' event_conversion_type,
           date_part(epoch_second, timestamp) timestamp,
           timestamp original_timestamp,
           email hashed_email,
           phone hashed_phone_number,
           user_agent,
           sha2(ip) hashed_ip_address,
           ip,
           price,
           currency,
           event_id transaction_id,
           event_id client_dedup_id,
           click_id,
           page_url,
           customer_gender gender,
           store_id,
           store,
           current_timestamp as event_post_datetime,
           first_name,
           last_name,
           zp
        FROM reporting_media_base_prod.dbo.conversions
        WHERE meta_update_datetime > '{self.low_watermark}'
            AND store = 'FLUS'
            AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
            AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
            AND store_brand_abbr||store_country='{self.store}'
            AND timestamp > dateadd(day, -37, current_timestamp())
            AND event in ('complete_registration','order_completed');
        """
        print(log_query)
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        cur.execute(log_query)
        print(cur.fetchall())

        query = f"""
        SELECT '{self.pixel_code}' pixel_id,
           CASE WHEN event ='order_completed' then 'PURCHASE' ELSE 'SIGN_UP' END AS event_type,
           'WEB' event_conversion_type,
           date_part(epoch_second, timestamp) timestamp,
           email hashed_email,
           phone hashed_phone_number,
           user_agent,
           sha2(ip) hashed_ip_address,
           price,
           currency,
           event_id transaction_id,
           event_id client_dedup_id,
           click_id,
           page_url,
           customer_gender gender,
           store_id,
           store,
           sha2(first_name) hashed_first_name_sha,
           sha2(last_name) hashed_last_name_sha,
           sha2(zp) hashed_zip
        FROM reporting_media_base_prod.dbo.conversions
        WHERE meta_update_datetime > '{self.low_watermark}'
            AND timestamp > dateadd(day, -37, current_timestamp())
            AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
            AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
            AND store_brand_abbr||store_country='{self.store}'
            AND event in ('complete_registration','order_completed')

        UNION ALL

        SELECT '{self.pixel_code}',
            CASE WHEN lower(customer_gender) = 'f' and store='FLUS' and event='complete_registration' then 'CUSTOM_EVENT_1'
            when lower(customer_gender) = 'f' and store='FLUS' and event='order_completed' then 'CUSTOM_EVENT_2'
            when lower(customer_gender) = 'm' and store='FLUS' and event='complete_registration' then 'CUSTOM_EVENT_3'
            when lower(customer_gender) = 'm' and store='FLUS' and event='order_completed' then 'CUSTOM_EVENT_4'
            END as event_type,
           'WEB' event_conversion_type,
           date_part(epoch_second, timestamp) timestamp,
           email hashed_email,
           phone hashed_phone_number,
           user_agent,
           sha2(ip) hashed_ip_address,
           price,
           currency,
           event_id transaction_id,
           event_id client_dedup_id,
           click_id,
           page_url,
           customer_gender gender,
           store_id,
           store,
           sha2(first_name) hashed_first_name_sha,
           sha2(last_name) hashed_last_name_sha,
           sha2(zp) hashed_zip
        FROM reporting_media_base_prod.dbo.conversions
        WHERE meta_update_datetime > '{self.low_watermark}'
            AND store = 'FLUS'
            AND not (replace(coalesce(first_name,''),' ','') ilike any ('api','test','example'))
            AND not (replace(coalesce(last_name,''),' ','') ilike any ('api','test','example'))
            AND store_brand_abbr||store_country='{self.store}'
            AND timestamp > dateadd(day, -37, current_timestamp())
            AND event in ('complete_registration','order_completed');
        """
        print(query)
        cur.execute(query)
        batch_count = 0
        while True:
            batch_count += 1
            print(f"fetching rows: batch {batch_count}")
            rows = cur.fetchmany(200)
            if rows:
                yield pd.DataFrame(rows, columns=[x[0] for x in cur.description])
            else:
                break

    def prepare_json(self, df):
        json_data = []
        for index, row in df.iterrows():
            json = {
                "pixel_id": row.PIXEL_ID,
                "event_type": row.EVENT_TYPE,
                "event_conversion_type": row.EVENT_CONVERSION_TYPE,
                "timestamp": row.TIMESTAMP,
                "hashed_email": row.HASHED_EMAIL,
                "hashed_phone_number": row.HASHED_PHONE_NUMBER,
                "user_agent": row.USER_AGENT,
                "hashed_ip_address": row.HASHED_IP_ADDRESS,
                "client_dedup_id": row.CLIENT_DEDUP_ID,
                "click_id": str(row.CLICK_ID),
                "page_url": row.PAGE_URL,
                "price": str(row.PRICE),
                "currency": row.CURRENCY,
                "transaction_id": str(row.TRANSACTION_ID),
                "hashed_first_name_sha": row.HASHED_FIRST_NAME_SHA,
                "hashed_last_name_sha": row.HASHED_LAST_NAME_SHA,
                "hashed_zip": str(row.HASHED_ZIP),
            }
            json_data.append(json)
        return json_data

    def post_data(self, json_data):
        hook = SnapchatAdsHook(snapchat_conn_id=conn_ids.Snapchat.conversions_default)
        conn = hook.get_conn_for_conversions_api()
        response = conn.request(
            method='POST', url='https://tr.snapchat.com/v2/conversion', json=json_data
        )
        if response.status_code == 200:
            try:
                response_raw = response.json()
                if response_raw['status'] != "SUCCESS":
                    raise ValueError(f"POST request failed with('{response_raw}').")
                print(response_raw)
            except Exception as e:
                print(f"Exception occured: ", e)
        else:
            print(f"headers: {response.headers}")
            print(f"content: {response.content}")
            print(f"status_code: {response.status_code}")
            response.raise_for_status()
            raise Exception(response.content)

    def watermark_execute(self, context=None):
        for df in self.prepare_batch():
            json_data = self.prepare_json(df)
            self.post_data(json_data)
