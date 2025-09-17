import pendulum
from functools import cached_property

from include.airflow.hooks.callminer import CallMinerHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvWatermarkOperator
from include.airflow.utils.utils import flatten_json, scrub_nones


class CallMinerContacts(BaseRowsToS3CsvWatermarkOperator):
    @cached_property
    def callminer_hook(self):
        return CallMinerHook(conn_id=self.hook_conn_id) if self.hook_conn_id else CallMinerHook()

    def get_high_watermark(self):
        return None

    def get_rows(self):
        params = {'startDate': self.low_watermark, 'stopDate': str(pendulum.now())}
        latest_update = self.low_watermark
        data = self.callminer_hook.make_get_request_all('export/lastupdated', params=params)

        count = 0
        for contact in data:
            count += 1
            if count % 10000 == 0:
                self.log.info(f'Record count: {count}')

            if latest_update < contact['LastModified']:
                latest_update = contact['LastModified']
            flat_contact = flatten_json(contact, flatten_list=False)
            scrub_nones(flat_contact)
            yield flat_contact

        # Ensures watermark moves forward even if no new contacts in interval
        if latest_update == self.low_watermark:
            latest_update = str(params['stopDate'])
        self.new_high_watermark = latest_update


class CallMinerContactsHistory(CallMinerContacts):
    def get_rows(self):
        self.new_high_watermark = str(pendulum.parse(self.low_watermark).add(days=-7))
        # low and high watermark reversed intentionally, process goes back in time
        params = {
            'startDate': self.new_high_watermark,
            'stopDate': self.low_watermark,
        }
        data = self.callminer_hook.make_get_request_all('export/lastupdated', params=params)

        for contact in data:
            yield flatten_json(contact, flatten_list=False)
