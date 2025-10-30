from functools import cached_property

import pandas as pd
import pendulum
import requests
from include.airflow.hooks.pivot88 import Pivot88Hook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.rows_to_s3 import (
    BaseRowsToS3CsvWatermarkOperator,
    BaseRowsToS3JsonWatermarkOperator,
)
from include.airflow.utils.utils import flatten_json_lookup_list
from include.utils.context_managers import ConnClosing
from include.utils.snowflake import generate_query_tag_cmd


class Pivot88InspectionsOperator(BaseRowsToS3JsonWatermarkOperator):
    @cached_property
    def project_88_hook(self):
        return Pivot88Hook(self.hook_conn_id) if self.hook_conn_id else Pivot88Hook()

    def get_high_watermark(self):
        pass

    def make_request(self, method, endpoint):
        pivot88 = self.project_88_hook
        base_url = pivot88.get_base_url()
        session = pivot88.session

        endpoint = endpoint[1:] if endpoint[0] == "/" else endpoint
        base_url = base_url[:-1] if base_url[-1] == "/" else base_url
        url = f'{base_url}/{endpoint}'
        r = session.request(
            method=method,
            url=url,
        )
        return r

    def get_rows(self):
        now = pendulum.DateTime.utcnow()
        updated_at = {"updated_at": str(now)}
        low_watermark = self.get_low_watermark()
        # Add 7 day look back in case of late arriving data, from past results I think they do
        from_updated = pendulum.parse(low_watermark).subtract(days=7)
        max_datetime = low_watermark
        window_min = 1440
        while True:
            if from_updated > now:
                break

            to_updated = from_updated.add(minutes=window_min).replace(microsecond=0)
            endpoint = (
                f'inspections?from_updated={str(from_updated).split("+")[0]}&'
                f'to_updated={str(to_updated.add(seconds=1)).split("+")[0]}&details=true'
            )
            try:
                r = self.make_request('GET', endpoint)
                r.raise_for_status()
            except requests.exceptions.HTTPError as err:
                if err.response.status_code == 500:
                    print("reducing interval due to 500 error status code")
                    window_min /= 4
                    continue
                else:
                    raise
            inspections = r.json()
            # Api limits objects returned to 100, but does not sort returned objects. To ensure no
            # objects missed, if returned length is 100, will recall api will smaller time window.
            if len(inspections) > 99:
                window_min /= 4
                continue

            for inspection in inspections:
                if inspection.get('date_modified') and inspection['date_modified'] > max_datetime:
                    max_datetime = inspection['date_modified']
                yield {**inspection, **updated_at}

            from_updated = to_updated
            # Increases time window to at least a day on successful (<100 obj returned) api calls
            if window_min < 1440:
                window_min *= 2

        self.new_high_watermark = max_datetime


class Pivot88AdvancedDetails(BaseRowsToS3CsvWatermarkOperator):
    @cached_property
    def project_88_hook(self):
        return Pivot88Hook(self.hook_conn_id) if self.hook_conn_id else Pivot88Hook()

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook()

    def get_high_watermark(self):
        # set watermark 7 days in the past
        with ConnClosing(self.snowflake_hook.get_conn()) as conn, conn.cursor() as cur:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            sql = (
                "SELECT DATEADD(day, -7, max(DATE_MODIFIED)) FROM LAKE_VIEW.GSC.PIVOT88_INSPECTIONS"
            )
            df = pd.read_sql(sql, con=conn)
            return str(df.iat[0, 0])

    def get_rows(self):
        now = str(pendulum.DateTime.utcnow())

        with ConnClosing(self.snowflake_hook.get_conn()) as conn, conn.cursor() as cur:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            sql = f"SELECT DISTINCT id FROM LAKE_VIEW.GSC.PIVOT88_INSPECTIONS WHERE DATE_MODIFIED > '{self.low_watermark}'"
            df = pd.read_sql(sql, con=conn)

        for inspection_id in df['ID']:
            resp_adv_details = self.project_88_hook.make_request(
                'GET', f'/inspections/{inspection_id}?advanced_details=true'
            )
            adv_details = resp_adv_details.json()

            lookup_list = [
                'assignment_group:id',
                'date_inspection',
                'inspector:name',
                'report_type:name',
                'factory_name',
            ]
            d = flatten_json_lookup_list(adv_details, lookup_list)

            for assignments_item in adv_details['assignments_items']:
                lookup_list = [
                    'inspection_status_text',
                    'inspection_result_text',
                    'qty_inspected',
                    'po_line:style',
                    'po_line:banner',
                    'po_line:po:suppliers:name',
                    'po_line:po:factory:erp_business_id',
                    'po_line:po:factory:country',
                    'po_line:sku:item_name',
                    'po_line:sku:product_family',
                    'po_line:department',
                    'po_line:region',
                    'inspection_report:sections',
                    'inspection_report:total_critical_defects',
                    'inspection_report:total_major_defects',
                    'inspection_report:total_minor_defects',
                    'inspection_report:defective_parts',
                    'inspection_completed_date',
                    'id',
                    'po_line:po:suppliers:erp_business_id',
                    'po_line:po:id',
                ]
                assignments_item_d = flatten_json_lookup_list(
                    assignments_item, lookup_list, 'assignments_items'
                )

                for section in assignments_item_d['assignments_items_inspection_report_sections']:
                    if section.get('title') == 'product':
                        if defects := section.get('defects'):
                            for defect in defects:
                                lookup_list = [
                                    'label',
                                    'level_label',
                                    'subsection',
                                    'code',
                                    'level_value',
                                ]
                                defect_d = flatten_json_lookup_list(
                                    defect,
                                    lookup_list,
                                    'assignments_items_inspection_report_sections_defects',
                                )
                                yield {**d, **assignments_item_d, **defect_d, 'updated_at': now}
