import json
from dataclasses import dataclass

from include.config import stages


@dataclass
class TiktokAds:
    table: str
    relative_path: str
    column_list: list
    date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"

    @property
    def files_path(self):
        return f"{stages.tsos_da_int_inbound}/media/tiktok/{self.relative_path}"

    @property
    def s3_prefix(self):
        return f"media/tiktok/{self.relative_path}"

    @property
    def request_params(self):
        return {
            "metrics": json.dumps(
                [
                    x.source_name
                    for x in self.column_list
                    if x.source_name
                    not in ["ad_id", "stat_time_day", "advertiser_id", "updated_at"]
                ]
            ),
            "data_level": "AUCTION_AD",
            "end_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
            "order_type": "ASC",
            "page_size": 1000,
            "start_date": "{{ macros.ds_add(data_interval_end.strftime('%Y-%m-%d'), -7) }}",
            "page": 1,
            "report_type": "BASIC",
            "dimensions": json.dumps(['ad_id', 'stat_time_day']),
        }


@dataclass
class TiktokAdsHourly:
    table: str
    relative_path: str
    column_list: list
    date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"

    @property
    def files_path(self):
        return f"{stages.tsos_da_int_inbound}/media/tiktok/{self.relative_path}"

    @property
    def s3_prefix(self):
        return f"media/tiktok/{self.relative_path}"

    @property
    def request_params(self):
        return {
            "metrics": json.dumps(
                [
                    x.source_name
                    for x in self.column_list
                    if x.source_name
                    not in ["ad_id", 'stat_time_hour', "advertiser_id", "updated_at"]
                ]
            ),
            "data_level": "AUCTION_AD",
            "end_date": "{{data_interval_end.strftime('%Y-%m-%d')}}",
            "order_type": "ASC",
            "page_size": 1000,
            "start_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
            "page": 1,
            "report_type": "BASIC",
            "dimensions": json.dumps(['ad_id', 'stat_time_hour']),
        }


ADVERTISER_IDS = [
    1640159623649285,
    1640159675483142,
    6855004866872344581,
    6852454831261483014,
    6906633621335998466,
    6906518299329167361,
    6938431609884426242,
    6907200064964263938,
    6907200315569750018,
    6974405283426615297,
    6974406559744966658,
    6974423702058369026,
    6974423416371740674,
    6974423945080553473,
    6938427647013683201,
    6890954547586400258,
    6938433649633181698,
    6907200609816936449,
    7042734071260266497,
    7043823263155388417,
    7043819437568393218,
    7043819499665080321,
    7068192560782639105,
    6974408281838059521,
    6974408370216271874,
    7122373039248916482,
    7143441141801697282,
    7200485908250886146,
]
