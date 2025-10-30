import json
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional

import pandas as pd
import requests
from facebook_business.api import FacebookAdsApi

from include.airflow.hooks.facebook import FacebookAdsHook
from include.utils.data_structures import batch_elem, chunk_list, split_list_by_field
from include.utils.decorators import cached

DEFAULT_API_VERSION = "v21.0"

ACCOUNT_CONFIG = {
    "na": [
        "act_67811128",
        "act_87956664",
        "act_77885569",
        "act_100298133406862",
        "act_106957382783892",
        "act_1376878552545927",
        "act_10155046618470508",
        "act_10155894994960508",
        "act_10156200174900508",
        "act_10156064982030508",
        "act_10156112003525508",
        "act_10156200176535508",
        "act_10156660861120508",
        "act_10157307458135508",
        "act_10157622848800508",
        "act_10157773643890508",
        "act_10157750490620508",
        "act_10157834693365508",
        "act_10158053873715508",
        "act_10158215557705508",
        "act_10158053794080508",
        "act_10158810759420508",
        "act_10158810768555508",
        "act_10158954555025508",
        "act_10159221047195508",
        "act_10160516591735508",
        "act_245475399493162",
        "act_698572610513658",
        "act_2177180699166617",
        "act_2170350153180186",
        "act_584044762051274",
        "act_338056876952277",
        "act_576884689440488",
        "act_1919949091453393",
        "act_291760788147373",
        "act_2395600940659587",
        "act_383142662245543",
        "act_356365288557886",
        "act_329776877655525",
        "act_1069661779910843",
        "act_359210034778870",
        "act_444959489669305",
        "act_557464508343940",
        "act_930644510652710",
        "act_496700144391854",
        "act_626542394784025",
        "act_2492161674395854",
        "act_867502650349544",
        "act_1576635752489340",
        "act_549380765919307",
        "act_582501782316064",
        "act_288796152126623",
        "act_1382850361914745",
        "act_569965817291077",
        "act_645384986080773",
        "act_656272245138578",
        "act_233031241403951",
        "act_780837282692243",
        "act_340224533907454",
        "act_664949607785660",
        "act_400590184636766",
        "act_714293649508670",
        "act_2811097472504056",
        "act_758329614786387",
        "act_417258539529444",
        "act_773631016616971",
        "act_1657534277766300",
        "act_445699436950696",
        "act_474944557472303",
        "act_487243922960307",
        "act_782571422951932",
        "act_537960995516654",
        "act_1213521053308393",
        "act_356670560833858",
        # "act_2686402231541801",
        # "act_1492523868130703",
        "act_1260019478408379",
        # "act_925639992823389",
        "act_28286696050975145",
    ],
    "eu": [
        "act_152900641498413",
        "act_107566116040761",
        "act_100186300139890",
        "act_106833432804078",
        "act_1385103101771689",
        "act_1503463316533389",
        "act_800249193364177",
        "act_256011294583335",
        "act_309287155913038",
        "act_357550384397883",
        "act_749780668411030",
        "act_818065031582593",
        "act_872880856101010",
        "act_768420703213693",
        "act_857784964277266",
        "act_907161652672930",
        "act_917195345002894",
        "act_903325933056502",
        "act_932564453465983",
        "act_932568640132231",
        "act_928808360508259",
        "act_927586657297096",
        "act_900672966655132",
        "act_903326769723085",
        "act_900673136655115",
        "act_943593362363092",
        "act_929314487124313",
        "act_996709607051467",
        "act_999588783430216",
        "act_1070284073027353",
        "act_1092380824151011",
        "act_1117958361593257",
        "act_1124173057638454",
        "act_1133616786694081",
        "act_1133618223360604",
        "act_1133616223360804",
        "act_1133619120027181",
        "act_1133617973360629",
        "act_1162435717145521",
        "act_1162419813813778",
        "act_1162434777145615",
        "act_1203972336325192",
        "act_1203974072991685",
        "act_1225974234125002",
        "act_1293209020734856",
        "act_1292939707428454",
        "act_1293156844073407",
        "act_1292939260761832",
        "act_1293207340735024",
        "act_1292937437428681",
        "act_1293208430734915",
        "act_1292939937428431",
        "act_1292939547428470",
        "act_1288378367884588",
        "act_1550063471716075",
        "act_1550060131716409",
        "act_2075124999410504",
        "act_140982636772464",
        "act_400985727051511",
        "act_200342937462865",
        "act_142550806615566",
        "act_239796986709389",
        "act_287781318579349",
        "act_267421560837596",
        "act_336262913664636",
        # "act_427699541315601",
        "act_308079826538488",
        "act_539760849860772",
        "act_2651653758184576",
        "act_372322433380632",
        "act_2101585763473732",
        "act_2050021318447381",
        "act_595754150893217",
        "act_2102008593353224",
        "act_412000939576835",
        "act_423374828485777",
        "act_447223319444773",
        # "act_259269574934533",
        # "act_632366540491847",
        # "act_409900433156014",
        "act_504556813629429",
        # "act_1972592639712649",
        # "act_936601229881069",
        "act_797000424079790",
        "act_176395713455619",
        "act_503097533973544",
        "act_467322124170543",
        "act_454769095197055",
        "act_2902214863162110",
        "act_216404546057302",
        "act_510665859585663",
        "act_506531286667910",
        "act_2465796217019913",
        "act_2795210657188976",
        "act_803706870113497",
        "act_200730237665533",
        "act_2261009834192507",
        "act_479530032981114",
        "act_178331586607309",
        "act_835092727230594",
        "act_284616719263939",
        "act_3628328613909441",
        "act_779816555951705",
        "act_433110691141550",
        "act_766393227313303",
        "act_539826073643044",
        "act_190685272800687",
        "act_236635014841838",
        "act_1374399442894301",
        "act_181059706957154",
        "act_187845939563682",
        "act_537969863839511",
        "act_1082253042270179",
        "act_537438333936399",
        "act_2863905450550331",
        "act_1140352393043571",
        "act_1164974370664369",
        "act_450057676678993",
        "act_246616454238917",
        "act_1596929200655875",
        "act_379003147416319",
        "act_714764429617158",
    ],
    "sxf": [
        "act_2686402231541801",
        "act_1492523868130703",
        "act_925639992823389",
    ],
}
HEAVY_ACCOUNTS = {
    "act_10157750490620508",
    "act_338056876952277",
    "act_10160516591735508",
    "act_106957382783892",
    "act_291760788147373",
    "act_1503463316533389",
    "act_1069661779910843",
    "act_383142662245543",
    "act_1376878552545927",
    "act_10158810768555508",
    "act_782571422951932",
    "act_474944557472303",
    "act_444959489669305",
    "act_549380765919307",
    "act_582501782316064",
}


def batch_get(batch_list, access_token):
    batch_response = requests.post(
        url=f"https://graph.facebook.com/{DEFAULT_API_VERSION}",
        json={"access_token": access_token, "batch": batch_list},
    )
    return batch_response


@dataclass
class Pixel:
    pixel_id: str
    pixel_name: str
    access_token: str
    business_id: str
    business_name: str
    account_id: str


def get_pixel_list() -> List[Pixel]:
    pixel_id_set = set()
    pixel_list: List[Pixel] = []
    newlist = split_list_by_field("access_token", get_account_list())
    for access_token, actlist in newlist.items():
        batch_list = [
            batch_elem(
                f"{x['account_id']}/adspixels"
                "?include_headers=false"
                "&fields=id,name,owner_business,owner_ad_account"
            )
            for x in actlist
        ]
        # print('actok:', access_token)
        for component in chunk_list(batch_list, size=1):
            resp = batch_get(batch_list=component, access_token=access_token)
            for elem in resp.json():
                if (
                    elem["code"] != 200
                    and json.loads(elem["body"])["error"]["type"] == "OAuthException"
                ):
                    print(
                        component[0]["relative_url"][: component[0]["relative_url"].find("/")],
                        "failed with error",
                        json.loads(elem["body"])["error"],
                    )
                elif elem["code"] != 200:
                    raise Exception("Error")
                else:
                    body = json.loads(elem["body"])
                    data = body["data"]
                    # print(data)
                    for p in data:
                        if p["id"] not in pixel_id_set:
                            pixel_id_set.add(p["id"])
                            pixel_list.append(
                                Pixel(
                                    pixel_id=p["id"],
                                    pixel_name=p["name"],
                                    access_token=access_token,
                                    business_id=p.get("owner_business", {}).get("id"),
                                    business_name=p.get("owner_business", {}).get("name"),
                                    account_id=p.get("owner_ad_account", {}).get("account_id"),
                                )
                            )
    return pixel_list


def get_account_list():
    account_list = []
    for conn_id in ("facebook_na", "facebook_eu"):
        hook = FacebookAdsHook(facebook_ads_conn_id=conn_id)
        api = FacebookAdsApi.init(access_token=hook.access_token, api_version=hook.api_version)
        region = conn_id.replace("facebook_", "").lower()
        curr_account_list = [
            {
                "account": act,
                "account_id": act.get("id"),
                "account_name": act.get("name"),
                "business_id": act.get("business", {}).get("id"),
                "access_token": hook.access_token,
                "api": api,
                "region": region,
            }
            for act in hook.get_account_list()
        ]
        account_list.extend(curr_account_list)
    return account_list


@cached(Path(tempfile.gettempdir(), "get_account_list_by_region"))
def get_account_list_by_region():
    account_list = get_account_list()
    split_list = split_list_by_field("region", account_list)
    ret = {k: [x["account_id"] for x in v] for k, v in split_list.items()}
    return ret


class BatchElement:
    """
    BatchElement is used in conjunction with Batch to make batch requests on facebook graph api.

    Together, these classes handle batch calling of facebook endpoints including paging etc.

    BatchElement takes care of paging and processing the data.

    For BatchElement, you just need to give a relative URL (relative to the base URL of the Batch) and a data_processor
    function (which does something with the rows in the data element of the response body, e.g. writing them to file).

    An individual BatchElement will retry on failure up to self.fail_threshold times.  It will keep track of failure
    stats and rows exported.

    """

    def __init__(self, relative_url: str, data_processor: Callable, name: str = None):
        """

        :param relative_url: url relative to base_url in Batch instance
        :param data_processor: function(data:List, name:str) -> int
        processes the data element in the api response.
        should return the number of rows processed.
        :param name: name is an optional string value that should represent uniquely the batch element in a manner more
        readable than the relative_url.  it is not strictly necessary.
        """
        self.next_page = None
        self.name = str(name)
        self.data_processor = data_processor
        self.rows_exported = 0
        self.curr_error_count = 0
        self.total_error_count = 0
        self.is_failed = False
        self.is_complete = False
        self.curr_response = None
        self.curr_response_body = None
        self.err_message = None
        self.relative_url = relative_url
        self.paging_after = None
        self.response_count = 0
        self.fail_threshold = 3

    def __eq__(self, other):
        return self.relative_url == other.relative_url

    def __repr__(self):
        return self.name if self.name else self.relative_url

    def __le__(self, other):
        return self.relative_url < other.relative_url

    def log(self, message):
        print(self.__repr__() + ": " + message)

    def handle_error(self):
        self.err_message = "code: {}; ".format(self.curr_response["code"]) + str(
            self.curr_response_body["error"]
        )
        self.log("error {}: {}".format(self.curr_error_count, self.err_message))
        self.curr_error_count += 1
        self.total_error_count += 1
        if self.curr_error_count >= self.fail_threshold:
            self.is_failed = True
        self.log("job failed with error %s" % self.err_message)

    def handle_data(self):
        """
        do something with self.curr_response_body['data']
        """
        rows_exported = self.data_processor(data=self.curr_response_body["data"], name=self.name)
        if isinstance(rows_exported, int):
            self.rows_exported += rows_exported

    def process_response(self, response):
        self.response_count += 1
        self.curr_response = response
        self.curr_response_body = json.loads(self.curr_response["body"])
        if "error" in self.curr_response_body:
            self.handle_error()
            return

        if "data" in self.curr_response_body:
            self.handle_data()
        else:
            self.log("no data element")
            self.log(str(json.dumps(self.curr_response_body)))

        self.paging_after = (
            self.curr_response_body.get("paging", {}).get("cursors", {}).get("after", None)
        )
        if self.paging_after is None:
            self.is_complete = True
            self.log("rows exported: %s" % self.rows_exported)
            self.is_failed = False

    def get_batch_elem(self):
        if self.paging_after is not None:
            relative_url = self.relative_url + "&after={}".format(self.paging_after)
        else:
            relative_url = self.relative_url
        return {"method": "GET", "relative_url": relative_url}


class Batch:
    """
    Used in conjunction with BatchElement to make batched requests to the facebook graph api.

    The Batch class will loop through batch list, processing match_batch_length requests at a time.

    All of the BatchElement objects in the batch_list must use the same access_token.

    At completion, Batch will summarize stats in a pandas DataFrame assigned to attribute stats_df, which can be used
    by the calling process for logging purposes or alerting.
    """

    BASE_URL: str = f"https://graph.facebook.com/{DEFAULT_API_VERSION}"

    def __init__(
        self,
        access_token: str,
        batch_list: List[BatchElement],
        max_batch_length: int = 50,
        name: str = None,
    ):
        self.access_token = access_token
        self.batch_list = batch_list
        self.max_batch_length = int(max_batch_length)
        self.execute_start = None
        self.execute_end = None
        self.stats_df: Optional[pd.DataFrame] = None
        self.name = str(name)

    @staticmethod
    def stats_dict(x):
        return {
            k: x.__getattribute__(k)
            for k in [
                "name",
                "is_failed",
                "rows_exported",
                "total_error_count",
                "response_count",
                "relative_url",
            ]
        }

    def execute(self):
        self.execute_start = datetime.utcnow()
        iter_count = 0
        while True:
            iter_count += 1
            print("iter count:", iter_count)
            curr_batch_list = [x for x in self.batch_list if not x.is_failed and not x.is_complete][
                0 : self.max_batch_length
            ]
            if len(curr_batch_list) == 0:
                break

            batch_element_list = [x.get_batch_elem() for x in curr_batch_list]

            for i in range(10):
                try:
                    batch_response = requests.post(
                        url=self.BASE_URL,
                        json={"access_token": self.access_token, "batch": batch_element_list},
                    )
                    break
                except requests.exceptions.ChunkedEncodingError:
                    if i < 3:
                        pass
                    else:
                        raise
            batch_response_dict = batch_response.json()
            try:
                batch_response_dict[0]
            except KeyError:
                raise Exception("could not access batch_response_dict[0]", batch_response_dict)

            for i, rep in enumerate(curr_batch_list):
                print(i, rep)
                curr_batch_list[i].process_response(batch_response_dict[i])
        self.execute_end = datetime.utcnow()
        self.stats_df = pd.DataFrame.from_records(map(self.stats_dict, self.batch_list))
        self.stats_df["error_rate"] = self.stats_df.total_error_count / self.stats_df.response_count
