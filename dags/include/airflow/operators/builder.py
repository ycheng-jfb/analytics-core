import datetime
import json
import tempfile
from functools import cached_property
from typing import Dict, Iterable, List
from urllib.parse import urlparse

import pytz
from airflow.models import BaseOperator
from include.airflow.hooks.builder import BuilderHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator


class BuilderBaseOperator(BaseOperator):
    """ """

    def __init__(
        self,
        request_params: Dict,
        path: str,
        method: str = "GET",
        builder_conn_id: str = "builder_default",
        brand: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.request_params = request_params
        self.builder_conn_id = builder_conn_id
        self.method = method
        self.path = path
        self.brand = brand

    @cached_property
    def hook(self):
        return BuilderHook(builder_conn_id=self.builder_conn_id)


class BuilderHDYHToS3Operator(BaseRowsToS3CsvOperator, BuilderBaseOperator):
    template_fields = ["key"]

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

    def get_rows(self) -> Iterable[dict]:
        response = self.hook.make_request(
            path=self.path,
            method="GET",
            params=self.request_params,
        )
        data = response.json()
        for result in data["results"]:
            print(result)
            result["data"] = json.dumps(result["data"])
            yield result


class BuilderABTestingToS3Operator(BaseRowsToS3CsvOperator, BuilderBaseOperator):
    template_fields = [
        "all_components_key",
        "all_targeting_components_key",
        "all_called_tests_column_key",
    ]

    def __init__(
        self,
        all_components_key: str,
        all_targeting_components_key: str,
        all_called_tests_column_key: str,
        all_components_column_list: List,
        all_targeting_components_column_list: List,
        all_called_tests_column_list: List,
        *args,
        **kwargs,
    ):
        self.all_components_key = all_components_key
        self.all_targeting_components_key = all_targeting_components_key
        self.all_called_tests_column_key = all_called_tests_column_key
        self.all_components_column_list = all_components_column_list
        self.all_targeting_components_column_list = all_targeting_components_column_list
        self.all_called_tests_column_list = all_called_tests_column_list
        super().__init__(*args, **kwargs, key=None, column_list=None)

    def get_builder_content(self, content_type):
        all_pages = {"results": []}
        request_params = {**self.request_params}
        while True:
            response = json.loads(
                self.hook.make_request(
                    path=content_type,
                    method="GET",
                    params={**request_params, "includeUnpublished": "true"},
                ).text
            )["results"]

            if len(response) > 0:
                all_pages["results"].extend(response)
                request_params["offset"] += request_params["limit"]
            else:
                print("end of list for unarchived data")
                break

        request_params = {**self.request_params}
        while True:
            response = json.loads(
                self.hook.make_request(
                    path=content_type,
                    method="GET",
                    params={**request_params, "query.published": "archived"},
                ).text
            )["results"]

            if len(response) > 0:
                all_pages["results"].extend(response)
                request_params["offset"] += request_params["limit"]
            else:
                print("end of list for archived data")
                break

        return all_pages

    def retrieve_variation_metadata(self, all_content):
        all_components = []
        # all_targeting = []
        violations = []

        for single_builder_page in all_content["results"]:
            print(single_builder_page["id"])

            # only keep the pages that have variations, include 0% variations for now
            if "variations" in single_builder_page and (
                len(single_builder_page["variations"]) > 0
            ):
                # get the data for the control / parent page first
                single_page_results = {}
                control_variation = []
                single_page_results["test_type"] = "Builder"
                single_page_results["test_name"] = (
                    single_builder_page["name"].split("- Control")[0].strip()
                )

                single_page_results.update(
                    self.get_builder_page_attributes(single_builder_page)
                )

                single_page_results["control"] = {}
                single_page_results["control"]["test_variation_name"] = (
                    single_builder_page["name"]
                )
                single_page_results["control"]["test_variation_id"] = (
                    single_builder_page["id"]
                )
                single_page_results["control"]["assignment"] = "control"

                # test ratio
                single_page_results["test_split"] = ""
                control_test_ratio = 100

                # determine psources
                control_psource = self.get_psource_single_page(single_builder_page)
                unique_psources = set(
                    (
                        item["psource"]
                        if isinstance(item["psource"], str)
                        else item["psource"]["Default"]
                    )
                    for item in control_psource
                )
                unique_psources_string = "|".join(list(unique_psources))
                single_page_results["control"]["psources"] = unique_psources_string

                variations_list = single_builder_page["variations"].keys()

                # get psources, and include tab iteration (see sortList script)
                for index, variation in enumerate(variations_list):
                    single_page_results[f"Variant_{index + 1}"] = {}
                    single_page_results[f"Variant_{index + 1}"][
                        "test_variation_name"
                    ] = single_builder_page["variations"][variation]["name"]
                    single_page_results[f"Variant_{index + 1}"]["test_variation_id"] = (
                        single_builder_page["variations"][variation]["id"]
                    )
                    single_page_results[f"Variant_{index + 1}"]["assignment"] = (
                        f"Variant_{index + 1}"
                    )

                    # test ratio
                    single_page_results["test_split"] = (
                        f"{single_page_results['test_split']}/"
                        f"{int(single_builder_page['variations'][variation]['testRatio'] * 100)}"
                    )
                    single_page_results[f"Variant_{index + 1}"]["testRatio"] = int(
                        single_builder_page["variations"][variation]["testRatio"] * 100
                    )
                    control_test_ratio -= int(
                        single_page_results[f"Variant_{index + 1}"]["testRatio"]
                    )
                    control_variation.append(
                        single_page_results[f"Variant_{index + 1}"]
                    )

                    # determine psources
                    variation_psource = self.get_psource_single_page(
                        single_builder_page["variations"][variation]
                    )
                    unique_psources = set(
                        (
                            item["psource"]
                            if isinstance(item["psource"], str)
                            else item["psource"]["Default"]
                        )
                        for item in control_psource
                    )
                    unique_psources_string = "|".join(list(unique_psources))
                    single_page_results[f"Variant_{index + 1}"]["psources"] = (
                        unique_psources_string
                    )

                single_page_results["control"]["testRatio"] = control_test_ratio
                single_page_results["test_split"] = (
                    f"{single_page_results['control']['testRatio']}{single_page_results['test_split']}"
                )
                control_variation.append(single_page_results["control"])
                if (
                    "control"
                    not in single_page_results["control"]["test_variation_name"].lower()
                    and single_page_results["control"]["testRatio"] != 100
                ):
                    violations.append(single_page_results)

                # test_split
                single_page_results["control_variation_attributes"] = control_variation
                all_components.append(single_page_results)
        return all_components

    def get_psource_single_page(self, single_builder_page):
        collections_list = []
        if (
            "data" in single_builder_page
            and isinstance(single_builder_page["data"], dict)
            and "blocks" in single_builder_page["data"]
        ):
            for block_element in single_builder_page["data"]["blocks"]:
                # find all blocks where the component where data.blocks.name = _Collection_
                if (
                    "component" in block_element
                    and isinstance(block_element["component"], dict)
                    and "name" in block_element["component"]
                    and block_element["component"]["name"] == "_Collection_"
                ):
                    collection_element = self.get_psource_value(block_element)
                    collections_list.append(collection_element)
                # find all blocks where the component where data.blocks.name = _tabs_
                # each tab then needs to be examined for psources
                if (
                    "component" in block_element
                    and isinstance(block_element["component"], dict)
                    and "name" in block_element["component"]
                    and block_element["component"]["name"] == "_Tabs_"
                ):
                    for tab_element in block_element["component"]["options"].get(
                        "tabs", []
                    ):
                        for content_element in tab_element["content"]:
                            if (
                                "component" in content_element
                                and isinstance(content_element["component"], dict)
                                and "name" in content_element["component"]
                                and content_element["component"]["name"]
                                == "_Collection_"
                            ):
                                collection_element = self.get_psource_value(
                                    content_element
                                )
                                # pdb.set_trace()
                                collections_list.append(collection_element)

        return collections_list

    def get_psource_value(self, block_element):
        collection_element = {}
        # retrive the block id
        collection_element["block_id"] = block_element["id"]
        # data.blocks.component.options.productSource should exist; retrieve the value
        # print(block_element['id'])
        if (
            "component" in block_element
            and isinstance(block_element["component"], dict)
            and "options" in block_element["component"]
        ):
            if "productSource" in block_element["component"]["options"]:
                collection_element["psource"] = block_element["component"]["options"][
                    "productSource"
                ]
            else:
                collection_element["psource"] = "ALERT: MISSING PSOURCE!!!!"

            # include gridAssetContainer if it exists:
            if "gridAssetContainer" in block_element["component"]["options"]:
                collection_element["gridAssetContainer"] = block_element["component"][
                    "options"
                ]["gridAssetContainer"]
            # # include gridUrlLink if it exists:
            if "gridUrlLink" in block_element["component"]["options"]:
                collection_element["gridUrlLink"] = block_element["component"][
                    "options"
                ]["gridUrlLink"]

        return collection_element

    def get_builder_page_attributes(self, single_builder_page):
        builder_page_level_attributes = {}

        builder_page_level_attributes["builder_id"] = single_builder_page["id"]
        builder_page_level_attributes["builder_page_id"] = (
            f"https://builder.io/content/{single_builder_page['id']}"
        )
        if (
            "data" in single_builder_page
            and "seoNoIndex" in single_builder_page["data"]
        ):
            builder_page_level_attributes["seoNoIndex"] = single_builder_page["data"][
                "seoNoIndex"
            ]
        else:
            builder_page_level_attributes["seoNoIndex"] = ""
        if (
            "meta" in single_builder_page
            and "winningTest" in single_builder_page["meta"]
            and isinstance(single_builder_page["meta"], dict)
            and single_builder_page["meta"]["winningTest"] is not None
        ):
            # print(f"{single_builder_page['id']} was a called experiment")
            builder_page_level_attributes["winningTest"] = single_builder_page["meta"][
                "winningTest"
            ]
            (
                builder_page_level_attributes["winningTest"]["called_at_utc"],
                builder_page_level_attributes["winningTest"]["called_at_pst"],
            ) = self.convert_unix_timestamp_to_date(
                single_builder_page["meta"]["winningTest"]["timestamp"]
            )
        # else:
        #     print(f"{single_builder_page['id']} has not been called")
        if "previewUrl" in single_builder_page:
            builder_page_level_attributes["builder_page_url"] = single_builder_page[
                "previewUrl"
            ]
        elif "data" in single_builder_page and "url" in single_builder_page["data"]:
            builder_page_level_attributes["builder_page_url"] = single_builder_page[
                "data"
            ]["url"]
        else:
            builder_page_level_attributes["builder_page_url"] = ""
        if builder_page_level_attributes["builder_page_url"]:
            parsed_url = urlparse(builder_page_level_attributes["builder_page_url"])
            builder_page_level_attributes["domain"] = parsed_url.netloc
            builder_page_level_attributes["path"] = parsed_url.path
            if parsed_url.path == "/":
                builder_page_level_attributes["page_type"] = "homepage"
            if builder_page_level_attributes["domain"] != "":
                builder_page_level_attributes["site_country"] = self.remap_domain(
                    builder_page_level_attributes["domain"]
                )
        else:
            builder_page_level_attributes["domain"] = ""
            builder_page_level_attributes["site_country"] = ""

        builder_page_level_attributes["builder_page_name"] = single_builder_page.get(
            "name"
        )

        if "published" in single_builder_page:
            builder_page_level_attributes["published"] = single_builder_page[
                "published"
            ]
        (
            builder_page_level_attributes["adjusted_activated_datetime_utc"],
            builder_page_level_attributes["adjusted_activated_datetime_pst"],
        ) = self.convert_unix_timestamp_to_date(single_builder_page["createdDate"])
        if "firstPublished" in single_builder_page:
            (
                builder_page_level_attributes["firstPublished_datetime_utc"],
                builder_page_level_attributes["firstPublished_datetime_pst"],
            ) = self.convert_unix_timestamp_to_date(
                single_builder_page["firstPublished"]
            )
        else:
            builder_page_level_attributes["firstPublished_datetime_utc"] = ""
            builder_page_level_attributes["firstPublished_datetime_pst"] = ""
        if "lastUpdated" in single_builder_page:
            (
                builder_page_level_attributes["lastUpdated_datetime_utc"],
                builder_page_level_attributes["lastUpdated_datetime_pst"],
            ) = self.convert_unix_timestamp_to_date(single_builder_page["lastUpdated"])
        else:
            builder_page_level_attributes["lastUpdated_datetime_utc"] = ""
            builder_page_level_attributes["lastUpdated_datetime_pst"] = ""

        return builder_page_level_attributes

    def get_ab_testing_metadata(self, builder_metadata):
        ab_testing_components = self.retrieve_variation_metadata(builder_metadata)
        brand = self.brand

        for component in ab_testing_components:
            component["brand"] = brand
            component["content_type"] = self.path
            if brand in ("FabKids", "JustFab", "ShoeDazzle"):
                if brand == "JustFab":
                    component["domain"] = "www.justfab.com"
                elif brand == "ShoeDazzle":
                    component["domain"] = "www.shoedazzle.com"
                elif brand == "FabKids":
                    component["domain"] = "www.fabkids.com"
                component["builder_page_url"] = (
                    f"https://{component['domain']}{component['builder_page_url']}"
                )
                component["site_country"] = "US"

        for cleaned_data in self.unpack_list_of_dicts(ab_testing_components):
            yield cleaned_data

    def get_all_targeting(self, builder_metadata):
        all_targeting = []
        for single_builder_page in builder_metadata["results"]:
            single_page_targeting = dict()
            single_page_targeting["targeting"] = self.parse_targeting(
                single_builder_page
            )
            if len(single_page_targeting["targeting"]) > 0:
                single_page_targeting["builder_id"] = single_builder_page["id"]
                all_targeting.append(single_page_targeting)

        for cleaned_data in self.unpack_list_of_dicts(all_targeting):
            yield cleaned_data

    def parse_targeting(self, single_builder_page):
        builder_page_targeting_list = []
        if "query" in single_builder_page:
            for target in single_builder_page["query"]:
                target_attributes = {}
                if target["property"] is None:
                    continue
                else:
                    target_attributes["property"] = target["property"]
                    target_attributes["operator"] = target["operator"]
                    if isinstance(target["value"], bool):
                        target_attributes["value"] = str(target["value"])
                    if isinstance(target["value"], str):
                        target_attributes["value"] = target["value"]
                    if isinstance(target["value"], list):
                        target_attributes["value"] = self.stringify_targeting_list(
                            target["property"], target["value"]
                        )
                        # for locales create a list of countries that matches values
                        # in dim store. e.g en-US -> US
                        if target["property"] == "locale":
                            country_list = target_attributes["value"].split("|")
                            # target_attributes['remapped_value'] = remap_targeting_countries(
                            #                                                       country_list)
                            target_attributes["value"] = self.remap_targeting_countries(
                                country_list
                            )
                # print(target_attributes)
                builder_page_targeting_list.append(target_attributes)

        return builder_page_targeting_list

    def retrieve_called_tests(self, builder_metadata):
        ab_testing_components = self.retrieve_variation_metadata(builder_metadata)

        all_called_tests = []
        for single_builder_page in ab_testing_components:
            called_test = {}
            if "winningTest" in single_builder_page:
                # print (single_builder_page['winningTest'])
                called_test["builder_id"] = single_builder_page["builder_id"]
                if "id" in single_builder_page["winningTest"]:
                    called_test["winning_variation"] = single_builder_page[
                        "winningTest"
                    ]["id"]
                if "called_at_utc" in single_builder_page["winningTest"]:
                    called_test["called_at_utc"] = single_builder_page["winningTest"][
                        "called_at_utc"
                    ]
                if "called_at_pst" in single_builder_page["winningTest"]:
                    called_test["called_at_pst"] = single_builder_page["winningTest"][
                        "called_at_pst"
                    ]
                all_called_tests.append(called_test)
        # pprint.pprint(all_called_tests)

        for cleaned_data in self.unpack_list_of_dicts(all_called_tests):
            yield cleaned_data

    def BaseRowsToS3CsvOperator_execute(self):
        all_content = self.get_builder_content(self.path)

        with tempfile.NamedTemporaryFile(mode="wb", delete=True) as temp_file:
            rows = self.get_ab_testing_metadata(all_content)
            self.write_dict_rows_to_file(
                rows=rows,
                filename=temp_file.name,
                column_list=self.all_components_column_list,
                write_header=self.write_header,
            )
            temp_file.flush()
            self.upload_to_s3(
                temp_file.name, bucket=self.bucket, key=self.all_components_key
            )

        with tempfile.NamedTemporaryFile(mode="wb", delete=True) as temp_file:
            rows = self.get_all_targeting(all_content)
            self.write_dict_rows_to_file(
                rows=rows,
                filename=temp_file.name,
                column_list=self.all_targeting_components_column_list,
                write_header=self.write_header,
            )
            temp_file.flush()
            self.upload_to_s3(
                temp_file.name,
                bucket=self.bucket,
                key=self.all_targeting_components_key,
            )

        with tempfile.NamedTemporaryFile(mode="wb", delete=True) as temp_file:
            rows = self.retrieve_called_tests(all_content)
            self.write_dict_rows_to_file(
                rows=rows,
                filename=temp_file.name,
                column_list=self.all_called_tests_column_list,
                write_header=self.write_header,
            )
            temp_file.flush()
            self.upload_to_s3(
                temp_file.name, bucket=self.bucket, key=self.all_called_tests_column_key
            )

    def get_rows(self):
        # we don't have to override this method as we are overwritten
        # BaseRowsToS3CsvOperator_execute method
        pass

    @staticmethod
    def unpack_list_of_dicts(original_list):
        for original_dict in original_list:
            if not any(isinstance(v, list) for v in original_dict.values()):
                yield original_dict
                continue

            # Get keys excluding the ones containing lists
            # keys = [key for key, value in original_dict.items() if not isinstance(value, list)]

            # Extract values from the original dictionary
            non_list_values = {
                key: value
                for key, value in original_dict.items()
                if not isinstance(value, list)
            }

            # Loop through the list values
            list_values = [
                value for value in original_dict.values() if isinstance(value, list)
            ]
            for list_value in list_values:
                for item in list_value:
                    unpacked_item = non_list_values.copy()
                    unpacked_item.update(item)
                    yield unpacked_item

    @staticmethod
    def convert_unix_timestamp_to_date(unixtimestamp):
        unixtimestamp_seconds = unixtimestamp / 1000
        utc_datetime = datetime.datetime.utcfromtimestamp(unixtimestamp_seconds)

        pacific_tz = pytz.timezone("America/Los_Angeles")
        pst_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(pacific_tz)

        utc_human_readable = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
        pst_human_readable = pst_datetime.strftime("%Y-%m-%d %H:%M:%S")

        return utc_human_readable, pst_human_readable

    @staticmethod
    def remap_domain(domain):
        domain_remap = {
            "www.fabletics.dk": "DK",
            "www.fabletics.de": "DE",
            "www.fabletics.ca": "CA",
            "www.fabletics.co.uk": "UK",
            "www.fabletics.com": "US",
            "yitty.fabletics.com": "US",
            "www.fabletics.es": "ES",
            "www.fabletics.fr": "FR",
            "www.fabletics.nl": "NL",
            "www.fabletics.se": "SE",
            "preview.savagex.de": "DE",
            "preview.savagex.ca": "CA",
            "preview.savagex.co.uk": "UK",
            "preview.savagex.com": "US",
            "preview.savagex.es": "ES",
            "preview.savagex.fr": "FR",
            "preview.savagex.nl": "NL",
            "preview.savagex.se": "SE",
        }
        if "domain" in domain_remap:
            dim_store_country = domain_remap[domain]
        else:
            dim_store_country = ""
        return dim_store_country

    @staticmethod
    def remap_targeting_countries(targeting_country_list):
        targeting_country_list_remap = {
            "da-DK": "DK",
            "de-DE": "DE",
            "en-CA": "CA",
            "en-GB": "UK",
            "en-US": "US",
            "es-ES": "ES",
            "en-EU": "EU",
            "fr-FR": "FR",
            "nl-NL": "NL",
            "sv-SE": "SE",
        }
        targeting_countries = set()
        for country in targeting_country_list:
            targeting_countries.add(targeting_country_list_remap[country])

        targeting_country_string = "|".join(list(targeting_countries))
        # print(targeting_country_string)
        return targeting_country_string

    @staticmethod
    def stringify_targeting_list(target_property, target_value_list):
        target_values = set()
        for value in target_value_list:
            target_values.add(value)
        targeting_list = "|".join(list(target_values))
        return targeting_list
