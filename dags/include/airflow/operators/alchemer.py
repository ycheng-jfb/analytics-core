import re
import traceback
from abc import abstractmethod

import pendulum
from functools import cached_property
from dateutil.parser import parse
from dateutil.tz import tz

from include.airflow.hooks.alchemer import AlchemerHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvWatermarkOperator


class AlchemerBase(BaseRowsToS3CsvWatermarkOperator):
    @cached_property
    def alchemer_hook(self):
        return (
            AlchemerHook(conn_id=self.hook_conn_id)
            if self.hook_conn_id
            else AlchemerHook()
        )

    @abstractmethod
    def get_rows(self):
        raise NotImplementedError


class AlchemerGetSurveyResponses(AlchemerBase):
    def __init__(
        self,
        full_survey_pull=True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cleaner = re.compile("<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});")
        self.full_survey_pull = full_survey_pull

    def get_high_watermark(self):
        return pendulum.DateTime.utcnow().isoformat()

    def format_str_timestamp(self, str_ts):
        return pendulum.parse(str_ts).format("YYYY-MM-DD+HH:mm:ss")

    def get_status_filter_param(self):
        return {
            "filter[field][2]": "status",
            "filter[operator][2]": "=",
            "filter[value][2]": "Complete",
        }

    def get_submitted_filter_params(self):
        return {
            "filter[field][0]": "date_submitted",
            "filter[operator][0]": ">=",
            "filter[value][0]": f"{self.format_str_timestamp(self.low_watermark)}",
            "filter[field][1]": "date_submitted",
            "filter[operator][1]": "<",
            "filter[value][1]": f"{self.format_str_timestamp(self.new_high_watermark)}",
        }

    def get_rows(self):
        updated_at_d = {"updated_at": pendulum.DateTime.utcnow()}
        surveys = self.alchemer_hook.make_get_request_all("survey")
        for survey in surveys:
            survey_questions = [
                x
                for x in self.alchemer_hook.make_get_request_all(
                    f'survey/{survey["id"]}/surveyquestion'
                )
            ]
            # If survey been modified after last job run, pull all responses
            if (
                self.full_survey_pull
                and parse(survey["modified_on"]).timestamp()
                > parse(self.low_watermark).timestamp()
            ):
                responses = self.alchemer_hook.make_get_request_all(
                    f'survey/{survey["id"]}/surveyresponse',
                    params=self.get_status_filter_param(),
                )
            else:
                responses = self.alchemer_hook.make_get_request_all(
                    f'survey/{survey["id"]}/surveyresponse',
                    params={
                        **self.get_submitted_filter_params(),
                        **self.get_status_filter_param(),
                    },
                )
            for resp in responses:
                try:
                    # if no response data, continue to next response
                    if (
                        type(resp["survey_data"]) == list
                        and len(resp["survey_data"]) == 0
                    ):
                        continue

                    # get standard fields
                    resp_dict = {
                        "survey_id": survey["id"],
                        "survey_title": survey["title"],
                        "respondent_id": resp["id"],
                        "session_id": resp["session_id"],
                        "contact_id": resp["contact_id"],
                        "date_submitted": parse(
                            resp["date_submitted"],
                            yearfirst=True,
                            tzinfos={
                                "EDT": tz.gettz("US/Eastern"),
                                "EST": tz.gettz("US/Eastern"),
                            },
                        ),
                    }
                    if isinstance(resp["url_variables"], dict):
                        if resp["url_variables"].get("customer_id"):
                            resp_dict["customer_id"] = resp["url_variables"][
                                "customer_id"
                            ].get("value")
                            resp_dict["customer_id_type"] = resp["url_variables"][
                                "customer_id"
                            ].get("type")

                    # get default metadata fields of interest
                    metadata_names = ["language", "status", "country"]
                    for metadata_name in metadata_names:
                        field_dict = {
                            "l1_question_id": metadata_name,
                            "question_type": "metadata",
                        }
                        field_details = self.format_details(
                            f'{survey["id"]}_{metadata_name}',
                            None,
                            None,
                            resp.get(metadata_name),
                            None,
                        )
                        yield {
                            **resp_dict,
                            **field_dict,
                            **field_details,
                            **updated_at_d,
                        }

                    # get question specific fields
                    for ques_id, ques_resp in resp["survey_data"].items():
                        if not ques_resp.get("shown"):
                            continue

                        ques_id_padded = ques_id.zfill(3)
                        l1_question_label = self.clean_text(ques_resp["question"])
                        ques_details = {
                            "l1_question_id": ques_id,
                            "l1_question_label": l1_question_label,
                            "question_type": ques_resp.get("type"),
                        }

                        # check which type of question to determine where final fields pulled from
                        if ques_resp.get("parent"):
                            resp_details = self.parent_resp_details(
                                survey_questions,
                                ques_resp,
                                ques_details,
                                ques_id_padded,
                                ques_id,
                                survey,
                            )
                        elif str(ques_resp["type"]).upper() == "RANK" and ques_resp.get(
                            "answer"
                        ):
                            resp_details = self.rank_resp_details(
                                ques_resp, survey, ques_id_padded, l1_question_label
                            )
                        elif ques_resp.get("subquestions"):
                            resp_details = self.subquestion_resp_details(
                                ques_resp, ques_details, survey, ques_id_padded
                            )
                        elif ques_resp.get("options"):
                            resp_details = self.options_resp_details(
                                survey_questions,
                                ques_resp,
                                ques_details,
                                survey,
                                ques_id_padded,
                            )
                        else:
                            resp_details = [
                                self.format_details(
                                    f'{survey["id"]}_{ques_id_padded}',
                                    None,
                                    None,
                                    ques_resp.get("answer"),
                                    l1_question_label,
                                    answer_id=ques_resp.get("answer_id"),
                                )
                            ]

                        for resp_deets in resp_details:
                            yield {
                                **resp_dict,
                                **ques_details,
                                **resp_deets,
                                **updated_at_d,
                            }
                except Exception:
                    print(
                        f"Error - Survey ID: {survey['id']} | Response ID: {resp['id']}"
                    )
                    print(f"Question Id: {locals().get('ques_id')}")
                    traceback.print_exc()

    def format_details(
        self,
        question_id,
        l3_question_id,
        l3_question_label,
        response,
        l1_question_label,
        is_checkbox=False,
        l2_question_id=None,
        l2_question_label=None,
        answer_id=None,
    ):
        a = {
            "question_id": question_id,
            "l3_question_id": l3_question_id,
            "l3_question_label": l3_question_label,
            "l2_question_id": l2_question_id,
            "l2_question_label": l2_question_label,
            "answer_id": answer_id,
        }

        if is_checkbox:
            if response:
                b = {
                    "response": 1,
                    "response_numeric": 1,
                    "response_text": "true",
                    "response_alt": response if response != l3_question_label else None,
                }
            else:
                b = {
                    "response": 0,
                    "response_numeric": 0,
                    "response_text": "false",
                }

        else:
            if response and str(response).isdigit():
                b = {
                    "response": response,
                    "response_numeric": response,
                }
            else:
                b = {
                    "response": response,
                    "response_text": response,
                }

        if l2_question_label:
            b["question_label"] = (
                f"{l1_question_label} | {l2_question_label} | {l3_question_label}"
            )
        elif l3_question_label:
            b["question_label"] = f"{l1_question_label} | {l3_question_label}"
        elif l1_question_label:
            b["question_label"] = f"{l1_question_label}"

        return {**a, **b}

    def clean_text(self, text, cleaner=None):
        if text:
            cleaner = cleaner if cleaner else self.cleaner
            text = re.sub(cleaner, "", text)
            text = text.replace("\r\n", " ")
            text = text.replace("\n", " ")
            text = text.replace("\r", " ")
        return text

    def parent_resp_details(
        self, survey_questions, ques_resp, ques_details, ques_id_padded, ques_id, survey
    ):
        l2_question_label = ques_details["l1_question_label"]

        for survey_ques in survey_questions:
            if survey_ques["id"] == ques_resp["parent"]:
                ques_details["l1_question_label"] = survey_ques["title"]["English"]
                break

        parent_id_padded = str(ques_resp["parent"]).zfill(3)
        ques_details["l1_question_id"] = ques_resp["parent"]
        resp_details = self.format_details(
            f'{survey["id"]}_{parent_id_padded}_{ques_id_padded}',
            ques_id,
            l2_question_label,
            ques_resp.get("answer"),
            ques_details["l1_question_label"],
            answer_id=ques_resp.get("answer_id"),
        )
        yield resp_details

    def rank_resp_details(self, ques_resp, survey, ques_id_padded, l1_question_label):
        for ans_id, ans_obj in ques_resp["answer"].items():
            resp_details = self.format_details(
                f'{survey["id"]}_{ques_id_padded}_{ans_id.zfill(3)}',
                ans_id,
                self.clean_text(ans_obj.get("option")),
                ans_obj.get("rank"),
                l1_question_label,
            )
            yield resp_details

    def subquestion_resp_details(self, ques_resp, ques_details, survey, ques_id_padded):
        for sub_ques_id, sub_ques_resp in ques_resp["subquestions"].items():
            if (
                not sub_ques_resp.get("id")
                and not sub_ques_resp.get("question")
                and not sub_ques_resp.get("answer")
            ):
                for key in sub_ques_resp.keys():
                    sub_sub_question = sub_ques_resp[key]
                    if sub_sub_question.get("type"):
                        ques_details["question_type"] = sub_sub_question["type"]

                    question_id = (
                        f'{survey["id"]}_{ques_id_padded}_'
                        f'{sub_ques_id.zfill(3)}_'
                        f'{str(sub_sub_question["id"]).zfill(3)}'
                    )
                    split_labels = self.clean_text(
                        sub_sub_question.get("question")
                    ).split(" : ")

                    if (
                        sub_sub_question.get("type")
                        and str(sub_sub_question.get("type")).upper() == "CHECKBOX"
                    ):
                        resp_details = self.format_details(
                            question_id,
                            sub_sub_question["id"],
                            split_labels[1],
                            sub_sub_question.get("answer"),
                            ques_details["l1_question_label"],
                            is_checkbox=True,
                            l2_question_id=sub_ques_id,
                            l2_question_label=split_labels[0],
                        )
                    else:
                        resp_details = self.format_details(
                            question_id,
                            sub_sub_question["id"],
                            split_labels[1],
                            sub_sub_question.get("answer"),
                            ques_details["l1_question_label"],
                            l2_question_id=sub_ques_id,
                            l2_question_label=split_labels[0],
                        )
                    yield resp_details
            else:
                if sub_ques_resp.get("type"):
                    ques_details["question_type"] = sub_ques_resp["type"]
                resp_details = self.format_details(
                    f'{survey["id"]}_{ques_id_padded}_{sub_ques_id.zfill(3)}',
                    sub_ques_id,
                    self.clean_text(sub_ques_resp.get("question")),
                    sub_ques_resp.get("answer"),
                    ques_details["l1_question_label"],
                    answer_id=sub_ques_resp.get("answer_id"),
                )
                yield resp_details

    def options_resp_details(
        self, survey_questions, ques_resp, ques_details, survey, ques_id_padded
    ):
        is_checkbox = False
        parent_ques = None
        for survey_ques in survey_questions:
            if survey_ques["id"] == ques_resp["id"]:
                ques_details["question_type"] = survey_ques["type"]
                if survey_ques["type"] == "CHECKBOX":
                    is_checkbox = True
                    parent_ques = survey_ques
                break

        if is_checkbox:
            for parent_opt in parent_ques["options"]:
                sub_ques_resp = ques_resp["options"].get(str(parent_opt["id"]))
                if sub_ques_resp:
                    resp_details = self.format_details(
                        f'{survey["id"]}_{ques_id_padded}_{str(sub_ques_resp["id"]).zfill(3)}',
                        sub_ques_resp["id"],
                        sub_ques_resp.get("option"),
                        sub_ques_resp.get("answer"),
                        ques_details["l1_question_label"],
                        is_checkbox=True,
                    )
                else:
                    resp_details = self.format_details(
                        f'{survey["id"]}_{ques_id_padded}_{str(parent_opt["id"]).zfill(3)}',
                        parent_opt["id"],
                        parent_opt.get("value"),
                        None,
                        ques_details["l1_question_label"],
                        is_checkbox=True,
                    )
                yield resp_details
        else:
            for sub_ques_id, sub_ques_resp in ques_resp["options"].items():
                resp_details = self.format_details(
                    f'{survey["id"]}_{ques_id_padded}_{sub_ques_id.zfill(3)}',
                    sub_ques_id,
                    sub_ques_resp.get("option"),
                    str(sub_ques_resp.get("answer")),
                    ques_details["l1_question_label"],
                    answer_id=sub_ques_resp.get("answer_id"),
                )
                yield resp_details
