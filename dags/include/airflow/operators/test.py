from typing import Any

from airflow.models import BaseOperator


class XcomPushOperator(BaseOperator):
    def __init__(self, xcom_val="testval", *args, **kwargs):
        self.xcom_val = xcom_val
        super(XcomPushOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.xcom_push(context, key="testxcom", value=self.xcom_val)


class XcomPullOperator(BaseOperator):
    def __init__(self, include_all=False, *args, **kwargs):
        self.include_all = include_all
        super(XcomPullOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        ret = self.xcom_pull(
            context, key="testxcom", include_prior_dates=self.include_all
        )
        print(ret)


class XcomReturnOperator(BaseOperator):
    def __init__(self, xcom_val="testval", *args, **kwargs):
        self.xcom_val = xcom_val
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return self.xcom_val


class TemplateOperator(BaseOperator):
    template_fields = ("tfield",)

    def __init__(self, tfield: Any, *args, **kwargs):
        self.tfield = tfield
        super().__init__(*args, **kwargs)

    def execute(self, context):
        print(f"templated field: {self.tfield}")
