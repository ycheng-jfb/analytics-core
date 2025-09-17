from abc import abstractmethod

from airflow.models import BaseOperator

from include.airflow.hooks.elasticsearch import ESHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvWatermarkOperator
from include.airflow.utils.utils import flatten_json, scrub_nones


class ElasticsearchGetBase(BaseRowsToS3CsvWatermarkOperator):
    """
    Get data from Elasticsearch

    Args:
        es_get_query_func: function that returns the query object, this class is passed as param
            when called
        es_sort: list on how to sort data when paginating results, required when paginating
            large datasets, see Elasticsearch docs
        es_host: Elasticsearch hostname
        es_protocol: protocol for requests to Elasticsearch
        es_port: Elasticsearch port
        es_index: index to query
    """

    def __init__(
        self,
        es_get_query_func,
        es_sort,
        es_host,
        es_protocol,
        es_port,
        es_index=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.es_get_query_func = es_get_query_func
        self.es_sort = es_sort
        self.es_host = es_host
        self.es_protocol = es_protocol
        self.es_port = es_port
        self.es_index = es_index

    @property
    def es_hook(self):
        return ESHook(
            es_conn_id=self.hook_conn_id,
            host=self.es_host,
            protocol=self.es_protocol,
            port=self.es_port,
        )

    @abstractmethod
    def format_result(self, result) -> dict:
        """
        Override with logic to format the outgoing objects
        """
        pass

    def get_rows(self, context=None):
        body = {"query": self.es_get_query_func(self), "sort": self.es_sort}
        results = self.es_hook.search_all(index=self.es_index, body=body)
        for result in results:
            formatted_result = self.format_result(result)
            yield formatted_result


class ElasticsearchGetSam(ElasticsearchGetBase):
    def get_high_watermark(self):
        body = {
            "aggs": {"max_datetime_modified": {"max": {"field": "datetime_modified"}}}
        }
        result = self.es_hook.conn.search(index=self.es_index, body=body)
        return result["aggregations"]["max_datetime_modified"]["value_as_string"]

    def format_result(self, result):
        flatten_result = flatten_json(result)
        flatten_result["json_blob"] = result
        scrub_nones(flatten_result)
        return flatten_result
