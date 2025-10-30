from dataclasses import dataclass


@dataclass
class Config:
    schema: str
    table: str
    version: str
    column_list: list
    database: str = 'lake'

    @property
    def s3_prefix(self):
        return f'lake/{self.database}.{self.schema}.{self.table}/{self.version}'
