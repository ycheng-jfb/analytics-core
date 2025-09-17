from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from include.airflow.utils.db import SQL_ALCHEMY_SCHEMA

metadata = (
    None
    if not SQL_ALCHEMY_SCHEMA or SQL_ALCHEMY_SCHEMA.isspace()
    else MetaData(schema=SQL_ALCHEMY_SCHEMA)
)

Base = declarative_base(metadata=metadata)
