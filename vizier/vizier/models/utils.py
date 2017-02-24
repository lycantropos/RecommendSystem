from typing import Generator

from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute


def get_columns_fields_names(cls: DeclarativeMeta) -> Generator[str, None, None]:
    for field_name, value in vars(cls).items():
        if isinstance(value, InstrumentedAttribute):
            yield field_name
