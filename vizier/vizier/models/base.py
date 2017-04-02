from typing import (Any,
                    Iterator)

from cetus.types import ColumnValueType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.attributes import InstrumentedAttribute

Base = declarative_base()


class ModelMixin:
    def __eq__(self, other: Base):
        cls = type(self)
        if not isinstance(other, cls):
            return False
        for column_field_name in self.columns_fields_names():
            if getattr(self, column_field_name) != getattr(other, column_field_name):
                return False
        return True

    def __repr__(self) -> str:
        cls = type(self)
        fields = (f'{column_field_name}={getattr(self, column_field_name)!r}'
                  for column_field_name in self.columns_fields_names())
        return f'{cls.__name__}({", ".join(fields)})'

    @classmethod
    def columns_fields_names(cls) -> Iterator[str]:
        for field_name, field_content in vars(cls).items():
            field_is_column = is_column_field(field_content)
            if not field_is_column:
                continue
            yield field_name

    @property
    def record(self) -> Iterator[ColumnValueType]:
        for column_field_name in self.columns_fields_names():
            yield getattr(self, column_field_name)


def is_column_field(field_content: Any) -> bool:
    return (isinstance(field_content, InstrumentedAttribute)
            and field_content.prop.strategy_wildcard_key == 'column')
