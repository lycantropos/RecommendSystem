from typing import Iterator

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
        for field_name, value in vars(cls).items():
            if isinstance(value, InstrumentedAttribute):
                yield field_name
