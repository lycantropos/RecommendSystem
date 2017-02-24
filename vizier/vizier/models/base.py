from sqlalchemy.ext.declarative import declarative_base

from vizier.models.utils import get_columns_fields_names

Base = declarative_base()


class ModelMixin:
    def __eq__(self, other: Base):
        cls = type(self)
        if not isinstance(other, cls):
            return False
        for column_field_name in get_columns_fields_names(cls):
            if getattr(self, column_field_name) != getattr(other, column_field_name):
                return False
        return True

    def __repr__(self) -> str:
        cls = type(self)
        fields = (f'{column_field_name}={getattr(self, column_field_name)!r}'
                  for column_field_name in get_columns_fields_names(cls))
        return f'{cls.__name__}({", ".join(fields)})'
