from sqlalchemy import (Column,
                        Integer,
                        String)

from .base import (Base,
                   ModelMixin)


class Director(ModelMixin, Base):
    __tablename__ = 'directors'

    id = Column('id', Integer,
                primary_key=True,
                autoincrement=True)
    name = Column('name', String,
                  nullable=False,
                  unique=True)

    def __init__(self, name: str):
        self.name = name

    def __hash__(self):
        return hash(self.name)


class Writer(ModelMixin, Base):
    __tablename__ = 'writers'

    id = Column('id', Integer,
                primary_key=True,
                autoincrement=True)
    name = Column('name', String,
                  nullable=False,
                  unique=True)

    def __init__(self, name: str):
        self.name = name

    def __hash__(self):
        return hash(self.name)


class Actor(ModelMixin, Base):
    __tablename__ = 'actors'

    id = Column('id', Integer,
                primary_key=True,
                autoincrement=True)
    name = Column('name', String,
                  nullable=False,
                  unique=True)

    def __init__(self, name: str):
        self.name = name

    def __hash__(self):
        return hash(self.name)
