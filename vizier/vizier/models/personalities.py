from sqlalchemy import Column, Integer, String

from .base import Base


class Director(Base):
    __tablename__ = 'directors'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    name = Column('name', String, unique=True)

    def __init__(self, name: str):
        self.name = name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other: Base):
        if not isinstance(other, type(self)):
            return False
        return self.name == other.name


class Writer(Base):
    __tablename__ = 'writers'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    name = Column('name', String, unique=True)

    def __init__(self, name: str):
        self.name = name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other: Base):
        if not isinstance(other, type(self)):
            return False
        return self.name == other.name


class Actor(Base):
    __tablename__ = 'actors'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    name = Column('name', String, unique=True)

    def __init__(self, name: str):
        self.name = name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other: Base):
        if not isinstance(other, type(self)):
            return False
        return self.name == other.name
