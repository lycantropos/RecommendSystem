from sqlalchemy import Column, Integer, String

from .base import Base


class Director(Base):
    __tablename__ = 'directors'

    id = Column('id', Integer, primary_key=True)
    name = Column('name', String, unique=True)
    origin = Column('origin', String)


class Writer(Base):
    __tablename__ = 'writers'

    id = Column('id', Integer, primary_key=True)
    name = Column('name', String, unique=True)
    origin = Column('origin', String)


class Actor(Base):
    __tablename__ = 'actors'

    id = Column('id', Integer, primary_key=True)
    name = Column('name', String, unique=True)
    origin = Column('origin', String)