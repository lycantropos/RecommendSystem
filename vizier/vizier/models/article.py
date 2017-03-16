from sqlalchemy import (Column,
                        BigInteger,
                        String)
from sqlalchemy import Integer

from .base import Base, ModelMixin


class Article(Base, ModelMixin):
    __tablename__ = 'articles'

    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    title = Column('title', String, nullable=False)
    year = Column('year', Integer, nullable=False)

    def __init__(self, title: str, year: int):
        self.title = title
        self.year = year
