from typing import Optional

from sqlalchemy import (Column,
                        BigInteger,
                        String)

from vizier.models.base import Base, ModelMixin


class Plot(Base, ModelMixin):
    __tablename__ = 'plots'

    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    imdb_content = Column('imdb_content', String)
    wikipedia_content = Column('wikipedia_content', String)

    def __init__(self, imdb_content: Optional[str],
                 wikipedia_content: Optional[str] = None):
        self.imdb_content = imdb_content
        self.wikipedia_content = wikipedia_content
