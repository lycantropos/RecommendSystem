from typing import Optional

from sqlalchemy import (Column,
                        Integer,
                        BigInteger,
                        String)

from vizier.models.base import (Base,
                                ModelMixin)


class Plot(ModelMixin, Base):
    __tablename__ = 'plots'

    id = Column('id', BigInteger,
                primary_key=True,
                autoincrement=True)
    imdb_id = Column('imdb_id', Integer,
                     unique=True,
                     nullable=False)
    imdb_content = Column('imdb_content', String)
    wikipedia_content = Column('wikipedia_content', String)

    def __init__(self, imdb_id: int,
                 imdb_content: Optional[str],
                 wikipedia_content: Optional[str] = None):
        self.imdb_id = imdb_id
        self.imdb_content = imdb_content
        self.wikipedia_content = wikipedia_content
