from sqlalchemy import Column, Integer
from sqlalchemy.dialects.postgresql import ENUM

from vizier.models.base import Base, ModelMixin

GENRES_NAMES = ('Action', 'Adult', 'Adventure', 'Animation', 'Biography',
                'Comedy', 'Crime', 'Documentary', 'Drama', 'Family',
                'Fantasy', 'Film-Noir', 'History', 'Horror', 'Music',
                'Musical', 'Mystery', 'News', 'Romance', 'Sci-Fi',
                'Short', 'Sport', 'Thriller', 'War', 'Western')
GenreName = ENUM(*GENRES_NAMES, name='genre_name')


class Genre(Base, ModelMixin):
    __tablename__ = 'genres'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    name = Column('name', GenreName)

    def __init__(self, name: str):
        self.name = name
