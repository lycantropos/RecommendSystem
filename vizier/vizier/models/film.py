from datetime import timedelta, date
from typing import Optional

from sqlalchemy import (Table, Column,
                        ForeignKey,
                        BigInteger, Integer,
                        Float, String,
                        Date, Interval)
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import relationship

from .base import Base
from .personalities import Director, Writer, Actor

GENRES_NAMES = ('Action', 'Adult', 'Adventure', 'Animation', 'Biography',
                'Comedy', 'Crime', 'Documentary', 'Drama', 'Family',
                'Fantasy', 'Film-Noir', 'History', 'Horror', 'Music',
                'Musical', 'Mystery', 'News', 'Romance', 'Sci-Fi',
                'Short', 'Sport', 'Thriller', 'War', 'Western')
# SQLAlchemy uses "PascalCase" for column type names
# while PostgreSQL uses "snake_case" for enum names
GenreName = ENUM(*GENRES_NAMES, name='genre_name')

FILM_TYPES = ('movie',)
FilmType = ENUM(*FILM_TYPES, name='film_type')

CONTENT_RATINGS = ('-12', '-17', '10', '11', '12', '12A', '12PG', '13', '13+',
                   '14', '14A', '15', '15A', '15PG', '16', '16+', '18', '18+',
                   '18A', '18PA', '18PL', '18SG', '18SX', '6', '7', 'A', 'A.G.',
                   'AL', 'All', 'Approved', 'Btl', 'E', 'G', 'GA', 'GY', 'I',
                   'I.C.-14', 'I.M.-18', 'IIA', 'IIB', 'III', 'K-10', 'K-10/K-7',
                   'K-12', 'K-12/K-9', 'K-13', 'K-14', 'K-16', 'K-18', 'K-8',
                   'K-8/K-5', 'KNT', 'KT', 'L', 'LH', 'Livre', 'M', 'M/12',
                   'M/16', 'M/18', 'M/4', 'M/6', 'M18', 'MA', 'NC-17', 'NC16',
                   'PG', 'PG-13', 'R', 'R(A)', 'R-13', 'R-18', 'R13', 'R16',
                   'R18', 'R21', 'RP13', 'RP16', 'RP18', 'S', 'T', 'TE', 'U',
                   'VM14', 'VM18', 'X', 'XXX', 'o.Al.', 'NOT RATED')
ContentRating = ENUM(*CONTENT_RATINGS, name='content_rating')

films_directors_table = Table('films_directors', Base.metadata,
                              Column('film_id', Integer,
                                     ForeignKey('films.id')),
                              Column('director_id', Integer,
                                     ForeignKey('directors.id')))

films_actors_table = Table('films_actors', Base.metadata,
                           Column('film_id', Integer,
                                  ForeignKey('films.id')),
                           Column('actor_id', Integer,
                                  ForeignKey('actors.id')))

films_writers_table = Table('films_writers', Base.metadata,
                            Column('film_id', Integer,
                                   ForeignKey('films.id')),
                            Column('writer_id', Integer,
                                   ForeignKey('writers.id')))

films_genres_table = Table('films_genres', Base.metadata,
                           Column('film_id', Integer,
                                  ForeignKey('films.id')),
                           Column('genre_id', Integer,
                                  ForeignKey('genres.id')))


class Genre(Base):
    __tablename__ = 'genres'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    name = Column('name', GenreName)

    def __init__(self, name: str):
        self.name = name


class Plot(Base):
    __tablename__ = 'plots'

    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    content = Column('content', String)

    def __init__(self, content: str):
        self.content = content


class Film(Base):
    __tablename__ = 'films'

    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    type = Column('type', FilmType, nullable=False)
    title = Column('title', String, nullable=False)
    countries = Column('countries', String)
    languages = Column('languages', String)
    duration = Column('duration', Interval)
    release_date = Column('release_date', Date)
    content_rating = Column('content_rating', ContentRating)
    imdb_id = Column('imdb_id', Integer, nullable=False)
    imdb_rating = Column('imdb_rating', Float)
    poster_url = Column('poster_url', String)
    plot_id = Column('plot_id', BigInteger, ForeignKey('plots.id'))
    wikipedia_article_title = Column('wikipedia_article_title', String, nullable=False)

    genres = relationship(Genre, secondary=films_genres_table)
    directors = relationship(Director, secondary=films_directors_table)
    actors = relationship(Actor, secondary=films_actors_table)
    writers = relationship(Writer, secondary=films_writers_table)
    plot = relationship(Plot, uselist=False)

    def __init__(self, title: str, type: str,
                 languages: str, countries: str,
                 content_rating: str,
                 year: int, release_date: Optional[date],
                 duration: Optional[timedelta],
                 imdb_id: Optional[int], imdb_rating: Optional[float],
                 poster_url: Optional[str],
                 wikipedia_article_title: str):
        self.type = type
        self.title = title
        self.countries = countries
        self.languages = languages
        self.duration = duration
        self.year = year
        self.release_date = release_date
        self.content_rating = content_rating
        self.poster_url = poster_url
        self.imdb_id = imdb_id
        self.imdb_rating = imdb_rating
        self.wikipedia_article_title = wikipedia_article_title
