from sqlalchemy import Interval
from sqlalchemy import (Table, Column,
                        BigInteger, Integer,
                        String, ForeignKey)
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import relationship

from .base import Base
from .personalities import Director, Writer, Actor


class Genre(Base):
    __tablename__ = 'genres'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    name = Column('name', String, unique=True)

    def __init__(self, name: str):
        self.name = name


class Plot(Base):
    __tablename__ = 'plots'

    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    film_id = Column('film_id', BigInteger, ForeignKey('films.id'))


ContentRatings = ENUM('-12', '-17', '10', '11', '12', '12A', '12PG', '13', '13+', '14',
                      '14A', '15', '15A', '15PG', '16', '16+', '18', '18+', '18A',
                      '18PA', '18PL', '18SG', '18SX', '6', '7', 'A', 'A.G.', 'AL', 'All',
                      'Approved', 'Btl', 'E', 'G', 'GA', 'GY', 'I', 'I.C.-14', 'I.M.-18',
                      'IIA', 'IIB', 'III', 'K-10', 'K-10/K-7', 'K-12', 'K-12/K-9',
                      'K-13', 'K-14', 'K-16', 'K-18', 'K-8', 'K-8/K-5', 'KNT', 'KT', 'L',
                      'LH', 'Livre', 'M', 'M/12', 'M/16', 'M/18', 'M/4', 'M/6', 'M18',
                      'MA', 'NC-17', 'NC16', 'PG', 'PG-13', 'R', 'R(A)', 'R-13', 'R-18',
                      'R13', 'R16', 'R18', 'R21', 'RP13', 'RP16', 'RP18', 'S', 'T', 'TE',
                      'U', 'VM14', 'VM18', 'X', 'XXX', 'o.Al.',
                      name='ContentRatings')

films_directors_table = Table('films_directors', Base.metadata,
                              Column('film_id', Integer, ForeignKey('films.id')),
                              Column('director_id', Integer, ForeignKey('directors.id')))

films_actors_table = Table('films_actors', Base.metadata,
                           Column('film_id', Integer, ForeignKey('films.id')),
                           Column('actor_id', Integer, ForeignKey('actors.id')))

films_writers_table = Table('films_writers', Base.metadata,
                            Column('film_id', Integer, ForeignKey('films.id')),
                            Column('writer_id', Integer, ForeignKey('writers.id')))

films_genres_table = Table('films_genres', Base.metadata,
                           Column('film_id', Integer, ForeignKey('films.id')),
                           Column('genre_id', Integer, ForeignKey('genres.id')))


class Film(Base):
    __tablename__ = 'films'

    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    title = Column('title', String, nullable=False)
    countries = Column('countries', String)
    languages = Column('languages', String)
    duration = Column('duration', Interval)
    content_rating = Column('content_rating', ContentRatings)
    plot_id = Column('plot_id', BigInteger, ForeignKey('plots.id'))
    imdb_id = Column('imdb_id', Integer)
    wikipedia_article_name = Column('wikipedia_article_name', String, nullable=True)
    genres = relationship(Genre, secondary=films_genres_table)
    directors = relationship(Director, secondary=films_directors_table)
    actors = relationship(Actor, secondary=films_actors_table)
    writers = relationship(Writer, secondary=films_writers_table)
