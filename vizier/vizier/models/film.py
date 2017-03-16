from datetime import (timedelta,
                      date)
from typing import (Optional,
                    Dict)

from sqlalchemy import (Table, Column,
                        ForeignKey,
                        BigInteger, Integer,
                        Float, String,
                        Date, Interval)
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import relationship

from .article import Article
from .base import Base, ModelMixin
from .genre import Genre
from .personalities import Director, Writer, Actor
from .plot import Plot
from .utils import (parse_year,
                    parse_imdb_id,
                    parse_content_rating,
                    parse_rating,
                    parse_date,
                    parse_duration,
                    normalize_value)

# SQLAlchemy uses "PascalCase" for column type names
# while PostgreSQL uses "snake_case" for enum names
FILM_TYPES = ('movie',)
FilmType = ENUM(*FILM_TYPES, name='film_type')

# more info at https://contribute.imdb.com/updates/guide/certificates
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
                   'VM14', 'VM18', 'X', 'XXX', 'o.Al.')
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


class Film(Base, ModelMixin):
    __tablename__ = 'films'

    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    type = Column('type', FilmType, nullable=False)
    title = Column('title', String, nullable=False)
    countries = Column('countries', String)
    languages = Column('languages', String)
    duration = Column('duration', Interval)
    release_date = Column('release_date', Date)
    content_rating = Column('content_rating', ContentRating)
    imdb_id = Column('imdb_id', Integer, unique=True, nullable=False)
    imdb_rating = Column('imdb_rating', Float)
    poster_url = Column('poster_url', String)
    plot_id = Column('plot_id', BigInteger, ForeignKey('plots.id'))
    article_id = Column('article_id', BigInteger, ForeignKey('articles.id'))

    genres = relationship(Genre, secondary=films_genres_table)
    directors = relationship(Director, secondary=films_directors_table)
    actors = relationship(Actor, secondary=films_actors_table)
    writers = relationship(Writer, secondary=films_writers_table)
    plot = relationship(Plot, uselist=False)
    article = relationship(Article, uselist=False)

    def __init__(self, title: str,
                 type: str,
                 languages: str,
                 countries: str,
                 content_rating: str,
                 year: int,
                 release_date: Optional[date],
                 duration: Optional[timedelta],
                 imdb_id: Optional[int],
                 imdb_rating: Optional[float],
                 poster_url: Optional[str]):
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

    @staticmethod
    def deserialize(raw_film: Dict[str, str]) -> 'Film':
        raw_film = dict(zip(raw_film.keys(),
                            map(normalize_value, raw_film.values())))
        title = raw_film['Title']
        type = raw_film['Type']
        languages = raw_film['Language']
        countries = raw_film['Country']
        content_rating = parse_content_rating(raw_film['Rated'])
        year = parse_year(raw_film['Year'])
        imdb_id = parse_imdb_id(raw_film['imdbID'])
        imdb_rating = parse_rating(raw_film['imdbRating'])
        duration = parse_duration(raw_film['Runtime'])
        release_date_str = raw_film['Released']
        release_date = parse_date(release_date_str)
        poster_url = raw_film['Poster']
        return Film(title=title,
                    type=type,
                    languages=languages,
                    countries=countries,
                    content_rating=content_rating,
                    year=year,
                    release_date=release_date,
                    duration=duration,
                    imdb_id=imdb_id,
                    imdb_rating=imdb_rating,
                    poster_url=poster_url)
