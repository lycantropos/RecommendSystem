import logging
import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Union, List, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, Session

from vizier.config import (NOT_AVAILABLE_VALUE_ALIAS,
                           RELEASE_DATE_FORMAT)
from vizier.imdb_parser import query_imdb
from vizier.models.base import Base
from vizier.models.film import Film, Genre, Plot
from vizier.models.personalities import Director, Actor, Writer
from vizier.wiki_parser import (get_wiki_articles_by_years,
                                get_imdb_id,
                                get_plot_content)

UNRATED_CONTENT_RATINGS = {NOT_AVAILABLE_VALUE_ALIAS, 'NOT RATED', 'UNRATED'}

DbUriType = Union[str, URL]


@contextmanager
def get_session(engine: Engine,
                autoflush: bool = True,
                autocommit: bool = False,
                expire_on_commit: bool = True):
    session_factory = sessionmaker(bind=engine,
                                   autoflush=autoflush,
                                   autocommit=autocommit,
                                   expire_on_commit=expire_on_commit)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def parse_films(postgres_db_uri: DbUriType, start_year: int, stop_year: int):
    engine = create_engine(postgres_db_uri)
    with get_session(engine=engine) as session:
        for year, article_titles in get_wiki_articles_by_years(start=start_year,
                                                               stop=stop_year):
            for article_title in article_titles:
                film = get_film(article_title, year, session)
                if film is None:
                    continue
                session.add(film)
            session.commit()


def get_film(article_title: str, year: int, session: Session) -> Optional[Film]:
    imdb_id = get_imdb_id(article_title)
    resp = query_imdb(imdb_id=imdb_id, title=article_title, year=year)
    if resp['Response'] != 'True':
        err_message = ('No IMDb entry found '
                       f'for title: "{article_title}"')
        logging.error(err_message)
        return
    title = resp['Title']
    type = resp['Type']
    languages = resp['Language']
    countries = resp['Country']
    content_rating = parse_content_rating(resp['Rated'])
    imdb_id = parse_imdb_id(resp['imdbID'])
    imdb_rating = parse_rating(resp['imdbRating'])
    duration = parse_duration(resp['Runtime'])
    release_date_str = resp['Released']
    release_date = parse_date(release_date_str)
    poster_url = resp['Poster']
    film = Film(title=title, type=type, languages=languages,
                countries=countries, content_rating=content_rating,
                year=year, release_date=release_date, duration=duration,
                imdb_id=imdb_id, imdb_rating=imdb_rating,
                poster_url=poster_url,
                wikipedia_article_title=article_title)
    genres = parse_related_instances(cls=Genre,
                                     names_str=resp['Genre'])
    film.genres = genres
    directors = parse_related_instances(cls=Director,
                                        names_str=resp['Director'])
    directors = load_existing_instances(cls=Director,
                                        instances=directors,
                                        session=session)
    film.directors = directors
    actors = parse_related_instances(cls=Actor,
                                     names_str=resp['Actors'])
    actors = load_existing_instances(cls=Actor,
                                     instances=actors,
                                     session=session)
    film.actors = actors
    writers = parse_related_instances(cls=Writer,
                                      names_str=resp['Writer'])
    writers = load_existing_instances(cls=Writer,
                                      instances=writers,
                                      session=session)
    film.writers = writers
    plot_content = get_plot_content(article_title) or resp['Plot']
    plot = Plot(plot_content)
    film.plot = plot
    return film


def load_existing_instances(*, cls: Base,
                            instances: List[Base],
                            session: Session) -> List[Base]:
    res = []
    q = session.query(cls)
    for instance in instances:
        old = q.filter(cls.name == instance.name).first()
        if old is None:
            res.append(instance)
        else:
            res.append(old)
    return res


def parse_related_instances(*, cls: Base, names_str: str) -> List[Base]:
    names = parse_names(names_str)
    return [cls(name)
            for name in names
            if name != NOT_AVAILABLE_VALUE_ALIAS]


def parse_names(names_str: str, sep=',') -> List[str]:
    names = (actor_name.strip()
             for actor_name in names_str.split(sep))
    return list(filter(None, names))


def parse_content_rating(content_rating: str) -> Optional[str]:
    if content_rating in UNRATED_CONTENT_RATINGS:
        return None
    return content_rating


IMDB_ID_RE = re.compile('(?<=^tt)\d+$')


def parse_imdb_id(imdb_id_str: str) -> int:
    search_res = IMDB_ID_RE.search(imdb_id_str)
    return int(search_res.group(0).lstrip('0'))


def parse_rating(rating_str: str) -> Optional[float]:
    if rating_str == NOT_AVAILABLE_VALUE_ALIAS:
        return None
    return float(rating_str)


def parse_date(date_str) -> Optional[date]:
    if date_str == NOT_AVAILABLE_VALUE_ALIAS:
        return None
    return datetime.strptime(date_str, RELEASE_DATE_FORMAT).date()


def parse_duration(duration_str: str) -> Optional[timedelta]:
    if duration_str == NOT_AVAILABLE_VALUE_ALIAS:
        return None
    minutes_count = int(re.match('^\d+(?= min$)', duration_str).group(0))
    return timedelta(minutes=minutes_count)
