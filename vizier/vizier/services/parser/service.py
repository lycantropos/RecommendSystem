import logging
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import requests
from sqlalchemy.orm import Session

from vizier.config import IMDB_API_URL
from vizier.config import (NOT_AVAILABLE_VALUE_ALIAS,
                           RELEASE_DATE_FORMAT)
from vizier.models import (Film, Genre, Plot,
                           Director, Actor, Writer)
from vizier.models.base import Base
from vizier.services.data_access import (DbUriType,
                                         get_engine,
                                         get_session)
from .imdb import query_imdb
from .wikipedia import (get_articles_titles_by_years,
                        get_imdb_id,
                        get_plot_content)

UNRATED_CONTENT_RATINGS = {NOT_AVAILABLE_VALUE_ALIAS, 'NOT RATED', 'UNRATED'}
IMDB_ID_RE = re.compile('(?<=^tt)\d+$')
DURATION_RE = re.compile('^\d+(?= min$)')


def parse_films(postgres_db_uri: DbUriType, start_year: int, stop_year: int):
    with get_engine(postgres_db_uri) as engine:
        with get_session(engine=engine) as session:
            for year, article_titles in get_articles_titles_by_years(start=start_year,
                                                                     stop=stop_year):
                logging.info(f'Processing films from year "{year}"')
                for article_title in article_titles:
                    logging.info(f'Processing article "{article_title}"')
                    imdb_id = get_imdb_id(article_title)
                    response = query_imdb(imdb_id=imdb_id,
                                          title=article_title,
                                          year=year)
                    if not response.ok:
                        status_code = response.status_code
                        err_message = ('IMDb API located '
                                       f'at "{IMDB_API_URL}" '
                                       'answered with '
                                       f'status code "{status_code}"')
                        logging.error(err_message)
                        continue
                    film_info = response.json()
                    if film_info['Response'] != 'True':
                        err_message = ('No IMDb entry found '
                                       f'for title: "{article_title}"')
                        logging.error(err_message)
                        continue
                    film = get_film(film_info=film_info,
                                    article_title=article_title,
                                    year=year,
                                    session=session)
                    session.add(film)
                session.commit()


def get_film(film_info: Dict[str, str], *,
             article_title: str,
             year: int, session: Session) -> Film:
    title = film_info['Title']
    type = film_info['Type']
    languages = film_info['Language']
    countries = film_info['Country']
    content_rating = parse_content_rating(film_info['Rated'])
    imdb_id = parse_imdb_id(film_info['imdbID'])
    imdb_rating = parse_rating(film_info['imdbRating'])
    duration = parse_duration(film_info['Runtime'])
    release_date_str = film_info['Released']
    release_date = parse_date(release_date_str)
    poster_url = film_info['Poster']
    film = Film(title=title,
                type=type,
                languages=languages,
                countries=countries,
                content_rating=content_rating,
                year=year,
                release_date=release_date,
                duration=duration,
                imdb_id=imdb_id,
                imdb_rating=imdb_rating,
                poster_url=poster_url,
                wikipedia_article_title=article_title)
    genres = parse_related_instances(cls=Genre,
                                     names_str=film_info['Genre'])
    directors = parse_related_instances(cls=Director,
                                        names_str=film_info['Director'])
    directors = load_existing_instances(cls=Director,
                                        instances=directors,
                                        session=session)
    actors = parse_related_instances(cls=Actor,
                                     names_str=film_info['Actors'])
    actors = load_existing_instances(cls=Actor,
                                     instances=actors,
                                     session=session)
    writers = parse_related_instances(cls=Writer,
                                      names_str=film_info['Writer'])
    writers = load_existing_instances(cls=Writer,
                                      instances=writers,
                                      session=session)
    plot_content = get_plot_content(article_title) or film_info['Plot']
    plot = Plot(plot_content)
    film.genres = genres
    film.directors = directors
    film.actors = actors
    film.writers = writers
    film.plot = plot
    return film


def load_existing_instances(*, cls: Base,
                            instances: List[Base],
                            session: Session) -> List[Base]:
    res = []
    for instance in instances:
        old = (session.query(cls)
               .filter(cls.name == instance.name)
               .first())
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
    match = DURATION_RE.match(duration_str)
    minutes_count = int(match.group(0))
    return timedelta(minutes=minutes_count)
