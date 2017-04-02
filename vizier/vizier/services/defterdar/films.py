import math
from asyncio import (AbstractEventLoop,
                     gather, ensure_future)
from typing import (Any,
                    Iterable,
                    Dict, List)

import logging
from aiohttp import ClientSession
from cetus.data_access import (get_connection_pool,
                               is_db_uri_mysql,
                               fetch,
                               fetch_records_count,
                               insert,
                               insert_returning)
from cetus.queries import ORDERS_ALIASES
from cetus.types import (ConnectionPoolType,
                         ConnectionType,
                         FiltersType,
                         OrderingType,
                         RecordType)
from sqlalchemy import Table
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import DeclarativeMeta

from vizier.config import NOT_AVAILABLE_VALUE_ALIAS
from vizier.models import (Article,
                           Genre, Plot,
                           Writer, Director,
                           Actor, Film)
from vizier.models.base import Base
from vizier.models.film import (films_genres_table,
                                films_directors_table,
                                films_writers_table,
                                films_actors_table)
from vizier.models.utils import parse_imdb_id
from vizier.services.imdb import get_raw_film

logger = logging.getLogger(__name__)

async def parse_films(*,
                      start_year: int,
                      stop_year: int,
                      max_connections: int = 50,
                      step: int = 10_000,
                      db_uri: URL,
                      loop: AbstractEventLoop) -> None:
    db_is_mysql = await is_db_uri_mysql(db_uri)
    table_name = Article.__tablename__
    columns_names = [column.name for column in Article.__table__.columns]
    filters = 'BETWEEN', (Article.year.name, (start_year, stop_year))
    orderings = [(Article.year.name, ORDERS_ALIASES['ascending'])]
    async with get_connection_pool(db_uri=db_uri,
                                   is_mysql=db_is_mysql,
                                   max_size=max_connections,
                                   loop=loop) as connection_pool, \
            ClientSession() as session:
        async with connection_pool.acquire() as connection:
            records_count = await fetch_records_count(table_name=table_name,
                                                      filters=filters,
                                                      is_mysql=db_is_mysql,
                                                      connection=connection)
        logger.info(f'Found {records_count} '
                    f'films articles records.')
        for offset in range(0, records_count, step):
            limit = min(records_count - offset, step)
            await parse_films_step(table_name=table_name,
                                   columns_names=columns_names,
                                   filters=filters,
                                   orderings=orderings,
                                   limit=limit,
                                   offset=offset,
                                   max_connections=max_connections,
                                   is_mysql=db_is_mysql,
                                   connection_pool=connection_pool,
                                   session=session)
        logger.info(f'Successfully processed parsing '
                    f'films by articles, '
                    f'records handled.')


async def parse_films_step(*,
                           table_name: str,
                           columns_names: List[str],
                           filters: FiltersType,
                           orderings: List[OrderingType],
                           limit: int,
                           offset: int,
                           max_connections: int,
                           is_mysql: bool,
                           connection_pool: ConnectionPoolType,
                           session: ClientSession) -> None:
    batch_size = max(math.ceil(limit / max_connections), 1)
    stop = offset + limit
    for batch_offset in range(offset, stop, batch_size):
        batch_limit = min(stop - offset, batch_size)
        await parse_films_batch(table_name=table_name,
                                columns_names=columns_names,
                                filters=filters,
                                orderings=orderings,
                                limit=batch_limit,
                                offset=batch_offset,
                                is_mysql=is_mysql,
                                connection_pool=connection_pool,
                                session=session)


async def parse_films_batch(*,
                            table_name: str,
                            columns_names: List[str],
                            filters: FiltersType,
                            orderings: List[OrderingType],
                            limit: int,
                            offset: int,
                            is_mysql: bool,
                            connection_pool: ConnectionPoolType,
                            session: ClientSession) -> None:
    logger.info('Processing '
                'films articles '
                f'from {offset + 1} '
                f'to {offset + limit}.')
    async with connection_pool.acquire() as connection:
        articles_records = await fetch(table_name=table_name,
                                       columns_names=columns_names,
                                       filters=filters,
                                       orderings=orderings,
                                       limit=limit,
                                       offset=offset,
                                       is_mysql=is_mysql,
                                       connection=connection)
    tasks = [ensure_future(get_raw_film(article_id=article_id,
                                        article_title=article_title,
                                        year=year,
                                        session=session))
             for article_id, article_title, year in articles_records]
    results = await gather(*tasks)
    raw_films = list(filter(None, results))
    logger.info(f'Parsed {len(raw_films)} films data.')
    films = list(map(Film.deserialize, raw_films))
    films_genres = map(parse_genres, raw_films)
    films_directors = map(parse_directors, raw_films)
    films_writers = map(parse_writers, raw_films)
    films_actors = map(parse_actors, raw_films)
    films_plots = map(parse_plot, raw_films)

    async with connection_pool.acquire() as connection:
        films_plots_ids = await save_instances(
            films_plots,
            cls=Plot,
            connection=connection,
            is_mysql=is_mysql)

        for film, film_plot_id in zip(films, films_plots_ids):
            film.plot_id = film_plot_id
        films_ids = await save_instances(
            films,
            cls=Film,
            connection=connection,
            is_mysql=is_mysql)

        films_genres_ids = [
            await save_instances(
                film_genres,
                cls=Genre,
                connection=connection,
                is_mysql=is_mysql)
            for film_genres in films_genres]
        films_directors_ids = [
            await save_instances(
                film_directors,
                cls=Director,
                connection=connection,
                is_mysql=is_mysql)
            for film_directors in films_directors]
        films_writers_ids = [
            await save_instances(
                film_writers,
                cls=Writer,
                connection=connection,
                is_mysql=is_mysql)
            for film_writers in films_writers]
        films_actors_ids = [
            await save_instances(
                film_actors,
                cls=Actor,
                connection=connection,
                is_mysql=is_mysql)
            for film_actors in films_actors]

        await save_relation(
            films_ids=films_ids,
            films_related_objects_ids=films_genres_ids,
            relation_table=films_genres_table,
            connection=connection,
            is_mysql=is_mysql)
        await save_relation(
            films_ids=films_ids,
            films_related_objects_ids=films_directors_ids,
            relation_table=films_directors_table,
            connection=connection,
            is_mysql=is_mysql)
        await save_relation(
            films_ids=films_ids,
            films_related_objects_ids=films_writers_ids,
            relation_table=films_writers_table,
            connection=connection,
            is_mysql=is_mysql)
        await save_relation(
            films_ids=films_ids,
            films_related_objects_ids=films_actors_ids,
            relation_table=films_actors_table,
            connection=connection,
            is_mysql=is_mysql)
    logger.info('Successfully processed '
                'films articles '
                f'from {offset + 1} '
                f'to {offset + limit}.')


async def save_relation(
        *, films_ids: Iterable[int],
        films_related_objects_ids: Iterable[List[int]],
        relation_table: Table,
        connection: ConnectionType,
        is_mysql: bool) -> None:
    table_name = relation_table.name
    columns_names = [column.name
                     for column in relation_table.columns]
    for film_id, film_related_objects_ids in zip(films_ids,
                                                 films_related_objects_ids):
        records_count = len(film_related_objects_ids)
        records = list(zip([film_id] * records_count,
                           film_related_objects_ids))
        await insert(table_name=table_name,
                     columns_names=columns_names,
                     records=records,
                     connection=connection,
                     is_mysql=is_mysql)


async def get_primary_key(table: Table
                          ) -> str:
    return next(column.name
                for column in table.columns
                if column.primary_key)


async def get_unique_columns_names(
        table: Table) -> List[str]:
    return [column.name
            for column in table.columns
            if column.unique]


async def save_instances(
        instances: Iterable[DeclarativeMeta], *,
        cls: DeclarativeMeta,
        connection: ConnectionType,
        is_mysql: bool) -> List[int]:
    columns_names = list(cls.columns_fields_names())
    primary_key = await get_primary_key(table=cls.__table__)
    primary_key_column_index = columns_names.index(primary_key)
    columns_names.pop(primary_key_column_index)
    unique_columns_names = await get_unique_columns_names(cls.__table__)
    returning_columns_names = [primary_key]

    def record_without_id(instance: DeclarativeMeta
                          ) -> RecordType:
        res = list(instance.record)
        res.pop(primary_key_column_index)
        return tuple(res)

    records = map(record_without_id, instances)
    resp = await insert_returning(
        table_name=cls.__tablename__,
        columns_names=columns_names,
        unique_columns_names=unique_columns_names,
        returning_columns_names=returning_columns_names,
        records=records,
        merge=True,
        connection=connection,
        is_mysql=is_mysql)
    return [row[0] for row in resp]


def parse_actors(raw_film: Dict[str, Any]
                 ) -> List[Actor]:
    return parse_related_instances(
        cls=Actor,
        names_str=raw_film['Actors'])


def parse_writers(raw_film: Dict[str, Any]
                  ) -> List[Writer]:
    return parse_related_instances(
        cls=Writer,
        names_str=raw_film['Writer'])


def parse_directors(raw_film: Dict[str, Any]
                    ) -> List[Director]:
    return parse_related_instances(
        cls=Director,
        names_str=raw_film['Director'])


def parse_genres(raw_film: Dict[str, str]
                 ) -> List[Genre]:
    return parse_related_instances(
        cls=Genre,
        names_str=raw_film['Genre'])


def parse_plot(raw_film: Dict[str, str]
               ) -> Plot:
    imdb_content = raw_film['Plot'] or None
    imdb_id = parse_imdb_id(raw_film['imdbID'])
    return Plot(imdb_id=imdb_id,
                imdb_content=imdb_content)


def parse_related_instances(*,
                            cls: Base,
                            names_str: str
                            ) -> List[Base]:
    names = parse_names(names_str)
    return [cls(name=name)
            for name in names
            if name != NOT_AVAILABLE_VALUE_ALIAS]


def parse_names(names_str: str, sep=',') -> filter:
    names = (name.strip()
             for name in names_str.split(sep))
    return filter(None, names)
