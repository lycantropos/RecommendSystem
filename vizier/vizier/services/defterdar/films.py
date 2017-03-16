import math
from asyncio import (AbstractEventLoop,
                     gather, ensure_future)
from typing import (Dict,
                    Tuple, List)

from aiohttp import ClientSession

from vizier.config import NOT_AVAILABLE_VALUE_ALIAS
from vizier.models import (Article,
                           Genre, Plot,
                           Writer, Director,
                           Actor, Film)
from vizier.models.base import Base
from vizier.services.data_access import (get_connection_pool,
                                         is_db_uri_mysql,
                                         fetch,
                                         fetch_records_count)
from vizier.services.imdb import get_raw_film
from vizier.services.queries import ORDERS_ALIASES
from vizier.types import (DbUriType,
                          ConnectionPoolType,
                          FiltersType,
                          OrderingType)


async def parse_films(*,
                      start_year: int,
                      stop_year: int,
                      max_connections: int = 50,
                      step: int = 1000,
                      db_uri: DbUriType,
                      loop: AbstractEventLoop) -> None:
    db_is_mysql = await is_db_uri_mysql(db_uri)
    table_name = Article.__tablename__
    columns_names = [Article.title.name,
                     Article.year.name]
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
    async with connection_pool.acquire() as connection:
        articles_data = await fetch(table_name=table_name,
                                    columns_names=columns_names,
                                    filters=filters,
                                    orderings=orderings,
                                    limit=limit,
                                    offset=offset,
                                    is_mysql=is_mysql,
                                    connection=connection)
    tasks = [ensure_future(get_raw_film(article_title=article_title,
                                        year=year,
                                        session=session))
             for article_title, year in articles_data]
    results = await gather(*tasks)
    raw_films = list(filter(None, results))
    films = map(Film.deserialize, raw_films)
    related_objects = map(parse_related_objects, raw_films)
    for film, (genres, directors, actors, writers, plot) in zip(films,
                                                                related_objects):
        # TODO: add film saving here
        continue


def parse_related_objects(raw_film: Dict[str, str]
                          ) -> Tuple[List[Genre],
                                     List[Director],
                                     List[Actor],
                                     List[Writer],
                                     Plot]:
    genres = parse_related_instances(cls=Genre,
                                     names_str=raw_film['Genre'])
    directors = parse_related_instances(cls=Director,
                                        names_str=raw_film['Director'])
    actors = parse_related_instances(cls=Actor,
                                     names_str=raw_film['Actors'])
    writers = parse_related_instances(cls=Writer,
                                      names_str=raw_film['Writer'])
    plot = parse_plot(raw_film['Plot'])
    return genres, directors, actors, writers, plot


def parse_plot(content: str) -> Plot:
    imdb_content = content or None
    return Plot(imdb_content)


def parse_related_instances(*, cls: Base, names_str: str) -> List[Base]:
    names = parse_names(names_str)
    return [cls(name)
            for name in names
            if name != NOT_AVAILABLE_VALUE_ALIAS]


def parse_names(names_str: str, sep=',') -> filter:
    names = (name.strip()
             for name in names_str.split(sep))
    return filter(None, names)
