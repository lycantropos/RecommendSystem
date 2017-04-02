import logging
from asyncio import (AbstractEventLoop,
                     gather, ensure_future)
from typing import List

from aiohttp import ClientSession
from cetus.types import ConnectionPoolType
from cetus.data_access import (is_db_uri_mysql,
                               get_connection_pool,
                               insert)
from sqlalchemy import Column
from sqlalchemy.engine.url import URL

from vizier.models import Article
from vizier.services.wikipedia import get_articles_titles

logger = logging.getLogger(__name__)


async def parse_films_articles(*, start_year: int,
                               stop_year: int,
                               max_connections: int = 50,
                               db_uri: URL,
                               loop: AbstractEventLoop
                               ) -> None:
    db_is_mysql = await is_db_uri_mysql(db_uri)

    table_name = Article.__tablename__
    table = Article.__table__
    columns_names = [Article.title.name,
                     Article.year.name]

    def is_column_unique(column: Column) -> bool:
        return column.unique or column.primary_key

    unique_columns = filter(is_column_unique, table.columns)
    unique_columns_names = [column.name
                            for column in unique_columns]
    async with get_connection_pool(db_uri=db_uri,
                                   is_mysql=db_is_mysql,
                                   max_size=max_connections,
                                   loop=loop) as connection_pool, \
            ClientSession() as session:
        for step_start_year in range(start_year, stop_year, max_connections):
            step_stop_year = min(step_start_year + max_connections, stop_year)
            await parse_films_article_step(start_year=step_start_year,
                                           stop_year=step_stop_year,
                                           table_name=table_name,
                                           columns_names=columns_names,
                                           unique_columns_names=unique_columns_names,
                                           is_mysql=db_is_mysql,
                                           connection_pool=connection_pool,
                                           session=session)


async def parse_films_article_step(
        *, start_year: int,
        stop_year: int,
        table_name: str,
        columns_names: List[str],
        unique_columns_names: List[str],
        is_mysql: bool,
        connection_pool: ConnectionPoolType,
        session: ClientSession) -> None:
    logger.info(f'Processing films articles '
                f'from {start_year} year '
                f'to {stop_year - 1} year.')
    tasks = [ensure_future(
        parse_films_article_batch(year=year,
                                  table_name=table_name,
                                  columns_names=columns_names,
                                  unique_columns_names=unique_columns_names,
                                  session=session,
                                  is_mysql=is_mysql,
                                  connection_pool=connection_pool))
             for year in range(start_year, stop_year)]
    await gather(*tasks)
    logger.info(f'Successfully finished '
                f'processing film articles '
                f'from {start_year} year '
                f'to {stop_year - 1} year.')


async def parse_films_article_batch(
        *,
        year: int,
        table_name: str,
        columns_names: List[str],
        unique_columns_names: List[str],
        session: ClientSession,
        is_mysql: bool,
        connection_pool: ConnectionPoolType) -> None:
    articles_titles = await get_articles_titles(year=year,
                                                session=session)
    records = [(title, year) for title in articles_titles]
    async with connection_pool.acquire() as connection:
        await insert(table_name=table_name,
                     columns_names=columns_names,
                     unique_columns_names=unique_columns_names,
                     records=records,
                     merge=True,
                     connection=connection,
                     is_mysql=is_mysql)
