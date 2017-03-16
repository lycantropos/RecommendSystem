from asyncio import AbstractEventLoop
from contextlib import contextmanager
from typing import Generator, Optional

import aiomysql
import asyncpg
from asyncio_extras import async_contextmanager
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from vizier.types import DbUriType


@contextmanager
def get_engine(db_uri: DbUriType) -> Generator[Engine, None, None]:
    engine = create_engine(db_uri)
    try:
        yield engine
    finally:
        engine.dispose()


@async_contextmanager
async def get_connection_pool(*, db_uri: DbUriType,
                              is_mysql: bool,
                              timeout: Optional[float] = None,
                              max_size: int,
                              loop: AbstractEventLoop):
    if is_mysql:
        async with get_mysql_connection_pool(db_uri,
                                             timeout=timeout,
                                             max_size=max_size,
                                             loop=loop) as connection_pool:
            yield connection_pool
    else:
        async with get_postgres_connection_pool(db_uri,
                                                timeout=timeout,
                                                max_size=max_size,
                                                loop=loop) as connection_pool:
            yield connection_pool


@async_contextmanager
async def get_mysql_connection_pool(db_uri: DbUriType, *,
                                    timeout: Optional[float],
                                    max_size: int,
                                    loop: AbstractEventLoop):
    async with aiomysql.create_pool(host=db_uri.host,
                                    port=db_uri.port,
                                    user=db_uri.username,
                                    password=db_uri.password,
                                    db=db_uri.database,
                                    connect_timeout=timeout,
                                    maxsize=max_size,
                                    loop=loop) as pool:
        yield pool


@async_contextmanager
async def get_postgres_connection_pool(db_uri: DbUriType, *,
                                       timeout: Optional[float],
                                       max_size: int,
                                       loop: AbstractEventLoop):
    async with asyncpg.create_pool(host=db_uri.host,
                                   port=db_uri.port,
                                   user=db_uri.username,
                                   password=db_uri.password,
                                   database=db_uri.database,
                                   timeout=timeout,
                                   max_size=max_size,
                                   loop=loop) as pool:
        yield pool
