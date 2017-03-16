from datetime import datetime
from typing import (Any, Union,
                    Generator,
                    Tuple, List)

from aiomysql.connection import Connection as MySQLConnection
from aiomysql.pool import Pool as MySQLConnectionPool
from asyncpg.connection import Connection as PostgresConnection
from asyncpg.pool import Pool as PostgresConnectionPool
from sqlalchemy.engine.url import URL

StringGenerator = Generator[str, None, None]

ColumnValueType = Union[int, bool,
                        float, str,
                        datetime, None]
RecordType = Tuple[ColumnValueType, ...]
FilterType = Tuple[str,
                   Union[
                       ColumnValueType,  # single value
                       List[ColumnValueType],  # list of values
                       Tuple[ColumnValueType, ColumnValueType]  # values range
                   ]]
FiltersType = Tuple[str, Any]
OrderingType = Tuple[str, str]

DbUriType = Union[str, URL]
ConnectionType = Union[MySQLConnection, PostgresConnection]
ConnectionPoolType = Union[MySQLConnectionPool, PostgresConnectionPool]
