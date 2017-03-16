from functools import partial
from typing import (Optional,
                    List)

from vizier.services.queries import (ALL_COLUMNS_ALIAS,
                                     generate_select_query,
                                     generate_group_wise_query)
from vizier.types import (ConnectionType,
                          RecordType,
                          ColumnValueType,
                          FiltersType, OrderingType)
from .utils import (normalize_pagination,
                    normalize_record)


async def fetch_column_function(
        *, table_name: str,
        column_name: str = ALL_COLUMNS_ALIAS,
        filters: Optional[FiltersType] = None,
        orderings: Optional[List[OrderingType]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        column_function_name: str,
        is_mysql: bool,
        connection: ConnectionType,
        default: ColumnValueType = 0) -> int:
    limit, offset = normalize_pagination(limit=limit,
                                         offset=offset,
                                         is_mysql=is_mysql)
    function_column = f'{column_function_name}({column_name})'
    query = await generate_select_query(
        table_name=table_name,
        columns_names=[function_column],
        filters=filters,
        orderings=orderings,
        limit=limit,
        offset=offset)
    resp = await fetch_row(query,
                           is_mysql=is_mysql,
                           connection=connection)
    return resp[0] if resp is not None else default


fetch_max_column_value = partial(fetch_column_function,
                                 column_function_name='MAX')
fetch_records_count = partial(fetch_column_function,
                              column_function_name='COUNT')


async def fetch_group_wise_column_function(
        *, table_name: str,
        column_name: str = ALL_COLUMNS_ALIAS,
        column_function_name: str,
        maximized_column_name: str,
        groupings: List[str],
        filters: Optional[FiltersType] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        is_maximum: bool = True,
        is_mysql: bool,
        connection: ConnectionType,
        default: ColumnValueType = 0) -> int:
    limit, offset = normalize_pagination(limit=limit,
                                         offset=offset,
                                         is_mysql=is_mysql)
    function_column = f'{column_function_name}({column_name})'
    query = await generate_group_wise_query(
        table_name=table_name,
        columns_names=[function_column],
        maximized_column_name=maximized_column_name,
        filters=filters,
        groupings=groupings,
        limit=limit,
        offset=offset,
        is_maximum=is_maximum,
        is_mysql=is_mysql)
    resp = await fetch_row(query,
                           is_mysql=is_mysql,
                           connection=connection)
    return resp[0] if resp is not None else default


fetch_group_wise_max_column_value = partial(fetch_group_wise_column_function,
                                            column_function_name='MAX')
fetch_group_wise_records_count = partial(fetch_group_wise_column_function,
                                         column_function_name='COUNT')


async def fetch(*, table_name: str,
                columns_names: List[str],
                filters: Optional[FiltersType] = None,
                orderings: Optional[List[OrderingType]] = None,
                limit: Optional[int] = None,
                offset: Optional[int] = None,
                is_mysql: bool,
                connection: ConnectionType) -> List[RecordType]:
    limit, offset = normalize_pagination(limit=limit,
                                         offset=offset,
                                         is_mysql=is_mysql)
    query = await generate_select_query(
        table_name=table_name,
        columns_names=columns_names,
        filters=filters,
        orderings=orderings,
        limit=limit,
        offset=offset)

    resp = await fetch_columns(query,
                               columns_names=columns_names,
                               is_mysql=is_mysql,
                               connection=connection)
    return resp


async def fetch_group_wise(
        *, table_name: str,
        columns_names: List[str],
        maximized_column_name: str,
        groupings: List[str],
        filters: Optional[FiltersType] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_maximum: bool = True,
        is_mysql: bool,
        connection: ConnectionType) -> List[RecordType]:
    limit, offset = normalize_pagination(limit=limit,
                                         offset=offset,
                                         is_mysql=is_mysql)

    query = await generate_group_wise_query(
        table_name=table_name,
        columns_names=columns_names,
        maximized_column_name=maximized_column_name,
        filters=filters,
        groupings=groupings,
        limit=limit,
        offset=offset,
        orderings=orderings,
        is_maximum=is_maximum,
        is_mysql=is_mysql)
    resp = await fetch_columns(query,
                               columns_names=columns_names,
                               is_mysql=is_mysql,
                               connection=connection)
    return resp


async def fetch_row(query: str, *,
                    is_mysql: bool,
                    connection: ConnectionType) -> ColumnValueType:
    if is_mysql:
        async with connection.cursor() as cursor:
            await cursor.execute(query)
            resp = await cursor.fetchone()
            return resp
    else:
        resp = await connection.fetchrow(query)
        return resp


async def fetch_columns(query: str, *,
                        columns_names: List[str],
                        is_mysql: bool,
                        connection: ConnectionType) -> List[RecordType]:
    if is_mysql:
        async with connection.cursor() as cursor:
            await cursor.execute(query)
            return [row async for row in cursor]
    else:
        resp = await connection.fetch(query)
        return [await normalize_record(row,
                                       columns_names=columns_names)
                for row in resp]
