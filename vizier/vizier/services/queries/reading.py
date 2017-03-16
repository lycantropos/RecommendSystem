from typing import (Any, Optional,
                    List, Tuple)

from vizier.types import FiltersType, OrderingType
from vizier.utils import join_str
from .utils import (ALL_COLUMNS_ALIAS,
                    add_filters,
                    add_orderings,
                    add_pagination, ORDERS_ALIASES)


async def generate_select_query(
        *, table_name: str,
        columns_names: List[str],
        filters: Optional[FiltersType] = None,
        orderings: Optional[List[OrderingType]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None) -> str:
    columns = join_str(columns_names)
    query = (f'SELECT {columns} '
             f'FROM {table_name} ')
    query = await add_filters(query,
                              filters=filters)
    query = await add_orderings(query,
                                orderings=orderings)
    query = await add_pagination(query,
                                 limit=limit,
                                 offset=offset)
    return query


async def generate_group_wise_query(
        *, table_name: str,
        columns_names: List[str],
        maximized_column_name: str,
        filters: Optional[FiltersType] = None,
        groupings: List[str],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_maximum: bool,
        is_mysql: bool) -> str:
    if is_mysql:
        query = await generate_mysql_group_wise_query(
            table_name=table_name,
            columns_names=columns_names,
            maximized_column_name=maximized_column_name,
            filters=filters,
            groupings=groupings,
            limit=limit,
            offset=offset,
            orderings=orderings,
            is_maximum=is_maximum)
    else:
        query = await generate_postgres_group_wise_query(
            table_name=table_name,
            columns_names=columns_names,
            maximized_column_name=maximized_column_name,
            groupings=groupings,
            limit=limit,
            offset=offset,
            orderings=orderings,
            is_maximum=is_maximum)
    return query


async def generate_mysql_group_wise_query(
        *, table_name: str,
        columns_names: List[str],
        maximized_column_name: str,
        filters: Optional[Tuple[str, Any]] = None,
        groupings: List[str],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_maximum: bool) -> str:
    # based on article
    # http://mysql.rjweb.org/doc.php/groupwise_max
    columns = join_str(columns_names)
    sub_orderings = [(grouping, ORDERS_ALIASES['ascending'])
                     for grouping in groupings]
    order = 'descending' if is_maximum else 'ascending'
    sub_orderings += [(maximized_column_name, ORDERS_ALIASES[order])]
    groupings = join_str(groupings)
    query = (f'SELECT CONCAT({groupings}) != @prev AS grouping_condition, '
             f'@prev := CONCAT({groupings}), '
             f'{table_name}.{ALL_COLUMNS_ALIAS} '
             f'FROM {table_name} ')
    query = await add_filters(query,
                              filters=filters)
    query = await add_orderings(query,
                                orderings=sub_orderings)
    query = await add_pagination(query,
                                 limit=limit,
                                 offset=offset)
    query = (f'SELECT {columns} '
             'FROM (SELECT @prev := \'\') as init '
             f'JOIN ({query}) AS step '
             'WHERE grouping_condition ')
    query = await add_orderings(query,
                                orderings=orderings)
    return query


async def generate_postgres_group_wise_query(
        *, table_name: str,
        columns_names: List[str],
        maximized_column_name: str,
        filters: Optional[Tuple[str, Any]] = None,
        groupings: List[str],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_maximum: bool) -> str:
    # based on article
    # https://explainextended.com/2009/11/26/postgresql-selecting-records-holding-group-wise-maximum/
    columns = join_str(columns_names)
    sub_orderings = [(grouping, ORDERS_ALIASES['ascending'])
                     for grouping in groupings]
    order = 'descending' if is_maximum else 'ascending'
    sub_orderings += [(maximized_column_name, ORDERS_ALIASES[order])]

    groupings = join_str(groupings)
    query = (f'SELECT DISTINCT ON ({groupings}) {columns} '
             f'FROM {table_name} ')
    query = await add_filters(query,
                              filters=filters)
    query = await add_pagination(query,
                                 limit=limit,
                                 offset=offset)
    query = await add_orderings(query,
                                orderings=sub_orderings)
    if orderings:
        query = (f'SELECT {ALL_COLUMNS_ALIAS} '
                 f'FROM ({query}) AS subquery ')
        query = await add_orderings(query,
                                    orderings=orderings)
    return query
