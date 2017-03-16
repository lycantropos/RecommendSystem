from typing import (Optional,
                    List)

from vizier.types import OrderingType, FiltersType
from vizier.utils import join_str
from .filters import filters_to_str

ALL_COLUMNS_ALIAS = '*'
ORDERS_ALIASES = dict(ascending='ASC',
                      descending='DESC')


async def add_filters(query: str, *,
                      filters: Optional[FiltersType]
                      ) -> str:
    if filters:
        filters_str = await filters_to_str(filters)
        query += f'WHERE {filters_str} '
    return query


async def add_orderings(query: str, *,
                        orderings: List[OrderingType]
                        ) -> str:
    if orderings:
        orderings_str = await orderings_to_str(orderings)
        query += f'ORDER BY {orderings_str} '
    return query


async def orderings_to_str(orderings: List[OrderingType]
                           ) -> str:
    orderings_strs = (' '.join(ordering)
                      for ordering in orderings)
    return join_str(orderings_strs)


async def add_pagination(query: str, *,
                         limit: Optional[int],
                         offset: Optional[int]
                         ) -> str:
    if limit is not None:
        query += f'LIMIT {limit} '
        if offset is not None:
            query += f'OFFSET {offset} '
    return query
