from typing import List, Optional

from vizier.services.queries import generate_insert_query
from vizier.types import (ConnectionType,
                          RecordType)
from .execution import execute_many


async def insert(*, table_name: str,
                 columns_names: List[str],
                 unique_columns_names: List[str],
                 values: List[RecordType],
                 merge: bool = False,
                 connection: ConnectionType,
                 is_mysql: bool) -> Optional[RecordType]:
    query = await generate_insert_query(
        table_name=table_name,
        columns_names=columns_names,
        unique_columns_names=unique_columns_names,
        merge=merge,
        is_mysql=is_mysql)
    resp = await execute_many(query,
                              args=values,
                              is_mysql=is_mysql,
                              connection=connection)
    return resp
