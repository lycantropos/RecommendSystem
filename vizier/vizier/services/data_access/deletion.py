from typing import Optional

from vizier.services.queries import generate_delete_query
from vizier.types import (ConnectionType,
                          FiltersType)
from .execution import execute


async def delete(*, table_name: str,
                 filters: Optional[FiltersType],
                 is_mysql: bool,
                 connection: ConnectionType) -> None:
    query = await generate_delete_query(
        table_name=table_name,
        filters=filters)
    await execute(query, is_mysql=is_mysql,
                  connection=connection)
