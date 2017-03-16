from typing import List

from vizier.utils import join_str


async def generate_insert_query(
        *, table_name: str,
        columns_names: List[str],
        unique_columns_names: List[str],
        merge: bool,
        is_mysql: bool) -> str:
    if is_mysql:
        query = await generate_mysql_insert_query(
            table_name=table_name,
            columns_names=columns_names,
            unique_columns_names=unique_columns_names,
            merge=merge)
    else:
        query = await generate_postgres_insert_query(
            table_name=table_name,
            columns_names=columns_names,
            unique_columns_names=unique_columns_names,
            merge=merge)
    return query


async def generate_mysql_insert_query(*, table_name: str,
                                      columns_names: List[str],
                                      unique_columns_names: List[str],
                                      merge: bool) -> str:
    if merge:
        updates = join_str(f'{column_name} = VALUES({column_name})'
                           for column_name in unique_columns_names)
    else:
        updates = join_str(f'{column_name} = VALUES({column_name})'
                           for column_name in columns_names)

    columns = join_str(columns_names)
    columns_count = len(columns_names)
    labels = join_str(f'%s' for _ in range(columns_count))
    return (f'INSERT INTO {table_name} ({columns}) '
            f'VALUES ({labels}) '
            f'ON DUPLICATE KEY UPDATE {updates} ')


async def generate_postgres_insert_query(
        *, table_name: str,
        columns_names: List[str],
        unique_columns_names: List[str],
        merge: bool) -> str:
    columns = join_str(columns_names)
    columns_count = len(columns_names)
    labels = join_str(f'${ind + 1}'
                      for ind in range(columns_count))
    unique_columns = join_str(unique_columns_names)
    if merge:
        updates = join_str(f'{column_name} = EXCLUDED.{column_name}'
                           for column_name in columns_names)
        on_conflict_action = f'UPDATE SET {updates}'
    else:
        on_conflict_action = 'NOTHING'

    return (f'INSERT INTO {table_name} ({columns}) '
            f'VALUES ({labels}) '
            f'ON CONFLICT ({unique_columns}) '
            f'DO {on_conflict_action} ')
