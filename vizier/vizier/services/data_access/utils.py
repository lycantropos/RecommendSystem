import logging
from typing import (Optional,
                    Dict,
                    Tuple, List)

from vizier.types import (DbUriType,
                          RecordType,
                          ColumnValueType)

MYSQL_DRIVER_NAME_PREFIX = 'mysql'
# to make pagination without limit
MYSQL_MAX_BIGINT_VALUE = 18_446_744_073_709_551_615

logger = logging.getLogger(__name__)


async def is_db_uri_mysql(db_uri: DbUriType) -> bool:
    try:
        return db_uri.drivername.startswith(MYSQL_DRIVER_NAME_PREFIX)
    except AttributeError:
        return db_uri.startswith(MYSQL_DRIVER_NAME_PREFIX)


async def normalize_record(record: Dict[str, ColumnValueType],
                           columns_names: List[str]) -> RecordType:
    return tuple(record[column_name] for column_name in columns_names)


def normalize_pagination(*, limit: Optional[int],
                         offset: Optional[int],
                         is_mysql: bool
                         ) -> Tuple[Optional[int], Optional[int]]:
    if is_mysql:
        if limit is None and offset is not None:
            warn_msg = ('Incorrect pagination parameters: '
                        'in MySQL "offset" parameter '
                        'should be specified '
                        'along with "limit" parameter, '
                        'but "limit" parameter '
                        f'has value "{limit}". '
                        f'Assuming that table\'s primary key '
                        f'has "BIGINT" type '
                        f'and setting limit '
                        f'to {MYSQL_MAX_BIGINT_VALUE}.')
            logger.warning(warn_msg)
            return MYSQL_MAX_BIGINT_VALUE, offset
    return limit, offset
