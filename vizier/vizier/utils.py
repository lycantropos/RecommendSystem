import logging
import re
from typing import Iterable

logger = logging.getLogger(__name__)

IMDB_ID_RE = re.compile(r'(?<=tt)(\d+)')


def join_str(elements: Iterable, sep=',') -> str:
    return sep.join(map(str, elements))
