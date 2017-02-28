import re
from typing import Optional

import requests

from vizier.config import IMDB_API_URL


def query_imdb(imdb_id: Optional[int],
               title: str, year: int) -> requests.Response:
    params = dict(plot='full', r='json', tomatoes=True, y=year)
    if imdb_id is not None:
        params['i'] = f'tt{imdb_id:0>7}'
    else:
        params['t'] = normalize_title(title)
    return requests.get(IMDB_API_URL, params=params)


FILM_TITLE_RE = re.compile('.+?(?= \((film|miniseries|video|manga)\))')


def normalize_title(title: str) -> str:
    alphanumeric_title = re.sub('\W', ' ', title)
    match = FILM_TITLE_RE.match(alphanumeric_title)
    return match.group(0) if match is not None else alphanumeric_title
