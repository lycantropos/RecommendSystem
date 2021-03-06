import re
from datetime import (datetime,
                      date,
                      timedelta)
from typing import Optional

from vizier.config import NOT_AVAILABLE_VALUE_ALIAS
from vizier.utils import IMDB_ID_RE

RELEASE_DATE_FORMAT = '%d %b %Y'
UNRATED_CONTENT_RATINGS = {'NOT RATED', 'UNRATED'}
DURATION_RE = re.compile('^(\d+ h\s*)?((\d+)(?= min$))?')


def parse_year(year_str: str) -> int:
    return int(year_str)


def parse_imdb_id(imdb_id_str: str) -> int:
    search_res = IMDB_ID_RE.search(imdb_id_str)
    return int(search_res.group(0).lstrip('0'))


def parse_content_rating(content_rating: Optional[str]
                         ) -> Optional[str]:
    if content_rating in UNRATED_CONTENT_RATINGS:
        return None
    return content_rating


def parse_rating(rating_str: Optional[str]
                 ) -> Optional[float]:
    try:
        return float(rating_str)
    except TypeError:
        return rating_str


def parse_date(date_str: Optional[str]
               ) -> Optional[date]:
    try:
        return (datetime.strptime(date_str,
                                  RELEASE_DATE_FORMAT)
                .date())
    except TypeError:
        return date_str


def parse_duration(duration_str: Optional[str]
                   ) -> Optional[timedelta]:
    try:
        # there are values like "1,428 min"
        duration_str = duration_str.replace(',', '')
    except AttributeError:
        return None
    match = DURATION_RE.match(duration_str)
    try:
        hours_count_str = re.sub('\D', '',
                                 match.group(1))
        hours_count = int(hours_count_str)
    except TypeError:
        hours_count = 0
    try:
        minutes_count = int(match.group(2))
    except TypeError:
        minutes_count = 0
    return timedelta(hours=hours_count,
                     minutes=minutes_count)


def normalize_value(value: str) -> Optional[str]:
    return None if value == NOT_AVAILABLE_VALUE_ALIAS else value
