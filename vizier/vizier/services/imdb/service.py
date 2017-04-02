import logging
from asyncio import sleep
from json import JSONDecodeError
from typing import (Any,
                    Optional,
                    Dict)

from aiohttp import ClientSession

from vizier.config import (IMDB_API_URL,
                           RETRY_INTERVAL_IN_SECONDS)
from vizier.services.utils import A_TIMEOUT_OCCURRED
from vizier.services.wikipedia import get_imdb_id
from .utils import IMDB_ID_LENGTH

logger = logging.getLogger(__name__)


async def get_raw_film(*, article_id: int,
                       article_title: str,
                       year: int,
                       session: ClientSession
                       ) -> Dict[str, Any]:
    imdb_id = await get_imdb_id(article_title=article_title,
                                session=session)
    if imdb_id is not None:
        resp = await query_imdb(imdb_id=imdb_id,
                                year=year,
                                session=session)
        resp['article_id'] = article_id
        return resp


async def query_imdb(*, imdb_id: Optional[int],
                     year: int,
                     session: ClientSession) -> Dict[str, Any]:
    params = dict(i=f'tt{imdb_id:0>{IMDB_ID_LENGTH}}',
                  y=year,
                  plot='full',
                  tomatoes='true',
                  r='json')
    while True:
        attempt_num = 0
        async with session.get(IMDB_API_URL, params=params) as response:
            attempt_num += 1
            if response.status == A_TIMEOUT_OCCURRED:
                logger.debug(f'Attempt #{attempt_num} failed: '
                             f'server "{IMDB_API_URL}" answered with '
                             f'status code {A_TIMEOUT_OCCURRED}. '
                             f'Waiting {RETRY_INTERVAL_IN_SECONDS} second(s) '
                             'before next attempt.')
                await sleep(RETRY_INTERVAL_IN_SECONDS)
                continue
            try:
                response_json = await response.json()
            except JSONDecodeError:
                logger.exception('')
                return dict()
            return response_json
