import itertools
import logging
from asyncio import sleep
from json import JSONDecodeError
from typing import (Any,
                    Dict, List)

from aiohttp import ClientSession

from vizier.config import (PETSCAN_API_URL,
                           RETRY_INTERVAL_IN_SECONDS)
from vizier.services.utils import A_TIMEOUT_OCCURRED

logger = logging.getLogger(__name__)

PETSCAN_ITEMS_KEY = '*'


async def query_petscan(session: ClientSession,
                        categories: str
                        ) -> List[Dict[str, Any]]:
    params = dict(project='wikipedia',
                  language='en',
                  format='json',
                  categories=categories,
                  doit='Do_it!',
                  type='subset')
    for attempt_num in itertools.count(1):
        async with session.get(PETSCAN_API_URL, params=params) as response:
            if response.status == A_TIMEOUT_OCCURRED:
                logger.debug(f'Attempt #{attempt_num} failed: '
                             f'server "{PETSCAN_API_URL}" answered with '
                             f'status code {A_TIMEOUT_OCCURRED}. '
                             f'Waiting {RETRY_INTERVAL_IN_SECONDS} '
                             'before next attempt')
                await sleep(RETRY_INTERVAL_IN_SECONDS)
                continue

            try:
                response_json = await response.json()
            except JSONDecodeError:
                logger.exception('')
                return []

            articles_dicts = (response_json[PETSCAN_ITEMS_KEY]
                              [0]['a'][PETSCAN_ITEMS_KEY])
            return articles_dicts
