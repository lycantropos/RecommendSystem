import re
from typing import (Optional,
                    Iterable)

from aiohttp import ClientSession

from vizier.config import WIKIPEDIA_API_URL
from vizier.utils import IMDB_ID_RE
from .petscan import query_petscan

FILE_ATTACHMENT_RE = re.compile(r'File:[^\.]+\.')

WIKILINKS_EXCEPTION = {'Keerthi Chakra',
                       'A Thousand Acres',
                       'Star Trek',
                       'Star Wars',
                       'Final Destination',
                       'Diary of a Wimpy Kid',
                       'Diary of a Wimpy Kid: Rodrick Rules',
                       'Halloween H20: 20 Years Later (film)',
                       'The Ten (film)',
                       'On Line'}


async def get_articles_titles(*, year: int,
                              session: ClientSession
                              ) -> Iterable[str]:
    articles_dicts = await query_petscan(categories=f'{year}_films',
                                         session=session)
    articles_titles = (article_dict['title']
                       for article_dict in articles_dicts)
    articles_titles = filter(is_title_correct, articles_titles)
    return articles_titles


def is_title_correct(title: str) -> bool:
    return (title and
            not (title.startswith('List') and 'of' in title and
                 ('film' in title or 'actor' in title)) and
            not ('film' in title and 'serie' in title) and
            title not in WIKILINKS_EXCEPTION and
            not FILE_ATTACHMENT_RE.search(title))


async def get_imdb_id(article_title: str, *,
                      session: ClientSession
                      ) -> Optional[int]:
    params = dict(action='expandtemplates',
                  text='{{IMDb title}}',
                  prop='wikitext',
                  title=article_title,
                  format='json')
    async with session.get(WIKIPEDIA_API_URL,
                           params=params) as response:
        response_json = await response.json()
        templates = response_json['expandtemplates']
        imdb_link = templates.get('wikitext', '')
        search_res = IMDB_ID_RE.search(imdb_link)
        if search_res is None:
            return None
        return int(search_res.group(0))
