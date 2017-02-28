import logging
import re
from typing import (Optional, Any,
                    Generator, Dict,
                    Tuple)

import requests
import wikipedia

from vizier.config import WIKIPEDIA_API_URL

StringGenerator = Generator[str, None, None]

logger = logging.getLogger(__name__)

WIKILINKS_EXCEPTION = {'Keerthi Chakra',
                       'A Thousand Acres',
                       'Star Trek',
                       'Star Wars',
                       'Final Destination',
                       'Diary of a Wimpy Kid',
                       'Diary of a Wimpy Kid: Rodrick Rules',
                       'Halloween H20: 20 Years Later (film)',
                       'The Ten (film)'}
PLOT_SECTION_NAMES = ['Plot', 'PlotEdit', 'Synopsis', 'Plot summary', 'Plot synopsis']

WIKILINKS_REPL = {'On Line': 'On_Line'}
WIKILINKS_EXCEPTION.update(set(WIKILINKS_REPL.keys()))


def get_articles_titles_by_years(start: int,
                                 stop: int) -> Generator[Tuple[int, StringGenerator],
                                                         None, None]:
    for year in range(start, stop):
        articles_titles = get_articles_titles_by_year(year)
        yield year, articles_titles


def get_articles_titles_by_year(year: int) -> StringGenerator:
    for response in query_wikipedia(year):
        pages = response['pages']
        yield from (value['title']
                    for value in pages.values()
                    if is_title_correct(value.get('title', '')))


def is_title_correct(title: str) -> bool:
    return (title and
            not (title.startswith('List') and 'of' in title and
                 ('film' in title or 'actor' in title)) and
            not ('film' in title and 'serie' in title) and
            title not in WIKILINKS_EXCEPTION and
            not re.search(r'File:[^\.]+\.', title))


def get_imdb_id(title: str) -> Optional[int]:
    request = dict(action='expandtemplates', text='{{IMDb title}}',
                   prop='wikitext', title=title, format='json')
    content = requests.get(WIKIPEDIA_API_URL, params=request).json()
    templates = content['expandtemplates']
    imdb_link = templates.get('wikitext', '')
    search_res = re.search(r'(?<=tt)(\d+)', imdb_link)
    if search_res is None:
        return None
    return int(search_res.group(0))


# from https://www.mediawiki.org/wiki/API:Query#Continuing_queries
def query_wikipedia(year: int) -> Generator[Dict[str, Any], None, None]:
    params = {'action': 'query', 'generator': 'categorymembers', 'prop': 'categories',
              'cllimit': 'max', 'gcmlimit': 'max', 'format': 'json',
              'gcmtitle': f'Category:{year}_films'}
    last_continue = {'continue': ''}
    while True:
        # clone original request
        params_copy = params.copy()
        # modify it with the values
        # returned in the 'continue' section of the last result
        params_copy.update(last_continue)
        # call API
        response_json = requests.get(WIKIPEDIA_API_URL, params=params_copy).json()
        if 'error' in response_json:
            raise ValueError(response_json['error'])
        if 'warnings' in response_json:
            logger.warning(response_json['warnings'])
        if 'query' in response_json:
            yield response_json['query']
        if 'continue' not in response_json:
            break
        last_continue = response_json['continue']


def get_plot_content(link: str) -> str:
    page = wikipedia.page(link, auto_suggest=False)
    plot_sections = filter(None,
                           (page.section(section_title)
                            for section_title in PLOT_SECTION_NAMES))
    return ''.join(plot_sections).replace('\n', '')
