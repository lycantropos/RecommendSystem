import codecs
import logging
import os
import re
from datetime import date
from typing import Optional, Dict, Any, Set, List, Generator, Tuple

import requests
import wikipedia
from distance import levenshtein

from vizier.config import (WIKILINKS_DATABASE_NAME,
                           FILMS_DATABASE_NAME,
                           FILMS_WITH_PLOT_DATABASE_NAME)

SEP = '},{'
logger = logging.getLogger(__name__)

FILM_ARTICLES_RANGES = {'numbers', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J-K',
                        'L', 'M', 'N-O', 'P', 'Q-R', 'S', 'T', 'U-V-W', 'X-Y-Z'}
LISTS_OF_FILM_ARTICLES = set('List of films: ' + name_range
                             for name_range in FILM_ARTICLES_RANGES)
WIKILINKS_EXCEPTION = {'Diary of a Wimpy Kid: Rodrick Rules', 'Keerthi Chakra',
                       'A Thousand Acres', 'Star Trek',
                       'Halloween H20: 20 Years Later (film)', 'Star Wars',
                       'Final Destination', 'Diary of a Wimpy Kid', 'The Ten (film)'}
WIKILINKS_EXTENSION = set()

WIKILINKS_REPL = {'On Line': 'On_Line'}
WIKILINKS_EXCEPTION.update(set(WIKILINKS_REPL.keys()))
WIKILINKS_EXTENSION.update(set(WIKILINKS_REPL.values()))

WIKIPEDIA_API_URL = 'https://en.wikipedia.org/w/api.php?'
FIRST_YEAR = 1887
NEXT_YEAR = date.today().year + 1


def get_wiki_articles_by_years(start: int,
                               stop: int) -> Generator[Tuple[int, Set[str]], None, None]:
    for year in range(start, stop):
        titles = get_titles_by_year(year)
        yield year, titles


def get_titles_by_year(year: int):
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


def set_wiki_film_articles_by_namespaces(path: str, mode='w'):
    with codecs.open(path, mode, 'utf_8') as res:
        links = set(link for name_range in LISTS_OF_FILM_ARTICLES
                    for link in wikipedia.page(name_range).links
                    if link not in WIKILINKS_EXCEPTION)
        links.union(WIKILINKS_EXTENSION)
        for link in links:
            if not (link.startswith('List') and 'of' in link and (
                            'film' in link or 'actors' in link)) and \
                    not ('film' in link and 'series' in link):
                res.write(link + '\n')
            else:
                logger.info('Not movie link: ' + link)


PLOT_SECTION_NAMES = ['Plot', 'PlotEdit', 'Synopsis', 'Plot summary', 'Plot synopsis']


def is_int(val):
    try:
        int(val)
        return True
    except (ValueError, TypeError):
        return False


TITLE_REPL = {'Star Wars: Episode IV - A New Hope': 'Star Wars',
              'A Midwinter\'s Tale': 'In the Bleak Midwinter',
              'Twelve Monkeys': '12 Monkeys',
              'Tales from the Crypt: Demon Knight': 'Demon Knight',
              'Ready to Wear': 'Prêt-à-Porter', 'Queen Margot': 'La Reine Margot',
              'L\'Enfer': 'Hell',
              'The Jerky Boys': 'The Jerky Boys: The Movie',
              'Poison Ivy II': 'Poison Ivy II: Lily',
              'Interview with the Vampire: The Vampire Chronicles':
                  'Interview with the Vampire'}

EXTRA_WORDS = {'film', 'miniseries', 'video', 'manga'}


def set_wiki_film_plots_by_year(path: str,
                                wiki_path: str,
                                mode='w'):
    with open(path + FILMS_DATABASE_NAME, 'r') as films:
        with open(path + FILMS_WITH_PLOT_DATABASE_NAME, mode) as res:
            dict_of_films = dict(read_films_by_years(wiki_path))

            films.readline()
            for film in films:
                film_info = film.split(SEP)
                title = parse_title(film_info)
                year = film_info[2]
                words = get_title_words(title)
                if year not in dict_of_films:
                    logger.error(f'There\'s no year of {year} '
                                 'in given wikipedia database.')
                    continue

                res_links = {}
                links = (dict_of_films[str(int(year) - 1)]
                         + dict_of_films[year]
                         + dict_of_films[str(int(year) + 1)])
                for link in links:
                    link_words = (link.replace(' & ', ' and ')
                                  .replace('_', ' ')
                                  .replace('⅓', ' 1/3 ')
                                  .split(' ('))
                    link_year = ''
                    if len(link_words) > 1:
                        last_word = link_words[-1]
                        link_year = ''.join(re.sub('\D', ' ', last_word)
                                            .split())

                        if any(extra_word in last_word
                               for extra_word in EXTRA_WORDS) or link_year:
                            link_words = link_words[:-1]
                    link_words = get_title_words(' ('.join(link_words))
                    if link_words:
                        if len(words) == len(link_words):
                            if set(words) == set(link_words):
                                if link_year == year or not link_year:
                                    res_links[link] = link_words
                            else:
                                for word, link_word in zip(words, link_words):
                                    words_are_equal = validate_difference(word,
                                                                          link_word)
                                    if words_are_equal:
                                        break
                                else:
                                    if link_year == year or not link_year:
                                        res_links[link] = link_words
                if not res_links:
                    logger.info('Wiki page is not found for ' + title)
                    res.write(film)
                    continue

                plot = ''
                if len(res_links) > 1:
                    copy_res_links = dict(res_links)
                    for l, l2 in zip(res_links, res_links):
                        if l != l2:
                            if res_links[l] == words and res_links[l2] != words:
                                del copy_res_links[l2]
                    res_links = copy_res_links
                if len(res_links) == 1:
                    res_link, = res_links.keys()
                else:
                    variants = '\n'.join(res_links)
                    logger.info(f'{title} ({year}) '
                                f'has {len(res_links)} '
                                f'different variants: {variants}')
                if plot:
                    film_info[-1] = plot
                res.write(SEP.join(film_info) + '\n')


def read_films_by_years(wiki_path: str):
    for root, _, files_names in os.walk(wiki_path):
        for file_name in files_names:
            year = os.path.splitext(file_name)[0]
            if is_int(year):
                film_file_path = os.path.join(root, file_name)
                with open(film_file_path, 'r') as film_file:
                    yield year, [film[:-1] for film in film_file]


def validate_difference(word: str, link_word: str) -> bool:
    return len(word) > 1 and levenshtein(word, link_word) > 1 or len(word) == 1


def get_title_words(title: str) -> List[str]:
    words = re.sub(r'(\bthe\b)', '', title)
    words = re.sub(r'(\bThe\b)', '', words)
    # there are films called $ and Ri¢hie Ri¢h
    return re.sub(r'[^\w\$¢]', ' ', words).lower().split()


def parse_title(film_info: List[str]) -> str:
    title = (film_info[1].replace(' & ', ' and ')
             .replace('_', ' ')
             .replace('⅓', '1/3'))
    return TITLE_REPL.get(title, title)


def set_wiki_film_plots_by_namespace(path: str, mode='w'):
    with open(path + FILMS_DATABASE_NAME, 'r') as films:
        with open(path + WIKILINKS_DATABASE_NAME, 'r') as links:
            with open(path + FILMS_WITH_PLOT_DATABASE_NAME, mode) as res:
                films.readline()
                wiki_links = [link[:-1] for link in links]

                for film in films:
                    film_info = film.split(SEP)
                    title = film_info[1]
                    year = film_info[2]
                    words = re.sub('[^\w\$]', ' ', title).lower().split()
                    if not words:
                        print(title)
                    res_links = dict()
                    for link in wiki_links:
                        if link.lower().startswith(words[0]):
                            link_words = link.split(' (')
                            link_year = ''
                            if 'film' in link_words[-1]:
                                link_year = link_words[-1]
                                link_year = ''.join(
                                    re.sub('[^\d]', ' ', link_year).split())
                                link_words = link_words[:-1]
                                link_title = ' ('.join(link_words)
                            else:
                                link_title = link
                            link_words = re.sub('[^\w\$]', ' ',
                                                ''.join(link_words)).lower().split()
                            if link_words == words:
                                if not link_year:
                                    res_links[link] = link_title
                                elif year == link_year:
                                    res_links[link] = link_title
                                    break

                        if not res_links:
                            logger.info('Wiki page is not found for ' + title)
                            res.write(film)
                            continue

                        plot = ''
                        if len(res_links) > 1:
                            # TODO: make function from this
                            for res_link, other_res_link in zip(res_links, res_links):
                                if (res_link != other_res_link and
                                            res_link.lower() == other_res_link.lower() and
                                            res_links[res_link] == title or
                                            res_links[other_res_link] != title):
                                    del res_links[res_link]
                        if len(res_links) == 1:
                            (res_link, _), = res_links.items()
                            plot = get_plot_content(res_link)
                        else:
                            logger.info(f'{title} ({year}) '
                                        f'has {len(res_links)} '
                                        f'different variants')
                        if not plot:
                            plot = film_info[-1]
                        unplotted_film = SEP.join(film_info[:-1])
                        res.write(unplotted_film + SEP + plot + '\n')


def get_plot_content(link: str) -> str:
    page = wikipedia.page(link, auto_suggest=False)
    plot_sections = filter(None,
                           (page.section(section_title)
                            for section_title in PLOT_SECTION_NAMES))
    return ''.join(plot_sections).replace('\n', '')
