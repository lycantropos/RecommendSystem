import codecs
import logging
import os
import re
from datetime import date
from typing import Optional, Dict, Any, Union, Set, List

import requests
import wikipedia
from distance import levenshtein

from vizier.config import (DATA_DIR, WIKILINKS_DATABASE_NAME, FILMS_DATABASE_NAME, SEP,
                           FILMS_WITH_PLOT_DATABASE_NAME, WIKILINKS_DATABASE_PATH,
                           LINKS_FILE_NAME, BASE_DIR)

logging.basicConfig(
    format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
    level=logging.WARNING)

logger = logging.getLogger(__name__)
logger.info('Parsing wikipedia film database')

FILM_NAMES_RANGES = {'numbers', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J-K',
                     'L', 'M', 'N-O', 'P', 'Q-R', 'S', 'T', 'U-V-W', 'X-Y-Z'}
LISTS_OF_FILMS_BY_NAME = set('List of films: ' + name_range for name_range in
                             FILM_NAMES_RANGES)
WIKILINKS_EXCEPTION = {'Diary of a Wimpy Kid: Rodrick Rules', 'Keerthi Chakra',
                       'A Thousand Acres',
                       'Star Trek', 'Halloween H20: 20 Years Later (film)', 'Star Wars',
                       'Final Destination', 'Diary of a Wimpy Kid', 'The Ten (film)'}
WIKILINKS_EXTENSION = set()

WIKILINKS_REPL = {'On Line': 'On_Line'}
WIKILINKS_EXCEPTION = WIKILINKS_EXCEPTION.union(set(WIKILINKS_REPL.keys()))
WIKILINKS_EXTENSION = WIKILINKS_EXTENSION.union(set(WIKILINKS_REPL.values()))

WIKI_API_URL = 'https://en.wikipedia.org/w/api.php?'
FIRST_YEAR = 1887
NEXT_YEAR = date.today().year + 1


# from https://www.mediawiki.org/wiki/API:Query#Continuing_queries
def query(request: Dict[str, Union[str, int]]):
    last_continue = {'continue': ''}
    while True:
        # clone original request
        req = request.copy()
        # modify it with the values
        # returned in the 'continue' section of the last result
        req.update(last_continue)
        # call API
        result = requests.get(WIKI_API_URL, params=req).json()
        if 'error' in result:
            raise ValueError(result['error'])
        if 'warnings' in result:
            print(result['warnings'])
        if 'query' in result:
            yield result['query']
        if 'continue' not in result:
            break
        last_continue = result['continue']


def is_title_correct(title: str):
    return (not (title.startswith('List') and 'of' in title and
                 ('film' in title or 'actor' in title)) and
            not ('film' in title and 'serie' in title) and
            title not in WIKILINKS_EXCEPTION and
            not re.search(r'File:[^\.]+\.', title))


def set_wiki_links_by_year(*,
                           path: str,
                           wiki_path: str,
                           res_file_path: str,
                           imdb_ids: Dict[int, Any],
                           append: bool = False,
                           year_start: int = FIRST_YEAR,
                           year_stop: int = NEXT_YEAR):
    mode = 'a' if append else 'w'
    total_titles_without_links = []
    with open(res_file_path, mode) as res_file:
        for year in range(year_start, year_stop):
            titles = get_titles_by_year(year, wiki_path=wiki_path)
            imdb_ids_by_titles = {title: get_imdb_id(title)
                                  for title in titles}
            titles_without_links = [title
                                    for title, imdb_id in imdb_ids_by_titles.items()
                                    if imdb_id is None]
            total_titles_without_links += titles_without_links

            for title in titles_without_links:
                imdb_ids_by_titles.pop(title)

            res_file.writelines(join_str(imdb_ids.get(imdb_id), imdb_id) + '\n'
                                for imdb_id in imdb_ids_by_titles.values())
            res_file.writelines(join_str(movie_lens_id, imdb_id) + '\n'
                                for imdb_id, movie_lens_id in imdb_ids.items())

    no_link_films_file_path = os.path.join(path, 'no_link_films.csv')
    with open(no_link_films_file_path, 'w') as no_link_films_file:
        no_link_films_file.writelines(total_titles_without_links)


def get_imdb_id(title: str) -> Optional[int]:
    request = dict(action='expandtemplates', text='{{IMDb title}}',
                   prop='wikitext', title=title, format='json')
    content = requests.get(WIKI_API_URL, params=request).json()
    templates = content['expandtemplates']
    if 'wikitext' not in templates:
        return None
    imdb_link = templates['wikitext']
    search_res = re.search(r'(?<=tt)(\d+)', imdb_link)
    if search_res is None:
        return None
    return int(search_res.group(0))


def join_str(*objects, sep=SEP) -> str:
    return sep.join(str(obj) for obj in objects)


def get_titles_by_year(year: int, wiki_path: str = None) -> Set[str]:
    titles_by_year_file_name = os.path.join(wiki_path, f'{year}.csv')
    with codecs.open(titles_by_year_file_name, 'r',
                     encoding='utf_8') as titles_by_year_file:
        titles = {title[:-1] for title in titles_by_year_file}
    return titles


def get_imdb_ids(links_file_path: str) -> Dict[int, int]:
    with codecs.open(links_file_path, 'r', 'utf_8') as links:
        links.readline()
        link_lists = [re.findall(r'\d+', link) for link in links]
        imdb_ids = {int(link_list[1]): int(link_list[0])
                    for link_list in link_lists}
    return imdb_ids


def set_wiki_film_articles_by_year(path: str,
                                   mode='w',
                                   start=FIRST_YEAR, stop=NEXT_YEAR) -> Optional[dict]:
    if not os.path.exists(path):
        os.makedirs(path)
    my_atts = {'action': 'query', 'generator': 'categorymembers', 'prop': 'categories',
               'cllimit': 'max', 'gcmlimit': 'max', 'format': 'json'}
    titles_dict = dict()
    for year in range(start, stop):
        titles = set()
        my_atts['gcmtitle'] = f'Category:{year}_films'
        for ans in query(my_atts):
            pages = ans['pages']
            titles |= {value['title']
                       for value in pages.values()
                       if 'title' in value and
                       is_title_correct(value['title'])}
        if titles:
            logger.info(f'Found {len(titles)} of {year} year\'s films')
            res_file_path = os.path.join(path, f'{year}.csv')
            with codecs.open(res_file_path, mode, encoding='utf_8') as res_file:
                if mode == 'a+':
                    titles.difference_update(set(film for film in res_file))
                for title in titles:
                    res_file.write(title + '\n')
            titles_dict[year] = titles
        else:
            logger.info(f'There\'s no films for {year}, searching process stops.')
            return None
    return titles_dict


def set_wiki_film_articles_by_namespaces(
        path=os.getcwd() + DATA_DIR + WIKILINKS_DATABASE_NAME, mode='w'):
    with codecs.open(path, mode, 'utf_8') as res:
        links = set(link for name_range in LISTS_OF_FILMS_BY_NAME for link in
                    wikipedia.page(name_range).links
                    if link not in WIKILINKS_EXCEPTION)
        links.union(WIKILINKS_EXTENSION)
        for link in links:
            if not (link.startswith('List') and 'of' in link and (
                            'film' in link or 'actors' in link)) and \
                    not ('film' in link and 'series' in link):
                # print(link)
                res.write(link + '\n')
            else:
                logger.info('NOT MOVIE LINK: ' + link)


PLOT_SECTION_NAMES = ['Plot', 'PlotEdit', 'Synopsis', 'Plot summary', 'Plot synopsis']


def is_int(val):
    try:
        int(val)
        return True
    except ValueError:
        return False


TITLE_REPL = {'Star Wars: Episode IV - A New Hope': 'Star Wars',
              'A Midwinter\'s Tale': 'In the Bleak Midwinter',
              'Twelve Monkeys': '12 Monkeys',
              'Tales from the Crypt: Demon Knight': 'Demon Knight',
              'Ready to Wear': 'Prêt-à-Porter', 'Queen Margot': 'La Reine Margot',
              'L\'Enfer': 'Hell',
              'The Jerky Boys': 'The Jerky Boys: The Movie',
              'Poison Ivy II': 'Poison Ivy II: Lily',
              'Interview with the Vampire: The Vampire Chronicles': 'Interview with the Vampire'}

EXTRA_WORDS = {'film', }


def set_wiki_film_plots_by_year(path=os.getcwd() + DATA_DIR,
                                wiki_path=os.getcwd() + WIKILINKS_DATABASE_PATH,
                                mode='w'):
    with open(path + FILMS_DATABASE_NAME, 'r') as films:
        with open(path + FILMS_WITH_PLOT_DATABASE_NAME, mode) as res:
            dict_of_films = {}
            for fn in os.listdir(wiki_path):
                if os.path.isfile(wiki_path + fn):
                    year = os.path.splitext(fn)[0]
                    if is_int(year):
                        with open(wiki_path + fn, 'r') as film_list:
                            dict_of_films[year] = [film[:-1] for film in film_list]

            films.readline()
            for film in films:
                film_info = film.split(SEP)
                title = parse_title(film_info)
                year = film_info[2]
                words = get_title_words(title)
                if year in dict_of_films:
                    res_links = {}
                    links = (dict_of_films[str(int(year) - 1)]
                             + dict_of_films[year]
                             + dict_of_films[str(int(year) + 1)])
                    for link in links:
                        link_words = link.replace(' & ', ' and ').replace('_',
                                                                          ' ').replace(
                            '⅓', ' 1/3 ').split(' (')
                        link_year = ''
                        if len(link_words) > 1:
                            last_word = link_words[-1]
                            link_year = ''.join(re.sub('\D', ' ', last_word)
                                                .split())

                            if 'film' in last_word or 'miniseries' in last_word or \
                                            'video' in last_word or 'manga' in \
                                    last_word or link_year:
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
                    if res_links:
                        plot = ''
                        if len(res_links) > 1:
                            copy_res_links = dict(res_links)
                            for l in res_links:
                                for l2 in res_links:
                                    if l != l2:
                                        if res_links[l] == words and res_links[
                                            l2] != words: del copy_res_links[l2]
                            res_links = copy_res_links
                        if len(res_links) == 1:
                            (res_link, _), = res_links.items()
                        else:
                            variants = '\n'.join(res_links)
                            logger.info(f'{title} ({year}) '
                                        f'has {len(res_links)} '
                                        f'different variants: {variants}')
                        if plot:
                            film_info[-1] = plot
                        res.write(SEP.join(film_info) + '\n')
                    else:
                        logger.info('Wiki page is not found for ' + title)
                        res.write(film)
                else:
                    logger.error(
                        'There\'s no year of ' + year + ' in given wikipedia database.')


def validate_difference(word: str, link_word: str) -> bool:
    return len(word) > 1 and levenshtein(word, link_word) > 1 or len(word) == 1


def get_title_words(title):
    words = re.sub(r'(\bthe\b)', '', title)
    words = re.sub(r'(\bThe\b)', '', words)
    # there are films called $ and Ri¢hie Ri¢h
    words = re.sub(r'[^\w\$¢]', ' ', words).lower().split()
    return words


def parse_title(film_info: List[str]) -> str:
    title = (film_info[1].replace(' & ', ' and ')
             .replace('_', ' ')
             .replace('⅓', '1/3'))
    if title in TITLE_REPL:
        title = TITLE_REPL[title]
    return title


def set_wiki_film_plots_by_namespace(path=os.getcwd() + DATA_DIR, mode='w'):
    with open(path + FILMS_DATABASE_NAME, 'r') as films:
        with open(path + WIKILINKS_DATABASE_NAME, 'r') as links:
            with open(path + FILMS_WITH_PLOT_DATABASE_NAME, mode) as res:
                films.readline()
                wikilinks = [link[:-1] for link in links]

                for film in films:
                    string = film.split(SEP)
                    title = string[1]
                    year = string[2]
                    film_unplotted = SEP.join(string[:-1])
                    words = re.sub('[^\w\$]', ' ', title).lower().split()
                    if not words:
                        print(title)
                    res_links = dict()
                    for link in wikilinks:
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
                            page = wikipedia.page(res_link, auto_suggest=False)
                            plot = ''.join(filter(None, [
                                page.section(section_title) for section_title in
                                PLOT_SECTION_NAMES
                                ])).replace('\n', '')
                        else:
                            logger.info(title + ' (' + year + ') has ' + str(
                                len(res_links)) + ' different variants')
                        if not plot:
                            plot = string[-1]
                        res.write(film_unplotted + SEP + plot + '\n')


def main():
    path = os.path.join(BASE_DIR, DATA_DIR)
    wiki_path = os.path.join(BASE_DIR, WIKILINKS_DATABASE_PATH)
    res_file_path = os.path.join(BASE_DIR, DATA_DIR, WIKILINKS_DATABASE_NAME)
    links_file_path = os.path.join(path, LINKS_FILE_NAME)
    imdb_ids = get_imdb_ids(links_file_path=links_file_path)

    set_wiki_links_by_year(imdb_ids=imdb_ids,
                           path=path,
                           wiki_path=wiki_path,
                           res_file_path=res_file_path)


if __name__ == '__main__':
    main()
