import codecs
import logging
import os
import re
from datetime import date

import requests
import wikipedia
from distance import levenshtein

from config import (DATA_DIR, WIKILINKS_DATABASE_NAME, FILMS_DATABASE_NAME, SEP,
                    FILMS_WITH_PLOT_DATABASE_NAME, WIKILINKS_DATABASE_PATH,
                    LINKS_DATABASE_NAME)

logging.basicConfig(
    format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
    level=logging.WARNING)

logger = logging.getLogger(__name__)
logger.info('Start parsing of wikipedia film database')

LISTS_OF_FILMS_BY_NAME = set('List of films: ' + name_range for name_range in
                             {'numbers', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
                              'J-K',
                              'L', 'M', 'N-O', 'P', 'Q-R', 'S', 'T', 'U-V-W', 'X-Y-Z'})
WIKILINKS_EXCEPTION = {"Diary of a Wimpy Kid: Rodrick Rules", "Keerthi Chakra",
                       "A Thousand Acres",
                       "Star Trek", "Halloween H20: 20 Years Later (film)", "Star Wars",
                       "Final Destination", "Diary of a Wimpy Kid", "The Ten (film)"}
WIKILINKS_EXTENSION = set()

WIKILINKS_REPL = {"On Line": "On_Line"}
WIKILINKS_EXCEPTION = WIKILINKS_EXCEPTION.union(set(WIKILINKS_REPL.keys()))
WIKILINKS_EXTENSION = WIKILINKS_EXTENSION.union(set(WIKILINKS_REPL.values()))

WIKI_API_URL = 'http://en.wikipedia.org/w/api.php?'
FIRST_YEAR = 1887
CURRENT_YEAR = date.today().year


# from https://www.mediawiki.org/wiki/API:Query#Continuing_queries
def query(request):
    last_continue = {'continue': ''}
    while True:
        # Clone original request
        req = request.copy()
        # Modify it with the values returned in the 'continue' section of the last result.
        req.update(last_continue)
        # Call API
        result = requests.get(WIKI_API_URL, params=req).json()
        if 'error' in result: raise ValueError(result['error'])
        if 'warnings' in result: print(result['warnings'])
        if 'query' in result: yield result['query']
        if 'continue' not in result: break
        last_continue = result['continue']
        # print(last_continue)


# rt = dict(action='expandtemplates', text='{{IMDb title}}', prop='wikitext', title='Toy Story', format='json')
# for r in expandtemplates(rt):
#     x = 5


def get_imdb_id(title: str) -> tuple:
    request = dict(action='expandtemplates', text='{{IMDb title}}', prop='wikitext',
                   title=title, format='json')
    for content in requests.get(WIKI_API_URL, params=request).json():
        if 'wikitext' in content:
            extlink = content['wikitext']
            imdb_link = re.search(r'tt\d+', extlink)
            if imdb_link:
                return imdb_link.group(0).strip('tt')
            else:
                return None
        else:
            return None


def is_title_correct(title: str):
    return not (title.startswith('List') and 'of' in title and
                ('film' in title or 'actors' in title)) and \
           not ('film' in title and 'series' in title) and \
           title not in WIKILINKS_EXCEPTION and \
           not re.search(r'File:[^\.]+\.', title)


def set_wiki_links_by_year(path=os.getcwd() + DATA_DIR,
                           wiki_path=os.getcwd() + WIKILINKS_DATABASE_PATH,
                           res_path=os.getcwd() + DATA_DIR + WIKILINKS_DATABASE_NAME,
                           mode='w',
                           start=FIRST_YEAR, stop=CURRENT_YEAR + 1):
    with codecs.open(path + LINKS_DATABASE_NAME, 'r', "utf_8") as links:
        links.readline()
        imdb_ids = {link_list[1]: link_list[0] for link_list in
                    [re.findall(r'\d+', link) for link in links]}
    with open(res_path, mode) as res:
        with open(path + 'no_temp.csv', 'w') as no_temp:
            for year in range(start, stop):
                with codecs.open(wiki_path + str(year) + '.csv', 'r',
                                 "utf_8") as titles_by_year:
                    titles = {title[:-1] for title in titles_by_year}
                ids = list()
                for title in titles:
                    imdb_id = get_imdb_id(title)
                    if imdb_id in imdb_ids:
                        logger.info(imdb_id + " is in given database of links")
                        ids.append((imdb_ids[imdb_id], imdb_id, title))
                        imdb_ids.pop(imdb_id, None)
                    else:
                        if imdb_id:
                            logger.info("Not in given database of links " + imdb_id)
                        else:
                            logger.warning("No imdb link for " + title)
                            no_temp.write(title + '\n')

                for i in ids:
                    res.write(SEP.join(i) + '\n')
                for i in imdb_ids:
                    res.write(imdb_ids[i] + SEP + i + SEP + '\n')


def set_wiki_film_articles_by_year(path=os.getcwd() + WIKILINKS_DATABASE_PATH, mode='w',
                                   start=FIRST_YEAR, stop=CURRENT_YEAR + 1) -> dict:
    if not os.path.exists(path):
        os.makedirs(path)
    my_atts = {'action': 'query', 'generator': 'categorymembers', 'prop': 'categories',
               'cllimit': 'max', 'gcmlimit': 'max', 'format': 'json'}
    titles_dict = dict()
    for year in range(start, stop):
        titles = set()
        my_atts['gcmtitle'] = 'Category:' + str(year) + '_films'
        for ans in query(my_atts):
            pages = ans['pages']
            titles |= {pages[key]['title']
                       for key in pages
                       if 'title' in pages[key] and is_title_correct(pages[key]['title'])
                       }
        if titles:
            logger.info(
                'Found ' + str(len(titles)) + ' of ' + str(year) + " year's films")
            with codecs.open(path + str(year) + '.csv', mode, "utf_8") as res:
                if mode == 'a+':
                    titles.difference_update(set(film for film in res))
                for title in titles:
                    res.write(title + '\n')
            titles_dict[year] = titles
        else:
            logger.info(
                "There's no films for " + str(year) + ", searching process stops.")
            return None
    return titles_dict


def get_dubs_from_file(path=os.getcwd() + DATA_DIR + WIKILINKS_DATABASE_NAME):
    with codecs.open(path, 'r', "utf_8") as analyzed_file:
        strings = [string[:-1] for string in analyzed_file]
        dubs = {}
        for ind0 in range(len(strings)):
            for ind1 in range(ind0 + 1, len(strings)):
                if strings[ind0] == strings[ind1]:
                    if strings[ind0] in dubs:
                        dubs[strings[ind0]] += 1
                    else:
                        dubs[strings[ind0]] = 1
        return dubs


def set_wiki_film_articles_by_namespaces(
        path=os.getcwd() + DATA_DIR + WIKILINKS_DATABASE_NAME, mode='w'):
    with codecs.open(path, mode, "utf_8") as res:
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


def set_wiki_film_plots_by_year(path=os.getcwd() + DATA_DIR,
                                wiki_path=os.getcwd() + WIKILINKS_DATABASE_PATH,
                                mode='w'):
    with open(path + FILMS_DATABASE_NAME, 'r') as films:
        with open(path + FILMS_WITH_PLOT_DATABASE_NAME, mode) as res:
            dict_of_films = {}
            for fn in os.listdir(wiki_path):
                if os.path.isfile(wiki_path + fn):
                    year = fn.split('.')[0]
                    if is_int(year):
                        with open(wiki_path + fn, 'r') as film_list:
                            dict_of_films[year] = [film[:-1] for film in film_list]

            films.readline()
            for film in films:
                string = film.split(SEP)
                title = string[1].replace(' & ', ' and ').replace('_', ' ').replace('⅓',
                                                                                    '1/3')
                if title in TITLE_REPL:
                    title = TITLE_REPL[title]
                year = string[2]
                words = re.sub(r'(\bthe\b)', '', title)
                words = re.sub(r'(\bThe\b)', '', words)
                words = re.sub(r'[^\w\$¢]', " ",
                               words).lower().split()  # because there's film called $ and Ri¢hie Ri¢h
                if year in dict_of_films:
                    res_links = {}
                    for link in dict_of_films[str(int(year) - 1)] + dict_of_films[year] + \
                            dict_of_films[
                                str(int(year) + 1)]:
                        link_words = link.replace(' & ', ' and ').replace('_',
                                                                          ' ').replace(
                            '⅓', ' 1/3 ').split(' (')
                        link_year = ''
                        if len(link_words) > 1:
                            link_year = ''.join(
                                re.sub('[^\d]', ' ', link_words[-1]).split())
                            if 'film' in link_words[-1] or 'miniseries' in link_words[
                                -1] or \
                                            'video' in link_words[-1] or 'manga' in \
                                    link_words[-1] or link_year:
                                link_words = link_words[:-1]
                        # link_title = ' ('.join(link_words)
                        # else:
                        #     link_title = link
                        link_words = re.sub(r'(\bthe\b)', '', ' ('.join(link_words))
                        link_words = re.sub(r'(\bThe\b)', '', link_words)
                        link_words = re.sub(r'[^\w\$¢]', ' ', link_words).lower().split()
                        if link_words:
                            if len(words) == len(link_words):
                                if set(words) == set(link_words):
                                    if link_year == year or not link_year:
                                        res_links[link] = link_words
                                else:
                                    for ind in range(len(words)):
                                        if levenshtein(words[ind],
                                                       link_words[ind]) > 1 and len(
                                                words[ind]) > 1 or \
                                                        len(words[ind]) == 1:
                                            break
                                        if ind == len(words) - 1 and (
                                                link_year == year or not link_year):
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
                            # page = wikipedia.page(res_link, auto_suggest=False)
                            # plot = ''.join(filter(None, [
                            #     page.section(section_title) for section_title in PLOT_SECTION_NAMES
                            #     ])).replace('\n', '')
                        else:
                            logger.info(title + " (" + year + ") has " + str(
                                len(res_links)) + " different variants")
                            print(res_links)
                        if plot:
                            string[-1] = plot
                        res.write(SEP.join(string) + '\n')
                    else:
                        logger.info("Wiki page is not found for " + title)
                        res.write(film)
                else:
                    logger.error(
                        "There's no year of " + year + " in given wikipedia database.")


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
                    words = re.sub("[^\w\$]", " ", title).lower().split()
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
                                    re.sub("[^\d]", " ", link_year).split())
                                link_words = link_words[:-1]
                                link_title = ' ('.join(link_words)
                            else:
                                link_title = link
                            link_words = re.sub("[^\w\$]", " ",
                                                ''.join(link_words)).lower().split()
                            if link_words == words:
                                if not link_year:
                                    res_links[link] = link_title
                                elif year == link_year:
                                    res_links[link] = link_title
                                    break

                    if res_links:
                        plot = ''
                        if len(res_links) > 1:
                            for l in res_links:
                                for l2 in res_links:
                                    if l != l2:
                                        if l.lower() == l2.lower() and res_links[
                                            l] == title or res_links[l2] != title:
                                            del res_links[l]
                        if len(res_links) == 1:
                            (res_link, _), = res_links.items()
                            page = wikipedia.page(res_link, auto_suggest=False)
                            plot = ''.join(filter(None, [
                                page.section(section_title) for section_title in
                                PLOT_SECTION_NAMES
                                ])).replace('\n', '')
                        else:
                            logger.info(title + " (" + year + ") has " + str(
                                len(res_links)) + " different variants")
                        if not plot:
                            plot = string[-1]
                        res.write(film_unplotted + SEP + plot + '\n')
                    else:
                        logger.info("Wiki page is not found for " + title)
                        res.write(film)


set_wiki_links_by_year()
