import csv
import os
import re
from typing import Dict, Optional

import requests

from vizier.config import LINKS_FILE_NAME, FILMS_DATABASE_NAME

OUTPUT_FILE_NAME = 'movies.txt'

IMDB_API_URL = 'https://www.omdbapi.com'


def query_imdb(imdb_id: Optional[int],
               title: str, year: int) -> Dict[str, str]:
    params = dict(plot='full', r='json', tomatoes=True, y=year)
    if imdb_id is not None:
        params['i'] = f'tt{imdb_id:0>7}'
    else:
        params['t'] = normalize_title(title)
    return requests.get(IMDB_API_URL, params=params).json()


FILM_TITLE_RE = re.compile('.+?(?= \((film|miniseries|video|manga)\))')


def normalize_title(title: str) -> str:
    alphanumeric_title = re.sub('\W', ' ', title)
    match = FILM_TITLE_RE.match(alphanumeric_title)
    return match.group(0) if match is not None else alphanumeric_title


def normalize_str(string: str) -> str:
    return string.replace(',', '|').replace('| ', '|')


def parse_imdb(start: int, path: str):
    links_file_path = os.path.join(path, LINKS_FILE_NAME)
    with open(links_file_path, mode='r') as links_file:
        links_reader = csv.reader(links_file, delimiter=',')
        for link in links_reader:
            fid = link[0]
            if int(fid) < start:
                continue
            imdb_id = link[1]
            title = ''
            year = None
            tags = query_imdb(imdb_id, title, year)
            if tags is None:
                genres = ''
                director = ''
                writer = ''
                actors = ''
                country = ''
                language = ''
                runtime = ''
                rated = ''
                plot = ''
                with open(path + FILMS_DATABASE_NAME, mode='r') as films:
                    films.readline()
                    for film in films:
                        film_info = film.split(',')
                        if int(film_info[0]) == int(fid):
                            if film_info[1].split('(')[-1][:-1] == \
                                    film_info[1].split(')')[-1][1:] and \
                                            film_info[1].split('(') == 2:
                                title = film_info[1].split('(')[0]
                                year = film_info[1].split('(')[-1][:-1]
                            yield (fid, title, year, imdb_id, genres,
                                   director, writer, actors,
                                   country, language, runtime,
                                   rated, plot)

            title = tags['title']
            year = tags.get('year', None)
            genres = normalize_str(tags['genre'])
            director = normalize_str(tags['director'])
            writer = normalize_str(tags.get('writer', ''))
            actors = normalize_str(tags.get('actors', ''))
            country = normalize_str(tags.get('country', ''))
            language = normalize_str(tags.get('language', ''))
            runtime = tags['runtime']
            rated = tags['rated']
            plot = tags.get('plot', '')
            yield (fid, title, year, imdb_id, genres,
                   director, writer, actors,
                   country, language, runtime,
                   rated, plot)
