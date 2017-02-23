import csv
import os
import urllib.request
import logging
from typing import Dict, Union

from bs4 import BeautifulSoup
import requests
from bs4 import Tag

from vizier.config import DATA_DIR, LINKS_FILE_NAME, FILMS_DATABASE_NAME, SEP

OUTPUT_FILE_NAME = 'movies.txt'

IMDB_API_URL = 'http://www.omdbapi.com/?'

logging.basicConfig(
    format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
    level=logging.INFO)


def main():
    link = 'http://www.omdbapi.com/?i=tt' + '0166924' + '&plot=short&r=xml'
    with urllib.request.urlopen(link) as url:
        html = url.read()
        soup = BeautifulSoup(html, 'xml')


def imdb_query(req: Dict[str, Union[str, int]]):
    r = requests.get(IMDB_API_URL, params=req).content
    soup = BeautifulSoup(r, 'xml')
    return getattr(soup._most_recent_element, 'attrs', None)


def parse_imdb(start: int, path: str, append: bool=False):
    links_file_path = os.path.join(path, LINKS_FILE_NAME)
    with open(links_file_path, mode='r') as links_file:
        mode = 'a' if append else 'w'
        links_reader = csv.reader(links_file, delimiter=',')
        with open(path + OUTPUT_FILE_NAME, mode) as res:
            for link in links_reader:
                fid = link[0]
                imdb_id = link[1]
                if int(fid) >= start:
                    genres = ''
                    director = ''
                    writer = ''
                    actors = ''
                    country = ''
                    language = ''
                    runtime = ''
                    rated = ''
                    plot = ''
                    my_atts = dict(plot='full', r='xml')
                    my_atts['i'] = 'tt' + imdb_id
                    tags = imdb_query(my_atts)
                    if tags:
                        title = tags['title']
                        if 'year' in tags.keys():
                            year = tags['year']
                        genres = tags['genre'].replace(',', '|').replace('| ', '|')
                        director = tags['director'].replace(',', '|').replace('| ', '|')
                        if 'writer' in tags.keys():
                            writer = tags['writer'].replace(',', '|').replace('| ', '|')
                        if 'actors' in tags.keys():
                            actors = tags['actors'].replace(',', '|').replace('| ', '|')
                        if 'country' in tags.keys():
                            country = tags['country'].replace(',', '|').replace('| ',
                                                                                '|')
                        if 'language' in tags.keys():
                            language = tags['language'].replace(',', '|').replace('| ',
                                                                                  '|')
                        if 'runtime' in tags.keys():
                            runtime = tags['runtime']
                        if 'rated' in tags.keys():
                            rated = tags['rated']
                        if 'plot' in tags.keys():
                            plot = tags['plot']
                        logging.info(
                            fid + ',' + title + ',' + year + ',' + genres + ',' +
                            director + ',' + writer + ',' + actors)
                    else:
                        with open(path + FILMS_DATABASE_NAME, mode='r') as films:
                            films.readline()
                            for film in films:
                                film_info = film.split(',')
                                if int(film_info[0]) == int(fid):
                                    print(film_info)
                                    if film_info[1].split('(')[-1][:-1] == \
                                            film_info[1].split(')')[-1][1:] and \
                                                    film_info[1].split('(') == 2:
                                        title = film_info[1].split('(')[0]
                                        year = film_info[1].split('(')[-1][:-1]
                    res.write(SEP.join(fid, imdb_id, genres, director, writer, actors,
                                       country, language, runtime, rated, plot) + '\n')


if __name__ == '__main__':
    main()
