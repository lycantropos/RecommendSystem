import os
import urllib.request
import logging

from bs4 import BeautifulSoup
import requests

from config import DATA_DIR, LINKS_DATABASE_NAME, FILMS_DATABASE_NAME, SEP

IMDB_API_URL = 'http://www.omdbapi.com/?'

logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
                    level=logging.INFO)


# link = 'http://www.omdbapi.com/?i=tt' + '0166924' + '&plot=short&r=xml'
# with urllib.request.urlopen(link) as url:
#     html = url.read()
#     soup = BeautifulSoup(html, 'xml')
#     director = soup.body.find('div', {'itemprop': 'director'})
#     writers = soup.body.find('div', {'itemprop': 'actors'})
#     actors = soup.body.find('div', {'itemprop': 'actors'})
#     str_actors = ''
#     str_director = ''
#     if director:
#         str_director = director.contents[3].contents[0].contents[0]
#     if actors:
#         for span in actors:
#             soup2 = BeautifulSoup(span)
#             if type(span) == Tag and span.name == 'a':
#                 str_actors += span.contents[0].contents[0] + '|'
#         str_actors = str_actors[:-1]

def imdb_query(req: str):
    r = requests.get(IMDB_API_URL, params=req).content
    soup = BeautifulSoup(r, 'xml')
    if hasattr(soup._most_recent_element, 'attrs'):
        return soup._most_recent_element.attrs


def parse_imdb(start: int = 0, path: str = os.getcwd() + DATA_DIR, mode='w'):
    with open(path + LINKS_DATABASE_NAME, mode='r') as links:
        with open(path + 'movies.txt', mode) as res:
            links.readline()
            for link in links:
                link = link.split(',')
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
                            country = tags['country'].replace(',', '|').replace('| ', '|')
                        if 'language' in tags.keys():
                            language = tags['language'].replace(',', '|').replace('| ', '|')
                        if 'runtime' in tags.keys():
                            runtime = tags['runtime']
                        if 'rated' in tags.keys():
                            rated = tags['rated']
                        if 'plot' in tags.keys():
                            plot = tags['plot']
                        logging.info(fid + ',' + title + ',' + year + ',' + genres + ',' +
                                     director + ',' + writer + ',' + actors)
                    else:
                        with open(path + FILMS_DATABASE_NAME, mode='r') as films:
                            films.readline()
                            for film in films:
                                film_info = film.split(',')
                                if int(film_info[0]) == int(fid):
                                    print(film_info)
                                    if film_info[1].split('(')[-1][:-1] == film_info[1].split(')')[-1][1:] and \
                                                    film_info[1].split('(') == 2:
                                        title = film_info[1].split('(')[0]
                                        year = film_info[1].split('(')[-1][:-1]
                    res.write(SEP.join(fid, imdb_id, genres, director, writer, actors,
                                       country, language, runtime, rated, plot) + '\n')

# if __name__ == '__main__':
#     parse_imdb(0)
