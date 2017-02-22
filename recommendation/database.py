import os
<<<<<<< HEAD
from config import DATA_DIR, RATINGS_DATABASE_NAME, FILMS_DATABASE_NAME


def invoke_user_db(path: str = os.getcwd() + DATA_DIR + RATINGS_DATABASE_NAME)->dict:
    with open(path, 'r') as f:
=======
from config import DATA_DIR, RATINGS_DATABASE_NAME, FILMS_DATABASE_NAME, SEP
from imdb_parser import SEP
from recommendation import Film


def invoke_user_db(ratings_path: str = os.getcwd() + DATA_DIR + RATINGS_DATABASE_NAME) -> dict:
    with open(ratings_path, 'r') as f:
>>>>>>> 215ba9c50fadc0c4e28387e4ff311c5bf55bea8f
        ratings = dict()
        f.readline()
        for string in f:
            string = string[:-1]
            string = string.split(',')
            uid = int(string[0])
            if uid in ratings.keys():
                ratings[uid][int(string[1])] = float(string[2])
            else:
                ratings[uid] = dict({int(string[1]): float(string[2])})
        return ratings


<<<<<<< HEAD
def get_genres(path: str = os.getcwd() + DATABASE_PATH):
    with open(path + FILMS_DATABASE_NAME, 'r') as f:
        genres = set()
        f.readline()
        for string in f:
            string = string[:-1]
            string = string.split(',')
            genre = string[-1].split('|')
            genres.update(set(genre))
        return genres

=======
def invoke_film_db(films_path: str = os.getcwd() + DATABASE_PATH + FILMS_DATABASE_NAME,
                   ratings_path: str = os.getcwd() + DATABASE_PATH + RATINGS_DATABASE_NAME) -> dict:
    with open(films_path, 'r') as f:
        films = set()
        f.readline()
        ratings = invoke_user_db(ratings_path)
        rated_films = dict()
        for uid in ratings:
            for fid in ratings[uid]:
                if fid in rated_films:
                    rated_films[fid][uid] = ratings[uid][fid]
                else:
                    rated_films[fid] = dict({uid: ratings[uid][fid]})
        for string in f:
            string = string[:-1]
            string = string.split(SEP)
            fid = int(string[0])
            if fid in rated_films:
                title = string[1]
                year = int(string[2])
                genres = set(string[3].split('|'))
                directors = set(string[4].split('|'))
                writers = set(string[5].split('|'))
                actors = set(string[6].split('|'))
                country = set(string[7].split('|'))
                language = set(string[8].split('|'))
                runtime = set(string[9].split('|'))
                plot = ''
                if len(string) == 11:
                    plot = string[10]
                films.add(Film(fid, title, year, genres, directors, writers, actors, country, language, runtime, plot,
                               rated_films[fid]))
        return films


def get_genres(films_path: str = os.getcwd() + DATABASE_PATH + FILMS_DATABASE_NAME):
    with open(films_path, 'r') as f:
        genres = set()
        for string in f:
            string = string[:-1]  # removing newline-symbol
            string = string.split(SEP)[3]  # getting substring with genres
            genre = string.split('|')
            genres.update(set(genre))
        return genres


>>>>>>> 215ba9c50fadc0c4e28387e4ff311c5bf55bea8f
if __name__ == '__main__':
    print(get_genres())
