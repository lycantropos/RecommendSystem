from vizier.config import SEP


def invoke_user_db(path: str) -> dict:
    with open(path, 'r') as f:
        ratings = dict()
        f.readline()
        for string in f:
            string = string[:-1]
            string = string.split(',')
            uid = int(string[0])
            if uid in ratings.keys():
                ratings[uid][int(string[1])] = float(string[2])
            else:
                ratings[uid] = {int(string[1]): float(string[2])}
        return ratings


def get_genres(films_path: str):
    with open(films_path, 'r') as f:
        genres = set()
        for string in f:
            string = string[:-1]  # removing newline-symbol
            string = string.split(SEP)[3]  # getting substring with genres
            genre = string.split('|')
            genres.update(set(genre))
        return genres
