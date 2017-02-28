import os

MAX_SCORE = 5
MIN_SCORE = 0

DATA_DIR = 'db'
RATINGS_DATABASE_NAME = 'ratings.csv'
FILMS_DATABASE_NAME = 'movies.txt'
FILMS_WITH_PLOT_DATABASE_NAME = 'movies_full.csv'
LINKS_FILE_NAME = 'links.csv'

BASE_DIR = os.path.dirname(__file__)

PACKAGE = 'vizier'
CONFIG_DIR_NAME = 'configurations'
LOGGING_CONF_FILE_NAME = 'logging.conf'

RELEASE_DATE_FORMAT = '%d %b %Y'
NOT_AVAILABLE_VALUE_ALIAS = 'N/A'
FIRST_FILM_YEAR = 1887

WIKIPEDIA_API_URL = 'https://en.wikipedia.org/w/api.php'
IMDB_API_URL = 'https://www.omdbapi.com'
