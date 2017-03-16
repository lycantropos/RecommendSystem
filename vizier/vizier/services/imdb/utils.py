import re

IMDB_ID_LENGTH = 7
FILM_TITLE_RE = re.compile('.+?(?= \((film|miniseries|video|manga)\))')
