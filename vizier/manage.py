import logging
import logging.config
import os
from asyncio import get_event_loop
from datetime import date

import click
import pkg_resources
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import (database_exists,
                              create_database,
                              drop_database)

from vizier.config import (PACKAGE,
                           CONFIG_DIR_NAME,
                           LOGGING_CONF_FILE_NAME,
                           FIRST_FILM_YEAR)
from vizier.models import Genre
from vizier.models.base import Base
from vizier.models.genre import GENRES_NAMES
from vizier.services.data_access import (get_session,
                                         get_engine)
from vizier.services.defterdar import (parse_films,
                                       parse_films_articles)

logger = logging.getLogger(__file__)


@click.group(name='main')
@click.option('--verbose', is_flag=True, help='Sets logging level to `DEBUG`.')
@click.pass_context
def main(ctx: click.Context, verbose: bool):
    resource_manager = pkg_resources.ResourceManager()
    conf_dir = resource_manager.resource_filename(PACKAGE, CONFIG_DIR_NAME)
    logging_conf_file_path = os.path.join(conf_dir, LOGGING_CONF_FILE_NAME)
    set_logging(logging_conf_file_path, verbose)

    db_uri = os.environ['DB_URI']
    ctx.obj = dict(db_uri=db_uri)


def set_logging(logging_conf_file_path: str, verbose: bool):
    logging.config.fileConfig(logging_conf_file_path)
    if not verbose:
        logging.getLogger().setLevel(logging.INFO)


@main.command(name='run')
@click.option('--clean', is_flag=True, help='Removes database.')
@click.option('--init', is_flag=True, help='Initializes database.')
@click.option('--seed', is_flag=True, help='Adds test data to database.')
@click.pass_context
def run(ctx: click.Context, clean: bool, init: bool, seed: bool):
    if clean:
        ctx.invoke(clean_db)
    if init:
        ctx.invoke(init_db)
    if seed:
        ctx.invoke(seed_data)
    logging.info('Running "Vizier" service.')
    db_uri = make_url(ctx.obj['db_uri'])
    next_year = date.today().year + 1
    loop = get_event_loop()
    loop.run_until_complete(parse_films_articles(db_uri=db_uri,
                                                 start_year=FIRST_FILM_YEAR,
                                                 stop_year=next_year,
                                                 loop=loop))
    loop.run_until_complete(parse_films(start_year=FIRST_FILM_YEAR,
                                        stop_year=next_year,
                                        db_uri=db_uri,
                                        loop=loop))


@main.command(name='clean_db')
@click.pass_context
def clean_db(ctx: click.Context):
    """Removes Postgres database."""
    db_uri = make_url(ctx.obj['db_uri'])
    db_uri_str = db_uri.__to_string__()
    if database_exists(db_uri):
        logging.info(f'Cleaning "{db_uri_str}" database.')
        drop_database(db_uri)


@main.command(name='init_db')
@click.pass_context
def init_db(ctx: click.Context):
    """Creates Postgres database."""
    db_uri = make_url(ctx.obj['db_uri'])
    db_uri_str = db_uri.__to_string__()

    if not database_exists(db_uri):
        logging.info(f'Creating "{db_uri_str}" database.')
        create_database(db_uri)

    with get_engine(db_uri) as engine:
        logging.info(f'Creating "{db_uri_str}" database schema.')
        Base.metadata.create_all(bind=engine)


@main.command(name='seed_data')
@click.pass_context
def seed_data(ctx: click.Context):
    """Adds test data to database."""
    db_uri = make_url(ctx.obj['db_uri'])
    with get_engine(db_uri) as engine:
        with get_session(engine) as session:
            logging.info('Seeding data')
            for genre_name in GENRES_NAMES:
                session.add(Genre(genre_name))
            session.commit()


if __name__ == '__main__':
    main()
