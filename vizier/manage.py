import logging
import logging.config
import os
from datetime import date

import click
import pkg_resources
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.exc import ProgrammingError

from vizier.config import (PACKAGE,
                           CONFIG_DIR_NAME,
                           LOGGING_CONF_FILE_NAME,
                           FIRST_FILM_YEAR)
from vizier.models.base import Base
from vizier.models.film import GENRES_NAMES, Genre
from vizier.services.data_access.service import get_session
from vizier.services.parser import parse_films

logger = logging.getLogger(__file__)


@click.group(name='main')
@click.option('--verbose', is_flag=True, help='Sets logging level to `DEBUG`.')
@click.pass_context
def main(ctx: click.Context, verbose: bool):
    resource_manager = pkg_resources.ResourceManager()
    conf_dir = resource_manager.resource_filename(PACKAGE, CONFIG_DIR_NAME)
    logging_conf_file_path = os.path.join(conf_dir, LOGGING_CONF_FILE_NAME)
    set_logging(logging_conf_file_path, verbose)

    postgres_uri = os.environ['POSTGRES_URI']
    ctx.obj = dict(postgres_uri=postgres_uri)


def set_logging(logging_conf_file_path: str, verbose: bool):
    logging.config.fileConfig(logging_conf_file_path)
    if not verbose:
        logging.getLogger().setLevel(logging.INFO)


@main.command(name='run')
@click.option('--clean', is_flag=True, help='Removes Postgres database.')
@click.option('--init', is_flag=True, help='Initializes Postgres database.')
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
    postgres_db_uri = make_url(ctx.obj['postgres_uri'])
    next_year = date.today().year + 1
    parse_films(postgres_db_uri,
                start_year=FIRST_FILM_YEAR,
                stop_year=next_year)


@main.command(name='clean_db')
@click.pass_context
def clean_db(ctx: click.Context):
    """Removes Postgres database."""
    postgres_db_uri = make_url(ctx.obj['postgres_uri'])
    clean_postgres(postgres_db_uri)


def clean_postgres(db_uri: URL):
    engine = create_engine(f'postgresql://postgres@{db_uri.host}')
    connection = engine.connect()
    connection.connection.set_isolation_level(0)
    connection.execution_options(autocommit=True)
    logging.info('Cleaning Postgres database')
    try:
        connection.execute(f'DROP DATABASE {db_uri.database}')
    except ProgrammingError:
        pass
    finally:
        connection.close()


@main.command(name='init_db')
@click.pass_context
def init_db(ctx: click.Context):
    """Creates Postgres database."""
    postgres_db_uri = make_url(ctx.obj['postgres_uri'])
    init_postgres(postgres_db_uri)


def init_postgres(db_uri: URL):
    logging.info(db_uri)
    engine = create_engine(f'postgresql://postgres@{db_uri.host}')
    connection = engine.connect()
    connection.connection.set_isolation_level(0)
    logging.info('Creating Postgres database')
    try:
        connection.execute(f'CREATE DATABASE {db_uri.database}')
    except ProgrammingError:
        logging.exception('')
    try:
        connection.execute('GRANT ALL PRIVILEGES '
                           f'ON DATABASE {db_uri.database} '
                           f'TO {db_uri.username}')
    except ProgrammingError:
        logging.exception('')
    connection.close()
    logging.info('Creating Postgres database schema')
    engine = create_engine(db_uri)
    Base.metadata.create_all(bind=engine)


@main.command(name='seed_data')
@click.pass_context
def seed_data(ctx: click.Context):
    """Adds test data to database."""
    postgres_db_uri = make_url(ctx.obj['postgres_uri'])
    engine = create_engine(postgres_db_uri)
    with get_session(engine) as session:
        logging.info('Seeding data')
        for genre_name in GENRES_NAMES:
            session.add(Genre(genre_name))
        session.commit()


if __name__ == '__main__':
    main()
