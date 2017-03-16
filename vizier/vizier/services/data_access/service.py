from contextlib import contextmanager

from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import sessionmaker

from vizier.types import DbUriType


@contextmanager
def get_session(engine: Engine,
                autoflush: bool = True,
                autocommit: bool = False,
                expire_on_commit: bool = True):
    session_factory = sessionmaker(bind=engine,
                                   autoflush=autoflush,
                                   autocommit=autocommit,
                                   expire_on_commit=expire_on_commit)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def get_engine(db_uri: DbUriType):
    engine = create_engine(db_uri)
    try:
        yield engine
    finally:
        engine.dispose()
