from setuptools import setup, find_packages

from vizier.config import PACKAGE

setup(name=PACKAGE,
      version='0.0.0',
      packages=find_packages(),
      install_requires=[
          'wikipedia==1.4.0',
          'psycopg2==2.6.2',
          'SQLAlchemy==1.0.12',
          # async
          'asyncio_extras',  # for async context managers
          'aiohttp',  # for working with http
          'asyncpg',  # for working with Postgres
          'aiomysql',  # for working with MySQL
      ])
