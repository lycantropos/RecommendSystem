from setuptools import setup, find_packages

from vizier.config import PACKAGE

setup(name=PACKAGE,
      version='0.0.0',
      packages=find_packages(),
      install_requires=[
          'requests==2.13.0',
          'wikipedia==1.4.0',
          'distance',
          # Postgres
          'psycopg2==2.6.2',
          'SQLAlchemy==1.0.12',
      ])
