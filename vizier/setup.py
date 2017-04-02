from setuptools import setup, find_packages

from vizier.config import PACKAGE

setup(name=PACKAGE,
      version='0.1.0',
      packages=find_packages(),
      install_requires=[
          'wikipedia==1.4.0',
          'psycopg2>=2.6.2',
          'aiohttp>=1.3.3',
          'cetus>=0.3.3',
          'SQLAlchemy>=1.0.12',
      ])
