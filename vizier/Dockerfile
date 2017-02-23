FROM python:latest
MAINTAINER lycantropos <azatibrakov@gmail.com>

COPY . "/opt"
WORKDIR "/opt/"

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

# psycopg2 package requirement http://stackoverflow.com/a/5450183
RUN apt-get update \
 && apt-get install -y libpq-dev \
                       python3-setuptools

RUN python3.6 -m pip install -r requirements.txt

EXPOSE 10000
ENTRYPOINT ["python3.6", "manage.py"]
