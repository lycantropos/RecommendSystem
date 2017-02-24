FROM python:latest
MAINTAINER lycantropos <azatibrakov@gmail.com>

WORKDIR "/opt/vizier"

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

# psycopg2 package requirement http://stackoverflow.com/a/5450183
RUN apt-get update \
 && apt-get install -y libpq-dev \
                       python3-setuptools

COPY requirements.txt ./

RUN python3.6 -m pip install -r requirements.txt

COPY . .

RUN python3.6 setup.py develop

EXPOSE 10000
ENTRYPOINT ["python3.6", "manage.py"]
