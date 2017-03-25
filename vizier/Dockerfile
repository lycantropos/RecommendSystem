FROM python:latest
MAINTAINER lycantropos <azatibrakov@gmail.com>

WORKDIR "/opt/vizier"

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

RUN apt-get update \
# psycopg2 package requirement http://stackoverflow.com/a/5450183
 && apt-get install -y git

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY . .

RUN python setup.py develop

EXPOSE 10000
ENTRYPOINT ["python", "manage.py"]
