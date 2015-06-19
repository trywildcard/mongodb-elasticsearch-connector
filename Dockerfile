FROM python:2.7
MAINTAINER William Huba <hexedpackets@gmail.com>

RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

ADD . /app
ENTRYPOINT ["/app/mongodb_elasticsearch_connector.py"]
