FROM python:3.7-slim

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ENV https_proxy=""
ENV http_proxy=""
ENV no_proxy="127.0.0.1,localhost"

RUN apt-get update
RUN apt-get install -y \
    gcc libmagic1 net-tools

RUN pip install --no-cache-dir pipenv httpie

ADD Pipfile* ./
RUN pipenv install --system --deploy --ignore-pipfile

ADD . .

ENTRYPOINT ["python3"]
CMD ["main.py"]
