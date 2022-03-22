FROM python:3.8.12-slim

RUN apt-get update && apt-get install -y git

COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml

COPY . .

RUN pip3 install --upgrade pip && pip3 install poetry && poetry install -E dataflow -v
