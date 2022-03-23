FROM python:3.8.12-slim

RUN apt-get update && apt-get install -y git curl

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
ENV PATH "/root/.poetry/bin:$PATH"
RUN poetry config virtualenvs.create false

COPY octue/cloud/deployment/google/dataflow/setup.py setup.py
COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml

COPY . .

RUN poetry install -E dataflow --no-dev -v
