FROM python:3.8.12-slim

RUN apt-get update && apt-get install -y git curl

ENV POETRY_HOME=/etc/poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH "$POETRY_HOME/bin:$PATH"
RUN poetry config virtualenvs.create false

# Install python dependencies. Note that poetry installs any root packages by default, but this is not available at this
# stage of caching dependencies. So we do a dependency-only install here to cache the dependencies, then a full poetry
# install post-create to install the root package, which will change more rapidly than dependencies.
COPY octue/cloud/deployment/google/dataflow/setup.py setup.py
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-ansi --no-interaction --no-root

COPY . .
RUN poetry install --no-ansi --no-interaction

RUN poetry install -E dataflow --no-dev -v
