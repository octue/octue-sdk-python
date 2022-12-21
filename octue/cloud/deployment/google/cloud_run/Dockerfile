FROM windpioneers/gdal-python:little-gecko-gdal-2.4.1-python-3.9-slim

# Ensure print statements and log messages appear promptly in Cloud Logging.
ENV PYTHONUNBUFFERED True

ENV PROJECT_ROOT=/app
WORKDIR $PROJECT_ROOT

RUN apt-get update -y && apt-get install -y --fix-missing build-essential && rm -rf /var/lib/apt/lists/*

# Install poetry if a `pyproject.toml` file is present.
ENV POETRY_HOME=/etc/poetry
ENV PATH "$POETRY_HOME/bin:$PATH"
RUN if [ -f "pyproject.toml" ]; then curl -sSL https://install.python-poetry.org | python3 - && poetry config virtualenvs.create false; fi

# Copy in the dependencies file(s) for caching. `requirements.txt`, `setup.py`, and `pyproject.toml and `poetry.lock`
# are supported.
COPY requirements.txt? setup.py? pyproject.toml? poetry.lock? ./

# If `pyproject.toml` is present, install  the dependencies only to utilise layer caching for quick rebuilds.
RUN if [ -f "pyproject.toml" ]; then poetry install --no-ansi --no-interaction --no-cache --no-root --only main; fi

# Copy local code to the application root directory.
COPY . .

# Install local packages if using poetry. Otherwise, install everything if using `setup.py` or `requirements.txt`. All
# dependency files are installed from if present.
RUN if [ -f "pyproject.toml" ]; then poetry install --only main; fi
RUN if [ -f "requirements.txt" ]; then pip install --upgrade pip && pip install -r requirements.txt; fi
RUN if [ -f "setup.py" ]; then pip install --upgrade pip && pip install -e .; fi

EXPOSE $PORT

ENV USE_OCTUE_LOG_HANDLER=1
ENV COMPUTE_PROVIDER=GOOGLE_CLOUD_RUN

ARG GUNICORN_WORKERS=1
ENV GUNICORN_WORKERS=$GUNICORN_WORKERS

ARG GUNICORN_THREADS=8
ENV GUNICORN_THREADS=$GUNICORN_THREADS

# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers $GUNICORN_WORKERS --threads $GUNICORN_THREADS --timeout 0 octue.cloud.deployment.google.cloud_run.flask_app:app
