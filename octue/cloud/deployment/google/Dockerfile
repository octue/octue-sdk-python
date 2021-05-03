FROM python:3.9-slim-buster

# Allow statements and log messages to immediately appear in the Knative logs on Google Cloud.
ENV PYTHONUNBUFFERED True

ENV PROJECT_ROOT=/app
WORKDIR $PROJECT_ROOT

RUN apt-get update -y && apt-get install -y --fix-missing build-essential && rm -rf /var/lib/apt/lists/*

COPY . .

# Install requirements (supports requirements.txt, requirements-dev.txt, and setup.py; all will be run if all are present.)
RUN if [ ! -f "requirements.txt" ] && [ ! -f "requirements-dev.txt" ] && [ ! -f "setup.py" ]; then exit 1; fi
RUN if [ -f "requirements.txt" ]; then pip install --upgrade pip && pip install -r requirements.txt; fi
RUN if [ -f "requirements-dev.txt" ]; then pip install --upgrade pip && pip install -r requirements-dev.txt; fi
RUN if [ -f "setup.py" ]; then pip install --upgrade pip && pip install -e .; fi

EXPOSE $PORT

ARG _SERVICE_ID
ENV SERVICE_ID=$_SERVICE_ID

ARG _GUNICORN_WORKERS=1
ENV _GUNICORN_WORKERS=$_GUNICORN_WORKERS

ARG _GUNICORN_THREADS=8
ENV _GUNICORN_THREADS=$_GUNICORN_THREADS

# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers $_GUNICORN_WORKERS --threads $_GUNICORN_THREADS --timeout 0 octue.cloud.deployment.google.cloud_run:app
