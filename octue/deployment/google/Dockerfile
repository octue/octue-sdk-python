FROM python:slim-buster

# Allow statements and log messages to immediately appear in the Knative logs on Google Cloud.
ENV PYTHONUNBUFFERED True

ENV PROJECT_ROOT=/app
WORKDIR $PROJECT_ROOT

RUN apt-get update -y && apt-get install -y --fix-missing build-essential && rm -rf /var/lib/apt/lists/*

# This will cache bust if any of the requirements change.
COPY requirements.txt ./

# Gunicorn is required to run the Flask server that connected the Docker container to Cloud Run.
RUN echo "gunicorn" >> requirements.txt

# Upgrade to latest pip and setuptools after the cache bust, then install requirements
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

EXPOSE $PORT

ARG _TRIGGER_ID
ENV SERVICE_ID=$_TRIGGER_ID

ARG _GUNICORN_WORKERS=1
ENV _GUNICORN_WORKERS=$_GUNICORN_WORKERS

ARG _GUNICORN_THREADS=8
ENV _GUNICORN_THREADS=$_GUNICORN_THREADS

# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers $_GUNICORN_WORKERS --threads $_GUNICORN_THREADS --timeout 0 octue.deployment.google.cloud_run:app
