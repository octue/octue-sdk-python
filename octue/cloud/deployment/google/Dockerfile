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

ARG COMPUTE_PROVIDER=GOOGLE_CLOUD_RUN
ENV COMPUTE_PROVIDER=$COMPUTE_PROVIDER

ARG SERVICE_ID
ENV SERVICE_ID=$SERVICE_ID

ARG GUNICORN_WORKERS=1
ENV GUNICORN_WORKERS=$GUNICORN_WORKERS

ARG GUNICORN_THREADS=8
ENV GUNICORN_THREADS=$GUNICORN_THREADS

# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers $GUNICORN_WORKERS --threads $GUNICORN_THREADS --timeout 0 octue.cloud.deployment.google.cloud_run:app
