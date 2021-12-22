FROM python:3.9-slim-buster

# Allow statements and log messages to immediately appear in the logs on Google Cloud.
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

ENV USE_OCTUE_LOG_HANDLER=1
ENV COMPUTE_PROVIDER=GOOGLE_CLOUD_RUN

ARG SERVICE_ID
ENV SERVICE_ID=$SERVICE_ID
