FROM apache/beam_python3.9_sdk:2.41.0

# Allow statements and log messages to immediately appear in the logs on Google Cloud.
ENV PYTHONUNBUFFERED=1

ENV PROJECT_ROOT=/app
WORKDIR $PROJECT_ROOT

RUN apt-get update -y && apt-get install -y --fix-missing build-essential && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --upgrade pip && pip install -e .

ENV USE_OCTUE_LOG_HANDLER=1
ENV COMPUTE_PROVIDER=GOOGLE_DATAFLOW

ARG OCTUE_SERVICE_NAMESPACE
ENV OCTUE_SERVICE_NAMESPACE=$OCTUE_SERVICE_NAMESPACE

ARG OCTUE_SERVICE_NAME
ENV OCTUE_SERVICE_NAME=$OCTUE_SERVICE_NAME

ARG OCTUE_SERVICE_REVISION_TAG
ENV OCTUE_SERVICE_REVISION_TAG=$OCTUE_SERVICE_REVISION_TAG
