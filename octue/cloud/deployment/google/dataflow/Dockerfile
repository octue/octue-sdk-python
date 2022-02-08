FROM apache/beam_python3.8_sdk:2.36.0

# Allow statements and log messages to immediately appear in the logs on Google Cloud.
ENV PYTHONUNBUFFERED=1

ENV PROJECT_ROOT=/app
WORKDIR $PROJECT_ROOT

RUN apt-get update -y && apt-get install -y --fix-missing build-essential && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --upgrade pip && pip install -e .[dataflow]

ENV USE_OCTUE_LOG_HANDLER=1
ENV COMPUTE_PROVIDER=GOOGLE_DATAFLOW

ARG SERVICE_ID
ENV SERVICE_ID=$SERVICE_ID

ARG SERVICE_NAME
ENV SERVICE_NAME=$SERVICE_NAME
