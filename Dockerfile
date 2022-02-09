FROM python:3.8.12-slim

RUN apt-get update && apt-get install -y git

COPY setup.py setup.py

COPY . .

RUN pip3 install --upgrade pip && pip3 install -e .[dataflow]
