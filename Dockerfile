# Docker file for tsb
# Use an official Python runtime as a parent image
FROM python:3.7-buster
MAINTAINER Roald Storm <roald.storm@niva.no>

# Set the working directory to /app
WORKDIR /app
RUN pip install psycopg2
COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY . /app/
RUN pycodestyle --config .pycodestyle src
RUN mypy --namespace-packages --ignore-missing-imports src
RUN pip install .
RUN pytest

ARG GIT_COMMIT_ID=unknown
LABEL git_commit_id=$GIT_COMMIT_ID
ENV GIT_COMMIT_ID=$GIT_COMMIT_ID

# Start gunicorn
CMD ["python", "/app/src/odm2_postgres_api/main.py"]
