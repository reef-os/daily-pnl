FROM public.ecr.aws/docker/library/python:3.12-slim

RUN apt-get update \
    && apt-get install curl -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    USE_IAM="True"

WORKDIR /usr/src

COPY requirements.txt /usr/src

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /usr/src

ENV PYTHONPATH "/usr/src"

RUN echo "Contents of /usr/src:" && ls -l /usr/src
RUN echo "PYTHONPATH is set to: $PYTHONPATH"
