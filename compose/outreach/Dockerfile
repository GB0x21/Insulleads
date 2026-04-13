FROM python:3.12-slim

ARG BUILD_ENV=local

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements/ /app/requirements/
RUN pip install -U pip && pip install -r /app/requirements/${BUILD_ENV}.txt

COPY . /app

RUN mkdir -p /app/data
VOLUME ["/app/data"]

EXPOSE 8000

CMD ["python", "manage.py", "rundaemon"]
