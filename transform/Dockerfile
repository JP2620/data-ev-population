FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ev_transform/ ev_transform/

WORKDIR /app/ev_transform
ENV DBT_PROFILES_DIR=/app/ev_transform

RUN dbt deps

CMD ["dbt", "run"]
