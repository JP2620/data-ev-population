FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY ingest.py ingest.py

CMD python ingest.py
