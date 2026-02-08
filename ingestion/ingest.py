import json
import os
import uuid
from datetime import datetime, timezone

import psycopg2
import requests

SOURCE_URL = "https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD"
DATA_DIR = "/data"

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

# Socrata metadata field indices (first 8 elements of each row)
SOCRATA_META_FIELDS = {
    0: "socrata_sid",
    1: "socrata_id",
    2: "socrata_position",
    3: "socrata_created_at",
    4: "socrata_created_meta",
    5: "socrata_updated_at",
    6: "socrata_updated_meta",
    7: "socrata_meta",
}
KEEP_SOCRATA_FIELDS = {1, 5}  # id, updated_at


def download_file(url, data_dir):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"ev_data_{timestamp}.json"
    filepath = os.path.join(data_dir, filename)

    print(f"Downloading from {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Saved to {filepath}")
    return filepath, filename


def parse_columns(meta):
    columns = []
    for col in meta["view"]["columns"]:
        if col["dataTypeName"] == "meta_data":
            continue
        columns.append(col["fieldName"])
    return columns


def row_to_record(row, columns):
    socrata_meta = {}
    for idx in KEEP_SOCRATA_FIELDS:
        socrata_meta[SOCRATA_META_FIELDS[idx]] = row[idx]

    data_values = row[8:]
    record = dict(zip(columns, data_values))
    record.update(socrata_meta)

    return record


def load_to_landing(filepath, filename):
    print(f"Parsing {filepath}...")
    with open(filepath, "r") as f:
        raw = json.load(f)

    columns = parse_columns(raw["meta"])
    rows = raw["data"]

    batch_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    loaded_date = now.date()

    print(f"Batch {batch_id}: {len(rows)} rows to load")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    try:
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO landing.ev_raw_data
                (value, el_loaded_timestamp, el_loaded_date, el_input_filename, el_batch_id)
            VALUES
                (%s, %s, %s, %s, %s)
        """

        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            values = []
            for row in batch:
                record = row_to_record(row, columns)
                values.append((
                    json.dumps(record),
                    now,
                    loaded_date,
                    filename,
                    batch_id,
                ))
            cur.executemany(insert_sql, values)
            conn.commit()
            print(f"  Loaded {min(i + batch_size, len(rows))}/{len(rows)} rows")

        cur.close()
    finally:
        conn.close()

    print("Done.")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath, filename = download_file(SOURCE_URL, DATA_DIR)
    load_to_landing(filepath, filename)


if __name__ == "__main__":
    main()
