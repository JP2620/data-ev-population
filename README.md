# EV Data Population Pipeline

An end-to-end data pipeline that ingests electric vehicle registration data from Washington State, transforms it through a medallion architecture (Landing → Bronze → Silver → Gold), and serves it as a star schema optimized for Power BI.

Orchestrated with **Apache Airflow**, transformed with **dbt**, and runs entirely in **Docker**.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with **WSL 2 backend** enabled
- **Git Bash** (included with [Git for Windows](https://git-scm.com/downloads))

All commands below should be run from **Git Bash**.

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd data-ev-population
```

### 2. Create the `.env` file

Copy the example and fill in the values:

```bash
cp .env.example .env
```

Edit `.env` with the following:

```
AIRFLOW_UID=<your_uid>
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
POSTGRES_PORT=5432
POSTGRES_DB=warehouse
```

### 3. Set your Airflow UID

In Git Bash, run:

```bash
echo -e "AIRFLOW_UID=$(id -u)"
```

Copy the output number and set it as `AIRFLOW_UID` in your `.env` file.

### 4. Build the custom Docker images

```bash
docker build -t ev-ingestion:latest ./ingestion
docker build -t ev-dbt:latest ./transform
```

### 5. Initialize Airflow

```bash
docker compose up airflow-init
```

Wait for it to complete successfully.

### 6. Start all services

```bash
docker compose up -d
```

### 7. Access the Airflow UI

Open your browser and go to **http://localhost:8080**

Log in with:
- **Username:** `airflow`
- **Password:** `airflow`

### 8. Trigger the DAGs

Both DAGs are set to manual trigger only. Run them **in order**:

1. **`ev_data_ingestor`** — Downloads raw EV data into the `landing` schema
   - Find `ev_data_ingestor` in the DAGs list
   - Toggle the DAG **ON** (unpause it)
   - Click the **Trigger DAG** button
   - Wait for all tasks to turn **green**

2. **`ev_dbt_transform`** — Runs dbt transformations (Bronze → Silver → Gold)
   - Find `ev_dbt_transform` in the DAGs list
   - Toggle the DAG **ON**
   - Click the **Trigger DAG** button
   - Wait for all tasks to turn **green**

> **Order matters!** The ingestion DAG must complete first so that dbt has data to transform.

### 9. Open the Power BI dashboard

Open `dashboard.pbix` in Power BI Desktop. The connection to the warehouse is pre-configured. When prompted for credentials, enter username `admin` and password `admin`.

## Teardown

To stop all services:

```bash
docker compose down
```

To stop and remove all data (volumes):

```bash
docker compose down -v
```
