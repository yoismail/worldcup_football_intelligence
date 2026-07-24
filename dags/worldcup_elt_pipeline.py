import json
import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


# GCP Configuration — CORRECTED DATASET NAME
PROJECT_ID = "worldcup-football-project"
BUCKET = "worldcup-football-bucket"
BQ_DATASET = "worldcup_dataset"
REGION = "us-central1"

# API (TheSportsDB) Configuration
LEAGUE_ID = "4429"
SEASON = "2026"
BASE_URL = "https://www.thesportsdb.com/api/v1/json/3"
DELAY = 7
RETRIES = 3

FILES = {
    "teams": "worldcup_teams.ndjson",
    "standings": "worldcup_standings.ndjson",
    "matches": "worldcup_matches.ndjson"
}

PATHS = {
    "raw": "raw",
    "silver": "silver",
    "gold": "gold",
    "scripts": "scripts"
}

SCRIPTS = {
    "bronze_silver": f"gs://{BUCKET}/{PATHS['scripts']}/bronze_to_silver.py",
    "silver_gold": f"gs://{BUCKET}/{PATHS['scripts']}/silver_to_gold.py"
}

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1)
}


# Async function to fetch JSON data with retries
async def fetch_json(url):
    for attempt in range(RETRIES):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
                async with s.get(url) as r:
                    if r.status == 200:
                        data = await r.json()
                        if any(data.values()):
                            return data
                    elif r.status == 429:
                        wait = DELAY * (2 ** attempt)
                        logging.warning(f"Rate limited — wait {wait}s")
                        await asyncio.sleep(wait)
                    else:
                        logging.warning(f"HTTP {r.status} — retry {attempt+1}")
                        await asyncio.sleep(DELAY)
        except Exception as e:
            wait = DELAY * (2 ** attempt)
            logging.error(f"Error: {e} — retry {attempt+1}")
            await asyncio.sleep(wait)
    return None


def extract_and_upload(**context):
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

    gcs = GCSHook()

    async def run():
        for name, fname in FILES.items():
            endpoint = {
                "teams": f"{BASE_URL}/lookup_all_teams.php?id={LEAGUE_ID}",
                "standings": f"{BASE_URL}/lookuptable.php?l={LEAGUE_ID}&s={SEASON}",
                "matches": f"{BASE_URL}/eventsseason.php?id={LEAGUE_ID}&s={SEASON}"
            }[name]
            data = await fetch_json(endpoint)
            if data:
                payload = "\n".join(json.dumps(i, ensure_ascii=False)
                                    for i in (data if isinstance(data, list) else [data]))
                gcs.upload(BUCKET, f"{PATHS['raw']}/{fname}",
                           data=payload, mime_type="application/x-ndjson")
                logging.info(f"Raw saved: {fname}")
            await asyncio.sleep(DELAY)
    asyncio.run(run())


# DATA QUALITY CHECK
def validate_silver(**context):
    import pandas as pd
    import pyarrow.parquet as pq
    from io import BytesIO
    gcs = GCSHook()
    errors = []

    for entity in ["teams", "standings", "matches"]:
        prefix = f"{PATHS['silver']}/{entity}/"
        blobs = gcs.list(BUCKET, prefix=prefix)
        parquet_files = [path for path in blobs if path.endswith(".parquet")]

        if not parquet_files:
            errors.append(f"{entity}: no Parquet files found at {prefix}")
            continue

        pq_bytes = gcs.download(BUCKET, parquet_files[0])
        df = pq.read_table(BytesIO(pq_bytes)).to_pandas()
        cnt = len(df)

        if cnt == 0:
            errors.append(f"{entity}: zero rows!")
        required = {
            "teams": ["team_id", "team_name"],
            "standings": ["standing_id", "team_id", "points"],
            "matches": ["match_id", "home_team_id", "away_team_id"]
        }[entity]
        for col in required:
            if col not in df.columns:
                errors.append(f"{entity}: missing column {col}")
        logging.info(
            f"Silver {entity}: {cnt} rows, all required columns present")

    if errors:
        raise ValueError(f"DQ CHECK FAILED: {errors}")


# FULL DAG — SERVERLESS + ALL LAYERS LOADED TO BIGQUERY
with DAG(
    "worldcup_elt_production",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["worldcup", "production", "elt", "spark", "serverless"]
) as dag:

    extract_raw = PythonOperator(
        task_id="extract_and_upload_raw",
        python_callable=extract_and_upload
    )

    sensors = [
        GCSObjectExistenceSensor(
            task_id=f"wait_{n}", bucket=BUCKET, object=f"raw/{f}",
            poke_interval=15, timeout=300, mode="reschedule"
        ) for n, f in FILES.items()
    ]

    # BRONZE → SILVER — SERVERLESS PYSPARK
    bronze_to_silver = DataprocCreateBatchOperator(
        task_id="bronze_to_silver",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SCRIPTS["bronze_silver"]
            }
        },
        region=REGION,
        project_id=PROJECT_ID
    )

    # LOAD BRONZE (RAW NDJSON) TO BIGQUERY
    load_bronze = [
        GCSToBigQueryOperator(
            task_id=f"load_bronze_{e}",
            bucket=BUCKET,
            source_objects=[f"{PATHS['raw']}/{f}"],
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.bronze_{e}",
            source_format="NEWLINE_DELIMITED_JSON",
            autodetect=True,
            write_disposition="WRITE_OVERWRITE",
            external_table=True
        ) for e, f in FILES.items()
    ]

    validate_silver_task = PythonOperator(
        task_id="validate_silver_data",
        python_callable=validate_silver
    )

    # LOAD SILVER (CLEAN PARQUET) TO BIGQUERY
    load_silver = [
        GCSToBigQueryOperator(
            task_id=f"load_silver_{e}",
            bucket=BUCKET,
            source_objects=[f"{PATHS['silver']}/{e}/*"],
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.silver_{e}",
            source_format="PARQUET",
            autodetect=True,
            write_disposition="WRITE_OVERWRITE",
            external_table=True
        ) for e in ["teams", "standings", "matches"]
    ]

    # SILVER → GOLD — SERVERLESS PYSPARK
    silver_to_gold = DataprocCreateBatchOperator(
        task_id="silver_to_gold",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SCRIPTS["silver_gold"]
            }
        },
        region=REGION,
        project_id=PROJECT_ID
    )

    # LOAD GOLD TO BIGQUERY
    load_gold = [
        GCSToBigQueryOperator(
            task_id=f"load_gold_{e}",
            bucket=BUCKET,
            source_objects=[f"{PATHS['gold']}/{e}/*"],
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.gold_{e}",
            source_format="PARQUET",
            autodetect=True,
            write_disposition="WRITE_OVERWRITE",
            external_table=True
        ) for e in ["teams", "standings", "matches"]
    ]

    # COMPLETE WORKFLOW
    extract_raw >> sensors >> bronze_to_silver >> load_bronze >> validate_silver_task >> load_silver >> silver_to_gold >> load_gold
