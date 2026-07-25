# Standard Python libraries
import json
import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta

# Airflow core
from airflow import DAG
from airflow.operators.python import PythonOperator

# GCP integrations
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.bigquery import SchemaField


# --------------------------
# CONFIGURATION
# --------------------------
# GCP project and resource names
PROJECT_ID = "worldcup-football-project"
BUCKET = "worldcup-football-bucket"
BQ_DATASET = "worldcup_dataset"
REGION = "us-central1"

# TheSportsDB API settings
LEAGUE_ID = "4429"
SEASON = "2026"
BASE_URL = "https://www.thesportsdb.com/api/v1/json/3"
DELAY = 7
RETRIES = 3

# File names for each dataset
FILES = {
    "teams": "worldcup_teams.ndjson",
    "standings": "worldcup_standings.ndjson",
    "matches": "worldcup_matches.ndjson"
}

# Folder paths in GCS bucket
PATHS = {
    "raw": "raw",
    "silver": "silver",
    "gold": "gold",
    "scripts": "scripts"
}

# Full GCS paths to your Spark scripts
SCRIPTS = {
    "bronze_silver": f"gs://{BUCKET}/{PATHS['scripts']}/bronze_to_silver.py",
    "silver_gold": f"gs://{BUCKET}/{PATHS['scripts']}/silver_to_gold.py"
}

# --------------------------
# EXPLICIT BIGQUERY SCHEMAS
# --------------------------
# Silver layer — cleaned, standardised raw data
SILVER_SCHEMAS = {
    "teams": [
        SchemaField("team_id", "STRING", mode="NULLABLE"),
        SchemaField("team_name", "STRING", mode="NULLABLE"),
        SchemaField("team_short_name", "STRING", mode="NULLABLE"),
        SchemaField("team_founded", "STRING", mode="NULLABLE"),
        SchemaField("team_colour_1", "STRING", mode="NULLABLE"),
        SchemaField("team_colour_2", "STRING", mode="NULLABLE"),
        SchemaField("team_stadium", "STRING", mode="NULLABLE"),
        SchemaField("team_location", "STRING", mode="NULLABLE"),
        SchemaField("team_website", "STRING", mode="NULLABLE"),
        SchemaField("team_logo", "STRING", mode="NULLABLE"),
        SchemaField("team_badge", "STRING", mode="NULLABLE"),
        SchemaField("team_country", "STRING", mode="NULLABLE"),
        SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "standings": [
        SchemaField("standing_id", "STRING", mode="NULLABLE"),
        SchemaField("position", "STRING", mode="NULLABLE"),
        SchemaField("team_id", "STRING", mode="NULLABLE"),
        SchemaField("team_name", "STRING", mode="NULLABLE"),
        SchemaField("team_badge", "STRING", mode="NULLABLE"),
        SchemaField("played_games", "STRING", mode="NULLABLE"),
        SchemaField("won", "STRING", mode="NULLABLE"),
        SchemaField("drawn", "STRING", mode="NULLABLE"),
        SchemaField("lost", "STRING", mode="NULLABLE"),
        SchemaField("goals_for", "STRING", mode="NULLABLE"),
        SchemaField("goals_against", "STRING", mode="NULLABLE"),
        SchemaField("goal_difference", "STRING", mode="NULLABLE"),
        SchemaField("points", "STRING", mode="NULLABLE"),
        SchemaField("recent_form", "STRING", mode="NULLABLE"),
        SchemaField("group_name", "STRING", mode="NULLABLE"),
        SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "matches": [
        SchemaField("match_id", "STRING", mode="NULLABLE"),
        SchemaField("match_name", "STRING", mode="NULLABLE"),
        SchemaField("match_date", "STRING", mode="NULLABLE"),
        SchemaField("match_utc_timestamp", "STRING", mode="NULLABLE"),
        SchemaField("match_time_utc", "STRING", mode="NULLABLE"),
        SchemaField("match_status", "STRING", mode="NULLABLE"),
        SchemaField("venue_name", "STRING", mode="NULLABLE"),
        SchemaField("venue_country", "STRING", mode="NULLABLE"),
        SchemaField("match_round", "STRING", mode="NULLABLE"),
        SchemaField("home_team_id", "STRING", mode="NULLABLE"),
        SchemaField("home_team_name", "STRING", mode="NULLABLE"),
        SchemaField("home_score", "STRING", mode="NULLABLE"),
        SchemaField("away_team_id", "STRING", mode="NULLABLE"),
        SchemaField("away_team_name", "STRING", mode="NULLABLE"),
        SchemaField("away_score", "STRING", mode="NULLABLE"),
        SchemaField("home_team_badge", "STRING", mode="NULLABLE"),
        SchemaField("away_team_badge", "STRING", mode="NULLABLE"),
        SchemaField("match_thumbnail", "STRING", mode="NULLABLE"),
        SchemaField("match_poster", "STRING", mode="NULLABLE"),
        SchemaField("match_video_url", "STRING", mode="NULLABLE"),
        SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
    ],
}

# Gold layer — business‑ready, calculated metrics
GOLD_SCHEMAS = {
    "teams": [
        SchemaField("team_id", "STRING", mode="NULLABLE"),
        SchemaField("team_name", "STRING", mode="NULLABLE"),
        SchemaField("team_short_name", "STRING", mode="NULLABLE"),
        SchemaField("team_founded", "STRING", mode="NULLABLE"),
        SchemaField("team_country", "STRING", mode="NULLABLE"),
        SchemaField("team_stadium", "STRING", mode="NULLABLE"),
        SchemaField("team_location", "STRING", mode="NULLABLE"),
        SchemaField("team_badge", "STRING", mode="NULLABLE"),
        SchemaField("team_logo", "STRING", mode="NULLABLE"),
        SchemaField("gold_loaded_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "standings": [
        SchemaField("standing_id", "STRING", mode="NULLABLE"),
        SchemaField("position", "STRING", mode="NULLABLE"),
        SchemaField("team_id", "STRING", mode="NULLABLE"),
        SchemaField("team_name", "STRING", mode="NULLABLE"),
        SchemaField("team_badge", "STRING", mode="NULLABLE"),
        SchemaField("group_name", "STRING", mode="NULLABLE"),
        SchemaField("played_games", "STRING", mode="NULLABLE"),
        SchemaField("won", "STRING", mode="NULLABLE"),
        SchemaField("drawn", "STRING", mode="NULLABLE"),
        SchemaField("lost", "STRING", mode="NULLABLE"),
        SchemaField("goals_for", "STRING", mode="NULLABLE"),
        SchemaField("goals_against", "STRING", mode="NULLABLE"),
        SchemaField("goal_difference", "STRING", mode="NULLABLE"),
        SchemaField("points", "STRING", mode="NULLABLE"),
        SchemaField("recent_form", "STRING", mode="NULLABLE"),
        SchemaField("points_per_game", "FLOAT", mode="NULLABLE"),
        SchemaField("win_percentage", "FLOAT", mode="NULLABLE"),
        SchemaField("goals_scored_per_game", "FLOAT", mode="NULLABLE"),
        SchemaField("goals_conceded_per_game", "FLOAT", mode="NULLABLE"),
        SchemaField("running_total_points", "FLOAT", mode="NULLABLE"),
        SchemaField("gold_loaded_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "matches": [
        SchemaField("match_id", "STRING", mode="NULLABLE"),
        SchemaField("match_date", "STRING", mode="NULLABLE"),
        SchemaField("match_time_utc", "STRING", mode="NULLABLE"),
        SchemaField("match_status", "STRING", mode="NULLABLE"),
        SchemaField("venue_name", "STRING", mode="NULLABLE"),
        SchemaField("venue_country", "STRING", mode="NULLABLE"),
        SchemaField("match_round", "STRING", mode="NULLABLE"),
        SchemaField("home_team_id", "STRING", mode="NULLABLE"),
        SchemaField("home_team_name", "STRING", mode="NULLABLE"),
        SchemaField("home_score", "STRING", mode="NULLABLE"),
        SchemaField("away_team_id", "STRING", mode="NULLABLE"),
        SchemaField("away_team_name", "STRING", mode="NULLABLE"),
        SchemaField("away_score", "STRING", mode="NULLABLE"),
        SchemaField("home_team_badge", "STRING", mode="NULLABLE"),
        SchemaField("away_team_badge", "STRING", mode="NULLABLE"),
        SchemaField("result", "STRING", mode="NULLABLE"),
        SchemaField("total_goals", "FLOAT", mode="NULLABLE"),
        SchemaField("home_goal_diff", "FLOAT", mode="NULLABLE"),
        SchemaField("away_goal_diff", "FLOAT", mode="NULLABLE"),
        SchemaField("gold_loaded_at", "TIMESTAMP", mode="NULLABLE"),
    ],
}

# --------------------------
# CONVERT SCHEMAS
# --------------------------
# Turns SchemaField objects into JSON‑safe dictionaries
SILVER_SCHEMA_DICTS = {
    name: [field.to_api_repr() for field in schema_list]
    for name, schema_list in SILVER_SCHEMAS.items()
}

GOLD_SCHEMA_DICTS = {
    name: [field.to_api_repr() for field in schema_list]
    for name, schema_list in GOLD_SCHEMAS.items()
}

# Default behaviour for all tasks
default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1)
}


# --------------------------
# API FETCH FUNCTION
# --------------------------
async def fetch_json(url):
    """Fetch JSON from API with retries and backoff"""
    for attempt in range(RETRIES):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if any(data.values()):
                            return data
                    elif response.status == 429:
                        wait_time = DELAY * (2 ** attempt)
                        logging.warning(f"Rate limited — wait {wait_time}s")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.warning(
                            f"HTTP {response.status} — retry {attempt+1}")
                        await asyncio.sleep(DELAY)
        except Exception as error:
            wait_time = DELAY * (2 ** attempt)
            logging.error(f"Error: {error} — retry {attempt+1}")
            await asyncio.sleep(wait_time)
    return None


def extract_and_upload(**context):
    """Fetch all datasets and save as NDJSON to GCS"""
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    gcs = GCSHook()

    async def run_all():
        for name, filename in FILES.items():
            endpoint = {
                "teams": f"{BASE_URL}/lookup_all_teams.php?id={LEAGUE_ID}",
                "standings": f"{BASE_URL}/lookuptable.php?l={LEAGUE_ID}&s={SEASON}",
                "matches": f"{BASE_URL}/eventsseason.php?id={LEAGUE_ID}&s={SEASON}"
            }[name]

            data = await fetch_json(endpoint)
            if data:
                rows = data if isinstance(data, list) else [data]
                ndjson_payload = "\n".join(json.dumps(
                    row, ensure_ascii=False) for row in rows)
                gcs.upload(
                    bucket_name=BUCKET,
                    object_name=f"{PATHS['raw']}/{filename}",
                    data=ndjson_payload,
                    mime_type="application/x-ndjson"
                )
                logging.info(f"Uploaded raw file: {filename}")
            await asyncio.sleep(DELAY)

    asyncio.run(run_all())


# --------------------------
# DATA VALIDATION FUNCTION
# --------------------------
def validate_silver(**context):
    """Check Silver layer Parquet files exist and have correct columns"""
    import pandas as pd
    import pyarrow.parquet as pq
    from io import BytesIO
    gcs = GCSHook()
    errors = []

    for entity in ["teams", "standings", "matches"]:
        folder_prefix = f"{PATHS['silver']}/{entity}/"
        all_files = gcs.list(BUCKET, prefix=folder_prefix)
        parquet_files = [
            path for path in all_files if path.endswith(".parquet")]

        if not parquet_files:
            errors.append(f"{entity}: no Parquet files found")
            continue

        file_bytes = gcs.download(BUCKET, parquet_files[0])
        table = pq.read_table(BytesIO(file_bytes))
        df = table.to_pandas()
        row_count = len(df)

        if row_count == 0:
            errors.append(f"{entity}: zero rows")

        required_columns = {
            "teams": ["team_id", "team_name"],
            "standings": ["standing_id", "team_id", "points"],
            "matches": ["match_id", "home_team_id", "away_team_id"]
        }[entity]

        for col in required_columns:
            if col not in df.columns:
                errors.append(f"{entity}: missing column {col}")

        logging.info(
            f"Silver {entity}: {row_count} rows, all required columns present")

    if errors:
        raise ValueError(f"Validation failed: {errors}")


# --------------------------
# DAG DEFINITION
# --------------------------
with DAG(
    dag_id="worldcup_elt_production",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["worldcup", "production", "elt", "spark", "serverless"]
) as dag:

    # Step 1: Fetch API data and save raw NDJSON
    extract_raw = PythonOperator(
        task_id="extract_and_upload_raw",
        python_callable=extract_and_upload
    )

    # Step 2: Wait until all 3 raw files exist in GCS
    sensors = [
        GCSObjectExistenceSensor(
            task_id=f"wait_{name}",
            bucket=BUCKET,
            object=f"raw/{filename}",
            poke_interval=15,
            timeout=300,
            mode="reschedule"
        ) for name, filename in FILES.items()
    ]

    # Step 3: Bronze → Silver transformation (Serverless Spark)
    bronze_to_silver = DataprocCreateBatchOperator(
        task_id="bronze_to_silver",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SCRIPTS["bronze_silver"]
            },
            "environment_config": {
                "execution_config": {}
            }
        },
        region=REGION,
        project_id=PROJECT_ID
    )

    # Step 4: Validate Silver layer
    validate_silver_task = PythonOperator(
        task_id="validate_silver_data",
        python_callable=validate_silver
    )

    # Step 5: Load Silver to BigQuery - uses converted schema dicts
    load_silver = [
        GCSToBigQueryOperator(
            task_id=f"load_silver_{entity}",
            bucket=BUCKET,
            source_objects=[f"{PATHS['silver']}/{entity}/*"],
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.silver_{entity}",
            source_format="PARQUET",
            schema_fields=SILVER_SCHEMA_DICTS[entity],
            write_disposition="WRITE_OVERWRITE",
            external_table=True
        ) for entity in ["teams", "standings", "matches"]
    ]

    # Step 6: Silver → Gold transformation (Serverless Spark)
    silver_to_gold = DataprocCreateBatchOperator(
        task_id="silver_to_gold",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SCRIPTS["silver_gold"]
            },
            "environment_config": {
                "execution_config": {}
            }
        },
        region=REGION,
        project_id=PROJECT_ID
    )

    # Step 7: Load Gold to BigQuery - uses converted schema dicts
    load_gold = [
        GCSToBigQueryOperator(
            task_id=f"load_gold_{entity}",
            bucket=BUCKET,
            source_objects=[f"{PATHS['gold']}/{entity}/*"],
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.gold_{entity}",
            source_format="PARQUET",
            schema_fields=GOLD_SCHEMA_DICTS[entity],
            write_disposition="WRITE_OVERWRITE",
            external_table=True
        ) for entity in ["teams", "standings", "matches"]
    ]

    # --------------------------
    # WORKFLOW ORDER
    # --------------------------
    extract_raw >> sensors >> bronze_to_silver >> validate_silver_task >> load_silver >> silver_to_gold >> load_gold
