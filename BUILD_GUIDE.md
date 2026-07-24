---

# COMPLETE STEP-BY-STEP BUILD GUIDE : CONNECTING ALL THE DOTS
This guide walks through every decision, resource, and code step from initial setup to final success, linking each component clearly.

---

## OVERALL APPROACH
We built a Cloud-Native Serverless ELT Pipeline following Medallion Architecture, designed for reliability, maintainability, and zero infrastructure management.

---

## PHASE 1 : PLANNING AND RESOURCE PREPARATION

### 1. Define Requirements and Architecture
- **Goal** : Automate fetching World Cup data, clean, enrich, and make available for analysis
- **Pattern** : Medallion (Bronze to Silver to Gold) plus Serverless ELT
- **Platform** : Google Cloud Platform using fully managed services
- **Services Selected** :

| Resource | Purpose | Why Chosen |
|---|---|---|
| **Cloud Storage (GCS)** | Multi-layer storage | Durable, low cost, integrates seamlessly with all GCP services |
| **Cloud Composer** | Orchestration | Managed Airflow, handles scheduling, dependencies, and retries |
| **Dataproc Serverless** | Spark transformations | No clusters to manage, avoids capacity limits, scales automatically |
| **BigQuery** | Analytics and querying | Serverless data warehouse, supports external tables directly from GCS |
| **TheSportsDB API** | Data source | Free, structured football data covering teams, standings, and matches |

### 2. Project Setup and Prerequisites
- Created GCP project : `worldcup-football-project`
- **Enabled Required APIs** :
  ```
  Cloud Composer API, Dataproc API, Cloud Storage API, BigQuery API,
  Cloud IAM API, Cloud Resource Manager API, Cloud Logging API
  ```
- **Created Service Account** : `cloud-composer@worldcup-football-project.iam.gserviceaccount.com`
  - Assigned Roles : `Storage Admin`, `BigQuery Data Editor`, `Dataproc Editor`, `Composer Worker`
- **Created Resources** :
  - Bucket : `worldcup-football-bucket`
  - BigQuery Dataset : `worldcup_football` (location : `us-central1`)
  - Composer Environment : `worldcup-football-composer` (region : `us-central1`)

---

## PHASE 2 : BUILDING EACH COMPONENT

### Step 1 : Write Extraction Logic
**Goal** : Pull data from API and save raw files to GCS
- **Technology** : Python plus `aiohttp` for efficient async requests
- **Key Code** :
  ```python
  async def fetch_json(url):
      for attempt in range(RETRIES):
          try:
              async with aiohttp.ClientSession() as s:
                  async with s.get(url) as r:
                      if r.status == 200:
                          return await r.json()
                      elif r.status == 429:
                          await asyncio.sleep(DELAY * (2 ** attempt))
          except Exception as e:
              logging.error(f"Retry {attempt+1}: {e}")
  ```
- **How it connects** : Called by Airflow, uses `GCSHook` to upload files directly to `gs://bucket/raw/`
- **Result** : Three files created : `worldcup_teams.ndjson`, `worldcup_standings.ndjson`, `worldcup_matches.ndjson`

---

### Step 2 : Build Spark Transformation Scripts
**Goal** : Convert raw NDJSON to structured Parquet layers

#### Bronze to Silver (`bronze_to_silver.py`)
- **What it does** :
  - Reads raw NDJSON from GCS
  - Explodes nested arrays into flat rows
  - Renames fields consistently and corrects data types
  - Adds timestamp for tracking
  - Writes compressed Parquet to `gs://bucket/silver/entity/`
- **Key Code** :
  ```python
  df.select(explode("teams").alias("team")) \
    .select(col("team.idTeam").alias("team_id"), ...) \
    .write.mode("overwrite").option("compression", "snappy").parquet(output_path)
  ```
- **Connection** : Runs on Dataproc Serverless, reads and writes directly to GCS without moving data between systems

#### Silver to Gold (`silver_to_gold.py`)
- **What it does** :
  - Reads cleaned Silver Parquet files
  - Calculates derived metrics such as match results, total goals, and win percentage
  - Joins and enriches datasets where needed
  - Applies final standardised schema
  - Writes output to `gs://bucket/gold/entity/`
- **Key Code** :
  ```python
  when(col("home_score") > col("away_score"), "Home Win")
  .when(col("home_score") < col("away_score"), "Away Win")
  .otherwise("Draw").alias("result")
  ```
- **Connection** : Same Serverless execution model, fully isolated from other workloads

---

### Step 3 : Build Validation Logic
**Goal** : Ensure data quality before moving to downstream steps
- **What it does** :
  - Lists all Parquet files in each Silver folder
  - Reads sample data from the latest files
  - Verifies row count is greater than zero
  - Checks all required columns are present
- **Key Fix** : Corrected handling of `GCSHook.list()` which returns file paths as strings rather than objects
- **Connection** : Runs in the Airflow worker to block bad data from progressing

---

### Step 4 : Orchestrate Everything with Airflow DAG
**Goal** : Define workflow order, dependencies, and execution rules
- **Key Components** :
  1. **Extract Task** : Runs async API fetch and upload
  2. **GCS Sensors** : Confirm all three raw files exist before proceeding
  3. **Serverless Spark Tasks** : Submit transformation scripts to Dataproc
  4. **Validation Task** : Check quality of Silver layer output
  5. **Load Tasks** : Create external tables in BigQuery
- **Key Operator** : `DataprocCreateBatchOperator` which does not require pre-existing clusters
  ```python
  DataprocCreateBatchOperator(
      task_id="bronze_to_silver",
      batch={"pyspark_batch": {"main_python_file_uri": "gs://bucket/scripts/bronze_to_silver.py"}},
      region=REGION,
      project_id=PROJECT_ID
  )
  ```
- **Connection** : Ties all steps together, manages retries, and centralises logging

---

### Step 5 : Load to BigQuery
**Goal** : Make all layers available for querying
- **What it does** :
  - Bronze : Load NDJSON files into `bronze_*` tables
  - Silver and Gold : Load Parquet files into `silver_*` and `gold_*` tables
  - All tables are external, pointing directly to files in GCS
- **Key Code** :
  ```python
  GCSToBigQueryOperator(
      task_id="load_gold_teams",
      source_objects=["gold/teams/*"],
      destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.gold_teams",
      source_format="PARQUET",
      external_table=True
  )
  ```
- **Connection** : Final step that makes data ready for analysis and reporting

---

## HOW ALL PIECES CONNECT
```
TheSportsDB API → Airflow Extract → GCS Raw/Bronze
                                             ↓
BigQuery ← Load Operators ← GCS Silver/Gold
                                 ↑
                      Dataproc Serverless
```
- **Data flow** : API to GCS to Spark to GCS to BigQuery
- **Control flow** : Airflow triggers tasks, monitors status, and retries on failure
- **Consistency** : All components use the same project, bucket, region, and dataset configuration

---

## KEY CHALLENGES AND RESOLUTIONS

| Issue | Stage | Root Cause | Resolution |
|---|---|---|---|
| Cluster creation failed | Spark execution | Zonal capacity limits | Switched to Dataproc Serverless, removing cluster management entirely |
| Module not found error | Spark job start | Custom modules unavailable in batch runtime | Rewrote scripts to be fully self-contained |
| Parquet file not found | Validation | Spark writes partitioned folders instead of single files | Updated logic to list all Parquet files in target paths |
| Dataset not found error | BigQuery load | Mismatched name or location | Standardised all configuration values to match exactly |

---

## FINAL VERIFICATION
1. All required resources exist and are configured consistently
2. The full workflow runs without manual intervention
3. Data is stored correctly across all three layers in GCS
4. All nine tables are present and queryable in BigQuery
5. Architecture follows established best practices for maintainability and scalability

---
