
---

# World Cup Football Intelligence : End-to-End Production ELT Pipeline
Cloud-native, serverless, fully automated data pipeline that transforms raw sports API data into analysis-ready datasets.

---

## Project Purpose
To build a reliable, maintainable, and scalable data pipeline that:
- Extracts official World Cup football data from TheSportsDB API
- Cleans, enriches, and structures data using industry-standard layered architecture
- Runs on fully managed services with zero infrastructure management
- Delivers queryable datasets ready for analysis, dashboards, or machine learning

---

## Full Architecture Diagram
```
┌─────────────────────┐     ┌─────────────────────────────┐     ┌─────────────────────────┐
│   TheSportsDB API   │────▶│  Cloud Composer (Airflow)   │────▶│ Google Cloud Storage    │
│  (Teams/Matches/    │     │  • Orchestration            │     │  • Raw NDJSON           │
│   Standings)        │     │  • DAG Execution            │     │  • Bronze Layer         │
└─────────────────────┘     └───────────────┬─────────────┘     └────────────┬───────────┘
                                            │                                │
                                            ▼                                ▼
                          ┌─────────────────────────────┐     ┌─────────────────────────┐
                          │  Dataproc Serverless Spark │────▶│ Google Cloud Storage    │
                          │  • Bronze → Silver         │     │  • Silver Parquet       │
                          │  • Silver → Gold           │     │  • Gold Parquet         │
                          └───────────────┬─────────────┘     └────────────┬───────────┘
                                          │                                │
                                          ▼                                ▼
                          ┌─────────────────────────────────────────────────────────────┐
                          │                     Google BigQuery                         │
                          │  • Bronze Tables (Raw)  • Silver Tables (Clean)  • Gold Tables │
                          │  • External Tables pointing directly to GCS files            │
                          └─────────────────────────────────────────────────────────────┘
```

---

## Complete Tech Stack
| Category | Tools & Services | Details |
|---|---|---|
| **Cloud Platform** | Google Cloud Platform | All services fully managed |
| **Orchestration** | Cloud Composer 2 (Apache Airflow 2.11) | DAG scheduling, task dependency, error handling |
| **Compute** | Dataproc Serverless for Spark | No clusters to manage, auto-scaling, pay-per-use |
| **Storage** | Cloud Storage (GCS) | Multi-layer storage: Raw → Bronze → Silver → Gold |
| **Query Layer** | BigQuery | Serverless data warehouse, external tables |
| **Ingestion** | Python 3.11 plus aiohttp | Async HTTP requests, retry logic, rate limit handling |
| **Processing** | PySpark 3.5 | Distributed data transformation, schema management |
| **Data Format** | NDJSON (Raw) plus Parquet plus Snappy | Efficient storage and performance |
| **Security** | IAM Service Accounts | Least-privilege permissions, no hardcoded credentials |

---

## Final Project Structure
```
worldcup-football-elt/
├── README.md                  # This documentation
├── .gitignore                 # Ignore sensitive/local files
├── requirements.txt            # Python dependencies
├── dag/
│   └── worldcup_elt_production.py   # Main Airflow DAG
├── spark_scripts/
│   ├── bronze_to_silver.py    # Flatten and clean raw data
│   └── silver_to_gold.py      # Enrich and aggregate final metrics
└── assets/
    └── architecture.png       # Optional: diagram image
```

---

## Full Pipeline Workflow : Step-by-Step
### Phase 1 : Extraction and Raw Storage
1. Airflow triggers `extract_and_upload_raw` task
2. Async function fetches 3 datasets from TheSportsDB API:
   - All teams in the competition
   - League standings and tables
   - All match results and fixtures
3. Built-in retry logic: exponential backoff for failures and rate limits
4. Convert responses to Newline Delimited JSON (NDJSON)
5. Upload directly to GCS bucket: `gs://worldcup-football-bucket/raw/`

### Phase 2 : Validation and Transformation
6. GCS Sensors confirm all 3 files exist before proceeding
7. Bronze to Silver (Serverless Spark):
   - Read raw NDJSON
   - Explode nested arrays into flat rows
   - Rename fields for consistency
   - Cast data types (dates, numbers, IDs)
   - Add `ingested_at` timestamp
   - Write compressed Parquet to `gs://.../silver/`
8. Data Quality Check:
   - Verify Parquet files exist
   - Check row count greater than 0
   - Validate all required columns are present
9. Silver to Gold (Serverless Spark):
   - Join and enrich datasets where needed
   - Calculate derived metrics:
     - Match results, total goals, goal difference
     - Points per game, win percentage
     - Running totals, group rankings
   - Final standardised schema
   - Write to `gs://.../gold/`

### Phase 3 : Load and Availability
10. Load ALL layers to BigQuery:
    - Bronze: Raw JSON → `bronze_teams/standings/matches`
    - Silver: Clean Parquet → `silver_teams/standings/matches`
    - Gold: Curated Parquet → `gold_teams/standings/matches`
11. All tables are external tables — no duplicate storage, always reflects latest files

---

## Key Technical Design Decisions and Rationale
| Choice | Reason |
|---|---|
| **Dataproc Serverless** | Avoided cluster provisioning delays and capacity errors; zero maintenance; auto-scales; stops when job finishes |
| **Parquet plus Snappy** | Columnar format equals faster queries; 3 to 5 times smaller than JSON; schema evolution support |
| **External Tables** | No ETL duplication; updates in GCS appear instantly in BigQuery; lower storage cost |
| **Multi-Layer Architecture** | Clear separation of concerns: <br>• Bronze equals immutable source of truth <br>• Silver equals cleansed data <br>• Gold equals business-ready analytics <br>• Easy to debug and re-run individual stages |
| **Async Extraction** | Faster API fetching; better handling of rate limits |
| **Idempotent Design** | All tasks use `mode="overwrite"` — safe to re-run at any stage without side effects |
| **Airflow Sensors** | Ensure upstream files are fully written before processing; prevents partial data errors |

---

## Core Production Code Snippets

### 1. Resilient Async API Extraction
```python
async def fetch_json(url):
    for attempt in range(RETRIES):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if any(data.values()):
                            logging.info(f"Successfully fetched: {url}")
                            return data
                    elif response.status == 429:
                        wait_time = DELAY * (2 ** attempt)
                        logging.warning(f"Rate limited! Waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.warning(f"HTTP {response.status} — attempt {attempt+1}/{RETRIES}")
                        await asyncio.sleep(DELAY)
        except Exception as e:
            wait_time = DELAY * (2 ** attempt)
            logging.error(f"Error: {str(e)} — retry {attempt+1} in {wait_time}s")
            await asyncio.sleep(wait_time)
    logging.error(f"FAILED after {RETRIES} attempts: {url}")
    return None
```

### 2. Serverless Spark Job Submission
```python
bronze_to_silver = DataprocCreateBatchOperator(
    task_id="bronze_to_silver",
    batch={
        "pyspark_batch": {
            "main_python_file_uri": SCRIPTS["bronze_silver"],
            "args": [
                "--project", PROJECT_ID,
                "--bucket", BUCKET,
                "--region", REGION
            ]
        }
    },
    region=REGION,
    project_id=PROJECT_ID,
    retry_on_failure=True
)
```

### 3. Spark Data Quality and Write Pattern
```python
def write_layer(df, output_path, layer_name):
    section(f"Writing {layer_name} data → {output_path}")
    if df is None or df.count() == 0:
        raise ValueError(f"No data to write for {layer_name}!")
    
    df.withColumn("processed_at", current_timestamp()) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .option("parquet.enable.dictionary", "true") \
      .parquet(output_path)
    logging.info(f"Successfully wrote {layer_name} data")
```

### 4. GCS to BigQuery External Table
```python
GCSToBigQueryOperator(
    task_id="load_gold_matches",
    bucket=BUCKET,
    source_objects=["gold/matches/*"],
    destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.gold_matches",
    source_format="PARQUET",
    autodetect=True,
    write_disposition="WRITE_OVERWRITE",
    external_table=True,
    location=REGION
)
```

---

## Challenges Overcome and Solutions
| Issue | Root Cause | Fix Applied |
|---|---|---|
| Dataproc cluster creation failed | Zonal capacity limits on free tier | Switched to Dataproc Serverless — no clusters needed |
| `ModuleNotFoundError: No module named 'scripts'` | Custom modules not available in isolated environments | Removed external imports — all scripts self-contained |
| `404 Parquet not found` | Spark writes folders with part files, not single files | Updated validation to list all `.parquet` paths in folder |
| `'str' object has no attribute 'name'` | Misunderstood `GCSHook.list()` return type | Corrected logic — it returns strings, not objects |
| `Dataset not found` | Name or location mismatch between DAG and BigQuery | Standardised config — verified dataset name and region match |
| `gcloud storage --force error` | Wrong flag name | Removed flag — overwrite is default behaviour |

---

## Production Readiness Features
- Automatic retries with exponential backoff
- Timeout protection on all tasks
- Data validation at every stage
- Structured logging for debugging
- Idempotent operations — safe to re-run
- No hardcoded secrets — all config centralised
- Cost-efficient — all services scale to zero when idle

---

## Example BigQuery Analysis Queries
```sql
-- Top 5 teams by points
SELECT team_name, points, goal_difference
FROM `worldcup-football-project.worldcup_football.gold_standings`
ORDER BY points DESC, goal_difference DESC
LIMIT 5;

-- Total goals scored per match
SELECT match_name, home_team_name, away_team_name, home_score + away_score AS total_goals
FROM `worldcup-football-project.worldcup_football.gold_matches`
ORDER BY total_goals DESC;
```

---

## Deployment Instructions
1. Create GCP project and enable required APIs
2. Create service account with proper permissions
3. Create Cloud Composer environment
4. Create BigQuery dataset: `bq mk --location=us-central1 worldcup-football-project:worldcup_football`
5. Upload scripts to `gs://<bucket>/scripts/`
6. Upload DAG to Composer
7. Trigger run manually or set schedule

---

## Key Skills Demonstrated
- Cloud infrastructure design and implementation
- Python and async programming
- PySpark distributed data processing
- Airflow DAG development and orchestration
- Data warehousing and layered architecture
- BigQuery and external table management
- Troubleshooting production pipelines

---

