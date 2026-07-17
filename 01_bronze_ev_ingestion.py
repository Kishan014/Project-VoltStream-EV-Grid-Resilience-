# Databricks notebook source
# MAGIC %md
# MAGIC # Project VoltStream — Phase 1: Bronze Ingestion
# MAGIC Ingests EV charging station data from the Open Charge Map API into a Delta bronze table.
# MAGIC
# MAGIC **Covers:**
# MAGIC - Task 1: Environment setup (secrets, not hardcoding keys)
# MAGIC - Task 2: Modular ingestion function with pagination + error handling
# MAGIC - Task 3: Bronze landing with audit columns, append mode, single-file writes
# MAGIC - Senior challenge: checkpointing via `modifiedsince`, secret-safe logging

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Environment Setup — Widgets & Secrets

# COMMAND ----------

dbutils.widgets.text("country_code", "US", "Country Code (e.g. US, GB)")
dbutils.widgets.text("latitude", "", "Latitude (optional, overrides country)")
dbutils.widgets.text("longitude", "", "Longitude (optional, overrides country)")
dbutils.widgets.text("distance_km", "50", "Search radius in km (used with lat/long)")
dbutils.widgets.text("max_records", "500", "Max records to fetch this run")
dbutils.widgets.dropdown("force_full_refresh", "false", ["true", "false"],
                          "Ignore checkpoint and fetch full history")

country_code = dbutils.widgets.get("country_code").strip()
latitude = dbutils.widgets.get("latitude").strip()
longitude = dbutils.widgets.get("longitude").strip()
distance_km = dbutils.widgets.get("distance_km").strip()
max_records = int(dbutils.widgets.get("max_records"))
force_full_refresh = dbutils.widgets.get("force_full_refresh") == "true"

# Build a stable region key for logging / checkpointing, regardless of which
# search mode (country vs lat/long) is used.
if latitude and longitude:
    region_key = f"latlong_{latitude}_{longitude}_{distance_km}km"
else:
    region_key = f"country_{country_code}"

print(f"Region key for this run: {region_key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secret Management
# MAGIC The API key is **never** hardcoded or printed. We pull it from Databricks Secrets.
# MAGIC
# MAGIC One-time setup (Databricks CLI, run locally — not in this notebook):
# MAGIC ```bash
# MAGIC databricks secrets create-scope voltstream
# MAGIC databricks secrets put-secret voltstream ocm_api_key
# MAGIC ```
# MAGIC Free Edition note: if secret scopes aren't available yet in your workspace,
# MAGIC the fallback widget below lets you paste a key for local testing only —
# MAGIC but it still won't be echoed anywhere in outputs.

# COMMAND ----------

def get_api_key() -> str:
    """
    Resolve the Open Charge Map API key without ever printing or logging it.
    Tries Databricks Secrets first, falls back to a masked-input widget.
    """
    try:
        key = dbutils.secrets.get(scope="voltstream", key="ocm_api_key")
        source = "Databricks Secrets"
    except Exception:
        dbutils.widgets.text("ocm_api_key_fallback", "", "OCM API Key (fallback only)")
        key = dbutils.widgets.get("ocm_api_key_fallback").strip()
        source = "fallback widget"

    if not key:
        raise ValueError(
            "No API key found. Set it via Databricks Secrets (scope='voltstream', "
            "key='ocm_api_key') or the fallback widget."
        )

    # Proof we aren't leaking the secret: log length + source only, never the value.
    print(f"API key loaded from {source}. Length: {len(key)} chars. Value is not printed.")
    return key


api_key = get_api_key()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 (setup) — Checkpoint Table
# MAGIC Tracks the last successful fetch time per region so re-runs only pull
# MAGIC records modified since then (`modifiedsince` param).

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze_ev_checkpoint (
        region_key STRING,
        last_run_at TIMESTAMP,
        last_record_count INT
    ) USING DELTA
""")


def get_last_checkpoint(region_key: str):
    """Return the last_run_at timestamp (as ISO string) for a region, or None."""
    if force_full_refresh:
        print("force_full_refresh=true — ignoring any existing checkpoint.")
        return None

    checkpoint_df = spark.table("bronze_ev_checkpoint").filter(f"region_key = '{region_key}'")
    row = (
        checkpoint_df
        .orderBy(checkpoint_df.last_run_at.desc())
        .limit(1)
        .collect()
    )
    if row:
        ts = row[0]["last_run_at"]
        print(f"Found checkpoint for '{region_key}': last run at {ts}")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"No checkpoint found for '{region_key}' — this will be a full fetch.")
    return None


def update_checkpoint(region_key: str, record_count: int):
    from datetime import datetime
    spark.sql(f"""
        INSERT INTO bronze_ev_checkpoint VALUES (
            '{region_key}', current_timestamp(), {record_count}
        )
    """)
    print(f"Checkpoint updated for '{region_key}': {record_count} records at this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: The Ingestion Function
# MAGIC Handles pagination via `offset`/`maxresults`, retries transient errors,
# MAGIC and stops at `max_records` or when the API returns an empty page.

# COMMAND ----------

import requests
import time

OCM_BASE_URL = "https://api.openchargemap.io/v3/poi/"


def fetch_ev_data(api_key: str, params: dict, max_records: int = 500,
                   page_size: int = 100, max_retries: int = 3) -> list:
    """
    Fetch EV charging station records from the Open Charge Map API,
    paginating with offset/maxresults until max_records is reached
    or the API returns no more data.

    Args:
        api_key: Open Charge Map API key.
        params: dict of search params, e.g. {"countrycode": "US"} or
                {"latitude": 40.7, "longitude": -74.0, "distance": 50}.
                May also include "modifiedsince" for incremental pulls.
        max_records: hard cap on total records fetched this run.
        page_size: records per API call (<=100 recommended by OCM).
        max_retries: retry attempts per page on timeout/transient failure.

    Returns:
        List of raw JSON station records (dicts).
    """
    all_records = []
    offset = 0

    while len(all_records) < max_records:
        remaining = max_records - len(all_records)
        this_page_size = min(page_size, remaining)

        query = {
            **params,
            "output": "json",
            "maxresults": this_page_size,
            "offset": offset,
            "compact": "true",
            "verbose": "false",
        }
        headers = {"X-API-Key": api_key}

        page_records = None
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    OCM_BASE_URL, params=query, headers=headers, timeout=15
                )
                if response.status_code == 401:
                    raise PermissionError(
                        "Open Charge Map rejected the API key (401 Unauthorized)."
                    )
                response.raise_for_status()
                page_records = response.json()
                break
            except requests.exceptions.Timeout:
                print(f"Timeout on offset={offset}, attempt {attempt}/{max_retries}. Retrying...")
                time.sleep(2 * attempt)
            except requests.exceptions.RequestException as e:
                print(f"Request error on offset={offset}, attempt {attempt}/{max_retries}: {e}")
                time.sleep(2 * attempt)

        if page_records is None:
            print(f"Giving up on offset={offset} after {max_retries} attempts. "
                  f"Returning {len(all_records)} records fetched so far.")
            break

        if not page_records:
            print(f"Empty page at offset={offset} — no more records available.")
            break

        all_records.extend(page_records)
        print(f"Fetched {len(page_records)} records (offset={offset}). "
              f"Running total: {len(all_records)}")

        if len(page_records) < this_page_size:
            # Short page = we've reached the end of available results.
            break

        offset += this_page_size

    return all_records[:max_records]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Fetch

# COMMAND ----------

search_params = {}
if latitude and longitude:
    search_params["latitude"] = float(latitude)
    search_params["longitude"] = float(longitude)
    search_params["distance"] = float(distance_km)
    search_params["distanceunit"] = "km"
else:
    search_params["countrycode"] = country_code

last_checkpoint = get_last_checkpoint(region_key)
if last_checkpoint:
    search_params["modifiedsince"] = last_checkpoint

print(f"Search params (key excluded): {search_params}")

raw_records = fetch_ev_data(
    api_key=api_key,
    params=search_params,
    max_records=max_records,
)

print(f"Total records fetched this run: {len(raw_records)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Bronze Landing

# COMMAND ----------

import json
from pyspark.sql import functions as F

if not raw_records:
    print("No records fetched — skipping write. Checkpoint will not be advanced.")
else:
    # Land as raw JSON strings first (schema-flexible), then wrap in a DataFrame.
    # This keeps Bronze resilient to the API adding new fields tomorrow —
    # nothing here assumes a fixed schema.
    json_strings = [json.dumps(r) for r in raw_records]
    raw_df = spark.createDataFrame(json_strings, "string").withColumnRenamed("value", "raw_json")

    bronze_df = (
        raw_df
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_endpoint", F.lit(OCM_BASE_URL))
        .withColumn("region_key", F.lit(region_key))
    )

    # Requirement: avoid creating many small files for this small-scale exercise.
    (
        bronze_df
        .coalesce(1)
        .write
        .mode("append")
        .format("delta")
        .saveAsTable("bronze_ev_stations")
    )

    print(f"Wrote {bronze_df.count()} records to bronze_ev_stations.")
    update_checkpoint(region_key, len(raw_records))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity Checks

# COMMAND ----------

display(spark.table("bronze_ev_stations").orderBy(F.col("ingested_at").desc()).limit(5))

# COMMAND ----------

display(spark.table("bronze_ev_checkpoint").orderBy(F.col("last_run_at").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for Task 1 (Repo Sync)
# MAGIC This file is plain Python with Databricks cell markers (`# COMMAND ----------`),
# MAGIC so it round-trips cleanly with Git:
# MAGIC 1. In Databricks: **Workspace settings → Linked accounts → Git integration** (or repo-level "Git folder").
# MAGIC 2. Clone this repo into a Databricks Repo, or push this notebook's `.py` export into
# MAGIC    your GitHub repo — Databricks Repos sync bidirectionally with a linked remote.
# MAGIC 3. Keep `bronze_ev_checkpoint` and the API key out of version control; only the
# MAGIC    notebook logic should live in Git.
