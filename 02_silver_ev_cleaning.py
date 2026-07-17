# Databricks notebook source
# MAGIC %md
# MAGIC # Project VoltStream — Week 2: The Silver Layer & Data Quality
# MAGIC Transforms raw JSON from `bronze_ev_stations` into a clean, flat, deduplicated
# MAGIC Delta table ready for business-level aggregation.
# MAGIC
# MAGIC **Covers:**
# MAGIC - Task 1: Flatten AddressInfo, explode Connections (grain decision documented below)
# MAGIC - Task 2: Type casting, UTC timestamps, null lat/long handling
# MAGIC - Task 3: Upsert strategy (documented)
# MAGIC - Task 4: `Is_Operational` derived from real OCM reference data
# MAGIC - Senior: SCD Type 2 history table, data-quality quarantine table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)
from pyspark.sql.window import Window
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1a: Schema for the Raw JSON
# MAGIC Bronze stored each record as a raw JSON string on purpose (schema-flexible).
# MAGIC Here we assert a schema so we can extract typed columns.

# COMMAND ----------

connection_schema = StructType([
    StructField("ID", IntegerType()),
    StructField("ConnectionTypeID", IntegerType()),
    StructField("StatusTypeID", IntegerType()),
    StructField("LevelID", IntegerType()),
    StructField("PowerKW", DoubleType()),
    StructField("Quantity", IntegerType()),
])

address_info_schema = StructType([
    StructField("ID", IntegerType()),
    StructField("Title", StringType()),
    StructField("AddressLine1", StringType()),
    StructField("Town", StringType()),
    StructField("StateOrProvince", StringType()),
    StructField("Postcode", StringType()),
    StructField("CountryID", IntegerType()),
    StructField("Latitude", DoubleType()),
    StructField("Longitude", DoubleType()),
])

station_schema = StructType([
    StructField("ID", IntegerType()),
    StructField("UUID", StringType()),
    StructField("OperatorID", IntegerType()),
    StructField("UsageTypeID", IntegerType()),
    StructField("StatusTypeID", IntegerType()),
    StructField("AddressInfo", address_info_schema),
    StructField("Connections", ArrayType(connection_schema)),
    StructField("DateCreated", StringType()),
    StructField("DateLastStatusUpdate", StringType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 (built early, used later): Pull Real Status Reference Data
# MAGIC Rather than guessing which `StatusTypeID` means "operational," pull it from
# MAGIC OCM's own `/v3/referencedata` endpoint — each `StatusType` includes an
# MAGIC `IsOperational` boolean. Falls back to a small hardcoded table (verified
# MAGIC against OCM's public schema/docs) if the API call fails.

# COMMAND ----------

def get_status_type_lookup():
    try:
        resp = requests.get(
            "https://api.openchargemap.io/v3/referencedata",
            params={"output": "json"},
            timeout=15,
        )
        resp.raise_for_status()
        status_types = resp.json().get("StatusTypes", [])
        rows = [(s["ID"], s.get("IsOperational", False), s.get("Title", "Unknown")) for s in status_types]
        if rows:
            print(f"Loaded {len(rows)} status types from OCM reference data API.")
            return spark.createDataFrame(rows, ["status_type_id", "is_operational_ref", "status_title"])
    except Exception as e:
        print(f"Reference data fetch failed ({e}); using fallback lookup.")

    # Fallback: verified against OCM's public OpenAPI spec / community docs.
    fallback_rows = [
        (0, False, "Unknown"),
        (50, True, "Operational"),
        (75, True, "Partly Operational"),
        (100, False, "Not Operational"),
        (150, False, "Planned For Future Date"),
        (200, False, "Currently In Service (Temporarily Unavailable)"),
        (400, False, "Removed / Decommissioned"),
    ]
    return spark.createDataFrame(fallback_rows, ["status_type_id", "is_operational_ref", "status_title"])


status_lookup_df = get_status_type_lookup()
display(status_lookup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1b: Read Bronze, Parse, Flatten AddressInfo, Cast Timestamps to UTC

# COMMAND ----------

bronze_df = spark.table("bronze_ev_stations")
parsed_df = bronze_df.withColumn("station", F.from_json(F.col("raw_json"), station_schema))

flattened_df = parsed_df.select(
    F.col("station.ID").alias("station_id"),
    F.col("station.UUID").alias("uuid"),
    F.col("station.OperatorID").alias("operator_id"),
    F.col("station.UsageTypeID").alias("usage_type_id"),
    F.col("station.StatusTypeID").alias("status_type_id"),
    F.col("station.AddressInfo.Title").alias("title"),
    F.col("station.AddressInfo.Town").alias("town"),
    F.col("station.AddressInfo.StateOrProvince").alias("state_or_province"),
    F.col("station.AddressInfo.Latitude").cast("double").alias("latitude"),
    F.col("station.AddressInfo.Longitude").cast("double").alias("longitude"),
    F.col("station.Connections").alias("connections"),
    # Task 2: Standardization — OCM timestamps are UTC by convention (ISO 8601, "Z" suffix).
    F.to_utc_timestamp(F.col("station.DateCreated"), "UTC").alias("date_created_utc"),
    F.to_utc_timestamp(F.col("station.DateLastStatusUpdate"), "UTC").alias("date_last_status_update_utc"),
    F.col("ingested_at"),
    F.col("region_key"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Null Handling for Latitude/Longitude
# MAGIC **Decision: flag, don't drop.** A station missing coordinates is useless for
# MAGIC the map/geo dashboard, but it may still be valid for non-geographic
# MAGIC aggregations (e.g. counts by operator, revenue-at-risk totals). Silently
# MAGIC dropping it would understate those numbers without anyone noticing. Instead
# MAGIC we flag it in `data_quality_error`, and downstream geo visuals (Gold /
# MAGIC Power BI) can filter `data_quality_error IS NULL` explicitly.

# COMMAND ----------

quality_flagged_df = flattened_df.withColumn(
    "data_quality_error",
    F.when(F.col("latitude").isNull() | F.col("longitude").isNull(), F.lit("missing_geolocation"))
     .otherwise(F.lit(None).cast("string"))
)

missing_geo_count = quality_flagged_df.filter(F.col("data_quality_error").isNotNull()).count()
print(f"Stations flagged with missing geolocation: {missing_geo_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Join Is_Operational From Reference Data

# COMMAND ----------

station_with_status_df = (
    quality_flagged_df
    .join(status_lookup_df, on="status_type_id", how="left")
    .withColumn("Is_Operational", F.coalesce(F.col("is_operational_ref"), F.lit(False)))
    .drop("is_operational_ref")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1c: Exploding Connections — Grain Decision (documented)
# MAGIC
# MAGIC **The grain problem:** one Bronze record (a *station*) can contain multiple
# MAGIC `Connections` (plugs). Exploding turns 1 station row into N connection rows.
# MAGIC
# MAGIC **Decision: separate dimension/fact table (`silver_ev_connections`), not
# MAGIC merged into `silver_ev_stations`.** Reasoning:
# MAGIC 1. **Grain consistency** — `silver_ev_stations` should stay one-row-per-station
# MAGIC    so simple counts ("how many stations are operational?") don't need a
# MAGIC    `DISTINCT`/dedup step just to avoid inflating by connector count.
# MAGIC 2. **No attribute duplication** — putting connection rows in the station
# MAGIC    table would repeat every station attribute (address, lat/long, status)
# MAGIC    once per plug, wasting storage and risking update anomalies.
# MAGIC 3. **BI-friendliness** — Power BI (and the planned Gold star schema) wants
# MAGIC    a station-grain fact table; connection-level detail is better served as
# MAGIC    its own table joined in only when needed (e.g. "average power by
# MAGIC    connector type").
# MAGIC
# MAGIC This mirrors standard dimensional modeling: don't mix grains in one table.

# COMMAND ----------

exploded_df = station_with_status_df.select(
    "station_id", "uuid",
    F.explode_outer("connections").alias("connection")
).select(
    "station_id",
    "uuid",
    F.col("connection.ID").alias("connection_id"),
    F.col("connection.ConnectionTypeID").cast("int").alias("connection_type_id"),
    F.col("connection.StatusTypeID").cast("int").alias("connection_status_type_id"),
    F.col("connection.LevelID").cast("int").alias("level_id"),
    F.col("connection.PowerKW").cast("double").alias("power_kw"),
    F.col("connection.Quantity").cast("int").alias("quantity"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Senior Challenge: Data Quality Framework — Quarantine Bad Connections
# MAGIC Basic validation: `PowerKW` should never be negative. Route violators to a
# MAGIC quarantine table instead of dropping them or failing the pipeline — keeps
# MAGIC the run alive and gives you an auditable place to investigate bad records.

# COMMAND ----------

quarantine_df = exploded_df.filter(F.col("power_kw") < 0).withColumn(
    "quarantine_reason", F.lit("negative_power_kw")
).withColumn("quarantined_at", F.current_timestamp())

clean_connections_df = exploded_df.filter(
    F.col("power_kw").isNull() | (F.col("power_kw") >= 0)
)

quarantine_count = quarantine_df.count()
if quarantine_count > 0:
    (quarantine_df.write.mode("append").format("delta").saveAsTable("quarantine_ev_connections"))
    print(f"Quarantined {quarantine_count} connection records with negative PowerKW.")
else:
    print("No connection records failed the PowerKW >= 0 check.")

clean_connections_df.write.mode("overwrite").format("delta").saveAsTable("silver_ev_connections")
print(f"silver_ev_connections written: {clean_connections_df.count()} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate Stations
# MAGIC Multiple ingestion runs can land the same station multiple times in Bronze.
# MAGIC Keep only the most recently ingested version per `uuid`.

# COMMAND ----------

dedup_window = Window.partitionBy("uuid").orderBy(F.col("ingested_at").desc())

silver_stations_df = (
    station_with_status_df
    .drop("connections")
    .withColumn("row_rank", F.row_number().over(dedup_window))
    .filter(F.col("row_rank") == 1)
    .drop("row_rank")
)

print(f"Deduplicated station count: {silver_stations_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Upsert Strategy — Documented
# MAGIC
# MAGIC **Options considered:**
# MAGIC - *Full snapshot (overwrite)*: simplest, but destroys history and is wasteful
# MAGIC   at scale — re-writes every station every run even if nothing changed.
# MAGIC - *SCD Type 2*: preserves full history of every status change, but adds
# MAGIC   complexity (valid_from/valid_to) that the *current-state* table doesn't
# MAGIC   need — most consumers (map, current-status dashboard) only want "now."
# MAGIC - **SCD Type 1 via `MERGE INTO` (chosen)**: `silver_ev_stations` represents
# MAGIC   current truth — update matched stations in place, insert new ones, no
# MAGIC   duplicates. This keeps the "current state" table simple and fast to query.
# MAGIC   Full history is *not* lost — it's captured separately in
# MAGIC   `silver_ev_stations_history` below (the SCD2 senior challenge), so we get
# MAGIC   both a simple current table AND an audit trail, rather than forcing one
# MAGIC   table to serve both purposes.

# COMMAND ----------

silver_table_exists = spark.catalog.tableExists("silver_ev_stations")

if not silver_table_exists:
    silver_stations_df.write.format("delta").saveAsTable("silver_ev_stations")
    print("silver_ev_stations created for the first time.")
else:
    silver_stations_df.createOrReplaceTempView("silver_updates")
    spark.sql("""
        MERGE INTO silver_ev_stations AS target
        USING silver_updates AS source
        ON target.uuid = source.uuid
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("silver_ev_stations upserted via MERGE (SCD Type 1).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Senior Challenge: SCD Type 2 — Status History
# MAGIC Keeps a full audit trail of every operational-status change per station,
# MAGIC using `valid_from` / `valid_to` / `is_current`. This is what lets the
# MAGIC business later ask "how long was this station down?" — something the
# MAGIC current-state table alone can't answer.

# COMMAND ----------

history_table_exists = spark.catalog.tableExists("silver_ev_stations_history")

scd2_source_df = silver_stations_df.select(
    "uuid", "status_type_id", "Is_Operational", "date_last_status_update_utc"
)

if not history_table_exists:
    (
        scd2_source_df
        .withColumn("valid_from", F.col("date_last_status_update_utc"))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .write.format("delta").saveAsTable("silver_ev_stations_history")
    )
    print("silver_ev_stations_history created for the first time.")
else:
    current_history_df = (
        spark.table("silver_ev_stations_history")
        .filter("is_current = true")
        .select("uuid", F.col("status_type_id").alias("prev_status_type_id"))
    )

    changed_df = (
        scd2_source_df
        .join(current_history_df, on="uuid", how="left")
        .filter(
            F.col("prev_status_type_id").isNull()
            | (F.col("status_type_id") != F.col("prev_status_type_id"))
        )
        .drop("prev_status_type_id")
    )

    changed_count = changed_df.count()
    if changed_count == 0:
        print("No status changes detected — history table unchanged.")
    else:
        changed_df.createOrReplaceTempView("scd2_changes")

        # Step 1: close out the current row for any station whose status changed.
        spark.sql("""
            MERGE INTO silver_ev_stations_history AS target
            USING scd2_changes AS source
            ON target.uuid = source.uuid AND target.is_current = true
            WHEN MATCHED THEN UPDATE SET
                target.valid_to = source.date_last_status_update_utc,
                target.is_current = false
        """)

        # Step 2: insert the new current row for each change.
        (
            changed_df
            .withColumn("valid_from", F.col("date_last_status_update_utc"))
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write.mode("append").format("delta").saveAsTable("silver_ev_stations_history")
        )
        print(f"SCD2 history updated: {changed_count} status changes recorded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity Checks

# COMMAND ----------

display(spark.table("silver_ev_stations").limit(10))

# COMMAND ----------

display(
    spark.table("silver_ev_stations_history")
    .filter("is_current = true")
    .orderBy(F.col("valid_from").desc())
    .limit(10)
)

# COMMAND ----------

if spark.catalog.tableExists("quarantine_ev_connections"):
    display(spark.table("quarantine_ev_connections"))
else:
    print("No quarantine table yet — no bad records encountered so far.")
