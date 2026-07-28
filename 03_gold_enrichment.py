# Databricks notebook source
# MAGIC %md
# MAGIC # Project VoltStream — Week 3: The Gold Layer & Data Enrichment
# MAGIC Enriches Silver EV station data with real-time weather, computes a
# MAGIC `Risk_Score`, and models a dimensional Star Schema.
# MAGIC
# MAGIC **Covers:**
# MAGIC - Task 1: Weather Zone strategy (rounded lat/long)
# MAGIC - Task 2: Weather API ingestion → `silver_weather`
# MAGIC - Task 3: Spatial join + Risk_Score business logic
# MAGIC - Task 4: Star Schema (Dim_Station, Dim_Weather, Fact_Station_Status)
# MAGIC - Senior: Est_Revenue_At_Risk; orchestration notes at the bottom

# COMMAND ----------

from pyspark.sql import functions as F
import requests
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather API Key (same secret-handling pattern as Bronze)

# COMMAND ----------

def get_weather_api_key() -> str:
    """Resolve the OpenWeatherMap API key without ever printing or logging it."""
    try:
        key = dbutils.secrets.get(scope="voltstream", key="owm_api_key")
        source = "Databricks Secrets"
    except Exception:
        dbutils.widgets.text("owm_api_key_fallback", "", "OpenWeatherMap API Key (fallback only)")
        key = dbutils.widgets.get("owm_api_key_fallback").strip()
        source = "fallback widget"

    if not key:
        raise ValueError(
            "No OpenWeatherMap API key found. Set it via Databricks Secrets "
            "(scope='voltstream', key='owm_api_key') or the fallback widget."
        )

    print(f"Weather API key loaded from {source}. Length: {len(key)} chars. Value is not printed.")
    return key


weather_api_key = get_weather_api_key()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: The "Weather Zone" Strategy
# MAGIC Rather than one API call per station (500+ calls, most redundant since
# MAGIC nearby stations share essentially the same weather), round each
# MAGIC station's lat/long to 1 decimal place — roughly an 8-11km grid cell —
# MAGIC and take the distinct set. Stations in the same zone all read the same
# MAGIC weather snapshot from one API call.

# COMMAND ----------

zones_df = (
    spark.table("silver_ev_stations")
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    .withColumn("zone_lat", F.round(F.col("latitude"), 1))
    .withColumn("zone_lon", F.round(F.col("longitude"), 1))
    .select("zone_lat", "zone_lon")
    .distinct()
)

zone_list = [(row.zone_lat, row.zone_lon) for row in zones_df.collect()]
print(f"Distinct weather zones to fetch: {len(zone_list)} (vs. one call per station).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Weather API Ingestion → `silver_weather`
# MAGIC Streamlined pipeline per the architecture note: no separate Bronze hop
# MAGIC this week — land cleaned weather directly into `silver_weather`.
# MAGIC Using `units=imperial` gets Fahrenheit and mph directly from the API,
# MAGIC skipping manual Celsius-to-Fahrenheit conversion math.
# MAGIC
# MAGIC **Design choice:** `silver_weather` is overwritten each run, not
# MAGIC appended — this is a live "current conditions" snapshot, not a history
# MAGIC table. If you later want a weather history, append with a
# MAGIC `fetched_at` timestamp instead of overwriting.

# COMMAND ----------

OWM_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather_for_zone(lat: float, lon: float, api_key: str, max_retries: int = 3):
    """Fetch current weather for one zone. Returns a dict or None on failure."""
    params = {"lat": lat, "lon": lon, "units": "imperial", "appid": api_key}

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(OWM_BASE_URL, params=params, timeout=10)
            if resp.status_code == 401:
                raise PermissionError("OpenWeatherMap rejected the API key (401 Unauthorized).")
            resp.raise_for_status()
            data = resp.json()

            temp_val = data.get("main", {}).get("temp")
            wind_val = data.get("wind", {}).get("speed")

            return {
                "zone_lat": float(lat),
                "zone_lon": float(lon),
                "temp_f": float(temp_val) if temp_val is not None else None,
                "condition": data.get("weather", [{}])[0].get("main", "Unknown"),
                "wind_speed_mph": float(wind_val) if wind_val is not None else None,
            }
        except requests.exceptions.Timeout:
            print(f"Timeout fetching zone ({lat}, {lon}), attempt {attempt}/{max_retries}. Retrying...")
            time.sleep(1.5 * attempt)
        except requests.exceptions.RequestException as e:
            print(f"Request error for zone ({lat}, {lon}), attempt {attempt}/{max_retries}: {e}")
            time.sleep(1.5 * attempt)

    print(f"Giving up on zone ({lat}, {lon}) after {max_retries} attempts.")
    return None

# COMMAND ----------

weather_rows = []
for lat, lon in zone_list:
    result = fetch_weather_for_zone(lat, lon, weather_api_key)
    if result:
        weather_rows.append(result)
    time.sleep(1.1)  # stay comfortably under free-tier rate limits (60 calls/min)

print(f"Successfully fetched weather for {len(weather_rows)} of {len(zone_list)} zones.")

# COMMAND ----------

weather_df = spark.createDataFrame(weather_rows).withColumn(
    "weather_zone_id", F.concat_ws("_", F.col("zone_lat"), F.col("zone_lon"))
).withColumn(
    "fetched_at", F.current_timestamp()
)

weather_df.write.mode("overwrite").format("delta").saveAsTable("silver_weather")
print(f"silver_weather written: {weather_df.count()} zone records.")
display(weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Spatial Join & Risk_Score
# MAGIC
# MAGIC **Fast-charger detection:** OCM's `LevelID = 3` denotes DC fast charging,
# MAGIC but that field isn't always populated consistently across data
# MAGIC providers — as a safety net, a station is also treated as "fast
# MAGIC charger" if any connector reports ≥50kW, since that's DC-fast-charging
# MAGIC territory regardless of how the level was tagged.
# MAGIC
# MAGIC **Risk_Score formula (base 10, capped at 100):**
# MAGIC - +40 if weather condition is Thunderstorm or Snow
# MAGIC - +20 if temperature > 95°F or < 20°F
# MAGIC - +10 if the station has at least one fast (DC) charger

# COMMAND ----------

station_connection_agg = (
    spark.table("silver_ev_connections")
    .groupBy("station_id", "uuid")
    .agg(
        F.max("power_kw").alias("max_power_kw"),
        F.max(
            F.when((F.col("level_id") == 3) | (F.col("power_kw") >= 50), 1).otherwise(0)
        ).alias("has_fast_charger"),
    )
)

# COMMAND ----------

enriched_df = (
    spark.table("silver_ev_stations")
    .join(station_connection_agg, on=["station_id", "uuid"], how="left")
    .withColumn("zone_lat", F.round(F.col("latitude"), 1))
    .withColumn("zone_lon", F.round(F.col("longitude"), 1))
    .withColumn("weather_zone_id", F.concat_ws("_", F.col("zone_lat"), F.col("zone_lon")))
    .join(
        weather_df.select("weather_zone_id", "temp_f", "condition", "wind_speed_mph", "fetched_at"),
        on="weather_zone_id",
        how="left",
    )
    .withColumn(
        "risk_score",
        F.least(
            F.lit(10)
            + F.when(F.col("condition").isin("Thunderstorm", "Snow"), F.lit(40)).otherwise(F.lit(0))
            + F.when((F.col("temp_f") > 95) | (F.col("temp_f") < 20), F.lit(20)).otherwise(F.lit(0))
            + F.when(F.col("has_fast_charger") == 1, F.lit(10)).otherwise(F.lit(0)),
            F.lit(100),
        ),
    )
)

display(enriched_df.select("uuid", "title", "temp_f", "condition", "has_fast_charger", "risk_score").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Senior Challenge: Est_Revenue_At_Risk
# MAGIC If a station is offline, estimate the lost revenue per hour it's been
# MAGIC down: its max power output (kW) × an average energy rate ($0.35/kWh).
# MAGIC An operational station has nothing at risk this hour.

# COMMAND ----------

ENERGY_RATE_PER_KWH = 0.35

enriched_with_revenue_df = enriched_df.withColumn(
    "Est_Revenue_At_Risk",
    F.when(
        F.col("Is_Operational") == False,  # noqa: E712 - explicit boolean compare reads clearer here
        F.coalesce(F.col("max_power_kw"), F.lit(0)) * F.lit(ENERGY_RATE_PER_KWH),
    ).otherwise(F.lit(0.0)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Modeling the Star Schema

# COMMAND ----------

dim_station_df = enriched_with_revenue_df.select(
    "station_id",
    "uuid",
    "title",
    "address_line1" if "address_line1" in enriched_with_revenue_df.columns else F.lit(None).alias("address_line1"),
    "town",
    "state_or_province",
    "latitude",
    "longitude",
    "max_power_kw",
).dropDuplicates(["station_id"])

dim_station_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("Dim_Station")
print(f"Dim_Station written: {dim_station_df.count()} rows.")

# COMMAND ----------

dim_weather_df = weather_df.select(
    "weather_zone_id", "zone_lat", "zone_lon", "temp_f", "condition", "wind_speed_mph", "fetched_at"
)

dim_weather_df.write.mode("overwrite").format("delta").saveAsTable("Dim_Weather")
print(f"Dim_Weather written: {dim_weather_df.count()} rows.")

# COMMAND ----------

fact_station_status_df = enriched_with_revenue_df.select(
    F.current_timestamp().alias("status_timestamp"),
    "station_id",
    "uuid",
    "weather_zone_id",
    "Is_Operational",
    F.when(F.col("Is_Operational") == False, F.lit(1)).otherwise(F.lit(0)).alias("Offline_Count"),  # noqa: E712
    "risk_score",
    "Est_Revenue_At_Risk",
)

fact_station_status_df.write.mode("append").format("delta").saveAsTable("Fact_Station_Status")
print(f"Fact_Station_Status appended: {fact_station_status_df.count()} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity Checks

# COMMAND ----------

display(spark.table("Fact_Station_Status").orderBy(F.col("status_timestamp").desc()).limit(10))

# COMMAND ----------

display(
    spark.table("Fact_Station_Status")
    .filter("Is_Operational = false")
    .orderBy(F.col("Est_Revenue_At_Risk").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Senior Challenge: Orchestration (Databricks Workflows)
# MAGIC This can't be done from inside a notebook — it's a UI/Jobs setup step:
# MAGIC
# MAGIC 1. **Jobs & Pipelines** (left sidebar) → **Create Job**
# MAGIC 2. **Task 1**: name it `bronze_ingestion`, point it at
# MAGIC    `01_bronze_ev_ingestion`, attach your serverless compute.
# MAGIC 3. **Task 2**: name it `silver_cleaning`, point it at
# MAGIC    `02_silver_ev_cleaning`, set **Depends on** → `bronze_ingestion`.
# MAGIC 4. **Task 3**: name it `gold_enrichment`, point it at this notebook
# MAGIC    (`03_gold_enrichment`), set **Depends on** → `silver_cleaning`.
# MAGIC 5. Add a **Schedule** (e.g. hourly or daily) at the top of the Job.
# MAGIC 6. Save — Databricks will now run all three notebooks in sequence,
# MAGIC    automatically, on schedule, stopping the chain if any task fails.

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.powerbi_exports")

# COMMAND ----------

export_path = "/Volumes/workspace/default/powerbi_exports"

spark.table("Fact_Station_Status").toPandas().to_csv(f"{export_path}/fact_station_status.csv", index=False)
spark.table("Dim_Station").toPandas().to_csv(f"{export_path}/dim_station.csv", index=False)
spark.table("Dim_Weather").toPandas().to_csv(f"{export_path}/dim_weather.csv", index=False)

print("Exported all three tables to the Volume.")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/powerbi_exports"))

# COMMAND ----------

display(spark.table("Dim_Station").select("address_line1").limit(10))

# COMMAND ----------

import json

sample = spark.table("bronze_ev_stations").select("raw_json").limit(1).collect()[0]["raw_json"]
parsed = json.loads(sample)
print(json.dumps(parsed.get("AddressInfo", {}), indent=2))

# COMMAND ----------

export_path = "/Volumes/workspace/default/powerbi_exports"

spark.table("Fact_Station_Status").toPandas().to_csv(f"{export_path}/fact_station_status.csv", index=False)
spark.table("Dim_Station").toPandas().to_csv(f"{export_path}/dim_station.csv", index=False)
spark.table("Dim_Weather").toPandas().to_csv(f"{export_path}/dim_weather.csv", index=False)
