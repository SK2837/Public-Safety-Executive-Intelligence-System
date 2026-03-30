"""
scripts/ingest_sf_data.py

Ingests real SF Police Incident data from the Socrata SODA API,
generates synthetic dispatch_logs and sensor_feeds per incident,
injects deliberate anomalies (volume spikes + geographic hotspot),
and writes everything to data/peregrine.duckdb.

Usage:
    python scripts/ingest_sf_data.py

Env vars:
    SF_APP_TOKEN   (optional) Socrata app token for higher rate limits
"""

import os
import sys
import uuid
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import duckdb
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "peregrine.duckdb"

SF_API = "https://data.sfgov.org/resource/wg3w-h783.json"
PAGE_SIZE = 50_000
DAYS_BACK = 90

# Tenderloin hotspot centre for anomaly injection
HOTSPOT_LAT = 37.7840
HOTSPOT_LON = -122.4130

PRIORITY_MAP = {
    "Homicide": 1,
    "Rape": 1,
    "Robbery": 1,
    "Assault": 1,
    "Weapons Carrying Etc": 1,
    "Arson": 1,
    "Kidnapping": 1,
    "Human Trafficking (A), Commercial Sex Acts": 1,
    "Burglary": 2,
    "Motor Vehicle Theft": 2,
    "Stolen Property": 2,
    "Fraud": 2,
    "Forgery And Counterfeiting": 2,
    "Larceny Theft": 3,
    "Drug Offense": 3,
    "Vandalism": 3,
    "Malicious Mischief": 3,
    "Suspicious Occ": 3,
    "Missing Person": 3,
    "Disorderly Conduct": 4,
    "Other Miscellaneous": 4,
    "Non-Criminal": 4,
    "Warrant": 4,
    "Traffic Violation Arrest": 4,
}

RESOLUTION_STATUS = {
    "Cite or Arrest Adult": "CLOSED",
    "Arrest, Booked": "CLOSED",
    "Exceptional Adult": "CLOSED",
    "Unfounded": "CLOSED",
    "Open or Active": "OPEN",
    "Pending Further Action": "OPEN",
    None: "OPEN",
}


# ---------------------------------------------------------------------------
# 1. Fetch SF Police data from SODA API
# ---------------------------------------------------------------------------

def fetch_sf_incidents(days_back: int = DAYS_BACK) -> pd.DataFrame:
    since = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00")
    headers = {}
    app_token = os.getenv("SF_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token

    frames = []
    offset = 0
    log.info("Fetching SF Police incidents since %s …", since)

    while True:
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$where": f"incident_datetime > '{since}'",
            "$select": (
                "row_id,incident_datetime,incident_category,"
                "incident_subcategory,resolution,police_district,"
                "latitude,longitude"
            ),
            "$order": "incident_datetime ASC",
        }

        for attempt in range(3):
            try:
                resp = requests.get(SF_API, params=params, headers=headers, timeout=60)
                resp.raise_for_status()
                break
            except requests.RequestException as exc:
                if attempt == 2:
                    log.error("SODA API failed after 3 attempts: %s", exc)
                    raise
                wait = 2 ** attempt
                log.warning("Attempt %d failed, retrying in %ds …", attempt + 1, wait)
                time.sleep(wait)

        batch = resp.json()
        if not batch:
            break

        frames.append(pd.DataFrame(batch))
        log.info("  Fetched %d rows (offset %d)", len(batch), offset)
        offset += len(batch)

        if len(batch) < PAGE_SIZE:
            break

        time.sleep(0.3)  # polite rate limiting

    if not frames:
        log.error("No data returned from SF API — check your network / date range.")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)
    log.info("Total raw rows from API: %d", len(df))
    return df


# ---------------------------------------------------------------------------
# 2. Clean and map to Peregrine schema
# ---------------------------------------------------------------------------

def map_to_incidents(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # Parse timestamps
    df["reported_at"] = pd.to_datetime(df["incident_datetime"], errors="coerce", utc=True)
    df = df.dropna(subset=["reported_at"])
    df["reported_at"] = df["reported_at"].dt.tz_localize(None)  # strip tz for DuckDB

    # Coordinates
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    # Drop zero-coordinate rows
    df = df[(df["latitude"] != 0) & (df["longitude"] != 0)]
    # Bounding box sanity check
    df = df[
        (df["latitude"].between(37.70, 37.83))
        & (df["longitude"].between(-122.52, -122.35))
    ]

    df["incident_id"] = df["row_id"].astype(str)
    df["agency"] = "POLICE"
    df["incident_type"] = df["incident_category"].fillna("Other Miscellaneous").str.strip()
    df["district"] = df["police_district"].fillna("UNKNOWN").str.upper().str.strip()
    df["priority"] = df["incident_type"].map(PRIORITY_MAP).fillna(3).astype(int)
    df["status"] = df["resolution"].map(RESOLUTION_STATUS).fillna("OPEN")

    incidents = df[
        ["incident_id", "agency", "incident_type", "reported_at",
         "latitude", "longitude", "priority", "district", "status"]
    ].copy()

    log.info("Cleaned POLICE incidents: %d rows", len(incidents))
    return incidents.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 3. Generate synthetic FIRE and EMS incidents
# ---------------------------------------------------------------------------

def generate_fire_ems(police_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """
    Generate synthetic FIRE and EMS incidents that mirror the police incident
    geographic and temporal distribution (30% extra volume, split 50/50).
    """
    n_police = len(police_df)
    n_fire = int(n_police * 0.15)
    n_ems = int(n_police * 0.15)

    fire_types = ["Structure Fire", "Vehicle Fire", "Brush Fire", "Alarm Activation",
                  "Smoke Investigation", "Hazmat", "Rescue", "Medical Assist"]
    ems_types = ["Cardiac Arrest", "Chest Pain", "Unconscious Person", "Fall Injury",
                 "Traffic Accident - Injuries", "Seizure", "Overdose", "Difficulty Breathing"]

    rows = []
    for agency, types, n in [("FIRE", fire_types, n_fire), ("EMS", ems_types, n_ems)]:
        sampled = police_df.sample(n=n, replace=True, random_state=rng.integers(0, 9999)).copy()
        # Jitter coordinates slightly
        sampled["latitude"] += rng.normal(0, 0.003, n)
        sampled["longitude"] += rng.normal(0, 0.003, n)
        # Jitter timestamps by ±30 minutes
        jitter = pd.to_timedelta(rng.integers(-1800, 1800, n), unit="s")
        sampled["reported_at"] = sampled["reported_at"] + jitter
        sampled["incident_id"] = [str(uuid.uuid4()) for _ in range(n)]
        sampled["agency"] = agency
        sampled["incident_type"] = rng.choice(types, n)
        sampled["priority"] = rng.choice([1, 2, 3], n, p=[0.20, 0.45, 0.35])
        sampled["status"] = rng.choice(["CLOSED", "OPEN"], n, p=[0.85, 0.15])
        rows.append(sampled[["incident_id", "agency", "incident_type", "reported_at",
                              "latitude", "longitude", "priority", "district", "status"]])

    return pd.concat(rows, ignore_index=True)


# ---------------------------------------------------------------------------
# 4. Inject anomalies
# ---------------------------------------------------------------------------

def inject_anomalies(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """
    Inject 3 volume spikes and 1 geographic hotspot cluster into the incident data.
    Returns augmented DataFrame with a new `is_injected` flag.
    """
    df = df.copy()
    df["is_injected"] = False

    date_range = df["reported_at"]
    min_date = date_range.min().date()
    max_date = date_range.max().date()
    total_days = (max_date - min_date).days

    # Pick 3 spike dates spread across the range
    spike_offsets = [int(total_days * 0.17), int(total_days * 0.50), int(total_days * 0.80)]
    spike_dates = [min_date + timedelta(days=d) for d in spike_offsets]

    spike_rows = []
    for spike_date in spike_dates:
        sd = pd.Timestamp(spike_date)
        day_mask = (df["reported_at"].dt.date == spike_date) & (df["agency"] == "POLICE")
        day_df = df[day_mask]
        if len(day_df) == 0:
            continue
        # Duplicate 2x to create 3x total volume on the spike day
        for _ in range(2):
            dup = day_df.copy()
            dup["incident_id"] = [str(uuid.uuid4()) for _ in range(len(dup))]
            dup["reported_at"] += pd.to_timedelta(rng.integers(-3600, 3600, len(dup)), unit="s")
            dup["is_injected"] = True
            spike_rows.append(dup)
        log.info("  Spike injected on %s (+%d POLICE incidents)", spike_date, len(day_df) * 2)

    # Geographic hotspot: cluster 10% of injected incidents near Tenderloin
    hotspot_n = max(50, int(len(df) * 0.05))
    sample = df[df["agency"] == "POLICE"].sample(n=hotspot_n, replace=True, random_state=42).copy()
    sample["incident_id"] = [str(uuid.uuid4()) for _ in range(hotspot_n)]
    sample["latitude"] = rng.normal(HOTSPOT_LAT, 0.003, hotspot_n)
    sample["longitude"] = rng.normal(HOTSPOT_LON, 0.003, hotspot_n)
    sample["is_injected"] = True
    spike_rows.append(sample)
    log.info("  Hotspot cluster injected: %d incidents near Tenderloin", hotspot_n)

    if spike_rows:
        df = pd.concat([df] + spike_rows, ignore_index=True)

    log.info("Total incidents after anomaly injection: %d", len(df))
    return df


# ---------------------------------------------------------------------------
# 5. Generate dispatch logs
# ---------------------------------------------------------------------------

def generate_dispatch_logs(
    incidents: pd.DataFrame,
    rng: np.random.Generator,
    spike_dates: list,
) -> pd.DataFrame:
    """
    Generate 1-3 dispatch log entries per incident.
    Spike-day incidents get response_time × 1.8.
    """
    rows = []
    unit_prefixes = {"POLICE": "P", "FIRE": "F", "EMS": "E"}

    for _, inc in incidents.iterrows():
        n_units = rng.integers(1, 4)  # 1-3 units
        prefix = unit_prefixes.get(inc["agency"], "X")
        is_spike = inc["reported_at"].date() in spike_dates

        for _ in range(n_units):
            # Log-normal response time: mean ~420s, std ~180s
            base_rt = int(np.clip(rng.lognormal(np.log(420), 0.4), 60, 3600))
            if is_spike:
                base_rt = int(base_rt * 1.8)

            dispatched_at = inc["reported_at"] + timedelta(seconds=int(rng.integers(30, 120)))
            arrived_at = dispatched_at + timedelta(seconds=base_rt)
            service_dur = int(rng.lognormal(np.log(900), 0.5))
            cleared_at = arrived_at + timedelta(seconds=service_dur)

            rows.append({
                "log_id": str(uuid.uuid4()),
                "incident_id": inc["incident_id"],
                "unit_id": f"{prefix}-{rng.integers(1, 30):02d}",
                "dispatched_at": dispatched_at,
                "arrived_at": arrived_at,
                "cleared_at": cleared_at,
                "response_time_seconds": base_rt,
                "units_available_at_dispatch": int(rng.integers(1, 13)),
            })

    df = pd.DataFrame(rows)
    log.info("Generated dispatch_logs: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# 6. Generate sensor feeds
# ---------------------------------------------------------------------------

def generate_sensor_feeds(
    incidents: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Generate 2000 synthetic sensor feed entries (CAMERA, GUNSHOT_DETECTOR,
    TRAFFIC, WEATHER) spatially correlated with real incident locations.
    """
    n = 2000
    sensor_types = ["CAMERA", "GUNSHOT_DETECTOR", "TRAFFIC", "WEATHER"]
    units = {"CAMERA": "events/hr", "GUNSHOT_DETECTOR": "db", "TRAFFIC": "mph", "WEATHER": "F"}

    # Sample anchor locations from incidents
    anchors = incidents.sample(n=n, replace=True, random_state=7).reset_index(drop=True)

    sensor_type_arr = rng.choice(sensor_types, n)
    rows = []
    for i in range(n):
        stype = sensor_type_arr[i]
        lat = anchors.loc[i, "latitude"] + rng.normal(0, 0.005)
        lon = anchors.loc[i, "longitude"] + rng.normal(0, 0.005)
        ts = anchors.loc[i, "reported_at"] + timedelta(seconds=int(rng.integers(-3600, 3600)))

        if stype == "CAMERA":
            value = float(rng.integers(0, 50))
        elif stype == "GUNSHOT_DETECTOR":
            value = float(rng.normal(70, 15))
        elif stype == "TRAFFIC":
            value = float(rng.normal(25, 10))
        else:  # WEATHER
            value = float(rng.normal(58, 8))

        rows.append({
            "feed_id": str(uuid.uuid4()),
            "sensor_id": f"{stype[:3]}-{rng.integers(100, 999)}",
            "sensor_type": stype,
            "recorded_at": ts,
            "latitude": lat,
            "longitude": lon,
            "value": round(value, 2),
            "unit": units[stype],
            "anomaly_flag": bool(rng.random() < 0.08),
        })

    df = pd.DataFrame(rows)
    log.info("Generated sensor_feeds: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# 7. Write to DuckDB
# ---------------------------------------------------------------------------

def write_to_duckdb(
    incidents: pd.DataFrame,
    dispatch_logs: pd.DataFrame,
    sensor_feeds: pd.DataFrame,
) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(DB_PATH)) as con:
        # Drop and recreate tables fresh
        con.execute("DROP TABLE IF EXISTS incidents")
        con.execute("DROP TABLE IF EXISTS dispatch_logs")
        con.execute("DROP TABLE IF EXISTS sensor_feeds")

        con.execute("""
            CREATE TABLE incidents (
                incident_id       VARCHAR,
                agency            VARCHAR,
                incident_type     VARCHAR,
                reported_at       TIMESTAMP,
                latitude          DOUBLE,
                longitude         DOUBLE,
                priority          INTEGER,
                district          VARCHAR,
                status            VARCHAR,
                is_injected       BOOLEAN
            )
        """)
        con.execute("""
            CREATE TABLE dispatch_logs (
                log_id                      VARCHAR,
                incident_id                 VARCHAR,
                unit_id                     VARCHAR,
                dispatched_at               TIMESTAMP,
                arrived_at                  TIMESTAMP,
                cleared_at                  TIMESTAMP,
                response_time_seconds       INTEGER,
                units_available_at_dispatch INTEGER
            )
        """)
        con.execute("""
            CREATE TABLE sensor_feeds (
                feed_id       VARCHAR,
                sensor_id     VARCHAR,
                sensor_type   VARCHAR,
                recorded_at   TIMESTAMP,
                latitude      DOUBLE,
                longitude     DOUBLE,
                value         DOUBLE,
                unit          VARCHAR,
                anomaly_flag  BOOLEAN
            )
        """)

        con.register("_incidents_df", incidents)
        con.execute("INSERT INTO incidents SELECT * FROM _incidents_df")

        con.register("_dispatch_df", dispatch_logs)
        con.execute("INSERT INTO dispatch_logs SELECT * FROM _dispatch_df")

        con.register("_sensor_df", sensor_feeds)
        con.execute("INSERT INTO sensor_feeds SELECT * FROM _sensor_df")

    log.info("Written to %s", DB_PATH)
    log.info("  incidents:    %d rows", len(incidents))
    log.info("  dispatch_logs: %d rows", len(dispatch_logs))
    log.info("  sensor_feeds:  %d rows", len(sensor_feeds))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    rng = np.random.default_rng(seed=42)

    # 1. Fetch real data
    raw = fetch_sf_incidents(DAYS_BACK)

    # 2. Map to Peregrine schema
    police_incidents = map_to_incidents(raw)

    # 3. Add synthetic FIRE + EMS
    fire_ems = generate_fire_ems(police_incidents, rng)
    all_incidents = pd.concat([police_incidents, fire_ems], ignore_index=True)

    # 4. Inject anomalies
    all_incidents = inject_anomalies(all_incidents, rng)

    # Derive spike dates for dispatch log degradation
    date_range = all_incidents["reported_at"]
    min_date = date_range.min().date()
    max_date = date_range.max().date()
    total_days = (max_date - min_date).days
    spike_dates = [
        min_date + timedelta(days=int(total_days * 0.17)),
        min_date + timedelta(days=int(total_days * 0.50)),
        min_date + timedelta(days=int(total_days * 0.80)),
    ]

    # 5. Generate dispatch logs
    dispatch_logs = generate_dispatch_logs(all_incidents, rng, spike_dates)

    # 6. Generate sensor feeds
    sensor_feeds = generate_sensor_feeds(all_incidents, rng)

    # 7. Write to DuckDB
    write_to_duckdb(all_incidents, dispatch_logs, sensor_feeds)
    log.info("Done. Run: cd dbt_project && dbt run")


if __name__ == "__main__":
    main()
