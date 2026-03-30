"""Cached DuckDB query helpers for the Streamlit dashboard."""

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "peregrine.duckdb"


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=300)
def load_kpi_snapshot() -> dict:
    """Returns the KPI snapshot dict from the AnomalyDetector (cached 5 min)."""
    import sys
    sys.path.insert(0, str(BASE_DIR))
    from anomaly_detection import AnomalyDetector
    detector = AnomalyDetector(DB_PATH)
    return detector.compute_kpi_snapshot()


@st.cache_data(ttl=300)
def load_anomalies() -> list:
    """Returns all detected anomalies, sorted by severity (cached 5 min)."""
    import sys
    sys.path.insert(0, str(BASE_DIR))
    from anomaly_detection import AnomalyDetector
    detector = AnomalyDetector(DB_PATH)
    return detector.run_all()


@st.cache_data(ttl=300)
def load_incident_map_data() -> pd.DataFrame:
    """Last 7 days of incidents with lat/lon for map rendering."""
    with _connect() as con:
        return con.execute("""
            SELECT incident_id, agency, incident_type, reported_at,
                   latitude, longitude, priority, district, status
            FROM incidents
            WHERE reported_at >= NOW() - INTERVAL 7 DAY
              AND latitude  BETWEEN 37.70 AND 37.83
              AND longitude BETWEEN -122.52 AND -122.35
        """).df()


@st.cache_data(ttl=300)
def load_daily_volume() -> pd.DataFrame:
    """Daily incident counts per agency for the trend chart."""
    with _connect() as con:
        return con.execute("""
            SELECT CAST(reported_at AS DATE) AS report_date,
                   agency,
                   COUNT(*) AS incident_count
            FROM incidents
            GROUP BY 1, 2
            ORDER BY 1
        """).df()


@st.cache_data(ttl=300)
def load_response_time_trend() -> pd.DataFrame:
    """Daily avg response time per agency."""
    with _connect() as con:
        return con.execute("""
            SELECT report_date, agency,
                   ROUND(avg_response_time_s, 1) AS avg_response_time_s,
                   ROUND(p90_response_time_s, 1) AS p90_response_time_s
            FROM mart_response_times
            ORDER BY 1
        """).df()
