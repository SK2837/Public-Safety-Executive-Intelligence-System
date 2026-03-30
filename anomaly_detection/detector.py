"""AnomalyDetector: runs all four detection methods against mart tables."""

import uuid
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

from .models import Anomaly, AnomalySeverity, AnomalyType

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "peregrine.duckdb"


def _severity_from_zscore(z: float, thresholds: tuple = (2.5, 3.5, 4.5)) -> AnomalySeverity:
    low, high, crit = thresholds
    if z >= crit:
        return AnomalySeverity.CRITICAL
    if z >= high:
        return AnomalySeverity.HIGH
    if z >= low:
        return AnomalySeverity.MEDIUM
    return AnomalySeverity.LOW


class AnomalyDetector:
    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or DB_PATH

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path), read_only=True)

    # ------------------------------------------------------------------
    # Volume spike detection — rolling 7-day z-score per agency
    # ------------------------------------------------------------------

    def detect_volume_spikes(self) -> list[Anomaly]:
        query = """
            SELECT report_date, agency, SUM(incident_count) AS daily_count
            FROM mart_response_times
            GROUP BY 1, 2
            ORDER BY 1
        """
        with self._connect() as con:
            df = con.execute(query).df()

        if df.empty:
            return []

        anomalies = []
        for agency, grp in df.groupby("agency"):
            grp = grp.sort_values("report_date").copy()
            grp["roll_mean"] = grp["daily_count"].rolling(7, min_periods=3).mean()
            grp["roll_std"] = grp["daily_count"].rolling(7, min_periods=3).std()
            grp["z_score"] = (grp["daily_count"] - grp["roll_mean"]) / grp["roll_std"].replace(0, np.nan)

            # Only flag the last 7 days
            cutoff = grp["report_date"].max() - timedelta(days=7)
            recent = grp[grp["report_date"] >= cutoff].dropna(subset=["z_score"])

            for _, row in recent[recent["z_score"] >= 2.5].iterrows():
                severity = _severity_from_zscore(row["z_score"], (2.5, 3.5, 4.5))
                anomalies.append(
                    Anomaly(
                        anomaly_id=str(uuid.uuid4()),
                        anomaly_type=AnomalyType.VOLUME_SPIKE,
                        severity=severity,
                        detected_at=datetime.combine(row["report_date"], datetime.min.time()),
                        description=(
                            f"{agency} incident volume spike on {row['report_date']}: "
                            f"{int(row['daily_count'])} incidents (z={row['z_score']:.2f})"
                        ),
                        z_score=round(row["z_score"], 2),
                        agency=agency,
                        affected_count=int(row["daily_count"]),
                        metadata={"daily_count": int(row["daily_count"]), "roll_mean": round(row["roll_mean"], 1)},
                    )
                )
        return anomalies

    # ------------------------------------------------------------------
    # Response time spike detection — rolling 14-day z-score per agency+district
    # ------------------------------------------------------------------

    def detect_response_time_spikes(self) -> list[Anomaly]:
        query = """
            SELECT report_date, agency, district,
                   avg_response_time_s, p90_response_time_s
            FROM mart_response_times
            ORDER BY 1
        """
        with self._connect() as con:
            df = con.execute(query).df()

        if df.empty:
            return []

        anomalies = []
        for (agency, district), grp in df.groupby(["agency", "district"]):
            grp = grp.sort_values("report_date").copy()
            grp["roll_mean"] = grp["avg_response_time_s"].rolling(14, min_periods=5).mean()
            grp["roll_std"] = grp["avg_response_time_s"].rolling(14, min_periods=5).std()
            grp["z_score"] = (
                (grp["avg_response_time_s"] - grp["roll_mean"])
                / grp["roll_std"].replace(0, np.nan)
            )

            cutoff = grp["report_date"].max() - timedelta(days=7)
            recent = grp[grp["report_date"] >= cutoff].dropna(subset=["z_score"])

            for _, row in recent.iterrows():
                z = row["z_score"]
                p90 = row["p90_response_time_s"]
                # Flag if z-score threshold OR p90 hard cap breached
                if z < 2.0 and p90 <= 1800:
                    continue

                if p90 > 1800 and z < 2.0:
                    severity = AnomalySeverity.HIGH
                else:
                    severity = _severity_from_zscore(z, (2.0, 3.0, 4.0))

                anomalies.append(
                    Anomaly(
                        anomaly_id=str(uuid.uuid4()),
                        anomaly_type=AnomalyType.RESPONSE_TIME_SPIKE,
                        severity=severity,
                        detected_at=datetime.combine(row["report_date"], datetime.min.time()),
                        description=(
                            f"{agency} / {district}: response time spike on {row['report_date']} "
                            f"— avg {row['avg_response_time_s']:.0f}s, p90 {p90:.0f}s (z={z:.2f})"
                        ),
                        z_score=round(z, 2),
                        agency=agency,
                        district=district,
                        metadata={
                            "avg_response_time_s": round(row["avg_response_time_s"], 1),
                            "p90_response_time_s": round(p90, 1),
                        },
                    )
                )
        return anomalies

    # ------------------------------------------------------------------
    # Geographic hotspot detection — grid-cell z-scores, last 7 days
    # ------------------------------------------------------------------

    def detect_geographic_hotspots(self) -> list[Anomaly]:
        query = """
            SELECT grid_lat, grid_lon,
                   SUM(incident_count)    AS total_count,
                   AVG(density_zscore)    AS avg_zscore,
                   MIN(reported_date)     AS first_seen,
                   MAX(reported_date)     AS last_seen
            FROM mart_incident_clusters
            WHERE reported_date >= CURRENT_DATE - INTERVAL 7 DAY
            GROUP BY 1, 2
            HAVING AVG(density_zscore) >= 2.0
            ORDER BY avg_zscore DESC
        """
        with self._connect() as con:
            df = con.execute(query).df()

        if df.empty:
            return []

        anomalies = []
        used = []  # for merging nearby cells

        for _, row in df.iterrows():
            lat, lon = row["grid_lat"], row["grid_lon"]
            # Merge if within ~500m of an already-flagged cell
            merged = False
            for u in used:
                if abs(lat - u[0]) <= 0.005 and abs(lon - u[1]) <= 0.005:
                    merged = True
                    break
            if merged:
                continue

            used.append((lat, lon))
            z = row["avg_zscore"]
            severity = _severity_from_zscore(z, (2.0, 3.0, 4.5))
            anomalies.append(
                Anomaly(
                    anomaly_id=str(uuid.uuid4()),
                    anomaly_type=AnomalyType.GEOGRAPHIC_HOTSPOT,
                    severity=severity,
                    detected_at=datetime.now(),
                    description=(
                        f"Incident hotspot at ({lat:.3f}, {lon:.3f}): "
                        f"{int(row['total_count'])} incidents in last 7 days (z={z:.2f})"
                    ),
                    z_score=round(z, 2),
                    latitude=lat,
                    longitude=lon,
                    affected_count=int(row["total_count"]),
                    metadata={
                        "first_seen": str(row["first_seen"]),
                        "last_seen": str(row["last_seen"]),
                    },
                )
            )
        return anomalies

    # ------------------------------------------------------------------
    # Resource gap detection — last 24 hours
    # ------------------------------------------------------------------

    def detect_resource_gaps(self) -> list[Anomaly]:
        query = """
            SELECT agency, district,
                   COUNT(*) AS gap_hours,
                   AVG(avg_response_time_s) AS avg_rt,
                   AVG(avg_units_available) AS avg_units
            FROM mart_resource_gaps
            WHERE resource_gap_flag = TRUE
              AND hour_bucket >= NOW() - INTERVAL 24 HOUR
            GROUP BY 1, 2
            HAVING COUNT(*) >= 2
        """
        with self._connect() as con:
            df = con.execute(query).df()

        if df.empty:
            return []

        anomalies = []
        for _, row in df.iterrows():
            gap_hours = int(row["gap_hours"])
            severity = AnomalySeverity.CRITICAL if gap_hours > 4 else AnomalySeverity.HIGH
            anomalies.append(
                Anomaly(
                    anomaly_id=str(uuid.uuid4()),
                    anomaly_type=AnomalyType.RESOURCE_GAP,
                    severity=severity,
                    detected_at=datetime.now(),
                    description=(
                        f"{row['agency']} / {row['district']}: resource gap — "
                        f"{gap_hours} hours under-resourced in last 24h "
                        f"(avg {row['avg_units']:.1f} units, avg RT {row['avg_rt']:.0f}s)"
                    ),
                    z_score=None,
                    agency=row["agency"],
                    district=row["district"],
                    affected_count=gap_hours,
                    metadata={
                        "avg_units_available": round(row["avg_units"], 2),
                        "avg_response_time_s": round(row["avg_rt"], 1),
                    },
                )
            )
        return anomalies

    # ------------------------------------------------------------------
    # KPI snapshot
    # ------------------------------------------------------------------

    def compute_kpi_snapshot(self) -> dict:
        with self._connect() as con:
            inc_24h = con.execute("""
                SELECT COUNT(*) AS cnt
                FROM incidents
                WHERE reported_at >= NOW() - INTERVAL 24 HOUR
            """).fetchone()[0]

            inc_7d = con.execute("""
                SELECT COUNT(*) AS cnt
                FROM incidents
                WHERE reported_at >= NOW() - INTERVAL 7 DAY
            """).fetchone()[0]

            rt_row = con.execute("""
                SELECT AVG(response_time_seconds), PERCENTILE_CONT(0.9)
                       WITHIN GROUP (ORDER BY response_time_seconds)
                FROM dispatch_logs
                WHERE dispatched_at >= NOW() - INTERVAL 24 HOUR
            """).fetchone()

            avg_rt = round(rt_row[0] or 0, 1)
            p90_rt = round(rt_row[1] or 0, 1)

            busiest = con.execute("""
                SELECT district, COUNT(*) AS cnt
                FROM incidents
                WHERE reported_at >= NOW() - INTERVAL 24 HOUR
                GROUP BY 1 ORDER BY 2 DESC LIMIT 1
            """).fetchone()

            agency_rows = con.execute("""
                SELECT agency, COUNT(*) AS cnt
                FROM incidents
                WHERE reported_at >= NOW() - INTERVAL 24 HOUR
                GROUP BY 1
            """).fetchdf()

        all_anomalies = self.run_all()
        return {
            "total_incidents_24h": inc_24h,
            "total_incidents_7d": inc_7d,
            "avg_response_time_24h_s": avg_rt,
            "p90_response_time_24h_s": p90_rt,
            "active_anomalies": len(all_anomalies),
            "critical_anomalies": sum(1 for a in all_anomalies if a.severity == AnomalySeverity.CRITICAL),
            "busiest_district": busiest[0] if busiest else "N/A",
            "agency_breakdown": dict(zip(agency_rows["agency"], agency_rows["cnt"].astype(int))),
        }

    # ------------------------------------------------------------------
    # Run all detectors
    # ------------------------------------------------------------------

    def run_all(self) -> list[Anomaly]:
        results = []
        for method in [
            self.detect_volume_spikes,
            self.detect_response_time_spikes,
            self.detect_geographic_hotspots,
            self.detect_resource_gaps,
        ]:
            try:
                found = method()
                results.extend(found)
                log.info("%s: %d anomalies", method.__name__, len(found))
            except Exception as exc:
                log.warning("Detector %s failed: %s", method.__name__, exc)

        # Sort: CRITICAL first, then by z-score desc
        severity_order = {AnomalySeverity.CRITICAL: 0, AnomalySeverity.HIGH: 1,
                          AnomalySeverity.MEDIUM: 2, AnomalySeverity.LOW: 3}
        results.sort(key=lambda a: (severity_order[a.severity], -(a.z_score or 0)))
        return results
