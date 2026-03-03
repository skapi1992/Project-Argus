"""
Argus JSON Export — Reads processed military data from SQLite and generates
static JSON files for the GitHub Pages frontend.
"""

import json
import logging
import os
import sqlite3
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("argus.export")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "argus.db")
DATA_DIR = os.path.join(BASE_DIR, "docs", "data")

TREND_DAYS = 7
TREND_INTERVAL_SECONDS = 3600  # hourly granularity


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def export_live(conn):
    """Export the latest military aircraft positions to live.json."""
    row = conn.execute("SELECT MAX(timestamp) FROM military_positions").fetchone()
    latest_ts = row[0] if row and row[0] else None

    aircraft = []
    timestamp = int(time.time())

    if latest_ts:
        timestamp = latest_ts
        rows = conn.execute(
            """SELECT icao24, callsign, origin_country, latitude, longitude,
                      baro_altitude, velocity, true_track, on_ground
               FROM military_positions WHERE timestamp = ?""",
            (latest_ts,),
        ).fetchall()

        for r in rows:
            aircraft.append({
                "icao24": r[0],
                "callsign": r[1].strip() if r[1] else None,
                "country": r[2],
                "lat": r[3],
                "lon": r[4],
                "altitude": r[5],
                "velocity": r[6],
                "heading": r[7],
                "on_ground": bool(r[8]),
            })

    live_data = {
        "timestamp": timestamp,
        "count": len(aircraft),
        "aircraft": aircraft,
    }

    path = os.path.join(DATA_DIR, "live.json")
    with open(path, "w") as f:
        json.dump(live_data, f, separators=(",", ":"))
    logger.info("Exported live.json: %d aircraft", len(aircraft))


def export_trend(conn):
    """Export hourly aircraft counts for the last 7 days to trend.json."""
    now = int(time.time())
    cutoff = now - (TREND_DAYS * 86400)

    rows = conn.execute(
        """SELECT timestamp, total_count, country_breakdown
           FROM military_counts
           WHERE timestamp >= ?
           ORDER BY timestamp ASC""",
        (cutoff,),
    ).fetchall()

    # Bucket into hourly intervals
    hourly = {}
    for ts, count, breakdown in rows:
        bucket = (ts // TREND_INTERVAL_SECONDS) * TREND_INTERVAL_SECONDS
        if bucket not in hourly or ts > hourly[bucket]["ts"]:
            hourly[bucket] = {"ts": ts, "count": count, "breakdown": breakdown}

    points = []
    for bucket in sorted(hourly.keys()):
        entry = hourly[bucket]
        points.append({
            "timestamp": bucket,
            "count": entry["count"],
        })

    trend_data = {
        "period_days": TREND_DAYS,
        "interval": "hourly",
        "points": points,
    }

    path = os.path.join(DATA_DIR, "trend.json")
    with open(path, "w") as f:
        json.dump(trend_data, f, separators=(",", ":"))
    logger.info("Exported trend.json: %d data points", len(points))


def export_stats(conn):
    """Export summary statistics to stats.json."""
    now = int(time.time())

    # Current count (latest snapshot)
    row = conn.execute(
        "SELECT total_count FROM military_counts ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    current_count = row[0] if row else 0

    # 24-hour average
    cutoff_24h = now - 86400
    row = conn.execute(
        "SELECT AVG(total_count) FROM military_counts WHERE timestamp >= ?",
        (cutoff_24h,),
    ).fetchone()
    avg_24h = round(row[0], 1) if row and row[0] is not None else 0

    # 7-day average
    cutoff_7d = now - (7 * 86400)
    row = conn.execute(
        "SELECT AVG(total_count) FROM military_counts WHERE timestamp >= ?",
        (cutoff_7d,),
    ).fetchone()
    avg_7d = round(row[0], 1) if row and row[0] is not None else 0

    # 30-day average
    cutoff_30d = now - (30 * 86400)
    row = conn.execute(
        "SELECT AVG(total_count) FROM military_counts WHERE timestamp >= ?",
        (cutoff_30d,),
    ).fetchone()
    avg_30d = round(row[0], 1) if row and row[0] is not None else 0

    # All-time peak and low
    row = conn.execute(
        "SELECT MAX(total_count), MIN(total_count) FROM military_counts"
    ).fetchone()
    peak = row[0] if row and row[0] is not None else 0
    low = row[1] if row and row[1] is not None else 0

    # Latest timestamp
    row = conn.execute(
        "SELECT MAX(timestamp) FROM military_counts"
    ).fetchone()
    last_updated = row[0] if row and row[0] else int(time.time())

    # Country breakdown from latest snapshot
    row = conn.execute(
        "SELECT country_breakdown FROM military_counts ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    country_breakdown = {}
    if row and row[0]:
        try:
            country_breakdown = json.loads(row[0])
        except json.JSONDecodeError:
            pass

    # Sort countries by count descending
    sorted_countries = sorted(
        country_breakdown.items(), key=lambda x: x[1], reverse=True
    )

    stats_data = {
        "current_count": current_count,
        "avg_24h": avg_24h,
        "avg_7d": avg_7d,
        "avg_30d": avg_30d,
        "peak": peak,
        "low": low,
        "last_updated": last_updated,
        "countries": [{"name": k, "count": v} for k, v in sorted_countries],
    }

    path = os.path.join(DATA_DIR, "stats.json")
    with open(path, "w") as f:
        json.dump(stats_data, f, separators=(",", ":"))
    logger.info("Exported stats.json: current=%d, peak=%d", current_count, peak)


def main():
    ensure_data_dir()

    conn = sqlite3.connect(DB_PATH)
    try:
        export_live(conn)
        export_trend(conn)
        export_stats(conn)
        logger.info("All exports complete")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
