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
HISTORY_DIR = os.path.join(DATA_DIR, "history")

TREND_DAYS = 7
TREND_INTERVAL_SECONDS = 3600  # hourly granularity
HISTORY_DAYS = 7


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(HISTORY_DIR, exist_ok=True)


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


def load_existing_trend():
    """Load previously committed trend.json to preserve historical data."""
    path = os.path.join(DATA_DIR, "trend.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            data = json.load(f)
        # Return as dict keyed by timestamp for easy merging
        return {p["timestamp"]: p["count"] for p in data.get("points", [])}
    except (json.JSONDecodeError, KeyError):
        return {}


def export_trend(conn):
    """Export hourly aircraft counts for the last 7 days to trend.json.

    Merges new data from SQLite with previously committed trend.json so that
    historical data survives across fresh GitHub Actions runs.
    """
    now = int(time.time())
    cutoff = now - (TREND_DAYS * 86400)

    # Start with previously committed data points
    merged = load_existing_trend()

    # Layer in fresh data from this run's SQLite
    rows = conn.execute(
        """SELECT timestamp, total_count, country_breakdown
           FROM military_counts
           WHERE timestamp >= ?
           ORDER BY timestamp ASC""",
        (cutoff,),
    ).fetchall()

    for ts, count, breakdown in rows:
        bucket = (ts // TREND_INTERVAL_SECONDS) * TREND_INTERVAL_SECONDS
        # New data wins over old for the same bucket
        merged[bucket] = count

    # Prune anything older than 7 days
    merged = {ts: count for ts, count in merged.items() if ts >= cutoff}

    points = [{"timestamp": ts, "count": merged[ts]} for ts in sorted(merged)]

    trend_data = {
        "period_days": TREND_DAYS,
        "interval": "hourly",
        "points": points,
    }

    path = os.path.join(DATA_DIR, "trend.json")
    with open(path, "w") as f:
        json.dump(trend_data, f, separators=(",", ":"))
    logger.info("Exported trend.json: %d data points", len(points))


def load_existing_stats():
    """Load previously committed stats.json to preserve historical peak/low."""
    path = os.path.join(DATA_DIR, "stats.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, KeyError):
        return None


def export_stats(conn):
    """Export summary statistics to stats.json.

    Merges with previously committed stats to preserve all-time peak/low
    and rolling averages across fresh GitHub Actions runs.
    """
    now = int(time.time())
    prev = load_existing_stats()

    # Current count (latest snapshot)
    row = conn.execute(
        "SELECT total_count FROM military_counts ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    current_count = row[0] if row else 0

    # 24-hour average — use trend data for better coverage
    avg_24h = current_count
    avg_7d = current_count
    avg_30d = current_count

    cutoff_24h = now - 86400
    row = conn.execute(
        "SELECT AVG(total_count) FROM military_counts WHERE timestamp >= ?",
        (cutoff_24h,),
    ).fetchone()
    if row and row[0] is not None:
        avg_24h = round(row[0], 1)

    cutoff_7d = now - (7 * 86400)
    row = conn.execute(
        "SELECT AVG(total_count) FROM military_counts WHERE timestamp >= ?",
        (cutoff_7d,),
    ).fetchone()
    if row and row[0] is not None:
        avg_7d = round(row[0], 1)

    cutoff_30d = now - (30 * 86400)
    row = conn.execute(
        "SELECT AVG(total_count) FROM military_counts WHERE timestamp >= ?",
        (cutoff_30d,),
    ).fetchone()
    if row and row[0] is not None:
        avg_30d = round(row[0], 1)

    # Compute averages from trend.json for better long-term accuracy
    existing_trend = load_existing_trend()
    if existing_trend:
        points_24h = [c for ts, c in existing_trend.items() if ts >= cutoff_24h]
        points_7d = [c for ts, c in existing_trend.items() if ts >= cutoff_7d]
        if points_24h:
            avg_24h = round(sum(points_24h) / len(points_24h), 1)
        if points_7d:
            avg_7d = round(sum(points_7d) / len(points_7d), 1)

    # Peak and low — merge with previously committed values
    row = conn.execute(
        "SELECT MAX(total_count), MIN(total_count) FROM military_counts"
    ).fetchone()
    run_peak = row[0] if row and row[0] is not None else 0
    run_low = row[1] if row and row[1] is not None else 0

    if prev:
        peak = max(run_peak, prev.get("peak", 0))
        low = min(run_low, prev.get("low", run_low)) if prev.get("low", 0) > 0 else run_low
    else:
        peak = run_peak
        low = run_low

    # Also consider trend history for peak/low
    if existing_trend:
        trend_peak = max(existing_trend.values())
        trend_low = min(existing_trend.values())
        peak = max(peak, trend_peak)
        if trend_low > 0:
            low = min(low, trend_low) if low > 0 else trend_low

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


def load_existing_history(date_str):
    """Load previously committed daily history file to preserve data across CI runs."""
    path = os.path.join(HISTORY_DIR, f"{date_str}.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            data = json.load(f)
        # Return as dict keyed by timestamp for easy merging
        return {s["timestamp"]: s["aircraft"] for s in data.get("snapshots", [])}
    except (json.JSONDecodeError, KeyError):
        return {}


def export_history(conn):
    """Export per-aircraft position snapshots grouped by day for timeline replay.

    Generates one JSON file per day in docs/data/history/YYYY-MM-DD.json,
    plus an index.json listing available dates.

    Always creates a snapshot for the current timestamp (even with 0 aircraft)
    so that history accumulates across CI runs.
    """
    from datetime import datetime, timezone

    now = int(time.time())
    cutoff = now - (HISTORY_DAYS * 86400)

    # Query all military positions within the history window
    rows = conn.execute(
        """SELECT timestamp, icao24, callsign, origin_country, latitude, longitude,
                  baro_altitude, velocity, true_track, on_ground
           FROM military_positions
           WHERE timestamp >= ?
           ORDER BY timestamp ASC""",
        (cutoff,),
    ).fetchall()

    # Group rows by day, then by timestamp within each day
    days = {}  # date_str -> {timestamp -> [aircraft]}
    for r in rows:
        ts = r[0]
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d")

        if date_str not in days:
            days[date_str] = {}

        if ts not in days[date_str]:
            days[date_str][ts] = []

        days[date_str][ts].append({
            "icao24": r[1],
            "callsign": r[2].strip() if r[2] else None,
            "country": r[3],
            "lat": r[4],
            "lon": r[5],
            "altitude": r[6],
            "velocity": r[7],
            "heading": r[8],
            "on_ground": bool(r[9]),
        })

    # Always add a snapshot for "now" so history accumulates even with 0 aircraft
    now_dt = datetime.fromtimestamp(now, tz=timezone.utc)
    today_str = now_dt.strftime("%Y-%m-%d")
    if today_str not in days:
        days[today_str] = {}
    if now not in days[today_str]:
        # Build from live data: use whatever aircraft we have right now
        live_aircraft = []
        latest_row = conn.execute(
            "SELECT MAX(timestamp) FROM military_positions"
        ).fetchone()
        if latest_row and latest_row[0]:
            live_rows = conn.execute(
                """SELECT icao24, callsign, origin_country, latitude, longitude,
                          baro_altitude, velocity, true_track, on_ground
                   FROM military_positions WHERE timestamp = ?""",
                (latest_row[0],),
            ).fetchall()
            for r in live_rows:
                live_aircraft.append({
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
        days[today_str][now] = live_aircraft

    # Merge with previously committed data and write daily files
    all_dates = set()
    for date_str, snapshots in days.items():
        # Merge with existing
        existing = load_existing_history(date_str)
        existing.update(snapshots)  # new data wins

        sorted_ts = sorted(existing.keys())
        day_data = {
            "date": date_str,
            "snapshots": [
                {"timestamp": ts, "aircraft": existing[ts]}
                for ts in sorted_ts
            ],
        }

        path = os.path.join(HISTORY_DIR, f"{date_str}.json")
        with open(path, "w") as f:
            json.dump(day_data, f, separators=(",", ":"))
        all_dates.add(date_str)
        logger.info("Exported history/%s.json: %d snapshots", date_str, len(sorted_ts))

    # Also include dates from existing files that we didn't touch
    for fname in os.listdir(HISTORY_DIR):
        if fname.endswith(".json") and fname != "index.json":
            d = fname[:-5]
            # Prune old files beyond the history window
            try:
                file_date = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                if file_date.timestamp() < cutoff - 86400:
                    os.remove(os.path.join(HISTORY_DIR, fname))
                    logger.info("Pruned old history file: %s", fname)
                else:
                    all_dates.add(d)
            except ValueError:
                pass

    # Write index.json
    index_data = {"dates": sorted(all_dates)}
    path = os.path.join(HISTORY_DIR, "index.json")
    with open(path, "w") as f:
        json.dump(index_data, f, separators=(",", ":"))
    logger.info("Exported history/index.json: %d dates", len(all_dates))


def main():
    ensure_data_dir()

    conn = sqlite3.connect(DB_PATH)
    try:
        export_live(conn)
        export_trend(conn)
        export_stats(conn)
        export_history(conn)
        logger.info("All exports complete")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
