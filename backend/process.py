"""
Argus Processing Engine — Filters raw aircraft states for military aircraft,
computes aggregates, and enforces data retention policies.
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
logger = logging.getLogger("argus.process")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "argus.db")
BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))

# Retention thresholds
RAW_RETENTION_HOURS = 24
MILITARY_RETENTION_DAYS = 30


def load_icao_ranges():
    """Load military ICAO 24-bit address ranges."""
    path = os.path.join(BACKEND_DIR, "military_icao_ranges.json")
    with open(path, "r") as f:
        data = json.load(f)
    ranges = []
    for r in data["ranges"]:
        ranges.append({
            "country": r["country"],
            "start": int(r["start"], 16),
            "end": int(r["end"], 16),
            "note": r.get("note", ""),
        })
    return ranges


def load_callsign_prefixes():
    """Load military callsign prefixes."""
    path = os.path.join(BACKEND_DIR, "military_callsigns.json")
    with open(path, "r") as f:
        data = json.load(f)
    return data["prefixes"]


def is_military_icao(icao24, ranges):
    """Check if an ICAO24 address falls within known military ranges."""
    try:
        addr = int(icao24, 16)
    except (ValueError, TypeError):
        return False, None
    for r in ranges:
        if r["start"] <= addr <= r["end"]:
            return True, r["country"]
    return False, None


def is_military_callsign(callsign, prefixes):
    """Check if a callsign matches known military prefixes."""
    if not callsign:
        return False, None
    cs = callsign.strip().upper()
    for p in prefixes:
        if cs.startswith(p["prefix"]):
            return True, p["country"]
    return False, None


def process_latest_snapshot(conn, icao_ranges, callsign_prefixes):
    """Filter the latest raw snapshot for military aircraft."""
    # Find the latest timestamp in raw_states
    row = conn.execute(
        "SELECT MAX(timestamp) FROM raw_states"
    ).fetchone()
    latest_ts = row[0] if row and row[0] else None

    if latest_ts is None:
        logger.info("No raw states to process")
        return

    # Check if we already processed this timestamp
    existing = conn.execute(
        "SELECT 1 FROM military_counts WHERE timestamp = ?", (latest_ts,)
    ).fetchone()
    if existing:
        logger.info("Timestamp %d already processed — skipping", latest_ts)
        return

    # Fetch all states for this timestamp
    states = conn.execute(
        """SELECT icao24, callsign, origin_country, latitude, longitude,
                  baro_altitude, velocity, true_track, on_ground
           FROM raw_states WHERE timestamp = ?""",
        (latest_ts,),
    ).fetchall()

    military = []
    country_counts = {}

    for s in states:
        icao24, callsign, origin_country, lat, lon, alt, vel, track, on_ground = s

        # Skip entries without position data
        if lat is None or lon is None:
            continue

        detected_country = None

        # Priority 1: ICAO range match
        is_mil, icao_country = is_military_icao(icao24, icao_ranges)
        if is_mil:
            detected_country = icao_country

        # Priority 2: Callsign match
        if not is_mil:
            is_mil, cs_country = is_military_callsign(callsign, callsign_prefixes)
            if is_mil:
                detected_country = cs_country

        if is_mil:
            military.append((
                latest_ts, icao24, callsign, detected_country or origin_country,
                lat, lon, alt, vel, track, on_ground,
            ))
            country = detected_country or origin_country or "Unknown"
            country_counts[country] = country_counts.get(country, 0) + 1

    # Insert military positions
    if military:
        conn.executemany(
            """INSERT INTO military_positions
               (timestamp, icao24, callsign, origin_country, latitude, longitude,
                baro_altitude, velocity, true_track, on_ground)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            military,
        )

    # Insert aggregate count
    conn.execute(
        """INSERT OR REPLACE INTO military_counts (timestamp, total_count, country_breakdown)
           VALUES (?, ?, ?)""",
        (latest_ts, len(military), json.dumps(country_counts)),
    )

    conn.commit()
    logger.info(
        "Processed timestamp %d: %d military aircraft detected (%d total states)",
        latest_ts, len(military), len(states),
    )


def purge_old_data(conn):
    """Enforce data retention policies."""
    now = int(time.time())

    # Purge raw_states older than 24 hours
    raw_cutoff = now - (RAW_RETENTION_HOURS * 3600)
    cursor = conn.execute("DELETE FROM raw_states WHERE timestamp < ?", (raw_cutoff,))
    if cursor.rowcount:
        logger.info("Purged %d raw states older than %d hours", cursor.rowcount, RAW_RETENTION_HOURS)

    # Purge military_positions older than 30 days
    mil_cutoff = now - (MILITARY_RETENTION_DAYS * 86400)
    cursor = conn.execute("DELETE FROM military_positions WHERE timestamp < ?", (mil_cutoff,))
    if cursor.rowcount:
        logger.info("Purged %d military positions older than %d days", cursor.rowcount, MILITARY_RETENTION_DAYS)

    conn.commit()


def main():
    icao_ranges = load_icao_ranges()
    callsign_prefixes = load_callsign_prefixes()

    logger.info(
        "Loaded %d ICAO ranges and %d callsign prefixes",
        len(icao_ranges), len(callsign_prefixes),
    )

    conn = sqlite3.connect(DB_PATH)
    try:
        process_latest_snapshot(conn, icao_ranges, callsign_prefixes)
        purge_old_data(conn)
        logger.info("Processing complete")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
