"""
Argus Data Ingestion — Fetches aircraft state vectors from ADSB.lol API
and stores them in the local SQLite database.
"""

import logging
import os
import sqlite3
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("argus.ingest")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "argus.db")

# ADSB.lol bounding box endpoint: Poland + direct neighbours
# latn=north, lats=south, lonw=west, lone=east
ADSB_LOL_URL = "https://api.adsb.lol/v2/latn/56.0/lats/48.0/lonw/12.0/lone/26.0"

# Unit conversion constants
FEET_TO_METERS = 0.3048
KNOTS_TO_MS = 0.514444
FPM_TO_MS = 0.00508  # feet per minute to meters per second


def init_db(conn):
    """Create tables and indices if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            icao24 TEXT NOT NULL,
            callsign TEXT,
            origin_country TEXT,
            latitude REAL,
            longitude REAL,
            baro_altitude REAL,
            velocity REAL,
            true_track REAL,
            vertical_rate REAL,
            on_ground INTEGER,
            category INTEGER
        );

        CREATE TABLE IF NOT EXISTS military_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            icao24 TEXT NOT NULL,
            callsign TEXT,
            origin_country TEXT,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            baro_altitude REAL,
            velocity REAL,
            true_track REAL,
            on_ground INTEGER
        );

        CREATE TABLE IF NOT EXISTS military_counts (
            timestamp INTEGER PRIMARY KEY,
            total_count INTEGER NOT NULL,
            country_breakdown TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw_states(timestamp);
        CREATE INDEX IF NOT EXISTS idx_military_timestamp ON military_positions(timestamp);
        CREATE INDEX IF NOT EXISTS idx_counts_timestamp ON military_counts(timestamp);
    """)


def fetch_adsb_data():
    """Fetch aircraft states from ADSB.lol API with retry."""
    for attempt in range(2):
        try:
            resp = requests.get(ADSB_LOL_URL, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning("API request failed (attempt %d): %s", attempt + 1, e)
            if attempt == 0:
                time.sleep(5)

    logger.error("Failed to fetch data from ADSB.lol after 2 attempts")
    return None


def insert_raw_states(conn, data):
    """Parse ADSB.lol response and insert aircraft states into the database."""
    api_timestamp = int(data.get("now", time.time() * 1000) / 1000)
    aircraft = data.get("ac", [])

    if not aircraft:
        logger.info("No aircraft states returned by the API")
        return 0

    rows = []
    for ac in aircraft:
        icao24 = ac.get("hex", "")
        if icao24.startswith("~"):
            icao24 = icao24[1:]

        alt_baro = ac.get("alt_baro")
        on_ground = 1 if alt_baro == "ground" else 0
        if isinstance(alt_baro, (int, float)):
            alt_meters = alt_baro * FEET_TO_METERS
        else:
            alt_meters = None

        gs = ac.get("gs")
        velocity_ms = gs * KNOTS_TO_MS if isinstance(gs, (int, float)) else None

        baro_rate = ac.get("baro_rate")
        vrate_ms = baro_rate * FPM_TO_MS if isinstance(baro_rate, (int, float)) else None

        rows.append((
            api_timestamp,
            icao24,
            ac.get("flight", "").strip() or None,
            None,              # origin_country (not available in ADSB.lol)
            ac.get("lat"),
            ac.get("lon"),
            alt_meters,
            velocity_ms,
            ac.get("track"),
            vrate_ms,
            on_ground,
            ac.get("category"),
        ))

    conn.executemany(
        """INSERT INTO raw_states
           (timestamp, icao24, callsign, origin_country, latitude, longitude,
            baro_altitude, velocity, true_track, vertical_rate, on_ground, category)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    logger.info("Inserted %d raw aircraft states (timestamp=%d)", len(rows), api_timestamp)
    return len(rows)


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)

        data = fetch_adsb_data()
        if data is None:
            logger.warning("No data retrieved — skipping ingestion")
            return

        count = insert_raw_states(conn, data)
        logger.info("Ingestion complete: %d states stored", count)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
