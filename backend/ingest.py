"""
Argus Data Ingestion — Fetches aircraft state vectors from OpenSky Network API
and stores them in the local SQLite database.
"""

import json
import logging
import os
import sqlite3
import sys
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

OPENSKY_API_URL = "https://opensky-network.org/api/states/all"

# Bounding box: Poland + direct neighbours
BBOX = {
    "lamin": 48.0,
    "lamax": 56.0,
    "lomin": 12.0,
    "lomax": 26.0,
}


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


def fetch_opensky_data():
    """Fetch aircraft states from OpenSky Network API with retry."""
    params = {
        "lamin": BBOX["lamin"],
        "lamax": BBOX["lamax"],
        "lomin": BBOX["lomin"],
        "lomax": BBOX["lomax"],
    }

    for attempt in range(2):
        try:
            resp = requests.get(OPENSKY_API_URL, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning("API request failed (attempt %d): %s", attempt + 1, e)
            if attempt == 0:
                time.sleep(5)

    logger.error("Failed to fetch data from OpenSky after 2 attempts")
    return None


def insert_raw_states(conn, data):
    """Parse and insert raw aircraft states into the database."""
    api_timestamp = data.get("time", int(time.time()))
    states = data.get("states", [])

    if not states:
        logger.info("No aircraft states returned by the API")
        return 0

    rows = []
    for s in states:
        rows.append((
            api_timestamp,
            s[0],             # icao24
            s[1].strip() if s[1] else None,  # callsign
            s[2],             # origin_country
            s[6],             # latitude
            s[5],             # longitude
            s[7],             # baro_altitude
            s[9],             # velocity
            s[10],            # true_track
            s[11],            # vertical_rate
            1 if s[8] else 0, # on_ground
            s[17] if len(s) > 17 else None,  # category
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

        data = fetch_opensky_data()
        if data is None:
            logger.error("No data retrieved — exiting")
            sys.exit(1)

        count = insert_raw_states(conn, data)
        logger.info("Ingestion complete: %d states stored", count)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
