"""
Argus Data Ingestion — Fetches military aircraft state vectors from ADS-B APIs
and stores them in the local SQLite database.

Uses a fallback chain of community ADS-B APIs (airplanes.live, adsb.one,
adsb.fi, adsb.lol) with dedicated /v2/mil endpoints that return pre-tagged
military aircraft worldwide. All APIs use the ADSBx v2 response format.
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

# Fallback chain of ADS-B military endpoints (all ADSBx v2 format)
ADSB_API_URLS = [
    "https://api.airplanes.live/v2/mil",
    "https://api.adsb.one/v2/mil",
    "https://api.adsb.fi/v2/mil",
    "https://api.adsb.lol/v2/mil",
]

# Geographic bounding box for Eastern Europe (Poland + neighbours)
BBOX = {"lat_min": 48.0, "lat_max": 56.0, "lon_min": 12.0, "lon_max": 26.0}

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
    """Fetch military aircraft from ADS-B APIs with fallback chain.

    Tries each API in ADSB_API_URLS sequentially. For each API, retries
    up to 2 times with exponential backoff before moving to the next.
    """
    for url in ADSB_API_URLS:
        for attempt in range(2):
            try:
                logger.info("Trying %s (attempt %d/2)", url, attempt + 1)
                resp = requests.get(url, timeout=30)
                logger.info("Response: HTTP %d, %d bytes", resp.status_code, len(resp.content))
                resp.raise_for_status()
                data = resp.json()
                ac_count = len(data.get("ac", []))
                logger.info("Success: %d military aircraft from %s", ac_count, url)
                return data
            except requests.RequestException as e:
                logger.warning("Failed: %s — %s", url, e)
                if attempt == 0:
                    time.sleep(2)

    logger.error("All ADS-B APIs failed")
    return None


def in_bbox(lat, lon):
    """Check if coordinates fall within the Eastern Europe bounding box."""
    if lat is None or lon is None:
        return False
    return (BBOX["lat_min"] <= lat <= BBOX["lat_max"]
            and BBOX["lon_min"] <= lon <= BBOX["lon_max"])


def insert_raw_states(conn, data):
    """Parse ADSBx v2 response and insert aircraft states into the database.

    Filters worldwide /v2/mil results to our Eastern Europe bounding box.
    """
    api_timestamp = int(data.get("now", time.time() * 1000) / 1000)
    aircraft = data.get("ac", [])

    if not aircraft:
        logger.info("No aircraft states returned by the API")
        return 0

    rows = []
    skipped = 0
    for ac in aircraft:
        lat = ac.get("lat")
        lon = ac.get("lon")

        # Filter to our region of interest
        if not in_bbox(lat, lon):
            skipped += 1
            continue

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
            None,              # origin_country (not in ADSBx v2 format)
            lat,
            lon,
            alt_meters,
            velocity_ms,
            ac.get("track"),
            vrate_ms,
            on_ground,
            ac.get("category"),
        ))

    logger.info("Filtered %d aircraft in bbox, %d outside region", len(rows), skipped)

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
            logger.error("No data retrieved — ingestion failed")
            raise SystemExit(1)

        count = insert_raw_states(conn, data)
        logger.info("Ingestion complete: %d states stored", count)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
