# Project Argus

Military Airspace Observation System — a static, read-only dashboard monitoring military aircraft activity over Poland and neighbouring countries.

## What it does

- Pulls real-time aircraft data from the [ADSB.lol](https://www.adsb.lol) API (free, no auth required)
- Filters for military aircraft using ICAO 24-bit address ranges and known callsign patterns
- Displays an interactive dark-themed map with live military aircraft positions
- Shows trend statistics: current count, daily/weekly averages, peaks, and per-country breakdown
- Runs entirely on GitHub Pages with data updates via GitHub Actions every 2 minutes

## Architecture

| Layer | Description |
|-------|-------------|
| **Ingestion** | `backend/ingest.py` — fetches from ADSB.lol API, stores raw states in SQLite |
| **Processing** | `backend/process.py` — filters military aircraft, computes aggregates |
| **Export** | `backend/export.py` — generates static JSON files for the frontend |
| **Frontend** | `docs/` — pure HTML/CSS/JS dashboard with Leaflet map and Chart.js |
| **Pipeline** | `.github/workflows/argus.yml` — scheduled GitHub Actions workflow |

## Coverage area

Poland and direct neighbours: Germany, Czech Republic, Slovakia, Ukraine, Belarus, Lithuania, Russia (Kaliningrad). Bounding box: 48°N–56°N, 12°E–26°E.

## Running locally

```bash
pip install -r backend/requirements.txt
python backend/ingest.py
python backend/process.py
python backend/export.py
# Then serve docs/ with any static file server
```

## Data sources

All data from [ADSB.lol](https://www.adsb.lol) API (free, no auth, ODbL 1.0 licensed).
# Project-Argus
Personal intelligence feed.
