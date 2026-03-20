/**
 * Project Argus — Cloudflare Worker
 *
 * Replaces the GitHub Actions pipeline entirely. Runs on a cron schedule
 * every 5 minutes to:
 *   1. Fetch military aircraft data from ADS-B APIs
 *   2. Filter to Eastern Europe bounding box
 *   3. Identify military aircraft via ICAO ranges & callsign prefixes
 *   4. Update state in Cloudflare KV
 *   5. Generate JSON files and commit to GitHub via API
 *
 * Required bindings:
 *   - KV namespace: ARGUS_KV
 *   - Secret: GITHUB_PAT (GitHub personal access token with repo scope)
 *
 * Environment variables:
 *   - GITHUB_OWNER (default: "skapi1992")
 *   - GITHUB_REPO  (default: "Project-Argus")
 *   - GITHUB_BRANCH (default: "claude/github-pages-hello-world-FhDLI")
 */

// ─── Configuration ──────────────────────────────────────────────────────────

const ADSB_API_URLS = [
  "https://api.airplanes.live/v2/mil",
  "https://api.adsb.one/v2/mil",
  "https://api.adsb.fi/v2/mil",
  "https://api.adsb.lol/v2/mil",
];

const BBOX = { lat_min: 48.0, lat_max: 56.0, lon_min: 12.0, lon_max: 26.0 };

const FEET_TO_METERS = 0.3048;
const KNOTS_TO_MS = 0.514444;
const FPM_TO_MS = 0.00508;

const TREND_DAYS = 7;
const TREND_INTERVAL_SECONDS = 3600;
const HISTORY_DAYS = 7;
const MAX_COUNTS_AGE_DAYS = 7;

// ─── Military Reference Data ────────────────────────────────────────────────

const ICAO_RANGES = [
  { country: "Poland", start: 0x48D800, end: 0x48D8FF },
  { country: "USA", start: 0xADF7C8, end: 0xAE1487 },
  { country: "USA", start: 0xAE1488, end: 0xAE6E45 },
  { country: "Germany", start: 0x3F4000, end: 0x3F7FFF },
  { country: "United Kingdom", start: 0x43C000, end: 0x43CFFF },
  { country: "France", start: 0x3B0000, end: 0x3BFFFF },
  { country: "NATO", start: 0x478000, end: 0x4781FF },
  { country: "Czech Republic", start: 0x498000, end: 0x4983FF },
  { country: "Slovakia", start: 0x506000, end: 0x5063FF },
  { country: "Lithuania", start: 0x4F0000, end: 0x4F03FF },
  { country: "Sweden", start: 0x4A8000, end: 0x4A9FFF },
  { country: "Italy", start: 0x33E000, end: 0x33EFFF },
  { country: "Spain", start: 0x340000, end: 0x343FFF },
  { country: "Turkey", start: 0x4B8000, end: 0x4B8FFF },
  { country: "Norway", start: 0x478800, end: 0x4789FF },
  { country: "Denmark", start: 0x458000, end: 0x4581FF },
  { country: "Netherlands", start: 0x480000, end: 0x4803FF },
  { country: "Belgium", start: 0x448000, end: 0x4481FF },
  { country: "Canada", start: 0xC0CDF9, end: 0xC0CFF8 },
  { country: "Romania", start: 0x4A0000, end: 0x4A03FF },
];

const CALLSIGN_PREFIXES = [
  { prefix: "PLF", country: "Poland" },
  { prefix: "RCH", country: "USA" },
  { prefix: "DUKE", country: "USA" },
  { prefix: "EVAC", country: "USA" },
  { prefix: "REACH", country: "USA" },
  { prefix: "MOOSE", country: "USA" },
  { prefix: "VALOR", country: "USA" },
  { prefix: "TOPCAT", country: "USA" },
  { prefix: "RRR", country: "United Kingdom" },
  { prefix: "ASCOT", country: "United Kingdom" },
  { prefix: "GAF", country: "Germany" },
  { prefix: "FAF", country: "France" },
  { prefix: "CTM", country: "France" },
  { prefix: "MMF", country: "France" },
  { prefix: "IAM", country: "Italy" },
  { prefix: "NATO", country: "NATO" },
  { prefix: "FORTE", country: "USA" },
  { prefix: "HOMER", country: "USA" },
  { prefix: "LAGR", country: "USA" },
  { prefix: "JAKE", country: "USA" },
  { prefix: "NCHO", country: "NATO" },
  { prefix: "TARTN", country: "United Kingdom" },
  { prefix: "SHF", country: "Sweden" },
  { prefix: "BAF", country: "Belgium" },
  { prefix: "HRZ", country: "Croatia" },
  { prefix: "CEF", country: "Czech Republic" },
  { prefix: "DAF", country: "Denmark" },
  { prefix: "HAF", country: "Greece" },
  { prefix: "HUF", country: "Hungary" },
  { prefix: "NAF", country: "Netherlands" },
  { prefix: "NOW", country: "Norway" },
  { prefix: "PAF", country: "Portugal" },
  { prefix: "ROF", country: "Romania" },
  { prefix: "SAF", country: "Slovakia" },
];

const HVA_CALLSIGN_PREFIXES = [
  "NATO", "MAGIC", "DARKSTAR", "DISCO",
  "LAGR", "STEEL", "GOLD", "ETHYL",
  "JAKE", "OLIVE", "HOMER",
];

// ─── Helpers ────────────────────────────────────────────────────────────────

function inBbox(lat, lon) {
  if (lat == null || lon == null) return false;
  return lat >= BBOX.lat_min && lat <= BBOX.lat_max &&
         lon >= BBOX.lon_min && lon <= BBOX.lon_max;
}

function isMilitaryIcao(icao24) {
  const addr = parseInt(icao24, 16);
  if (isNaN(addr)) return { match: false, country: null };
  for (const r of ICAO_RANGES) {
    if (addr >= r.start && addr <= r.end) {
      return { match: true, country: r.country };
    }
  }
  return { match: false, country: null };
}

function isMilitaryCallsign(callsign) {
  if (!callsign) return { match: false, country: null };
  const cs = callsign.trim().toUpperCase();
  for (const p of CALLSIGN_PREFIXES) {
    if (cs.startsWith(p.prefix)) {
      return { match: true, country: p.country };
    }
  }
  return { match: false, country: null };
}

function utcDateStr(ts) {
  return new Date(ts * 1000).toISOString().slice(0, 10);
}

function utcIsoStr(ts) {
  return new Date(ts * 1000).toISOString().replace(/\.\d{3}Z$/, "Z");
}

// ─── Step 1: Fetch ADS-B Data ───────────────────────────────────────────────

async function fetchAdsbData() {
  for (const url of ADSB_API_URLS) {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        console.log(`Trying ${url} (attempt ${attempt + 1}/2)`);
        const resp = await fetch(url, {
          headers: { "User-Agent": "Project-Argus-Worker/1.0" },
          signal: AbortSignal.timeout(15000),
        });
        if (!resp.ok) {
          console.log(`HTTP ${resp.status} from ${url}`);
          continue;
        }
        const data = await resp.json();
        const count = (data.ac || []).length;
        console.log(`Success: ${count} military aircraft from ${url}`);
        return data;
      } catch (e) {
        console.log(`Failed: ${url} — ${e.message}`);
        if (attempt === 0) {
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    }
  }
  console.error("All ADS-B APIs failed");
  return null;
}

// ─── Step 2: Filter & Identify Military ─────────────────────────────────────

function processAircraftData(data) {
  const apiTimestamp = Math.floor((data.now || Date.now()) / 1000);
  const aircraft = data.ac || [];
  const military = [];
  const countryCounts = {};

  for (const ac of aircraft) {
    const lat = ac.lat;
    const lon = ac.lon;
    if (!inBbox(lat, lon)) continue;

    let icao24 = ac.hex || "";
    if (icao24.startsWith("~")) icao24 = icao24.slice(1);

    // Determine altitude
    const altBaro = ac.alt_baro;
    const onGround = altBaro === "ground";
    const altMeters = typeof altBaro === "number" ? altBaro * FEET_TO_METERS : null;

    // Velocity
    const gs = ac.gs;
    const velocityMs = typeof gs === "number" ? gs * KNOTS_TO_MS : null;

    // Heading
    const heading = ac.track || null;

    // Callsign
    const callsign = (ac.flight || "").trim() || null;

    // Military detection
    let detectedCountry = null;
    const icaoResult = isMilitaryIcao(icao24);
    if (icaoResult.match) {
      detectedCountry = icaoResult.country;
    } else {
      const csResult = isMilitaryCallsign(callsign);
      if (csResult.match) {
        detectedCountry = csResult.country;
      } else {
        continue; // Not military
      }
    }

    const country = detectedCountry || "Unknown";
    countryCounts[country] = (countryCounts[country] || 0) + 1;

    military.push({
      icao24,
      callsign,
      country,
      lat,
      lon,
      altitude: altMeters,
      velocity: velocityMs,
      heading,
      on_ground: onGround,
    });
  }

  console.log(`Processed: ${military.length} military aircraft in bbox`);
  return { timestamp: apiTimestamp, aircraft: military, countryCounts };
}

// ─── Step 3: State Management (KV) ─────────────────────────────────────────

async function loadState(kv) {
  const [stateRaw, trendRaw] = await Promise.all([
    kv.get("state", "json"),
    kv.get("trend", "json"),
  ]);

  return {
    // Recent 5-min counts for averages and alert evaluation
    counts: (stateRaw && stateRaw.counts) || [],
    // Persistent peak/low
    peak: (stateRaw && stateRaw.peak) || 0,
    low: (stateRaw && stateRaw.low) || 0,
    // Trend data (hourly buckets)
    trend: (trendRaw && trendRaw.points) || [],
  };
}

async function saveState(kv, state, todayStr) {
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - MAX_COUNTS_AGE_DAYS * 86400;

  // Prune old counts
  const recentCounts = state.counts.filter(c => c.timestamp >= cutoff);

  await Promise.all([
    kv.put("state", JSON.stringify({
      counts: recentCounts,
      peak: state.peak,
      low: state.low,
    })),
    kv.put("trend", JSON.stringify({
      period_days: TREND_DAYS,
      interval: "hourly",
      points: state.trend,
    })),
  ]);
}

// ─── Step 4: Generate Exports ───────────────────────────────────────────────

function generateLive(snapshot) {
  return {
    timestamp: snapshot.timestamp,
    count: snapshot.aircraft.length,
    aircraft: snapshot.aircraft,
  };
}

function updateTrend(state, snapshot) {
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - TREND_DAYS * 86400;

  // Build map from existing points
  const bucketMap = {};
  for (const p of state.trend) {
    if (p.timestamp >= cutoff) {
      bucketMap[p.timestamp] = p.count;
    }
  }

  // Add current data point
  const bucket = Math.floor(snapshot.timestamp / TREND_INTERVAL_SECONDS) * TREND_INTERVAL_SECONDS;
  bucketMap[bucket] = snapshot.aircraft.length;

  // Convert back to sorted array
  const points = Object.keys(bucketMap)
    .map(Number)
    .sort((a, b) => a - b)
    .map(ts => ({ timestamp: ts, count: bucketMap[ts] }));

  state.trend = points;

  return {
    period_days: TREND_DAYS,
    interval: "hourly",
    points,
  };
}

function evaluateAlerts(state, currentCount, avg24h, avg7d, peak, now) {
  const alerts = [];

  // Rule 1: Count above 24h average by 50%+
  if (avg24h > 0 && currentCount > avg24h * 1.5) {
    const ratio = Math.round((currentCount / avg24h) * 10) / 10;
    alerts.push({
      level: "ELEVATED",
      rule: "count_above_average",
      message: `${currentCount} aircraft — ${ratio}x above 24h avg (${avg24h})`,
      triggered_at: now,
    });
  }

  // Rule 2: Count exceeds 7-day peak
  if (peak && currentCount > peak) {
    alerts.push({
      level: "HIGH",
      rule: "count_exceeds_peak",
      message: `${currentCount} aircraft exceeds 7-day peak (${peak})`,
      triggered_at: now,
    });
  }

  // Rule 3: Surge detection — count doubled within last 30 minutes
  const cutoff30m = now - 1800;
  const recent30m = state.counts.filter(c => c.timestamp >= cutoff30m);
  if (recent30m.length > 0) {
    const prevMin = Math.min(...recent30m.map(c => c.count));
    if (prevMin > 0 && currentCount >= prevMin * 2) {
      alerts.push({
        level: "CRITICAL",
        rule: "count_surge",
        message: `Surge detected: ${currentCount} aircraft (was ${prevMin} within 30 min)`,
        triggered_at: now,
      });
    }
  }

  // Rule 4: High-value asset detection (≥ 3)
  // We check current aircraft for HVA callsign prefixes
  // (aircraft list is passed via state._currentAircraft)
  if (state._currentAircraft) {
    const hvaNames = [];
    for (const ac of state._currentAircraft) {
      const cs = (ac.callsign || "").toUpperCase();
      if (HVA_CALLSIGN_PREFIXES.some(p => cs.startsWith(p))) {
        hvaNames.push(cs);
      }
    }
    if (hvaNames.length >= 3) {
      alerts.push({
        level: "HIGH",
        rule: "high_value_assets",
        message: `${hvaNames.length} high-value assets airborne (${hvaNames.slice(0, 4).join(", ")})`,
        triggered_at: now,
      });
    }
  }

  // Rule 5: New country detected (not seen in last 7 days)
  const cutoff7d = now - 7 * 86400;
  const recentCountries = new Set();
  for (const c of state.counts) {
    if (c.timestamp >= cutoff7d && c.timestamp < now - 600) {
      if (c.countries) {
        for (const country of Object.keys(c.countries)) {
          recentCountries.add(country);
        }
      }
    }
  }
  if (state._currentCountries && recentCountries.size > 0) {
    const currentCountries = Object.keys(state._currentCountries);
    const newCountries = currentCountries.filter(c => !recentCountries.has(c));
    if (newCountries.length > 0) {
      alerts.push({
        level: "ELEVATED",
        rule: "new_country_detected",
        message: `New origin ${newCountries.length > 1 ? "countries" : "country"}: ${newCountries.sort().join(", ")}`,
        triggered_at: now,
      });
    }
  }

  // Sort by severity
  const levelOrder = { CRITICAL: 0, HIGH: 1, ELEVATED: 2 };
  alerts.sort((a, b) => (levelOrder[a.level] || 9) - (levelOrder[b.level] || 9));

  if (alerts.length > 0) {
    console.log(`Alerts triggered: ${alerts.map(a => a.rule).join(", ")}`);
  }

  return alerts;
}

function generateStats(state, snapshot) {
  const now = Math.floor(Date.now() / 1000);
  const currentCount = snapshot.aircraft.length;

  // Add current count to state
  state.counts.push({
    timestamp: snapshot.timestamp,
    count: currentCount,
    countries: snapshot.countryCounts,
  });

  // Compute averages from counts history
  const cutoff24h = now - 86400;
  const cutoff7d = now - 7 * 86400;
  const cutoff30d = now - 30 * 86400;

  const counts24h = state.counts.filter(c => c.timestamp >= cutoff24h).map(c => c.count);
  const counts7d = state.counts.filter(c => c.timestamp >= cutoff7d).map(c => c.count);
  const counts30d = state.counts.filter(c => c.timestamp >= cutoff30d).map(c => c.count);

  // Also use trend data for better coverage
  const trendCounts24h = state.trend.filter(p => p.timestamp >= cutoff24h).map(p => p.count);
  const trendCounts7d = state.trend.filter(p => p.timestamp >= cutoff7d).map(p => p.count);

  const all24h = counts24h.length > trendCounts24h.length ? counts24h : trendCounts24h;
  const all7d = counts7d.length > trendCounts7d.length ? counts7d : trendCounts7d;

  const avg = arr => arr.length > 0 ? Math.round((arr.reduce((a, b) => a + b, 0) / arr.length) * 10) / 10 : currentCount;

  const avg24h = avg(all24h);
  const avg7d = avg(all7d);
  const avg30d = avg(counts30d);

  // Peak/low — merge with persistent values
  const allCounts = state.counts.map(c => c.count);
  const runPeak = allCounts.length > 0 ? Math.max(...allCounts) : 0;
  const runLow = allCounts.filter(c => c > 0).length > 0 ? Math.min(...allCounts.filter(c => c > 0)) : 0;

  // Also consider trend data
  const trendValues = state.trend.map(p => p.count);
  const trendPeak = trendValues.length > 0 ? Math.max(...trendValues) : 0;
  const trendLow = trendValues.filter(v => v > 0).length > 0 ? Math.min(...trendValues.filter(v => v > 0)) : 0;

  state.peak = Math.max(state.peak, runPeak, trendPeak);
  if (runLow > 0) {
    state.low = state.low > 0 ? Math.min(state.low, runLow) : runLow;
  }
  if (trendLow > 0) {
    state.low = state.low > 0 ? Math.min(state.low, trendLow) : trendLow;
  }

  // Country breakdown from current snapshot
  const sortedCountries = Object.entries(snapshot.countryCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ name, count }));

  // Stash current data for alert evaluation
  state._currentAircraft = snapshot.aircraft;
  state._currentCountries = snapshot.countryCounts;

  const alerts = evaluateAlerts(state, currentCount, avg24h, avg7d, state.peak, now);

  // Clean up temp fields
  delete state._currentAircraft;
  delete state._currentCountries;

  return {
    current_count: currentCount,
    avg_24h: avg24h,
    avg_7d: avg7d,
    avg_30d: avg30d,
    peak: state.peak,
    low: state.low,
    last_updated: snapshot.timestamp,
    countries: sortedCountries,
    alerts,
  };
}

async function generateHistory(kv, snapshot) {
  const now = Math.floor(Date.now() / 1000);
  const todayStr = utcDateStr(now);
  const cutoffDate = now - HISTORY_DAYS * 86400;

  // Load today's existing history from KV
  const existingRaw = await kv.get(`history_${todayStr}`, "json");
  const existing = existingRaw ? existingRaw.snapshots || [] : [];

  // Build snapshot map (timestamp -> aircraft)
  const snapshotMap = {};
  for (const s of existing) {
    snapshotMap[s.timestamp] = s.aircraft;
  }

  // Add current snapshot
  snapshotMap[snapshot.timestamp] = snapshot.aircraft;

  // Convert back to sorted array
  const snapshots = Object.keys(snapshotMap)
    .map(Number)
    .sort((a, b) => a - b)
    .map(ts => ({ timestamp: ts, aircraft: snapshotMap[ts] }));

  const todayData = { date: todayStr, snapshots };

  // Save today's history to KV
  await kv.put(`history_${todayStr}`, JSON.stringify(todayData));

  // Load and update the history index
  const indexRaw = await kv.get("history_index", "json");
  const existingDates = new Set((indexRaw && indexRaw.dates) || []);
  existingDates.add(todayStr);

  // Prune old dates
  const validDates = [];
  for (const d of existingDates) {
    const dateTs = new Date(d + "T00:00:00Z").getTime() / 1000;
    if (dateTs >= cutoffDate - 86400) {
      validDates.push(d);
    }
  }
  validDates.sort();

  const indexData = { dates: validDates };
  await kv.put("history_index", JSON.stringify(indexData));

  // Build history files to commit (today + index)
  const historyFiles = {
    [`docs/data/history/${todayStr}.json`]: todayData,
    ["docs/data/history/index.json"]: indexData,
  };

  // Also load and include other recent days' history files for the commit
  for (const d of validDates) {
    if (d !== todayStr) {
      const dayData = await kv.get(`history_${d}`, "json");
      if (dayData) {
        historyFiles[`docs/data/history/${d}.json`] = dayData;
      }
    }
  }

  return historyFiles;
}

// ─── Step 5: Commit to GitHub ───────────────────────────────────────────────

async function commitToGitHub(env, files) {
  const owner = env.GITHUB_OWNER || "skapi1992";
  const repo = env.GITHUB_REPO || "Project-Argus";
  const branch = env.GITHUB_BRANCH || "claude/github-pages-hello-world-FhDLI";
  const token = env.GITHUB_PAT;

  if (!token) {
    console.error("GITHUB_PAT not configured — skipping commit");
    return;
  }

  const apiBase = `https://api.github.com/repos/${owner}/${repo}`;
  const headers = {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.github+json",
    "User-Agent": "Project-Argus-Worker/1.0",
    "Content-Type": "application/json",
  };

  try {
    // 1. Get the current commit SHA for the branch
    const refResp = await fetch(`${apiBase}/git/ref/heads/${branch}`, { headers });
    if (!refResp.ok) {
      console.error(`Failed to get ref: HTTP ${refResp.status}`);
      return;
    }
    const refData = await refResp.json();
    const latestCommitSha = refData.object.sha;

    // 2. Get the tree SHA from the latest commit
    const commitResp = await fetch(`${apiBase}/git/commits/${latestCommitSha}`, { headers });
    const commitData = await commitResp.json();
    const baseTreeSha = commitData.tree.sha;

    // 3. Create blobs for each file
    const tree = [];
    for (const [path, content] of Object.entries(files)) {
      const blobResp = await fetch(`${apiBase}/git/blobs`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          content: JSON.stringify(content, null, null),
          encoding: "utf-8",
        }),
      });
      if (!blobResp.ok) {
        console.error(`Failed to create blob for ${path}: HTTP ${blobResp.status}`);
        return;
      }
      const blobData = await blobResp.json();
      tree.push({
        path,
        mode: "100644",
        type: "blob",
        sha: blobData.sha,
      });
    }

    // 4. Create a new tree
    const treeResp = await fetch(`${apiBase}/git/trees`, {
      method: "POST",
      headers,
      body: JSON.stringify({ base_tree: baseTreeSha, tree }),
    });
    if (!treeResp.ok) {
      console.error(`Failed to create tree: HTTP ${treeResp.status}`);
      return;
    }
    const treeData = await treeResp.json();

    // 5. Create a new commit
    const timestamp = utcIsoStr(Math.floor(Date.now() / 1000));
    const newCommitResp = await fetch(`${apiBase}/git/commits`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        message: `Update Argus data ${timestamp}`,
        tree: treeData.sha,
        parents: [latestCommitSha],
      }),
    });
    if (!newCommitResp.ok) {
      console.error(`Failed to create commit: HTTP ${newCommitResp.status}`);
      return;
    }
    const newCommitData = await newCommitResp.json();

    // 6. Update the branch reference
    const updateRefResp = await fetch(`${apiBase}/git/refs/heads/${branch}`, {
      method: "PATCH",
      headers,
      body: JSON.stringify({ sha: newCommitData.sha }),
    });
    if (!updateRefResp.ok) {
      console.error(`Failed to update ref: HTTP ${updateRefResp.status}`);
      return;
    }

    console.log(`Committed ${Object.keys(files).length} files: ${newCommitData.sha.slice(0, 7)}`);
  } catch (e) {
    console.error(`GitHub commit failed: ${e.message}`);
  }
}

// ─── Main Pipeline ──────────────────────────────────────────────────────────

async function runPipeline(env) {
  console.log("Pipeline started");

  // 1. Fetch ADS-B data
  const rawData = await fetchAdsbData();
  if (!rawData) {
    console.error("No ADS-B data — aborting");
    return;
  }

  // 2. Process: filter bbox + identify military
  const snapshot = processAircraftData(rawData);

  // 3. Load state from KV
  const state = await loadState(env.ARGUS_KV);

  // 4. Generate all export data
  const liveData = generateLive(snapshot);
  const trendData = updateTrend(state, snapshot);
  const statsData = generateStats(state, snapshot);
  const historyFiles = await generateHistory(env.ARGUS_KV, snapshot);

  // 5. Build file map for GitHub commit
  const files = {
    "docs/data/live.json": liveData,
    "docs/data/stats.json": statsData,
    "docs/data/trend.json": trendData,
    ...historyFiles,
  };

  // 6. Commit to GitHub
  await commitToGitHub(env, files);

  // 7. Save state to KV
  const todayStr = utcDateStr(Math.floor(Date.now() / 1000));
  await saveState(env.ARGUS_KV, state, todayStr);

  console.log(`Pipeline complete: ${snapshot.aircraft.length} military aircraft`);
}

// ─── Worker Entry Point ─────────────────────────────────────────────────────

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Health check endpoint
    if (url.pathname === "/health") {
      const state = await loadState(env.ARGUS_KV);
      const lastCount = state.counts.length > 0 ? state.counts[state.counts.length - 1] : null;
      return new Response(JSON.stringify({
        status: "ok",
        last_run: lastCount ? lastCount.timestamp : null,
        last_count: lastCount ? lastCount.count : null,
        total_trend_points: state.trend.length,
        peak: state.peak,
      }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // Manual trigger (POST /run)
    if (url.pathname === "/run" && request.method === "POST") {
      await runPipeline(env);
      return new Response(JSON.stringify({ status: "triggered" }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response("Project Argus Worker", { status: 200 });
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runPipeline(env));
  },
};
