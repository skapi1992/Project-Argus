/**
 * Argus — Military Airspace Monitor
 * Frontend application: map, stats, and auto-refresh.
 */

(function () {
    'use strict';

    // Data paths (relative to docs/ root served by GitHub Pages)
    const DATA_BASE = 'data';
    const LIVE_URL = `${DATA_BASE}/live.json`;
    const TREND_URL = `${DATA_BASE}/trend.json`;
    const STATS_URL = `${DATA_BASE}/stats.json`;

    const REFRESH_INTERVAL = 60000; // 60 seconds
    const STALE_WARNING_SECS = 300;   // 5 minutes — amber warning
    const STALE_CRITICAL_SECS = 600;  // 10 minutes — red critical
    const MAP_CENTER = [52.0, 19.5];
    const MAP_ZOOM = 6;

    let map;
    let markersLayer;
    let trendChart;
    let activeDetailIcao = null;
    const aircraftCache = {}; // cache for model + photo lookups

    // Timeline state
    let timelineMode = 'live'; // 'live' or 'history'
    const historyCache = {};   // date string -> { snapshots: [...] }
    let historyDates = [];     // sorted available dates
    let flatSnapshots = [];    // flattened list of {timestamp, date, idx} for slider
    let playInterval = null;
    let refreshTimer = null;

    // --- Map ---

    function initMap() {
        map = L.map('map', {
            center: MAP_CENTER,
            zoom: MAP_ZOOM,
            zoomControl: false,
            attributionControl: false,
        });

        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            maxZoom: 18,
        }).addTo(map);

        markersLayer = L.layerGroup().addTo(map);
    }

    function createMarkerIcon(isNew) {
        return L.divIcon({
            className: '',
            html: `<div class="aircraft-marker${isNew ? ' new' : ''}"></div>`,
            iconSize: [12, 12],
            iconAnchor: [6, 6],
        });
    }

    function formatAltitude(meters) {
        if (meters === null || meters === undefined) return '—';
        const feet = Math.round(meters * 3.28084);
        return `${feet.toLocaleString()} ft`;
    }

    function formatSpeed(ms) {
        if (ms === null || ms === undefined) return '—';
        const knots = Math.round(ms * 1.94384);
        return `${knots} kts`;
    }

    function formatHeading(deg) {
        if (deg === null || deg === undefined) return '—';
        return `${Math.round(deg)}°`;
    }

    function buildTooltip(ac) {
        const callsign = ac.callsign || 'Unknown';
        return `<div class="aircraft-tooltip">
            <div class="tooltip-callsign">${escapeHtml(callsign)}</div>
            <div class="tooltip-detail">${escapeHtml(ac.country || '—')}</div>
            <div class="tooltip-detail">Alt: ${formatAltitude(ac.altitude)}</div>
            <div class="tooltip-detail">Spd: ${formatSpeed(ac.velocity)}</div>
            <div class="tooltip-detail">Hdg: ${formatHeading(ac.heading)}</div>
            <div class="tooltip-detail">ICAO: ${escapeHtml(ac.icao24 || '—')}</div>
        </div>`;
    }

    // --- Aircraft detail panel ---

    function showDetail(ac) {
        var panel = document.getElementById('aircraft-detail');
        var icao = (ac.icao24 || '').toLowerCase();

        activeDetailIcao = icao;

        // Populate known fields immediately
        setText('detail-callsign', ac.callsign || 'Unknown');
        setText('detail-country', ac.country || '—');
        setText('detail-icao', (ac.icao24 || '—').toUpperCase());
        setText('detail-alt', formatAltitude(ac.altitude));
        setText('detail-spd', formatSpeed(ac.velocity));
        setText('detail-hdg', formatHeading(ac.heading));
        setText('detail-status', ac.on_ground ? 'On Ground' : 'Airborne');

        // Reset photo + model while loading
        var photoWrap = document.getElementById('detail-photo-wrap');
        var placeholder = document.getElementById('detail-photo-placeholder');
        photoWrap.querySelectorAll('img, .detail-photo-credit').forEach(function (el) { el.remove(); });
        placeholder.style.display = 'flex';
        placeholder.textContent = 'Loading…';

        var modelEl = document.getElementById('detail-model');
        modelEl.innerHTML = '<span class="loading-dots">Looking up aircraft</span>';

        panel.classList.add('visible');

        // Check cache first
        if (aircraftCache[icao]) {
            applyDetailData(aircraftCache[icao]);
            return;
        }

        // Fetch aircraft type + photo in parallel
        var result = { model: null, photo: null, photographer: null };

        var typePromise = fetch('https://hexdb.io/api/v1/aircraft/' + icao)
            .then(function (r) { return r.ok ? r.json() : null; })
            .then(function (data) {
                if (data) {
                    var parts = [];
                    if (data.Manufacturer) parts.push(data.Manufacturer);
                    if (data.Type) parts.push(data.Type);
                    if (data.RegisteredOwners) parts.push('(' + data.RegisteredOwners + ')');
                    result.model = parts.join(' ') || null;
                }
            })
            .catch(function () { /* ignore */ });

        var photoPromise = fetch('https://api.planespotters.net/pub/photos/hex/' + icao)
            .then(function (r) { return r.ok ? r.json() : null; })
            .then(function (data) {
                if (data && data.photos && data.photos.length) {
                    var photo = data.photos[0];
                    result.photo = photo.thumbnail_large ? photo.thumbnail_large.src : (photo.thumbnail ? photo.thumbnail.src : null);
                    result.photographer = photo.photographer || null;
                }
            })
            .catch(function () { /* ignore */ });

        Promise.all([typePromise, photoPromise]).then(function () {
            aircraftCache[icao] = result;
            // Only apply if this aircraft is still selected
            if (activeDetailIcao === icao) {
                applyDetailData(result);
            }
        });
    }

    function applyDetailData(data) {
        var modelEl = document.getElementById('detail-model');
        modelEl.textContent = data.model || 'Unknown aircraft type';

        var photoWrap = document.getElementById('detail-photo-wrap');
        var placeholder = document.getElementById('detail-photo-placeholder');

        if (data.photo) {
            placeholder.style.display = 'none';
            // Remove any existing image
            photoWrap.querySelectorAll('img, .detail-photo-credit').forEach(function (el) { el.remove(); });
            var img = document.createElement('img');
            img.src = data.photo;
            img.alt = 'Aircraft photo';
            img.loading = 'lazy';
            photoWrap.appendChild(img);
            if (data.photographer) {
                var credit = document.createElement('span');
                credit.className = 'detail-photo-credit';
                credit.textContent = data.photographer;
                photoWrap.appendChild(credit);
            }
        } else {
            placeholder.style.display = 'flex';
            placeholder.textContent = 'No photo available';
        }
    }

    function hideDetail() {
        document.getElementById('aircraft-detail').classList.remove('visible');
        activeDetailIcao = null;
    }

    function initDetailPanel() {
        document.getElementById('detail-close').addEventListener('click', function (e) {
            e.stopPropagation();
            hideDetail();
        });

        // Close on Escape key
        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') hideDetail();
        });
    }

    function updateMapMarkers(aircraft) {
        markersLayer.clearLayers();

        aircraft.forEach(function (ac) {
            if (ac.lat == null || ac.lon == null) return;

            const marker = L.marker([ac.lat, ac.lon], {
                icon: createMarkerIcon(true),
            });

            marker.bindTooltip(buildTooltip(ac), {
                direction: 'top',
                offset: [0, -8],
                className: '',
            });

            marker.on('click', (function (aircraft) {
                return function () { showDetail(aircraft); };
            })(ac));

            markersLayer.addLayer(marker);
        });
    }

    // --- Chart ---

    // Chart.js plugin: vertical cursor line for timeline position
    const timelineCursorPlugin = {
        id: 'timelineCursor',
        afterDraw: function (chart) {
            if (timelineMode !== 'history') return;
            var cursorTs = chart.options.plugins.timelineCursor &&
                           chart.options.plugins.timelineCursor.timestamp;
            if (!cursorTs) return;

            // Find the closest trend point index by timestamp
            var timestamps = chart._trendTimestamps || [];
            if (!timestamps.length) return;

            var closestIdx = 0;
            var closestDiff = Math.abs(timestamps[0] - cursorTs);
            for (var i = 1; i < timestamps.length; i++) {
                var diff = Math.abs(timestamps[i] - cursorTs);
                if (diff < closestDiff) {
                    closestDiff = diff;
                    closestIdx = i;
                }
            }

            var meta = chart.getDatasetMeta(0);
            if (!meta.data[closestIdx]) return;

            var x = meta.data[closestIdx].x;
            var ctx = chart.ctx;
            var yAxis = chart.scales.y;

            ctx.save();
            ctx.beginPath();
            ctx.moveTo(x, yAxis.top);
            ctx.lineTo(x, yAxis.bottom);
            ctx.lineWidth = 1.5;
            ctx.strokeStyle = 'rgba(212, 168, 83, 0.7)';
            ctx.stroke();

            // Small diamond at the data point
            var y = meta.data[closestIdx].y;
            ctx.beginPath();
            ctx.moveTo(x, y - 4);
            ctx.lineTo(x + 4, y);
            ctx.lineTo(x, y + 4);
            ctx.lineTo(x - 4, y);
            ctx.closePath();
            ctx.fillStyle = '#d4a853';
            ctx.fill();
            ctx.restore();
        },
    };

    function initTrendChart() {
        const ctx = document.getElementById('trend-chart').getContext('2d');

        trendChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    data: [],
                    borderColor: '#d4a853',
                    backgroundColor: 'rgba(212, 168, 83, 0.08)',
                    borderWidth: 1.5,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                    pointHitRadius: 8,
                }],
            },
            plugins: [timelineCursorPlugin],
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    timelineCursor: { timestamp: null },
                    tooltip: {
                        backgroundColor: '#181824',
                        borderColor: 'rgba(255,255,255,0.06)',
                        borderWidth: 1,
                        titleColor: '#8a8a9a',
                        bodyColor: '#e8e8e8',
                        titleFont: { family: 'Inter', size: 11 },
                        bodyFont: { family: 'Inter', size: 13, weight: '500' },
                        padding: 8,
                        displayColors: false,
                        callbacks: {
                            title: function (items) {
                                if (!items.length) return '';
                                return items[0].label;
                            },
                            label: function (item) {
                                return item.raw + ' aircraft';
                            },
                        },
                    },
                },
                scales: {
                    x: {
                        display: true,
                        grid: { display: false },
                        ticks: {
                            color: '#8a8a9a',
                            font: { family: 'Inter', size: 10 },
                            maxRotation: 0,
                            autoSkip: false,
                            callback: function (value, index) {
                                var label = this.getLabelForValue(index);
                                return label || null;
                            },
                        },
                        border: { display: false },
                    },
                    y: {
                        display: true,
                        grid: { display: false },
                        ticks: {
                            color: '#8a8a9a',
                            font: { family: 'Inter', size: 10 },
                            maxTicksLimit: 4,
                            precision: 0,
                        },
                        border: { display: false },
                        beginAtZero: true,
                    },
                },
            },
        });
    }

    function updateTrendChart(trendData) {
        if (!trendData || !trendData.points || !trendData.points.length) return;

        var lastDateStr = '';
        const labels = trendData.points.map(function (p) {
            const d = new Date(p.timestamp * 1000);
            const dateStr = d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
            if (dateStr === lastDateStr) {
                return '';
            }
            lastDateStr = dateStr;
            return dateStr;
        });

        const values = trendData.points.map(function (p) {
            return p.count;
        });

        trendChart._trendTimestamps = trendData.points.map(function (p) { return p.timestamp; });
        trendChart.data.labels = labels;
        trendChart.data.datasets[0].data = values;
        trendChart.update('none');
    }

    // --- Stats ---

    function updateStats(stats) {
        setText('current-count', stats.current_count != null ? stats.current_count : '—');
        setText('avg-24h', stats.avg_24h != null ? stats.avg_24h : '—');
        setText('avg-7d', stats.avg_7d != null ? stats.avg_7d : '—');
        setText('peak', stats.peak != null ? stats.peak : '—');
        setText('low', stats.low != null ? stats.low : '—');

        updateCountryList(stats.countries || []);
        updateLastUpdated(stats.last_updated);
    }

    function updateCountryList(countries) {
        const list = document.getElementById('country-list');
        list.innerHTML = '';

        if (!countries.length) {
            list.innerHTML = '<li class="country-item country-empty">No data yet</li>';
            return;
        }

        countries.forEach(function (c) {
            const li = document.createElement('li');
            li.className = 'country-item';
            li.innerHTML = `<span class="country-name">${escapeHtml(c.name)}</span>
                            <span class="country-count">${c.count}</span>`;
            list.appendChild(li);
        });
    }

    function updateLastUpdated(timestamp) {
        var banner = document.getElementById('stale-banner');
        var headerStatus = document.getElementById('header-status');

        if (!timestamp) {
            setText('last-updated', 'Updated: —');
            if (banner) banner.classList.remove('visible', 'stale-warning', 'stale-critical');
            return;
        }

        const now = Math.floor(Date.now() / 1000);
        const diff = now - timestamp;

        let text;
        if (diff < 60) {
            text = 'just now';
        } else if (diff < 3600) {
            const mins = Math.floor(diff / 60);
            text = mins + ' min ago';
        } else if (diff < 86400) {
            const hours = Math.floor(diff / 3600);
            text = hours + 'h ago';
        } else {
            const days = Math.floor(diff / 86400);
            text = days + 'd ago';
        }

        setText('last-updated', 'Updated: ' + text);

        // Staleness indicator (only in live mode)
        if (timelineMode !== 'live' || !banner) return;

        if (diff >= STALE_CRITICAL_SECS) {
            banner.className = 'stale-banner stale-critical visible';
            banner.textContent = 'Data is stale — last update ' + text;
            if (headerStatus) headerStatus.className = 'header-live stale-critical-dot';
        } else if (diff >= STALE_WARNING_SECS) {
            banner.className = 'stale-banner stale-warning visible';
            banner.textContent = 'Data may be delayed — last update ' + text;
            if (headerStatus) headerStatus.className = 'header-live stale-warning-dot';
        } else {
            banner.classList.remove('visible', 'stale-warning', 'stale-critical');
            if (headerStatus) headerStatus.className = 'header-live';
        }
    }

    // --- Timeline ---

    const HISTORY_INDEX_URL = DATA_BASE + '/history/index.json';

    function loadHistoryIndex() {
        return fetchJSON(HISTORY_INDEX_URL)
            .then(function (data) {
                historyDates = (data.dates || []).sort();
            })
            .catch(function () {
                historyDates = [];
            });
    }

    function loadHistoryDay(dateStr) {
        if (historyCache[dateStr]) return Promise.resolve(historyCache[dateStr]);
        var url = DATA_BASE + '/history/' + dateStr + '.json';
        return fetchJSON(url).then(function (data) {
            historyCache[dateStr] = data;
            return data;
        });
    }

    function buildFlatSnapshots() {
        // Build a flat list from all cached days for slider indexing
        flatSnapshots = [];
        historyDates.forEach(function (dateStr) {
            var dayData = historyCache[dateStr];
            if (!dayData || !dayData.snapshots) return;
            dayData.snapshots.forEach(function (snap, idx) {
                flatSnapshots.push({
                    timestamp: snap.timestamp,
                    date: dateStr,
                    idx: idx,
                });
            });
        });
        // Sort by timestamp
        flatSnapshots.sort(function (a, b) { return a.timestamp - b.timestamp; });
    }

    function initTimeline() {
        var slider = document.getElementById('tl-slider');
        var playBtn = document.getElementById('tl-play');
        var liveBtn = document.getElementById('tl-live');

        slider.addEventListener('input', function () {
            if (timelineMode === 'live') {
                enterHistoryMode();
            }
            showSnapshotAt(parseInt(slider.value, 10));
        });

        playBtn.addEventListener('click', function () {
            togglePlay();
        });

        liveBtn.addEventListener('click', function () {
            enterLiveMode();
        });

        // Load history index, then preload all days for the slider
        loadHistoryIndex().then(function () {
            if (!historyDates.length) return;
            var promises = historyDates.map(function (d) { return loadHistoryDay(d); });
            return Promise.all(promises);
        }).then(function () {
            buildFlatSnapshots();
            if (flatSnapshots.length) {
                slider.max = flatSnapshots.length - 1;
                slider.value = slider.max;
            }
        });
    }

    function showSnapshotAt(sliderValue) {
        if (sliderValue < 0 || sliderValue >= flatSnapshots.length) return;

        var entry = flatSnapshots[sliderValue];
        var dayData = historyCache[entry.date];
        if (!dayData || !dayData.snapshots[entry.idx]) return;

        var snap = dayData.snapshots[entry.idx];
        updateMapMarkers(snap.aircraft || []);

        // Update timestamp label
        var d = new Date(snap.timestamp * 1000);
        var label = d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' }) +
                    ' ' + d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', timeZone: 'UTC' }) +
                    ' UTC';
        setText('tl-timestamp', label);

        // Update chart cursor
        if (trendChart && trendChart.options.plugins.timelineCursor) {
            trendChart.options.plugins.timelineCursor.timestamp = snap.timestamp;
            trendChart.update('none');
        }
    }

    function enterHistoryMode() {
        if (timelineMode === 'history') return;
        timelineMode = 'history';

        // Pause auto-refresh
        if (refreshTimer) {
            clearInterval(refreshTimer);
            refreshTimer = null;
        }

        // Update UI
        document.getElementById('tl-live').classList.remove('active');
        document.getElementById('header-status').textContent = 'History';
    }

    function enterLiveMode() {
        timelineMode = 'live';
        stopPlay();

        // Reset UI
        document.getElementById('tl-live').classList.add('active');
        document.getElementById('header-status').textContent = 'Live';
        setText('tl-timestamp', '');

        // Reset slider to end
        var slider = document.getElementById('tl-slider');
        if (flatSnapshots.length) {
            slider.value = slider.max;
        }

        // Clear chart cursor
        if (trendChart && trendChart.options.plugins.timelineCursor) {
            trendChart.options.plugins.timelineCursor.timestamp = null;
            trendChart.update('none');
        }

        // Resume auto-refresh and fetch fresh data
        refreshData();
        refreshTimer = setInterval(refreshData, REFRESH_INTERVAL);
    }

    function togglePlay() {
        if (playInterval) {
            stopPlay();
        } else {
            startPlay();
        }
    }

    function startPlay() {
        var slider = document.getElementById('tl-slider');
        var playBtn = document.getElementById('tl-play');

        if (timelineMode === 'live') {
            enterHistoryMode();
            slider.value = 0;
        }

        playBtn.innerHTML = '&#10074;&#10074;';
        playBtn.classList.add('playing');

        playInterval = setInterval(function () {
            var val = parseInt(slider.value, 10) + 1;
            if (val >= flatSnapshots.length) {
                stopPlay();
                return;
            }
            slider.value = val;
            showSnapshotAt(val);
        }, 500);
    }

    function stopPlay() {
        if (playInterval) {
            clearInterval(playInterval);
            playInterval = null;
        }
        var playBtn = document.getElementById('tl-play');
        playBtn.innerHTML = '&#9654;';
        playBtn.classList.remove('playing');
    }

    // --- Data fetching ---

    function fetchJSON(url) {
        return fetch(url + '?t=' + Date.now())
            .then(function (resp) {
                if (!resp.ok) throw new Error('HTTP ' + resp.status);
                return resp.json();
            });
    }

    function refreshData() {
        // Only update map markers if in live mode
        if (timelineMode === 'live') {
            fetchJSON(LIVE_URL)
                .then(function (data) {
                    updateMapMarkers(data.aircraft || []);
                })
                .catch(function (err) {
                    console.warn('Failed to fetch live data:', err);
                });
        }

        fetchJSON(STATS_URL)
            .then(function (data) {
                updateStats(data);
            })
            .catch(function (err) {
                console.warn('Failed to fetch stats:', err);
            });

        fetchJSON(TREND_URL)
            .then(function (data) {
                updateTrendChart(data);
            })
            .catch(function (err) {
                console.warn('Failed to fetch trend data:', err);
            });
    }

    // --- Utilities ---

    function setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    }

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // --- Init ---

    function init() {
        initMap();
        initTrendChart();
        initDetailPanel();
        initTimeline();
        refreshData();
        refreshTimer = setInterval(refreshData, REFRESH_INTERVAL);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
