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
    const MAP_CENTER = [52.0, 19.5];
    const MAP_ZOOM = 6;

    let map;
    let markersLayer;
    let trendChart;
    let activeDetailIcao = null;
    const aircraftCache = {}; // cache for model + photo lookups

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
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
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
        if (!timestamp) {
            setText('last-updated', 'Updated: —');
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
        fetchJSON(LIVE_URL)
            .then(function (data) {
                updateMapMarkers(data.aircraft || []);
            })
            .catch(function (err) {
                console.warn('Failed to fetch live data:', err);
            });

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
        refreshData();
        setInterval(refreshData, REFRESH_INTERVAL);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
