(function(window, document) {
    'use strict';

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function toNumber(value) {
        if (value === null || value === undefined) {
            return null;
        }
        if (typeof value === 'string' && value.trim() === '') {
            return null;
        }
        var number = Number(value);
        return Number.isFinite(number) ? number : null;
    }

    function ActivityMap(options) {
        this.element = document.getElementById(options.elementId);
        this.emptyElement = document.getElementById(options.emptyId);
        this.summaryElement = document.getElementById(options.summaryId);
        this.resetButton = document.getElementById(options.resetId);
        this.tileUrl = options.tileUrl || '';
        this.attribution = options.attribution || '';
        this.maxZoom = Number(options.maxZoom) || 20;
        this.hasStadiaKey = Boolean(options.hasStadiaKey);
        this.initialStreams = Array.isArray(options.initialStreams) ? options.initialStreams : [];
        this.map = null;
        this.markerLayer = null;
        this.baseLayer = null;
        this.defaultCenter = [20, 0];
        this.defaultZoom = 2;
        this.cartoTileUrl = 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png';
        this.cartoAttribution =
            '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap</a> ' +
            '&copy; <a href="https://carto.com/attributions" target="_blank" rel="noopener">CARTO</a>';
        this.hasUserViewport = false;
        this.lastAutoFit = null;
        this.serverColors = {
            serverA: '#E6B413',
            serverB: '#f18a3d',
            mixedStroke: '#ffd8a8',
            mixedFill: '#f7c84a'
        };
    }

    ActivityMap.prototype.shouldUseHostedTiles = function() {
        return this.hasStadiaKey && Boolean(this.tileUrl);
    };

    ActivityMap.prototype.init = function() {
        if (this.map || !this.element || !window.L) {
            return;
        }

        this.map = window.L.map(this.element, {
            worldCopyJump: true,
            dragging: true,
            touchZoom: true,
            doubleClickZoom: true,
            boxZoom: false,
            keyboard: true,
            tap: true,
            zoomControl: true,
            attributionControl: true
        }).setView(this.defaultCenter, this.defaultZoom);
        if (this.map.attributionControl) {
            this.map.attributionControl.setPrefix(false);
        }
        if (this.map.dragging) {
            this.map.dragging.enable();
        }
        this.map.on('dragstart zoomstart', function() {
            this.hasUserViewport = true;
        }, this);
        if (this.resetButton) {
            this.resetButton.addEventListener('click', this.resetView.bind(this));
        }

        if (this.shouldUseHostedTiles()) {
            this.baseLayer = window.L.tileLayer(this.tileUrl, {
                attribution: this.attribution,
                maxZoom: this.maxZoom,
                tileSize: 256
            }).addTo(this.map);
        } else {
            this.baseLayer = window.L.tileLayer(this.cartoTileUrl, {
                attribution: this.cartoAttribution,
                maxZoom: 20,
                subdomains: 'abcd',
                tileSize: 256
            }).addTo(this.map);
        }

        this.markerLayer = window.L.layerGroup().addTo(this.map);
    };

    ActivityMap.prototype.groupStreams = function(streams) {
        var grouped = new Map();
        var localCount = 0;
        var unresolvedCount = 0;

        streams.forEach(function(stream) {
            var latitude = toNumber(stream.geo_lat);
            var longitude = toNumber(stream.geo_lon);
            if (latitude === null || longitude === null) {
                if ((stream.location || '').toLowerCase() === 'local network') {
                    localCount += 1;
                } else {
                    unresolvedCount += 1;
                }
                return;
            }

            var key = latitude.toFixed(4) + ',' + longitude.toFixed(4);
            if (!grouped.has(key)) {
                grouped.set(key, {
                    latitude: latitude,
                    longitude: longitude,
                    location: stream.location || 'Unknown',
                    streams: []
                });
            }
            grouped.get(key).streams.push(stream);
        });

        return {
            grouped: Array.from(grouped.values()),
            localCount: localCount,
            unresolvedCount: unresolvedCount
        };
    };

    ActivityMap.prototype.buildPopupHtml = function(group) {
        var headerLabel = group.location || 'Unknown location';
        var html = '<div class="map-popup-group-title">' + escapeHtml(headerLabel);
        if (group.streams.length > 1) {
            html += ' <span>(' + group.streams.length + ' streams)</span>';
        }
        html += '</div>';

        group.streams.forEach(function(stream) {
            var title = stream.title || 'Unknown';
            if (stream.subtitle) {
                title += ' ' + stream.subtitle;
            }

            html += '<div class="map-popup-stream">';
            html += '<div class="map-popup-stream-title">' + escapeHtml(title) + '</div>';
            html += '<div class="map-popup-stream-meta">' + escapeHtml(stream.user || 'Unknown user') + ' • ' + escapeHtml(stream.server || 'Unknown server') + '</div>';
            html += '<div class="map-popup-stream-meta">' + escapeHtml(stream.platform || 'Unknown platform');
            if (stream.quality) {
                html += ' • ' + escapeHtml(stream.quality);
            }
            html += '</div>';
            html += '</div>';
        });

        return html;
    };

    ActivityMap.prototype.getGroupServerStyle = function(group) {
        var hasServerA = false;
        var hasServerB = false;

        (group.streams || []).forEach(function(stream) {
            if (stream.server_order === 'server-a') {
                hasServerA = true;
            } else if (stream.server_order === 'server-b') {
                hasServerB = true;
            }
        });

        if (hasServerA && hasServerB) {
            return {
                variant: 'mixed',
                fillColor: this.serverColors.mixedFill,
                fillOpacity: 0.92,
                dashArray: '3 3',
                weight: 0
            };
        }

        if (hasServerA) {
            return {
                variant: 'server-a',
                fillColor: this.serverColors.serverA,
                fillOpacity: 0.88,
                dashArray: null,
                weight: 0
            };
        }

        return {
            variant: 'server-b',
            fillColor: this.serverColors.serverB,
            fillOpacity: 0.84,
            dashArray: null,
            weight: 0
        };
    };

    ActivityMap.prototype.getMarkerRadius = function(group) {
        return Math.min(8 + ((group.streams || []).length - 1) * 2.5, 16);
    };

    ActivityMap.prototype.createMarker = function(group) {
        var serverStyle = this.getGroupServerStyle(group);
        var marker = window.L.circleMarker([group.latitude, group.longitude], {
            radius: this.getMarkerRadius(group),
            weight: serverStyle.weight,
            fillColor: serverStyle.fillColor,
            fillOpacity: serverStyle.fillOpacity,
            dashArray: serverStyle.dashArray
        });

        marker.bindPopup(this.buildPopupHtml(group), {
            maxWidth: 280,
            className: 'current-activity-map-popup'
        });

        return marker;
    };

    ActivityMap.prototype.updateSummary = function(totalCount, mappedCount, localCount, unresolvedCount) {
        if (!this.summaryElement) {
            return;
        }

        var modeLabel = this.shouldUseHostedTiles()
            ? 'Stadia dark basemap'
            : 'CARTO dark fallback';

        if (!totalCount) {
            this.summaryElement.textContent = modeLabel + ' • No active streams right now.';
            return;
        }

        var parts = [modeLabel, totalCount + ' active'];
        if (mappedCount) {
            parts.push(mappedCount + ' mapped');
        }
        if (localCount) {
            parts.push(localCount + ' local/private hidden');
        }
        if (unresolvedCount) {
            parts.push(unresolvedCount + ' unresolved');
        }
        this.summaryElement.textContent = parts.join(' • ');
    };

    ActivityMap.prototype.updateResetButtonState = function() {
        if (!this.resetButton) {
            return;
        }
        this.resetButton.disabled = !this.lastAutoFit;
    };

    ActivityMap.prototype.updateEmptyState = function(message, shouldShow) {
        if (!this.emptyElement) {
            return;
        }
        this.emptyElement.textContent = message || '';
        this.emptyElement.hidden = !shouldShow;
    };

    ActivityMap.prototype.render = function(streams) {
        this.init();
        if (!this.map || !this.markerLayer) {
            return;
        }

        var allStreams = Array.isArray(streams) ? streams : [];
        var grouped = this.groupStreams(allStreams);
        var points = grouped.grouped;

        this.markerLayer.clearLayers();
        this.updateSummary(allStreams.length, points.length, grouped.localCount, grouped.unresolvedCount);

        if (!points.length) {
            var emptyMessage = allStreams.length
                ? 'Active streams are local/private or have no geolocation yet.'
                : 'No active streams to map right now.';
            this.updateEmptyState(emptyMessage, true);
            this.lastAutoFit = {
                type: 'view',
                center: this.defaultCenter.slice(),
                zoom: this.defaultZoom
            };
            this.updateResetButtonState();
            if (!this.hasUserViewport) {
                this.map.setView(this.defaultCenter, this.defaultZoom);
            }
            return;
        }

        this.updateEmptyState('', false);

        var bounds = [];
        points.forEach(function(group) {
            var marker = this.createMarker(group);
            marker.addTo(this.markerLayer);
            bounds.push([group.latitude, group.longitude]);
        }, this);

        if (bounds.length === 1) {
            this.lastAutoFit = {
                type: 'view',
                center: bounds[0].slice(),
                zoom: 5
            };
            this.updateResetButtonState();
            if (!this.hasUserViewport) {
                this.map.setView(bounds[0], 5);
            }
            return;
        }

        this.lastAutoFit = {
            type: 'bounds',
            bounds: bounds.map(function(point) { return point.slice(); }),
            options: {
                padding: [24, 24],
                maxZoom: 5
            }
        };
        this.updateResetButtonState();
        if (!this.hasUserViewport) {
            this.map.fitBounds(bounds, {
                padding: [24, 24],
                maxZoom: 5
            });
        }
    };

    ActivityMap.prototype.setStreams = function(streams) {
        this.render(streams);
    };

    ActivityMap.prototype.resetView = function() {
        if (!this.map || !this.lastAutoFit) {
            return;
        }

        this.hasUserViewport = false;
        if (this.lastAutoFit.type === 'bounds' && this.lastAutoFit.bounds) {
            this.map.fitBounds(this.lastAutoFit.bounds, this.lastAutoFit.options || {});
            return;
        }

        if (this.lastAutoFit.center) {
            this.map.setView(this.lastAutoFit.center, this.lastAutoFit.zoom || this.defaultZoom);
        }
    };

    window.CurrentActivityMap = {
        create: function(options) {
            var map = new ActivityMap(options || {});
            map.render(map.initialStreams);
            return map;
        }
    };
})(window, document);
