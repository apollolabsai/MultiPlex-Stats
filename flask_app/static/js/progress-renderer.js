/**
 * Shared progress bar rendering and timestamp formatting utilities.
 *
 * Usage:
 *   renderServerProgress('container-id', serversArray, true);
 *   formatLastUpdatedValue('2024-01-01T00:00:00Z');
 */
(function(root) {
    'use strict';

    /**
     * Format a date/time string into a human-friendly local timestamp.
     */
    function formatLastUpdatedValue(value) {
        if (!value) {
            return 'Never';
        }
        var parsed = new Date(value);
        if (isNaN(parsed.getTime())) {
            return value;
        }
        var year = parsed.getFullYear();
        var month = String(parsed.getMonth() + 1).padStart(2, '0');
        var day = String(parsed.getDate()).padStart(2, '0');
        var hour24 = parsed.getHours();
        var minute = String(parsed.getMinutes()).padStart(2, '0');
        var period = hour24 >= 12 ? 'PM' : 'AM';
        var hour12 = hour24 % 12;
        if (hour12 === 0) {
            hour12 = 12;
        }
        var hour = String(hour12).padStart(2, '0');
        return year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ' ' + period;
    }

    /**
     * Render per-server progress bars into a container element.
     *
     * @param {string} containerId - DOM element ID to render into.
     * @param {Array} servers - Array of server progress objects.
     * @param {boolean} [includeHistoryCounts=false] - Show inserted/skipped counts.
     * @param {Object} [cssClasses] - Optional CSS class overrides.
     */
    function renderServerProgress(containerId, servers, includeHistoryCounts, cssClasses) {
        var container = document.getElementById(containerId);
        if (!container) {
            return;
        }

        var cls = cssClasses || {};
        var progressClass = cls.progress || 'server-progress';
        var headerClass = cls.header || 'server-progress-header';
        var nameClass = cls.name || 'server-name';
        var badgeClass = cls.badge || 'server-status-badge';
        var stepClass = cls.step || 'server-step';
        var barClass = cls.bar || 'server-progress-bar';
        var fillClass = cls.fill || 'server-progress-fill';
        var fetchedClass = cls.fetched || 'server-fetched';
        var errorClass = cls.error || 'server-error';
        var unitLabel = cls.unitLabel || 'rows';

        var html = '';
        (servers || []).forEach(function(server) {
            var percent = 0;
            if (server.total && server.total > 0) {
                percent = Math.min(100, (server.fetched / server.total) * 100);
            } else if (server.status === 'success') {
                percent = 100;
            }

            var statusFillClass = '';
            if (server.status === 'success') statusFillClass = 'success';
            else if (server.status === 'failed') statusFillClass = 'failed';

            html += '<div class="' + progressClass + '">';
            html += '<div class="' + headerClass + '">';
            html += '<span class="' + nameClass + '">' + (server.name || 'Server');
            html += '<span class="' + badgeClass + ' ' + (server.status || 'idle') + '">' + (server.status || 'idle') + '</span>';
            html += '</span>';
            html += '<span class="' + stepClass + '">' + (server.step || '') + '</span>';
            html += '</div>';
            html += '<div class="' + barClass + '">';
            html += '<div class="' + fillClass + ' ' + statusFillClass + '" style="width: ' + percent + '%;"></div>';
            html += '</div>';
            html += '<div class="' + fetchedClass + '">' + ((server.fetched || 0).toLocaleString()) + ' ' + unitLabel;
            if (server.total) {
                html += ' / ' + server.total.toLocaleString() + ' total';
            }
            if (includeHistoryCounts) {
                html += ' (' + ((server.inserted || 0).toLocaleString()) + ' new, ' + ((server.skipped || 0).toLocaleString()) + ' duplicates)';
            }
            html += '</div>';
            if (server.error) {
                html += '<div class="' + errorClass + '">' + server.error + '</div>';
            }
            html += '</div>';
        });
        container.innerHTML = html;
    }

    root.formatLastUpdatedValue = formatLastUpdatedValue;
    root.renderServerProgress = renderServerProgress;
})(window);
