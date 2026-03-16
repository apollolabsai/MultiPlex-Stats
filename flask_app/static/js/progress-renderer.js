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

    function formatDurationValue(seconds) {
        if (seconds == null || isNaN(seconds)) {
            return '';
        }
        var total = Math.max(0, Math.round(Number(seconds)));
        if (total < 60) {
            return total + 's';
        }
        var mins = Math.floor(total / 60);
        var secs = total % 60;
        if (mins < 60) {
            return mins + 'm ' + String(secs).padStart(2, '0') + 's';
        }
        var hours = Math.floor(mins / 60);
        mins = mins % 60;
        return hours + 'h ' + String(mins).padStart(2, '0') + 'm';
    }

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function formatStepCountValue(item) {
        var total = Number(item && item.total || 0);
        var current = Number(item && item.current || 0);
        if (total > 0) {
            return current.toLocaleString() + ' / ' + total.toLocaleString();
        }
        if (current > 0) {
            return current.toLocaleString();
        }
        return '';
    }

    function formatSummaryStatusText(item) {
        var countText = formatStepCountValue(item);
        var durationText = formatDurationValue(item && item.duration_seconds);
        var parts = [];

        if (!item) {
            return '';
        }

        if (item.status === 'success') {
            parts.push('Success');
            if (countText) {
                parts.push(countText);
            }
            if (durationText) {
                parts.push(durationText);
            }
            return parts.join(' ');
        }

        if (item.status === 'failed') {
            parts.push('Fail');
            if (countText) {
                parts.push(countText);
            }
            return parts.join(' ');
        }

        if (item.status === 'skipped') {
            return 'Skipped';
        }

        if (item.status === 'running') {
            parts.push('Working');
            if (countText) {
                parts.push(countText);
            }
            return parts.join(' ');
        }

        return 'Pending';
    }

    function renderPipelineSections(containerId, sections) {
        var container = document.getElementById(containerId);
        if (!container) {
            return;
        }

        var html = '';
        (sections || []).forEach(function(section) {
            var items = section && Array.isArray(section.items) ? section.items : [];
            if (!items.length) {
                return;
            }
            html += '<div class="pipeline-stage">';
            html += '<div class="pipeline-stage-title">' + escapeHtml(section.title || 'Progress') + '</div>';
            html += '<div class="pipeline-step-list">';

            items.forEach(function(item) {
                var total = Number(item.total || 0);
                var current = Number(item.current || 0);
                var percent = 0;
                if (total > 0) {
                    percent = Math.min(100, (current / total) * 100);
                } else if (item.status === 'success') {
                    percent = 100;
                }

                var progressText = '';
                if (item.status === 'success') {
                    progressText = formatDurationValue(item.duration_seconds);
                } else if (item.status === 'skipped') {
                    progressText = 'Skipped';
                } else if (item.status === 'running') {
                    if (total > 0) {
                        progressText = 'Overall: ' + current.toLocaleString() + ' / ' + total.toLocaleString();
                    } else if (current > 0) {
                        progressText = 'Overall: ' + current.toLocaleString();
                    } else {
                        progressText = 'Working';
                    }
                } else if (item.status === 'failed') {
                    progressText = 'Failed';
                } else {
                    progressText = 'Pending';
                }

                var iconHtml = '';
                if (item.status === 'success') {
                    iconHtml = '<span class="pipeline-step-icon success">&#10003;</span>';
                } else if (item.status === 'failed') {
                    iconHtml = '<span class="pipeline-step-icon failed">&#10005;</span>';
                } else if (item.status === 'skipped') {
                    iconHtml = '<span class="pipeline-step-icon pending">&#8211;</span>';
                } else if (item.status === 'pending') {
                    iconHtml = '<span class="pipeline-step-icon pending">&#8226;</span>';
                }

                html += '<div class="pipeline-step ' + escapeHtml(item.status || 'pending') + '">';
                html += '<div class="pipeline-step-copy">';
                html += '<div class="pipeline-step-label">' + escapeHtml(item.label || 'Step') + '</div>';
                html += '<div class="pipeline-step-detail">' + escapeHtml(item.detail || '') + '</div>';
                html += '</div>';
                html += '<div class="pipeline-step-side">';
                html += '<div class="pipeline-step-meta">' + escapeHtml(progressText) + iconHtml + '</div>';
                html += '<div class="pipeline-step-bar"><div class="pipeline-step-fill" style="width:' + percent + '%;"></div></div>';
                html += '</div>';
                html += '</div>';
            });

            html += '</div>';
            html += '</div>';
        });

        container.innerHTML = html;
    }

    function renderPipelineSummarySections(containerId, sections) {
        var container = document.getElementById(containerId);
        if (!container) {
            return;
        }

        var html = '';
        (sections || []).forEach(function(section) {
            var items = section && Array.isArray(section.items) ? section.items : [];
            if (!items.length) {
                return;
            }

            html += '<div class="pipeline-summary-stage">';
            html += '<div class="pipeline-summary-stage-title">' + escapeHtml(section.title || 'Summary') + '</div>';

            items.forEach(function(item) {
                var detail = '';
                if (item.status === 'failed') {
                    detail = item.error || item.detail || '';
                } else if (item.status === 'skipped') {
                    detail = item.detail || '';
                }

                html += '<div class="pipeline-summary-row ' + escapeHtml(item.status || 'pending') + '">';
                html += '<div class="pipeline-summary-main">';
                html += '<span class="pipeline-summary-label">' + escapeHtml(item.label || 'Step') + '</span>';
                html += '<span class="pipeline-summary-dots"></span>';
                html += '<span class="pipeline-summary-status">' + escapeHtml(formatSummaryStatusText(item)) + '</span>';
                html += '</div>';

                if (detail) {
                    html += '<div class="pipeline-summary-detail">' + escapeHtml(detail) + '</div>';
                }
                html += '</div>';
            });

            html += '</div>';
        });

        container.innerHTML = html;
    }

    root.formatLastUpdatedValue = formatLastUpdatedValue;
    root.renderServerProgress = renderServerProgress;
    root.renderPipelineSections = renderPipelineSections;
    root.renderPipelineSummarySections = renderPipelineSummarySections;
    root.formatDurationValue = formatDurationValue;
})(window);
