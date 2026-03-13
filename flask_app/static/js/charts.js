/**
 * Highcharts rendering functions for MultiPlex Stats
 */

/**
 * Check if Highcharts is loaded and log an error if not.
 * Optionally displays an error message inside the target container.
 */
function _requireHighcharts(containerId) {
    if (typeof Highcharts !== 'undefined') {
        return true;
    }
    var msg = '[MultiPlex] Highcharts is not loaded — cannot render chart' +
              (containerId ? ' in #' + containerId : '') + '. Check browser console for CDN errors.';
    console.error(msg);
    if (containerId) {
        var el = document.getElementById(containerId);
        if (el) {
            el.innerHTML = '<div style="color:#e8833a;padding:2em;text-align:center;">' +
                'Chart library failed to load. Check browser console for details.</div>';
        }
    }
    return false;
}

function getDynamicBarHeight(count, minHeight, rowHeight, padding) {
    var items = Math.max(count || 0, 0);
    return Math.max(minHeight, items * rowHeight + padding);
}

/**
 * Render a stacked bar chart (daily/monthly)
 */
function renderStackedBarChart(containerId, chartData, options) {
    if (!_requireHighcharts(containerId)) return;
    options = options || {};
    var chartHeight = typeof options.height === 'number' ? options.height : 600;
    var labelRotation = typeof options.labelRotation === 'number' ? options.labelRotation : -45;

    try { Highcharts.chart(containerId, {
        chart: {
            type: 'column',
            height: chartHeight
        },
        title: {
            text: chartData.title
        },
        xAxis: {
            categories: chartData.categories,
            crosshair: true,
            labels: {
                rotation: labelRotation,
                style: {
                    fontSize: '13px'
                }
            }
        },
        yAxis: {
            min: 0,
            title: {
                text: 'Play Count'
            },
            stackLabels: {
                enabled: true,
                style: {
                    color: 'white',
                    fontSize: '11px',
                    fontWeight: 'bold',
                    textOutline: 'none'
                },
                formatter: function() {
                    return Highcharts.numberFormat(this.total, 0, '', ',');
                }
            }
        },
        legend: {
            align: 'center',
            verticalAlign: 'bottom',
            layout: 'horizontal'
        },
        tooltip: {
            headerFormat: '<b>{point.category}</b><br/>',
            pointFormat: '{series.name}: {point.y:,.0f}<br/>Total: {point.stackTotal:,.0f}'
        },
        plotOptions: {
            column: {
                stacking: 'normal',
                borderWidth: 0,
                dataLabels: {
                    enabled: false
                }
            }
        },
        series: chartData.series
    }); } catch (e) { console.error('[MultiPlex] renderStackedBarChart error for #' + containerId + ':', e); }
}

/**
 * Render a bar chart with gradient coloring (user/movie/tv)
 */
function renderGradientBarChart(containerId, chartData, options) {
    if (!_requireHighcharts(containerId)) return;
    options = options || {};
    var minHeight = typeof options.minHeight === 'number' ? options.minHeight : 700;
    var rowHeight = typeof options.rowHeight === 'number' ? options.rowHeight : 22;
    var padding = typeof options.padding === 'number' ? options.padding : 220;
    var height = getDynamicBarHeight(chartData.categories ? chartData.categories.length : 0, minHeight, rowHeight, padding);
    try { return Highcharts.chart(containerId, {
        chart: {
            type: 'bar',
            height: height
        },
        title: {
            text: chartData.title
        },
        xAxis: {
            categories: chartData.categories,
            labels: {
                style: {
                    fontSize: '15px'
                }
            }
        },
        yAxis: {
            min: 0,
            title: {
                text: ''
            }
        },
        legend: {
            enabled: false
        },
        tooltip: {
            pointFormat: '<b>{point.y:,.0f}</b> plays'
        },
        plotOptions: {
            bar: {
                borderWidth: 0,
                colorByPoint: true,
                dataLabels: {
                    enabled: true,
                    color: 'white',
                    style: {
                        fontSize: '14px',
                        fontWeight: 'bold',
                        textOutline: 'none'
                    },
                    formatter: function() {
                        return Highcharts.numberFormat(this.y, 0, '', ',');
                    }
                }
            }
        },
        series: [{
            name: 'Plays',
            data: chartData.data
        }]
    }); } catch (e) { console.error('[MultiPlex] renderGradientBarChart error for #' + containerId + ':', e); }
}

/**
 * Render a stacked bar chart with gradient colors (users by server)
 */
function renderUserStackedBarChart(containerId, chartData) {
    if (!_requireHighcharts(containerId)) return;
    var height = getDynamicBarHeight(chartData.categories ? chartData.categories.length : 0, 700, 22, 220);
    try { Highcharts.chart(containerId, {
        chart: {
            type: 'bar',
            height: height
        },
        title: {
            text: chartData.title
        },
        xAxis: {
            categories: chartData.categories,
            labels: {
                style: {
                    fontSize: '15px'
                }
            }
        },
        yAxis: {
            min: 0,
            title: {
                text: ''
            },
            stackLabels: {
                enabled: true,
                style: {
                    color: 'white',
                    fontSize: '16px',
                    fontWeight: 'bold',
                    textOutline: 'none'
                },
                formatter: function() {
                    return Highcharts.numberFormat(this.total, 0, '', ',');
                }
            }
        },
        legend: {
            align: 'center',
            verticalAlign: 'bottom',
            layout: 'horizontal'
        },
        tooltip: {
            headerFormat: '<b>{point.category}</b><br/>',
            pointFormat: '{series.name}: {point.y:,.0f}<br/>Total: {point.stackTotal:,.0f}'
        },
        plotOptions: {
            series: {
                stacking: 'normal',
                borderWidth: 0,
                dataLabels: {
                    enabled: false
                }
            }
        },
        series: chartData.series
    }); } catch (e) { console.error('[MultiPlex] renderUserStackedBarChart error for #' + containerId + ':', e); }
}

/**
 * Render a pie chart with percentage labels
 */
function renderPieChart(containerId, chartData) {
    if (!_requireHighcharts(containerId)) return;
    try { Highcharts.chart(containerId, {
        chart: {
            type: 'pie',
            height: 400
        },
        title: {
            text: chartData.title
        },
        tooltip: {
            headerFormat: '<span style="font-size:11px">{series.name}</span><br>',
            pointFormat: '<span style="color:{point.color}">{point.name}</span>: <b>{point.percentage:.1f}%</b> ({point.y:,.0f} plays)'
        },
        accessibility: {
            point: {
                valueSuffix: '%'
            }
        },
        plotOptions: {
            pie: {
                allowPointSelect: true,
                cursor: 'pointer',
                borderWidth: 0,
                borderRadius: 5,
                dataLabels: [{
                    enabled: true,
                    distance: 15,
                    format: '{point.name}'
                }, {
                    enabled: true,
                    distance: '-30%',
                    filter: {
                        property: 'percentage',
                        operator: '>',
                        value: 5
                    },
                    format: '{point.percentage:.1f}%',
                    style: {
                        fontSize: '0.9em',
                        textOutline: 'none'
                    }
                }]
            }
        },
        series: [{
            name: 'Share',
            colorByPoint: true,
            data: chartData.data
        }]
    }); } catch (e) { console.error('[MultiPlex] renderPieChart error for #' + containerId + ':', e); }
}

/**
 * Render an area chart with gradient fill and optional line overlays (concurrent streams)
 */
function renderAreaChart(containerId, chartData) {
    if (!_requireHighcharts(containerId)) return;
    try { Highcharts.chart(containerId, {
        chart: {
            height: 400
        },
        title: {
            text: chartData.title
        },
        xAxis: {
            categories: chartData.categories,
            crosshair: true,
            labels: {
                rotation: -45,
                style: {
                    fontSize: '12px'
                }
            },
            tickInterval: Math.ceil(chartData.categories.length / 15)
        },
        yAxis: {
            min: 0,
            title: {
                text: 'Concurrent Streams'
            },
            allowDecimals: false
        },
        legend: {
            enabled: chartData.series.length > 1,
            align: 'center',
            verticalAlign: 'bottom',
            layout: 'horizontal'
        },
        tooltip: {
            shared: true,
            headerFormat: '<b>{point.category}</b><br/>'
        },
        plotOptions: {
            area: {
                marker: {
                    enabled: false,
                    states: {
                        hover: { enabled: true, radius: 4 }
                    }
                },
                lineWidth: 2,
                states: {
                    hover: { lineWidth: 2 }
                },
                threshold: null
            },
            line: {
                marker: {
                    enabled: false,
                    states: {
                        hover: { enabled: true, radius: 4 }
                    }
                },
                states: {
                    hover: { lineWidth: 3 }
                }
            }
        },
        series: chartData.series
    }); } catch (e) { console.error('[MultiPlex] renderAreaChart error for #' + containerId + ':', e); }
}

/**
 * Helper function to update chart controls active state
 * @param {string} formId - The form element ID
 * @param {string} customInputName - Name attribute of custom input field
 * @param {number} value - The current value
 * @param {number[]} presetValues - Array of preset button values
 */
function setActiveRange(formId, customInputName, value, presetValues) {
    var form = document.getElementById(formId);
    if (!form) return;

    // Determine the data attribute to look for based on form
    var dataAttr = formId === 'monthly-range-form' ? 'data-months' : 'data-days';
    var buttons = form.querySelectorAll('.range-pill[' + dataAttr + ']');
    var customInput = form.querySelector('input[name="' + customInputName + '"]');
    var matched = false;

    buttons.forEach(function(button) {
        var btnValue = parseInt(button.getAttribute(dataAttr), 10);
        var isActive = btnValue === value;
        button.classList.toggle('active', isActive);
        if (isActive) matched = true;
    });

    if (customInput) {
        customInput.value = matched ? '' : value;
    }
}
