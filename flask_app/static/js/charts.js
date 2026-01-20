/**
 * Highcharts rendering functions for MultiPlex Stats
 */

/**
 * Render a stacked bar chart (daily/monthly)
 */
function renderStackedBarChart(containerId, chartData) {
    Highcharts.chart(containerId, {
        chart: {
            type: 'column',
            height: 600
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
                    fontSize: '11px'
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
            headerFormat: '<b>{point.x}</b><br/>',
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
    });
}

/**
 * Render a bar chart with gradient coloring (user/movie/tv)
 */
function renderGradientBarChart(containerId, chartData) {
    Highcharts.chart(containerId, {
        chart: {
            type: 'bar',
            height: 700
        },
        title: {
            text: chartData.title
        },
        xAxis: {
            categories: chartData.categories,
            labels: {
                style: {
                    fontSize: '13px'
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
    });
}

/**
 * Render a pie chart with percentage labels
 */
function renderPieChart(containerId, chartData) {
    Highcharts.chart(containerId, {
        chart: {
            type: 'pie',
            height: 400
        },
        title: {
            text: chartData.title
        },
        tooltip: {
            pointFormat: '<b>{point.percentage:.1f}%</b> ({point.y:,.0f} plays)'
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
                dataLabels: [{
                    enabled: true,
                    distance: 20,
                    format: '{point.name}'
                }, {
                    enabled: true,
                    distance: -40,
                    format: '{point.percentage:.1f}%',
                    style: {
                        fontSize: '1.2em',
                        textOutline: 'none',
                        opacity: 0.7
                    },
                    filter: {
                        operator: '>',
                        property: 'percentage',
                        value: 10
                    }
                }]
            }
        },
        series: [{
            name: 'Share',
            colorByPoint: true,
            data: chartData.data
        }]
    });
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
