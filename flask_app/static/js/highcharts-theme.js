/**
 * Global Highcharts dark theme for MultiPlex Stats
 */
Highcharts.setOptions({
    chart: {
        backgroundColor: '#000000',
        style: {
            fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
        }
    },
    title: {
        style: {
            color: 'white',
            fontSize: '16px'
        }
    },
    xAxis: {
        labels: {
            style: { color: 'white' }
        },
        title: {
            style: { color: 'white' }
        },
        gridLineColor: 'grey',
        lineColor: 'grey',
        tickColor: 'grey'
    },
    yAxis: {
        labels: {
            style: { color: 'white' }
        },
        title: {
            style: { color: 'white' }
        },
        gridLineColor: 'rgba(128, 128, 128, 0.3)'
    },
    legend: {
        itemStyle: {
            color: 'white'
        },
        itemHoverStyle: {
            color: '#ed542b'
        }
    },
    tooltip: {
        backgroundColor: 'rgba(0, 0, 0, 0.85)',
        style: { color: 'white' }
    },
    credits: {
        enabled: false
    },
    exporting: {
        enabled: false
    }
});

// Color constants matching current scheme
var CHART_COLORS = {
    SERVER_A_TV: '#758bfd',
    SERVER_A_MOVIES: '#aeb8fe',
    SERVER_B_TV: '#ff7900',
    SERVER_B_MOVIES: '#ffb600',
    PIE_YELLOW: '#FCE762',
    PIE_RED: '#D35F3D',
    GRADIENT_ORANGE: '#ff9800',
    GRADIENT_RED: '#ed542b'
};
