import unittest

import pandas as pd

from multiplex_stats.visualization import (
    get_day_of_week_stacked_data,
    get_hour_of_day_stacked_data,
    get_stream_type_stacked_data,
)


class DistributionVisualizationTests(unittest.TestCase):
    def test_day_of_week_chart_stacks_by_server_in_expected_order(self):
        df = pd.DataFrame([
            {'date_pt': '2026-02-14', 'Server': 'Apollo', 'count': 2},    # Sat
            {'date_pt': '2026-02-15', 'Server': 'ApolloSS', 'count': 3},  # Sun
            {'date_pt': '2026-02-16', 'Server': 'Apollo', 'count': 1},    # Mon
        ])

        chart = get_day_of_week_stacked_data(df, 'Apollo', 'ApolloSS', 60)

        self.assertEqual(chart['categories'], ['Sat', 'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri'])
        self.assertEqual(chart['series'][0]['name'], 'Apollo')
        self.assertEqual(chart['series'][1]['name'], 'ApolloSS')
        self.assertEqual(chart['series'][0]['data'][:3], [2, 0, 1])
        self.assertEqual(chart['series'][1]['data'][:3], [0, 3, 0])

    def test_stream_type_chart_normalizes_copy_as_direct_stream(self):
        df = pd.DataFrame([
            {'transcode_decision': 'direct play', 'Server': 'Apollo', 'count': 5},
            {'transcode_decision': 'transcode', 'Server': 'ApolloSS', 'count': 4},
            {'transcode_decision': 'copy', 'Server': 'Apollo', 'count': 3},
            {'transcode_decision': 'direct stream', 'Server': 'ApolloSS', 'count': 2},
            {'transcode_decision': 'unknown', 'Server': 'Apollo', 'count': 99},
        ])

        chart = get_stream_type_stacked_data(df, 'Apollo', 'ApolloSS', 30)

        self.assertEqual(chart['categories'], ['Direct Play', 'Transcode', 'Direct Stream'])
        self.assertEqual(chart['series'][0]['data'], [5, 0, 3])
        self.assertEqual(chart['series'][1]['data'], [0, 4, 2])

    def test_hour_of_day_chart_groups_into_12_hour_labels(self):
        df = pd.DataFrame([
            {'time_pt': '12:15am', 'Server': 'Apollo', 'count': 2},   # 12 AM
            {'time_pt': '1:20pm', 'Server': 'Apollo', 'count': 1},    # 1 PM
            {'time_pt': '1:45pm', 'Server': 'ApolloSS', 'count': 4},  # 1 PM
        ])

        chart = get_hour_of_day_stacked_data(df, 'Apollo', 'ApolloSS', 7)

        self.assertEqual(chart['categories'][0], '12 AM')
        self.assertEqual(chart['categories'][13], '1 PM')
        self.assertEqual(chart['series'][0]['data'][0], 2)
        self.assertEqual(chart['series'][0]['data'][13], 1)
        self.assertEqual(chart['series'][1]['data'][13], 4)


if __name__ == '__main__':
    unittest.main()
