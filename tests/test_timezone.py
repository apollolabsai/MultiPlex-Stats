import os
import unittest

from multiplex_stats.data_processing import process_history_data


class TimezoneHandlingTests(unittest.TestCase):
    def test_history_dates_follow_tz_env(self):
        old_tz = os.environ.get('TZ')
        os.environ['TZ'] = 'America/New_York'
        try:
            sample_record = [
                0,  # date (Unix timestamp)
                'user',
                'friendly',
                1,
                'movie',
                'Title',
                '',
                '',
                '',
                1970,
                '1.1.1.1',
                'iOS',
                'Plex',
                100,
                '',
                '',
                ''
            ]
            data = {'response': {'data': {'data': [sample_record]}}}

            df = process_history_data(data, None, 'ServerA', None)

            self.assertEqual(df.loc[0, 'date_pt'], '1969-12-31')
            self.assertEqual(df.loc[0, 'time_pt'], '7:00pm')
        finally:
            if old_tz is None:
                os.environ.pop('TZ', None)
            else:
                os.environ['TZ'] = old_tz


if __name__ == '__main__':
    unittest.main()
