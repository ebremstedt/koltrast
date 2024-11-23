import unittest
from pendulum import datetime
from koltrast.interval import Interval, _split_interval, make_intervals
from koltrast.chunks import Chunk
from pendulum.exceptions import ParserError

class TestInterval(unittest.TestCase):
    def test_since_is_earlier_than_until(self):
        since = datetime(2023, 1, 1)
        until = datetime(2023, 1, 2)
        interval = Interval(since, until)
        self.assertEqual(interval.since, since)
        self.assertEqual(interval.until, until)

        since = datetime(2023, 1, 2)
        until = datetime(2023, 1, 1)
        with self.assertRaises(ValueError):
            Interval(since, until)

    def test_since_is_earlier_than_until_with_strings(self):
        since = "2023-01-01"
        until = "2023-01-02"
        interval = Interval(since, until)
        self.assertEqual(interval.since, datetime(2023, 1, 1, tz='UTC'))
        self.assertEqual(interval.until, datetime(2023, 1, 2, tz='UTC'))

        since = "2023-01-02"
        until = "2023-01-01"
        with self.assertRaises(ValueError):
            Interval(since, until)

    def test_since_equals_until(self):
        since = "2023-01-01"
        until = "2023-01-01"
        with self.assertRaises(ValueError):
            Interval(since, until)

    def test_invalid_since_string(self):
        since = "invalid-date"
        until = "2023-01-01"
        with self.assertRaises(ParserError):
            Interval(since, until)

    def test_invalid_until_string(self):
        since = "2023-01-01"
        until = "invalid-date"
        with self.assertRaises(ParserError):
            Interval(since, until)

    def test_invalid_both_strings(self):
        since = "invalid-date"
        until = "another-invalid-date"
        with self.assertRaises(ParserError):
            Interval(since, until)

    def test_valid_datetime_objects(self):
        since = datetime(2023, 1, 1)
        until = datetime(2023, 1, 2)
        interval = Interval(since, until)
        self.assertEqual(interval.since, since)
        self.assertEqual(interval.until, until)

    def test_valid_date_strings(self):
        since = "2023-01-01"
        until = "2023-01-02"
        interval = Interval(since, until)
        self.assertEqual(interval.since, datetime(2023, 1, 1, tz='UTC'))
        self.assertEqual(interval.until, datetime(2023, 1, 2, tz='UTC'))


class TestSplitInterval(unittest.TestCase):
    def test_split_one_and_half_weeks(self):
        since = datetime(2023, 1, 1)
        until = datetime(2023, 1, 11)
        chunk = Chunk.WEEK
        interval = Interval(since=since, until=until)
        result = _split_interval(interval, chunk)

        expected_intervals= [
            Interval(datetime(2023, 1, 1),datetime(2023, 1, 8)),
            Interval(datetime(2023, 1, 8),datetime(2023, 1, 11))
            ]

        self.assertEqual(result, expected_intervals)


    def test_split_half_year(self):
        since = datetime(2023, 1, 1)
        until = datetime(2023, 7, 1)
        chunk = Chunk.YEAR

        result = make_intervals(since, until, chunk)

        expected_intervals = [
            Interval(datetime(2023, 1, 1), datetime(2023, 7, 1))
        ]

        self.assertEqual(result, expected_intervals)

    def test_split_one_month(self):
        since = datetime(2023, 1, 1)
        until = datetime(2023, 2, 1)
        chunk = Chunk.MONTH
        result = make_intervals(since, until, chunk)

        expected_interval = [Interval(datetime(2023, 1, 1), datetime(2023, 2, 1))]

        self.assertEqual(result, expected_interval)

    def test_inclusive(self):
        since = datetime(2023, 1, 1)
        until = datetime(2023, 1, 31)
        chunk = Chunk.DAY

        result = len(make_intervals(since, until, chunk))

        expected_len = 30

        self.assertEqual(result, expected_len)

if __name__ == '__main__':
    unittest.main()
