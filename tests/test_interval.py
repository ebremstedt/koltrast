import unittest
import pendulum
from koltrast.interval import Interval  # Replace with the actual import if the class is in another file.

class TestInterval(unittest.TestCase):

    def setUp(self):
        """Set up basic data for testing."""
        # Set a common start and end time for the interval.
        self.start = pendulum.datetime(2024, 11, 20, 10, 0, tz="UTC")
        self.end = pendulum.datetime(2024, 11, 20, 16, 0, tz="UTC")
        self.interval = Interval(since=self.start, until=self.end)

    def test_interval_creation(self):
        """Test the creation of an interval."""
        self.assertEqual(self.interval.since, self.start)
        self.assertEqual(self.interval.until, self.end)
        self.assertEqual(self.interval.duration, self.end - self.start)

    def test_invalid_interval_creation(self):
        """Test that an exception is raised if 'since' is after 'until'."""
        with self.assertRaises(ValueError):
            Interval(since=self.end, until=self.start)

    def test_overlaps_with_true(self):
        """Test the 'overlaps_with' method when intervals overlap."""
        another_interval = Interval(
            since=pendulum.datetime(2024, 11, 20, 14, 0, tz="UTC"),
            until=pendulum.datetime(2024, 11, 20, 18, 0, tz="UTC"),
        )
        self.assertTrue(self.interval.overlaps_with(another_interval))

    def test_overlaps_with_false(self):
        """Test the 'overlaps_with' method when intervals do not overlap."""
        another_interval = Interval(
            since=pendulum.datetime(2024, 11, 20, 18, 0, tz="UTC"),
            until=pendulum.datetime(2024, 11, 20, 20, 0, tz="UTC"),
        )
        self.assertFalse(self.interval.overlaps_with(another_interval))

    def test_contains_true(self):
        """Test the 'contains' method when a point is inside the interval."""
        moment = pendulum.datetime(2024, 11, 20, 12, 0, tz="UTC")
        self.assertTrue(self.interval.contains(moment))

    def test_contains_false(self):
        """Test the 'contains' method when a point is outside the interval."""
        moment = pendulum.datetime(2024, 11, 20, 9, 0, tz="UTC")
        self.assertFalse(self.interval.contains(moment))

    def test_split_cron_expression(self):
        """Test splitting an interval using a cron expression."""
        cron_expression = "0 */6 * * *"  # Every 6 hours
        split_intervals = self.interval.split(cron_expression)

        self.assertEqual(len(split_intervals), 1)  # Only 1 chunk should fit in the 6-hour span.
        self.assertEqual(split_intervals[0].since, self.start)
        self.assertEqual(split_intervals[0].until, self.end)

    def test_split_cron_expression_multiple_chunks(self):
        """Test splitting a larger interval using a cron expression."""
        long_start = pendulum.datetime(2024, 11, 20, 0, 0, tz="UTC")
        long_end = pendulum.datetime(2024, 11, 21, 0, 0, tz="UTC")
        long_interval = Interval(since=long_start, until=long_end)

        cron_expression = "0 */6 * * *"  # Every 6 hours
        split_intervals = long_interval.split(cron_expression)

        self.assertEqual(len(split_intervals), 4)  # 4 intervals of 6 hours should fit in a 24-hour period.
        self.assertEqual(split_intervals[0].since, long_start)
        self.assertEqual(split_intervals[0].until, long_start.add(hours=6))
        self.assertEqual(split_intervals[3].since, long_start.add(hours=18))
        self.assertEqual(split_intervals[3].until, long_end)

    def test_last_complete_interval(self):
        """Test the 'last_complete_interval' method to return the last full interval."""
        cron_expression = "0 */6 * * *"  # Every 6 hours
        last_interval = self.interval.last_complete_interval(cron_expression)

        self.assertEqual(last_interval.since, self.start)
        self.assertEqual(last_interval.until, self.end)

    def test_invalid_cron_expression_in_last_complete_interval(self):
        """Test that an exception is raised if the cron expression is invalid in 'last_complete_interval'."""
        with self.assertRaises(Exception):
            self.interval.last_complete_interval("invalid cron expression")

    def test_last_complete_interval_with_anchor(self):
        """Test that the last complete interval respects the anchor date."""
        anchor = pendulum.datetime(2024, 11, 20, 14, 0, tz="UTC")
        cron_expression = "0 */6 * * *"
        last_interval = self.interval.last_complete_interval(cron_expression, anchor)

        # For an anchor at 14:00, the last full 6-hour interval would be from 8:00 to 14:00
        self.assertEqual(last_interval.since, pendulum.datetime(2024, 11, 20, 8, 0, tz="UTC"))
        self.assertEqual(last_interval.until, anchor)

if __name__ == "__main__":
    unittest.main()
