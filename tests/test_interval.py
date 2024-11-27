import unittest
import pendulum
from koltrast.interval import Interval, last_complete_interval  # Replace with the actual import if the class is in another file.

class TestInterval(unittest.TestCase):

    def setUp(self):
        """Set up basic data for testing."""
        # Set a common start and end time for the interval.
        self.interval = Interval(since=pendulum.datetime(2024, 11, 20, 10, 0, tz="UTC"), until=pendulum.datetime(2024, 11, 20, 16, 0, tz="UTC"))


    def test_invalid_interval_creation(self):
        """Test that an exception is raised if 'since' is after 'until'."""
        since = pendulum.datetime(2024, 11, 20, 14, 0, tz="UTC")

        with self.assertRaises(ValueError):
            Interval(since=since, until=since, tz='UTC')

    def test_overlaps_with_true(self):
        """Test the 'overlaps_with' method when intervals overlap."""
        another_interval = Interval(
            since=pendulum.datetime(2024, 11, 20, 14, 0, tz="UTC"),
            until=pendulum.datetime(2024, 11, 20, 18, 0, tz="UTC"),
            tz="UTC"
        )
        self.assertTrue(self.interval.overlaps_with(another_interval))

    def test_overlaps_with_false(self):
        """Test the 'overlaps_with' method when intervals do not overlap."""
        another_interval = Interval(
            since=pendulum.datetime(2024, 11, 20, 18, 0, tz="UTC"),
            until=pendulum.datetime(2024, 11, 20, 20, 0, tz="UTC"),
            tz="UTC"
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

    def test_split_cron_expression_multiple_chunks(self):
        """Test splitting a larger interval using a cron expression."""
        long_start = pendulum.datetime(2024, 11, 20, 0, 0, tz="UTC")
        long_end = pendulum.datetime(2024, 11, 21, 0, 0, tz="UTC")
        long_interval = Interval(since=long_start, until=long_end)

        cron_expression = "0 */6 * * *"  # Every 6 hours
        split_intervals = long_interval.split(cron_expression)

        self.assertEqual(split_intervals[3].until, long_end)

    def test_last_complete_interval(self):
        """Test the 'last_complete_interval' method to return the last full interval."""
        cron_expression = "0 */6 * * *"  # Every 6 hours

        this_date = pendulum.datetime(2024, 11, 21, 0, 0, tz="UTC")

        example_interval = Interval(since=this_date.subtract(hours=6), until=this_date, tz="UTC")
        last_interval = last_complete_interval(cron_expression=cron_expression, anchor=this_date)

        self.assertEqual(example_interval, last_interval)

    def test_invalid_cron_expression_in_last_complete_interval(self):
        """Test that an exception is raised if the cron expression is invalid in 'last_complete_interval'."""
        with self.assertRaises(Exception):
            last_complete_interval(cron_expression="invalid cron expression")


if __name__ == "__main__":
    unittest.main()
