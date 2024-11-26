from __future__ import annotations
from dataclasses import dataclass
from pendulum import DateTime, Duration, now, instance, parse
from croniter import croniter


@dataclass
class Interval:
    """
    Represents a time interval between two points in time (`since` and `until`), with an associated timezone.

    Args:
        since (DateTime | str): The start of the interval. Can be a `DateTime` object or str parsable by pendulum.parse
        until (DateTime | str): The end of the interval. Can be a `DateTime` object or str parsable by pendulum.parse
        tz (str): The timezone for the interval. Defaults to 'UTC'.

    Raises:
        ValueError: If `since` is not before `until`.

    """

    since: DateTime
    until: DateTime
    tz: str = 'UTC'

    def __post_init__(self):
        if isinstance(self.since, str):
            self.since = parse(self.since)

        if isinstance(self.until, str):
            self.until = parse(self.until)

        if self.since >= self.until:
            raise ValueError("`since` must be before `until`")

        self.since = self.since.in_timezone(self.tz)
        self.until = self.until.in_timezone(self.tz)

    @property
    def duration(self) -> Duration:
        """Return the duration of the interval."""
        return self.until - self.since

    def overlaps_with(self, other_interval: Interval) -> bool:
        """Check if two intervals overlap."""
        return not (self.until <= other_interval.since or self.since >= other_interval.until)

    def contains(self, moment: DateTime) -> bool:
        """Check if a given moment falls within the interval."""
        return self.since <= moment < self.until

    def split(self, cron_expression: str) -> list[Interval]:
        """
        Split the current interval into smaller intervals based on a cron expression.
        The cron expression defines the times when the interval should be split.

        Args:
            cron_expression (str): A cron-style expression for the split pattern (e.g., "0 */6 * * *").

        Returns:
            list[Interval]: A list of smaller intervals.
        """
        if not croniter.is_valid(expression=cron_expression):
            raise Exception(f"{cron_expression} is not a valid cron expression")

        cron = croniter(cron_expression, self.since)

        intervals = []
        while True:
            this_time: DateTime = instance(cron.get_current(DateTime))
            next_time: DateTime = instance(cron.get_next(DateTime))

            if next_time > self.until:
                break  # Break the loop when the next time exceeds 'until'

            intervals.append(Interval(since=this_time, until=next_time, tz=self.tz))

        return intervals


def last_complete_interval(cron_expression: str, anchor: DateTime = now(), tz: str = 'UTC') -> Interval:
    """
    Calculate the last complete interval for a given cron expression.

    Args:
        cron_expression (str): A valid cron expression defining the desired schedule.
        anchor (DateTime): The reference point for calculating intervals. Defaults to the current time.
        tz (str): The timezone for interpreting the `anchor` and calculating the schedule. Defaults to 'UTC'.

    Returns:
        Interval: An object representing the last completed interval, with `since` and `until` attributes.

    Raises:
        Exception: If the provided `cron_expression` is invalid.
    """
    if not croniter.is_valid(expression=cron_expression):
        raise Exception(f"{cron_expression} is not a valid cron expression")

    cron = croniter(expr_format=cron_expression, start_time=anchor.in_timezone(tz))
    since = instance(cron.get_prev(DateTime))
    until = instance(cron.get_next(DateTime))

    if anchor < until:
        one_step_back = instance(cron.get_prev(DateTime))
        two_step_back = instance(cron.get_prev(DateTime))
        return Interval(since=two_step_back, until=one_step_back, tz=tz)

    return Interval(since=since, until=until, tz=tz)
