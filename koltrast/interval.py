from __future__ import annotations
from dataclasses import dataclass
from pendulum import DateTime, Duration, now, instance
from croniter import croniter


@dataclass
class Interval:
    since: DateTime
    until: DateTime

    def __post_init__(self):
        print(self.since, self.until)

        if self.since >= self.until:
            raise ValueError("`since` must be before `until`")

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

            intervals.append(Interval(since=this_time, until=next_time))

        return intervals


def last_complete_interval(cron_expression: str, anchor: DateTime = now(), tz: str = 'UTC') -> Interval:
    if not croniter.is_valid(expression=cron_expression):
        raise Exception(f"{cron_expression} is not a valid cron expression")

    cron = croniter(expr_format=cron_expression, start_time=anchor.in_timezone(tz))

    return Interval(since=instance(cron.get_prev(DateTime)), until=instance(cron.get_next(DateTime)))
