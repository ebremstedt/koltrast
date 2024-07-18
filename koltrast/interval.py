
""" Interval """
from dataclasses import dataclass
from pendulum import DateTime
from typing import List
from pendulum import parse
from koltrast.chunks import Chunk, parse_chunk, add_chunk, is_half_chunk


@dataclass
class Interval:
    """Interval

    since: interval since when? (inclusive: =>)
    until: interval until when? (not inclusive: <)
    """

    since: DateTime
    until: DateTime

    def __init__(self, since: DateTime | str, until: DateTime | str):

        if isinstance(since, str):
            since=parse(since)

        if isinstance(until, str):
            until=parse(until)

        if since >= until:
            raise ValueError("Since must be < until")

        self.since = since
        self.until = until


def _split_interval(
    interval: Interval,
    chunk: Chunk
) -> List[Interval]:
    """ Split an interval into smaller intervals

    Parameters:
        interval: an unfixed amount of time
        chunk: a fixed amount of time

    Returns:
        List of intervals
    """

    sub_intervals = []

    start_here = interval.since
    end_here = interval.until

    while start_here < end_here:

        sub_interval_start = start_here
        sub_interval_end = add_chunk(_datetime=start_here, chunk=chunk)

        if is_half_chunk(start=start_here, end=interval.until, chunk=chunk):
            sub_interval_end = interval.until

        sub_intervals.append(
            Interval(since=sub_interval_start, until=sub_interval_end)
        )

        start_here = sub_interval_end

    return sub_intervals


def generate_intervals(since: str, until: str, chunk: str) -> List[Interval]:
    """ Create generate a list of intervals from an interval
    Parameters:
        since: The lower bound of an interval. Inclusive (>=)
        until: The upper bound of an interval. Not inclusive (<)
        chunk: How big or small should the intervals be.
            Accepted: HOUR, DAY, WEEK, MONTH, YEAR, FULL

    Returns: List of intervals
    """

    return _split_interval(
        interval=Interval(since=since, until=until),
        chunk=parse_chunk(string=chunk)
    )
