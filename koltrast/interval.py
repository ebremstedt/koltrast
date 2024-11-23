
""" Interval """
from dataclasses import dataclass
from pendulum import DateTime
from typing import List
from pendulum import parse, now
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


def make_intervals(since: str, until: str, chunk: str) -> List[Interval]:
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


def last_complete_interval(chunk: Chunk) -> Interval:
    """
    Returns the last complete interval of a specified type (Chunk.HOUR, Chunk.DAY, Chunk.MONTH).

    :param interval_type: Type of interval (IntervalType.HOUR, IntervalType.DAY, or IntervalType.MONTH)
    :return: Interval object representing the period from the beginning of the last complete interval
             until its end.
    """
    right_now = now()

    if chunk == Chunk.HOUR:
        latest_complete_interval_start = right_now.subtract(hours=1).start_of("hour")
    elif chunk == Chunk.DAY:
        latest_complete_interval_start = right_now.subtract(days=1).start_of("day")
    elif chunk == Chunk.MONTH:
        latest_complete_interval_start = right_now.subtract(months=1).start_of("month")
    else:
        raise ValueError("Invalid chunk. Use Chunk.HOUR, Chunk.DAY, or Chunk.MONTH.")

    latest_complete_interval_end = latest_complete_interval_start.end_of(chunk.value)

    return Interval(since=latest_complete_interval_start, until=latest_complete_interval_end)
