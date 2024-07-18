from enum import Enum
from pendulum import DateTime


class Chunk(str, Enum):
    """ A chunk is a fixed amount of time, unless it is Chunk.FULL, then it is variable """
    DAY = 'day'
    HOUR = 'hour'
    WEEK = 'week'
    MONTH = 'month'
    YEAR = 'year'

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """"""
        return value in {item.value for item in cls}

def parse_chunk(string: str) -> Chunk:
    """
    Parse string to a valid chunk
    """
    string = string.lower()
    if string.endswith("s"):
        string = string[:-1]

    if not Chunk.is_valid(value=string):
        allowed_chunks = ([inc.value for inc in Chunk])
        raise ValueError(f"Unsupported chunk: {string}. Choose one of {allowed_chunks}")

    return Chunk(value=string)


def add_chunk(_datetime: DateTime, chunk: Chunk) -> DateTime:
    if chunk == Chunk.HOUR:
        return _datetime.add(hours=1)
    if chunk == Chunk.DAY:
        return _datetime.add(days=1)
    if chunk == Chunk.WEEK:
        return _datetime.add(weeks=1)
    if chunk == Chunk.MONTH:
        return _datetime.add(months=1)
    if chunk == Chunk.YEAR:
        return _datetime.add(years=1)


def is_half_chunk(start: DateTime, end: DateTime, chunk: Chunk) -> bool:
    return add_chunk(_datetime=start, chunk=chunk) > end
