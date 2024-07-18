# Koltrast

Koltrast is a very small module that splits a time interval into multiple, shorter intervals.

It was made to help simplify the process of backfilling pipelines.

## How to install

```sh
pip install koltrast
```

## Example usage

```python
import koltrast

intervals = koltrast.interval.generate_intervals(since="2014-06-01", until="2014-06-04", chunk="day")

for interval in intervals:
    print(interval)
```
```python
Interval(since=DateTime(2014, 6, 1, 0, 0, 0, tzinfo=Timezone('UTC')), until=DateTime(2014, 6, 2, 0, 0, 0, tzinfo=Timezone('UTC')))
Interval(since=DateTime(2014, 6, 2, 0, 0, 0, tzinfo=Timezone('UTC')), until=DateTime(2014, 6, 3, 0, 0, 0, tzinfo=Timezone('UTC')))
Interval(since=DateTime(2014, 6, 3, 0, 0, 0, tzinfo=Timezone('UTC')), until=DateTime(2014, 6, 4, 0, 0, 0, tzinfo=Timezone('UTC')))
```

## Notes

Accepts the following chunks: `day, hour, week, month, year`

Koltrast always uses "left", which means for a range from A > D, only A B C are selected.

If you provide a since and until, which cannot be divided evenly by the chunk you provided, the last interval will be shorter.
