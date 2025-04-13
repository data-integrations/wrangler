# Aggregate Stats

The AGGREGATE-STATS directive calculates aggregate statistics on byte sizes and time durations,
converting them to standardized units (megabytes and seconds).

## Syntax
```
aggregate-stats <size-column> <time-column> <total-size-column> <total-time-column>
```

* The `<size-column>` specifies the name of the column containing byte sizes (e.g., "1.5MB", "10KB")
* The `<time-column>` specifies the name of the column containing time durations (e.g., "100ms", "1.5s")
* The `<total-size-column>` specifies the name of the output column for total size in megabytes
* The `<total-time-column>` specifies the name of the output column for total time in seconds

## Usage Notes

The directive accepts byte sizes with these units:
- B (bytes)
- KB (kilobytes)
- MB (megabytes)
- GB (gigabytes)
- TB (terabytes)

And time durations with these units:
- ms (milliseconds)
- s (seconds)
- m (minutes)
- h (hours)

The directive converts all sizes to bytes internally, then outputs the total in megabytes.
Similarly, all time durations are converted to milliseconds internally, then output in seconds.

## Example

Using these records as an example:
```
{
  "data_transfer_size": "1.5MB",
  "response_time": "500ms"
}
{
  "data_transfer_size": "512KB",
  "response_time": "1.5s"
}
```

Applying this directive:
```
aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec
```

would result in this single record:
```
{
  "total_size_mb": 2.0,
  "total_time_sec": 2.0
}
```
