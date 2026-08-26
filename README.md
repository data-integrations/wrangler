# Wrangler

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)

Wrangler is an interactive tool for data cleansing and transformation.

It provides the following capabilities:

* Transform and cleanse data using many pre-defined directives or transform functions.
* Cache and analyze results at each step to verify results.
* Create a reproducible transformation pipeline.

## Byte Size and Time Duration Parsers

Wrangler has been enhanced with native support for parsing and utilizing byte size and time duration units within recipes. This allows users to easily handle units like Kilobytes (KB), Megabytes (MB), milliseconds (ms), or seconds (s) without requiring complex multi-step recipes.

### Byte Size Parser

The ByteSize parser supports the following units:
- B (Bytes)
- KB (Kilobytes, 1 KB = 1024 B)
- MB (Megabytes, 1 MB = 1024 KB)
- GB (Gigabytes, 1 GB = 1024 MB)
- TB (Terabytes, 1 TB = 1024 GB)
- PB (Petabytes, 1 PB = 1024 TB)

Example byte size values:
- `10B`
- `1.5KB`
- `2MB`
- `3.5GB`

### Time Duration Parser

The TimeDuration parser supports the following units:
- ns (nanoseconds)
- ms (milliseconds, 1 ms = 1,000,000 ns)
- s (seconds, 1 s = 1,000 ms)
- m (minutes, 1 m = 60 s)
- h (hours, 1 h = 60 m)
- d (days, 1 d = 24 h)

Example time duration values:
- `100ns`
- `1.5ms`
- `2s`
- `3.5m`
- `1h`
- `0.5d`

### New Aggregate-Stats Directive

The `aggregate-stats` directive utilizes these new parsers to perform aggregation on columns containing byte sizes and time durations. 

Syntax:
```
aggregate-stats :<source-size-column> :<source-time-column> :<target-size-column> :<target-time-column> [<size-unit> <time-unit>]
```

Parameters:
- `source-size-column`: Column containing byte size values (e.g., "10KB", "5MB")
- `source-time-column`: Column containing time duration values (e.g., "100ms", "2s")
- `target-size-column`: Output column name for the aggregated size value
- `target-time-column`: Output column name for the aggregated time value
- `size-unit` (optional): Output unit for size (B, KB, MB, GB, TB, PB), defaults to MB
- `time-unit` (optional): Output unit for time (ns, ms, s, m, h, d), defaults to s

Examples:
```
# Basic usage with default output units (MB and seconds)
aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec

# Specify output units (gigabytes and minutes)
aggregate-stats :data_transfer_size :response_time :total_size :total_time GB m
```

This directive works as an aggregator, processing multiple input rows and producing a single output row with the total size and time values converted to the specified units.
