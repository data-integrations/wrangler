# Parse as TimeDuration

The `parse-as-timeduration` directive parses strings containing time duration values with units (such as ms, s, m, h) into a standardized TimeDuration type. This enables consistent handling and conversion of time interval values throughout your data preparation workflow.

## Syntax
```
parse-as-timeduration :column [to-column]
```

## Arguments
* **column**: Column containing time duration strings to be parsed
* **to-column**: Optional. If provided, the result is placed in this column; if not provided, the original column is overwritten

## Usage Notes
* Input values must have a number followed by a valid unit (e.g., "10ms", "5s")
* Supported units: 
  * ns (nanoseconds)
  * us (microseconds)
  * ms (milliseconds)
  * s (seconds)
  * m (minutes)
  * h (hours)
  * d (days)
  * w (weeks)
* All values are standardized to milliseconds internally for consistent calculations
* Units are case-insensitive (e.g., "s" and "S" are treated the same)
* The TimeDuration type can be used with the `aggregate-stats` directive for aggregating time values

## Example
```
data :duration
100ms
5s
2m
1h
```

Applying the directive:
```
parse-as-timeduration :duration
```

Results in the duration column containing parsed TimeDuration objects that represent the following millisecond values:
```
data :duration (milliseconds)
100
5000
120000
3600000
```

## Direct Token Usage
Besides using the `parse-as-timeduration` directive, you can also use time duration values directly as tokens in other directives:

```
set-column :timeout 30s
```

This creates a new column with a TimeDuration value representing 30 seconds (30000 milliseconds).

## Converting Between Units
You can use the TimeDuration utility methods to convert values between different units. For example, when using the `aggregate-stats` directive, you can specify the output unit:

```
aggregate-stats :size_column :time_column total_size total_time MB s
```

This will aggregate the values and output the total time in seconds (s). 