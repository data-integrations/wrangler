# aggregate-stats

The `aggregate-stats` directive aggregates byte sizes and time durations across rows and produces summary statistics.

## Syntax

```
aggregate-stats :<byte_column> :<time_column> :<byte_total_column> :<time_total_column> [<byte_output_unit>] [<time_output_unit>]
```

## Usage Notes

* The directive operates on columns containing byte size values (e.g., "10KB", "1.5MB") and time duration values (e.g., "50ms", "2.1s").
* It accumulates the values across all input rows, converting everything to canonical units (bytes and nanoseconds) for accurate calculations.
* When processing completes, it outputs a single row with the aggregated totals in the specified units.
* The directive processes data in batches and uses the transient store to maintain state across batches.

## Arguments

* `byte_column`: The column name containing byte size values
* `time_column`: The column name containing time duration values
* `byte_total_column`: The name of the output column for the total byte size
* `time_total_column`: The name of the output column for the total time duration
* `byte_output_unit` (optional): The unit to use for the byte size output (default: MB)
   * Valid values: B, KB, MB, GB, TB, PB, EB, ZB, YB
* `time_output_unit` (optional): The unit to use for the time duration output (default: seconds)
   * Valid values: ns, ms, s, m, h, d (or nanoseconds, milliseconds, seconds, minutes, hours, days)

## Example

Given this input data:

| data_transfer_size | response_time |
|--------------------|---------------|
| 100KB              | 50ms          |
| 200KB              | 75ms          |
| 300KB              | 100ms         |

And the directive:

```
aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec
```

The output would be a single row:

| total_size_mb | total_time_sec |
|---------------|----------------|
| 0.586         | 0.225          |

## Example with Custom Output Units

Given this input data:

| data_transfer_size | response_time |
|--------------------|---------------|
| 1GB                | 5m            |
| 1GB                | 5m            |

And the directive:

```
aggregate-stats :data_transfer_size :response_time :total_size_gb :total_time_min 'GB' 'm'
```

The output would be a single row:

| total_size_gb | total_time_min |
|---------------|----------------|
| 2.0           | 10.0           |

## String Parsing

The directive can also parse string representations of byte sizes and time durations. For example, if your data contains string values like "1MB" or "2s", the directive will automatically parse and convert these values.

Example with string input:

| data_transfer_size | response_time |
|--------------------|---------------|
| "1MB"              | "1s"          |
| "1MB"              | "1s"          |

Using the directive:

```
aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec
```

The output would be:

| total_size_mb | total_time_sec |
|---------------|----------------|
| 2.0           | 2.0            |

## Handling Null Values

Null values in source columns are simply ignored during aggregation. The directive only aggregates the non-null values it encounters. 