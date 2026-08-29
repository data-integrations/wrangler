# Aggregate Stats

The `aggregate-stats` directive aggregates byte size and time duration values across multiple records. It automatically parses values with appropriate units, computes sums or averages, and produces a single result row with aggregated statistics.

## Syntax

```
aggregate-stats :size_column :time_column target_size_column target_time_column [size_unit] [time_unit] [aggregation_type]
```

## Arguments

* **size_column**: Source column containing byte size values (such as "10KB", "5MB")
* **time_column**: Source column containing time duration values (such as "100ms", "5s")
* **target_size_column**: Name of the output column for the aggregated size value
* **target_time_column**: Name of the output column for the aggregated time value
* **size_unit** (optional): Output unit for the size value (default: "MB")
  * Supported units: B, KB, MB, GB, TB, PB
* **time_unit** (optional): Output unit for the time value (default: "s")
  * Supported units: ns, us, ms, s, m, h, d, w
* **aggregation_type** (optional): Type of aggregation to perform (default: "sum")
  * Supported types: "sum", "avg"

## Usage Notes

* The directive automatically parses the source columns as ByteSize and TimeDuration types
* All size values are standardized to bytes internally before aggregation
* All time values are standardized to milliseconds internally before aggregation
* The result is converted to the specified output units before being placed in the target columns
* Only a single aggregated row is generated when the pipeline is executed
* If no size_unit is specified, the default is MB (megabytes)
* If no time_unit is specified, the default is s (seconds)
* If no aggregation_type is specified, the default is sum

## Examples

### Example 1: Basic Sum Aggregation

Input data:
```
data_transfer_size | response_time
--------------------|-------------
10KB                | 100ms
5MB                 | 2s
2.5GB               | 1m
```

Directive:
```
aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec
```

Output:
```
total_size_mb | total_time_sec
--------------|---------------
2560          | 62.1
```

### Example 2: Average with Custom Units

Input data:
```
data_transfer_size | response_time
--------------------|-------------
10KB                | 100ms
5MB                 | 2s
2.5GB               | 1m
```

Directive:
```
aggregate-stats :data_transfer_size :response_time avg_size_gb avg_time_ms GB ms avg
```

Output:
```
avg_size_gb | avg_time_ms
------------|------------
0.835       | 20700
```

## Combining with Parse Directives

The `aggregate-stats` directive can be combined with `parse-as-bytesize` and `parse-as-timeduration` directives when the source columns contain string values that are not already in the expected format:

```
parse-as-bytesize :data_size
parse-as-timeduration :response_time
aggregate-stats :data_size :response_time total_size_mb total_time_sec
```

## Performance Considerations

* The aggregation happens in memory, so very large data sets may require additional resources
* The directive internally maintains running totals of the size and time values
* For extremely large data sets, consider partitioning or sampling the data before aggregation 