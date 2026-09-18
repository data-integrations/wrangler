# aggregate-stats

The `aggregate-stats` directive analyzes columns containing byte size and time duration values,
aggregating statistics and storing the results in a new column.

This directive is particularly useful for analyzing log files, performance data, or any dataset containing
file sizes or time durations. It automatically detects and normalizes various units, making it easier to
perform statistical analysis across mixed unit representations.

## Syntax

```ini
aggregate-stats :source_column :target_column 'type'
```

where:
- `:source_column` - The column containing size/time values to analyze
- `:target_column` - The column name where results will be stored
- `'type'` - Either 'byte' for file sizes or 'time' for time durations

## Example 1: Aggregating Byte Size Statistics

```rb
aggregate-stats :file_sizes :size_stats 'byte'
```

This directive will analyze the values in the `file_sizes` column (which should contain byte size values like "10KB", "1.5MB", etc.)
and store aggregated statistics in a new column called `size_stats`.

### Sample Input Data (file_sizes column):

```json
[
  {"file_name": "document.pdf", "file_sizes": "1.5MB"},
  {"file_name": "image.jpg", "file_sizes": "750KB"},
  {"file_name": "data.csv", "file_sizes": "45KB"},
  {"file_name": "backup.zip", "file_sizes": "4.2GB"},
  {"file_name": "log.txt", "file_sizes": "128B"}
]
```

### Sample Result (size_stats column):

```json
{
  "count": 5,
  "sum": 4514824738,
  "min": 128,
  "max": 4509715660,
  "avg": 902964947.6,
  "units": {"B": 1, "KB": 2, "MB": 1, "GB": 1},
  "sum_kb": 4409984.12,
  "sum_mb": 4306.62,
  "sum_gb": 4.2
```

## Example 2: Aggregating Time Duration Statistics

```ini
aggregate-stats :response_times :time_stats 'time'
```

This directive will analyze the values in the `response_times` column (which should contain time duration values like "100ms", "2.5s", etc.)
and store aggregated statistics in a new column called `time_stats`.

### Sample Input Data (response_times column):

```json
[
  {"endpoint": "/api/users", "response_times": "120ms"},
  {"endpoint": "/api/products", "response_times": "1.8s"},
  {"endpoint": "/api/orders", "response_times": "3.2s"},
  {"endpoint": "/api/login", "response_times": "85ms"},
  {"endpoint": "/api/reports", "response_times": "45s"}
]
```

### Sample Result (time_stats column):

```json
{
  "count": 5,
  "sum_nanos": 50205000000,
  "min_nanos": 85000000,
  "max_nanos": 45000000000,
  "avg_nanos": 10041000000,
  "units": {"ms": 2, "s": 3},
  "sum_ms": 50205,
  "sum_s": 50.205,
  "sum_m": 0.837,
  "sum_h": 0.014
}
```

## Result Structure

For byte size aggregation (`'byte'` type), the target column will contain a map with the following keys:

* `count` - Number of valid byte size values analyzed
* `sum` - Total size in bytes
* `min` - Minimum size in bytes
* `max` - Maximum size in bytes
* `avg` - Average size in bytes
* `units` - Map counting occurrences of each unit (B, KB, MB, etc.)
* `sum_kb` - Total size in kilobytes
* `sum_mb` - Total size in megabytes
* `sum_gb` - Total size in gigabytes

For time duration aggregation (`'time'` type), the target column will contain a map with the following keys:

* `count` - Number of valid time duration values analyzed
* `sum_nanos` - Total duration in nanoseconds
* `min_nanos` - Minimum duration in nanoseconds
* `max_nanos` - Maximum duration in nanoseconds
* `avg_nanos` - Average duration in nanoseconds
* `units` - Map counting occurrences of each unit (ns, ms, s, m, h, d)
* `sum_ms` - Total duration in milliseconds
* `sum_s` - Total duration in seconds
* `sum_m` - Total duration in minutes
* `sum_h` - Total duration in hours

## Supported Byte Size Units

* `B` - Bytes
* `KB` - Kilobytes (1 KB = 1024 bytes)
* `MB` - Megabytes (1 MB = 1024 KB)
* `GB` - Gigabytes (1 GB = 1024 MB)
* `TB` - Terabytes (1 TB = 1024 GB)
* `PB` - Petabytes (1 PB = 1024 TB)

## Supported Time Duration Units

* `ns` - Nanoseconds
* `ms` - Milliseconds (1 ms = 1,000,000 ns)
* `s` - Seconds (1 s = 1,000 ms)
* `m` - Minutes (1 m = 60 s)
* `h` - Hours (1 h = 60 m)
* `d` - Days (1 d = 24 h)

## Usage Tips

1. **Handling Invalid Values**: If the source column contains values that can't be parsed as byte sizes or time durations, they will be ignored in the statistics calculation.

2. **Dealing with Mixed Units**: This directive automatically normalizes different units, so you can have values like "1.5MB", "750KB", and "4.2GB" in the same column and still get meaningful statistics.

3. **Accessing Stats in Expressions**: You can use the resulting map in expressions for further processing:

```ini
// After running aggregate-stats
set-column :avg_file_size_mb exp:{size_stats.avg / 1048576}
```

## Example 3: Advanced Usage with Log Analysis

```ini
// Parse a log file
parse-as-log :body "%h %l %u %t \"%r\" %>s %b"

// Aggregate statistics on response size
aggregate-stats :body_b :response_size_stats 'byte'

// Send large responses to error for further investigation
send-to-error exp:{response_size_stats.avg > 1048576} // Avg > 1MB
```

In this example, we parse Apache log entries, use `aggregate-stats` to analyze response sizes, and then conditionally send records to the error collector if the average response size exceeds 1MB.