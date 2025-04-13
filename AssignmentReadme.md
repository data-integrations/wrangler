# Wrangler - New Token Parsers: `BYTE_SIZE` and `TIME_DURATION`

This update introduces new token types `BYTE_SIZE` and `TIME_DURATION` into the Wrangler pipeline, allowing for more flexible parsing and handling of byte sizes (e.g., `10KB`, `2.5MB`, `1GB`) and time durations (e.g., `500ms`, `2s`, `1.5min`). These tokens are supported in directives for aggregation and data transformation tasks.

## 🧩 Supported Token Types

### 1. `BYTE_SIZE`
- Represents byte sizes with units like KB, MB, GB, etc.
- **Examples**: 
  - `10KB`
  - `1.5MB`
  - `2GB`
  - `100B`

### 2. `TIME_DURATION`
- Represents time durations with units like ms, s, min, h.
- **Examples**:
  - `500ms`
  - `2s`
  - `1.5min`
  - `1h`

### Canonical Units
- `BYTE_SIZE`: All byte sizes are converted and stored in **bytes**.
- `TIME_DURATION`: All time durations are converted and stored in **nanoseconds**.

---

## 🆕 New Directive: `aggregate-stats`

The `aggregate-stats` directive has been introduced to allow for the aggregation of data, specifically leveraging the newly added `BYTE_SIZE` and `TIME_DURATION` token types.

### Syntax:
```wrangler
aggregate-stats :<byteSizeColumn> :<durationColumn> <totalSizeColumn> <totalTimeColumn>
```
### Arguments:
- byteSizeColumn: The column containing byte size values (e.g., 10KB, 2MB).
- durationColumn: The column containing time durations (e.g., 500ms, 2s).
- totalSizeColumn: The output column name for the total byte size (e.g., total_size_mb).
- totalTimeColumn: The output column name for the total time (e.g., total_time_sec).

### Example Recipe:
Given input data:

data_transfer_size  |	response_time
                    |  
10KB	              |  500ms
                    |  
2MB	                |  1.5s
                    |  
500KB	              |  1s

You can use the following aggregate-stats directive to aggregate the data:

wrangler
```
aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec
```

### Output:
total_size_mb   |	 total_time_sec
                |  
2.98	          |  3.0

### Calculation Details:
- Size Calculation: All data_transfer_size values are summed up and converted to MB (using 1 MB = 1024 * 1024 bytes).
- Time Calculation: All response_time values are summed up and converted to seconds (1 second = 1000ms = 1_000_000_000ns).

### ✅ Testing & Validation
Unit Tests:
- Unit tests for both ByteSize and TimeDuration ensure correct parsing and conversion from string representation (e.g., 1.5MB, 500ms) to canonical units.

- Tests also cover edge cases, such as:
Zero values: 0KB, 0ms
Large numbers: 100GB, 10h
Various unit cases: 1KB, 2.5KB, 3.6GB

### Parser Tests:
- Ensure the parser correctly handles syntax using the new tokens.
- Ensure invalid syntax (e.g., wrong units or invalid number formats) is rejected.

### Integration Tests:
- Comprehensive tests for the aggregate-stats directive verify that the output aggregates the size and time correctly.
- The tests also ensure that the system works with different aggregation types (total, average, etc.) and handles various output units.
