## New Parsers: ByteSize and TimeDuration

Wrangler now supports parsing of:
- Byte Sizes: e.g., 10KB, 5MB, 2.5GB
- Time Durations: e.g., 100ms, 2s, 3m

### Usage in Directive:
```
aggregate-stats :data_size :time_duration total_size_mb total_time_sec
```