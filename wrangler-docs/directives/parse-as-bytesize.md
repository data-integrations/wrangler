# Parse as ByteSize

The `parse-as-bytesize` directive parses strings containing byte size values with units (such as KB, MB, GB) into a standardized ByteSize type. This enables consistent handling and conversion of data size values throughout your data preparation workflow.

## Syntax
```
parse-as-bytesize :column [to-column]
```

## Arguments
* **column**: Column containing byte size strings to be parsed
* **to-column**: Optional. If provided, the result is placed in this column; if not provided, the original column is overwritten

## Usage Notes
* Input values must have a number followed by a valid unit (e.g., "10KB", "5MB")
* Supported units: B (bytes), KB (kilobytes), MB (megabytes), GB (gigabytes), TB (terabytes), PB (petabytes)
* All values are standardized to bytes internally for consistent calculations
* Units are case-insensitive (e.g., "kb" and "KB" are treated the same)
* The ByteSize type can be used with the `aggregate-stats` directive for aggregating data size values

## Example
```
data :value
1KB
5MB
2.5GB
```

Applying the directive:
```
parse-as-bytesize :value
```

Results in the value column containing parsed ByteSize objects that represent the following byte counts:
```
data :value (bytes)
1024
5242880
2684354560
```

## Direct Token Usage
Besides using the `parse-as-bytesize` directive, you can also use byte size values directly as tokens in other directives:

```
set-column :newcol 5MB
```

This creates a new column with a ByteSize value representing 5 megabytes (5242880 bytes).

## Converting Between Units
You can use the ByteSize utility methods to convert values between different units. For example, when using the `aggregate-stats` directive, you can specify the output unit:

```
aggregate-stats :size_column :time_column total_size total_time MB s
```

This will aggregate the values and output the total size in megabytes (MB). 