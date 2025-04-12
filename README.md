# Wrangler Enhanced with ByteSize and TimeDuration Parsers

This project enhances the CDAP Wrangler library with additional functionality for parsing and working with byte sizes and time durations.

## Overview

The enhanced Wrangler library adds two new token types to the recipe parser:

1. **ByteSize** - For working with data size values with units (e.g., "5KB", "2.5MB", "1GB")
2. **TimeDuration** - For working with time duration values with units (e.g., "10ms", "2.5s", "1h")

These new token types enable more efficient data transformation operations when dealing with data that includes size or time measurements.

## ByteSize

The `ByteSize` class provides functionality for parsing and converting between different size units:

- Supported units: B, KB, MB, GB, TB, PB
- Case-insensitive unit recognition
- Automatic conversion to bytes (base unit)
- Methods for converting between units
- Support for fractional values (e.g., "2.5MB")

### Usage Examples

```java
// Parse a byte size string
ByteSize size = new ByteSize("2.5GB");

// Get the value in bytes
double bytes = size.value(); // Returns 2.5 * 1024^3

// Convert to other units
double megabytes = size.convertTo("MB"); // Returns 2560
double kilobytes = size.convertTo("KB"); // Returns 2621440
```

## TimeDuration

The `TimeDuration` class provides functionality for parsing and converting between different time duration units:

- Supported units: ns (nanoseconds), μs (microseconds), ms (milliseconds), s (seconds), m (minutes), h (hours), d (days)
- Automatic conversion to nanoseconds (base unit)
- Methods for converting between units
- Support for fractional values (e.g., "1.5s")

### Usage Examples

```java
// Parse a time duration string
TimeDuration duration = new TimeDuration("1.5s");

// Get the value in nanoseconds
double nanoseconds = duration.value(); // Returns 1.5 * 10^9

// Convert to other units
double milliseconds = duration.convertTo("ms"); // Returns 1500
double minutes = duration.convertTo("m"); // Returns 0.025
```

## AggregateStats Directive

A new directive called `aggregate-stats` has been implemented to showcase the functionality of the new token types. This directive aggregates byte size and time duration values across rows, with options for output units and operation type.

### Syntax

```
aggregate-stats :column1 :column2 output1 output2 [unit1] [unit2] [operation]
```

- `:column1` - Column containing byte size values
- `:column2` - Column containing time duration values
- `output1` - Name of output column for aggregated byte size
- `output2` - Name of output column for aggregated time duration
- `unit1` (optional) - Target unit for byte size output (B, KB, MB, GB, TB, PB)
- `unit2` (optional) - Target unit for time duration output (ns, μs, ms, s, m, h, d)
- `operation` (optional) - Aggregation operation to perform (total, average), defaults to total

### Examples

Basic usage (defaults to total in base units):
```
aggregate-stats :data_size :response_time total_size total_time
```

Specifying output units:
```
aggregate-stats :data_size :response_time total_size_mb total_time_ms MB ms
```

Calculating averages:
```
aggregate-stats :data_size :response_time avg_size avg_time MB ms average
```

## Implementation Details

The implementation includes:

1. Updates to the ANTLR grammar to recognize byte size and time duration tokens
2. New token types in the TokenType enum: BYTE_SIZE and TIME_DURATION
3. ByteSize and TimeDuration implementations that implement the Token interface
4. A new AggregateStats directive that demonstrates the usage
5. Comprehensive tests for each implementation

## Testing

To test the implementation, you can compile and run the test classes:

```bash
# Compile the test files
javac -cp wrangler_updated/wrangler-api/src/main/java ByteSizeTest.java
javac -cp wrangler_updated/wrangler-api/src/main/java TimeDurationTest.java

# Run the tests
java -cp .:wrangler_updated/wrangler-api/src/main/java ByteSizeTest
java -cp .:wrangler_updated/wrangler-api/src/main/java TimeDurationTest
```

When running the tests, you should see output validating that the ByteSize and TimeDuration classes work as expected, handling different units and conversions correctly.
