# Wrangler Integration : Byte Size and Time Duration Parsers

This repository contains additional integrations to the [CDAP Wrangler](https://github.com/data-integrations/wrangler) to support native parsing and aggregation of byte size and time duration values in transformation recipes.

## Overview

Wrangler previously lacked support for handling common units like kilobytes (KB), megabytes (MB), milliseconds (ms), or seconds (s). This enhancement introduces:

- New token types for byte sizes and time durations
- New parser logic and token classes in the `wrangler-api` module
- A new directive for performing aggregations using the parsed values
- Comprehensive test coverage for both the token parsing and the directive

## New Token Types

### ByteSize
Parses string values representing data sizes and converts them into bytes.

**Supported Units:**

- B (bytes)
- KB (kilobytes)
- MB (megabytes)
- GB (gigabytes)
- TB (terabytes)
- PB (petabytes)

### TimeDuration
Parses string values representing time durations and converts them into nanoseconds.

**Supported Units:**

- ns, nanoseconds
- us, microseconds
- ms, milliseconds
- s, sec, secs, seconds
- m, min, mins, minutes
- h, hr, hrs, hours

## Implementation Details

### Grammar Update
- Modified `Directives.g4` to include `BYTE_SIZE` and `TIME_DURATION` lexer rules
- Added helper fragments for parsing unit suffixes
- Regenerated the ANTLR parser and lexer

### Token Classes
- `ByteSize.java`: Converts input like `"1.5MB"` to bytes
- `TimeDuration.java`: Converts input like `"2min"` to nanoseconds
- Both classes implement the `Token` interface
- Both types are added to `TokenType`

### Aggregate Directive: `aggregate-stats`
A new directive for performing aggregation on byte size and time duration fields.

**Arguments:**
1. Source column for byte size
2. Source column for time duration
3. Target column for total size
4. Target column for total or average time

**Optional Arguments:**
- Output unit (e.g., MB, GB, seconds, minutes)
- Aggregation type (e.g., total, average)

**Behavior:**
- Parses values to canonical units (bytes and nanoseconds)
- Accumulates totals using the directive context store
- Converts results to target units
- Outputs a single row containing the aggregated results

## Testing

### Unit Tests
- Added unit tests for `ByteSize` and `TimeDuration` parsing logic
- Validated correct handling of whitespace, case sensitivity, and invalid formats

### Directive Tests
- Created tests for `aggregate-stats` directive using sample rows and recipes
- Verified correct aggregation behavior and unit conversion
- Included tolerance-based assertions for floating-point results
