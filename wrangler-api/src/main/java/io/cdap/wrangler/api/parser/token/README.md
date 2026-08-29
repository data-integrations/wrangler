# Token Classes for Wrangler Directives

This package contains classes that represent different types of tokens used in the Wrangler directive grammar.
Each token type corresponds to a specific data type or structure recognized in directives.

## TimeDuration.java

The `TimeDuration` class represents a duration of time with a specific unit. It parses string representations of time
durations like "100ms", "5s", "2.5h" and provides methods to get the value in different time units.

### Supported Time Units:

- `ns` - Nanoseconds
- `ms` - Milliseconds (1 ms = 1,000,000 ns)
- `s` - Seconds (1 s = 1,000 ms)
- `m` - Minutes (1 m = 60 s)
- `h` - Hours (1 h = 60 m)
- `d` - Days (1 d = 24 h)

### Usage Example:

```java
TimeDuration duration = new TimeDuration("100ms");
long nanos = duration.getNanos();    // Get value in nanoseconds
long millis = duration.getMillis();  // Get value in milliseconds
String unit = duration.getUnit();    // Get the unit ("ms")
double value = duration.getNumericValue(); // Get the numeric part (100.0)
```

## ByteSize.java

The `ByteSize` class represents a size in bytes with a specific unit. It parses string representations of byte sizes
like "10KB", "1.5MB", "2GB" and provides methods to get the value in different byte units.

### Supported Byte Units:

- `B` - Bytes
- `KB` - Kilobytes (1 KB = 1024 bytes)
- `MB` - Megabytes (1 MB = 1024 KB)
- `GB` - Gigabytes (1 GB = 1024 MB)
- `TB` - Terabytes (1 TB = 1024 GB)
- `PB` - Petabytes (1 PB = 1024 TB)

### Usage Example:

```java
ByteSize size = new ByteSize("1.5MB");
long bytes = size.getBytes();        // Get value in bytes
double kb = size.getKilobytes();     // Get value in kilobytes
double mb = size.getMegabytes();     // Get value in megabytes
String unit = size.getUnit();        // Get the unit ("MB")
double value = size.getNumericValue(); // Get the numeric part (1.5)
```

## Related Classes

These token classes work with the `TokenType` enum which defines all the token types in the system.
The token types corresponding to these classes are:

- `TokenType.TIME_DURATION` - For TimeDuration tokens
- `TokenType.BYTE_SIZE` - For ByteSize tokens

Each token implements the `Token` interface, providing methods to:

- Get the original value
- Get the token type
- Convert to JSON representation