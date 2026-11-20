Wrangler Byte Size and Time Duration Support
New Parsing Capabilities
Byte Size Parsing
Format: [number][unit]
Supported Units:

Unit	Description
B	Bytes
KB	Kilobytes (1024 B)
MB	Megabytes (1024 KB)
GB	Gigabytes (1024 MB)
TB	Terabytes (1024 GB)
Examples:

wranglescript
set-column :file_size "2.5MB"
set-column :memory "8GB"
Time Duration Parsing
Format: [number][unit]
Supported Units:

Unit	Description
ns	Nanoseconds
ms	Milliseconds
s	Seconds
m	Minutes
h	Hours
d	Days
Examples:

wranglescript
set-column :response_time "150ms"
set-column :uptime "2h30m"
AggregateStats Directive
Usage
wranglescript
Copy
aggregate-stats <size_column> <time_column> <output_size> <output_time> [units]
Parameters
Parameter	Description	Default Unit
size_column	Column containing byte size values	-
time_column	Column containing duration values	-
output_size	Output column for size aggregation	MB
output_time	Output column for time aggregation	s
units	Optional output units specification	-
Examples
Basic Usage:

wranglescript
Copy
aggregate-stats :data_transfer :response_time :total_size :total_time
With Custom Units:

wranglescript
Copy
aggregate-stats :data_transfer :response_time :total_size_gb :total_time_min "GB min"
Complete Example:

wranglescript
Copy
# Sample data preparation
set-column :data_transfer "10MB";
set-column :response_time "500ms";

# Processing
aggregate-stats :data_transfer :response_time :total_size_mb :total_time_sec;

# Expected output columns:
# :total_size_mb (sum in megabytes)
# :total_time_sec (sum in seconds)
Implementation Notes
Internal Conversion:

Byte sizes convert to bytes for calculation

Time durations convert to nanoseconds for calculation

Precision:

Floating-point numbers fully supported

Output precision controlled by unit conversion

Error Handling:

Invalid units throw clear error messages

Missing columns fail gracefully with diagnostics

Performance:

Optimized for large datasets

Minimal memory overhead during aggregation