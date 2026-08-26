# Data Prep

![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)
![cdap-transform](https://cdap-users.herokuapp.com/assets/cdap-transform.svg)
... (original content truncated for simulation) ...

### 🆕 Enhancement: Byte Size and Time Duration Parsing

Wrangler now includes built-in support for parsing and aggregating values with byte size (`KB`, `MB`, `GB`, etc.) and time duration (`ms`, `s`, `m`, etc.) units.

#### ➕ New Token Types Supported
- `BYTE_SIZE` → supports values like `10KB`, `1.5MB`, `2GB`
- `TIME_DURATION` → supports values like `100ms`, `2.5s`, `3m`, `1h`

These are now valid in directive recipes and supported in both parsing and aggregation logic.

#### 🧮 New Directive: `aggregate-stats`

This directive performs aggregation of byte size and time duration fields.

**Syntax:**
```text
aggregate-stats :sourceByteColumn :sourceTimeColumn targetSizeColumn targetTimeColumn
```

**Example:**
```text
aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec
```

**Features:**
- Converts sizes to bytes internally and outputs in MB (or specified unit)
- Converts time durations to milliseconds or seconds as needed
- Aggregation options can be extended to support average, p95, etc.

#### ✅ Example Use Case:
If your dataset contains data sizes like `5MB`, `10KB` and times like `500ms`, `1.5s`, you can aggregate them efficiently using a single directive.