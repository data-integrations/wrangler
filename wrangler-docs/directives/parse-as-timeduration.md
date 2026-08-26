# Parse as TimeDuration

The PARSE-AS-TIMEDURATION directive parses a string representation of time duration into a numeric value.

## Syntax
```
parse-as-timeduration <column>
```

## Usage Notes

The PARSE-AS-TIMEDURATION directive will parse strings representing time durations (e.g., "2h", "30m") into numeric values in milliseconds. The column to be parsed should be of type string.

If the column is `null` or is already a numeric field, applying this directive is a no-op.

## Examples
Input: "2h" → Output: 7200000 (milliseconds)
