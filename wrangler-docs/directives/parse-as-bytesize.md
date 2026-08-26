# Parse as ByteSize

The PARSE-AS-BYTESIZE directive parses a string representation of byte size into a numeric value.

## Syntax
```
parse-as-bytesize <column>
```

## Usage Notes

The PARSE-AS-BYTESIZE directive will parse strings representing byte sizes (e.g., "10KB", "5MB") into numeric values in bytes. The column to be parsed should be of type string.

If the column is `null` or is already a numeric field, applying this directive is a no-op.

## Examples
Input: "10KB" → Output: 10240 (bytes)