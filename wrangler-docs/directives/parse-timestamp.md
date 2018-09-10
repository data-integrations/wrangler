# Parse Timestamp

The PARSE-TIMESTAMP directive parses column values representing unix timestamp as date.


## Syntax
```
parse-timestamp :<column> '<timeunit>'
```


## Usage Notes

The PARSE-TIMESTAMP directive will parse a column representing Unix timestamp. The column type can be string or long.
 long or string as date objects. The directive also has optional parameter timeunit which can be seconds, milliseconds
 or microseconds. The default timeunit is milliseconds. If the column is `null` or has already been parsed as a date,
 applying this directive is a no-op. The column to be parsed as a date should be of type string.


## Examples
if the column has unix timestamp 1536332271894 or "1536332271894", then after applying this directive,
the column value represents date equivalent of UTC: Friday, September 7, 2018 2:57:51.894 PM.
