# Format UNIX Timestamp

The `format-unix-timestamp` directive formats a UNIX timestamp as a date.


## Syntax
```
format-unix-timestamp <column> <pattern>
```

* `<column>` is the column to be formatted as a date
* `<pattern>` is the pattern string to be used in formatting the timestamp


## Usage Notes

The `format-unix-timestamp` directive will parse a UNIX timestamp, using a pattern string.
The `<column>` should contain valid UNIX-style timestamps.


## Examples

See [format-date](format-date.md) for an explanation and example of these pattern strings.
